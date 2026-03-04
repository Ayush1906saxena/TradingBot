"""Order construction, lifecycle tracking, and fee calculation."""
import logging
from datetime import datetime

import pytz

from db import get_connection
from utils.helpers import now_ist

logger = logging.getLogger("trades")

IST = pytz.timezone("Asia/Kolkata")


def calculate_fees(price: float, quantity: int, side: str, trade_type: str) -> dict:
    turnover = price * quantity
    brokerage = min(20, turnover * 0.0003)
    if trade_type == "delivery":
        stt = turnover * 0.001
    else:  # intraday
        stt = turnover * 0.00025 if side == "SELL" else 0
    transaction_charges = turnover * 0.0000345
    gst = (brokerage + transaction_charges) * 0.18
    sebi_charges = turnover * 0.000001
    stamp_duty = turnover * 0.00003 if side == "BUY" else 0
    total = brokerage + stt + transaction_charges + gst + sebi_charges + stamp_duty
    return {
        "brokerage": round(brokerage, 2),
        "stt": round(stt, 2),
        "transaction_charges": round(transaction_charges, 2),
        "gst": round(gst, 2),
        "sebi_charges": round(sebi_charges, 4),
        "stamp_duty": round(stamp_duty, 2),
        "total": round(total, 2),
    }


class OrderManager:
    def __init__(self, broker_gateway, risk_manager, config: dict,
                 db_path: str, mode: str, virtual_portfolio=None, telegram_alert=None):
        self.broker = broker_gateway
        self.risk_manager = risk_manager
        self.config = config
        self.db_path = db_path
        self.mode = mode
        self.virtual_portfolio = virtual_portfolio
        self.telegram = telegram_alert
        self.slippage_pct = config.get("paper_trading", {}).get("slippage_pct", 0.05) / 100
        self.simulate_fees = config.get("paper_trading", {}).get("simulate_fees", True)

    def _now(self) -> str:
        return now_ist().strftime("%Y-%m-%d %H:%M:%S")

    def _get_open_positions(self) -> list:
        conn = get_connection(self.db_path)
        try:
            rows = conn.execute(
                "SELECT * FROM positions WHERE status='OPEN'"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def process_signal(self, signal: dict) -> dict | None:
        """Process a trading signal through risk manager and execute."""
        open_positions = self._get_open_positions()
        result = self.risk_manager.evaluate_signal(signal, open_positions)

        if not result["approved"]:
            self._insert_trade(signal, status="REJECTED",
                               notes=result["reject_reason"])
            return None

        approved_signal = result["signal"]
        symbol = approved_signal["symbol"]
        side = approved_signal["action"]
        quantity = approved_signal["quantity"]
        signal_price = approved_signal["price"]
        strategy = approved_signal["strategy"]
        stop_loss = approved_signal.get("stop_loss")
        target = approved_signal.get("target")

        # Determine trade type
        strategy_cfg = self.config.get("strategies", {}).get(strategy, {})
        trade_type = strategy_cfg.get("trade_type", "intraday")

        if self.mode == "backtest":
            fill_price = self._apply_slippage(signal_price, side, 0.05)
            fees = calculate_fees(fill_price, quantity, side, trade_type)
            trade_id = self._insert_trade(
                approved_signal, status="FILLED",
                fill_price=fill_price, fees=fees["total"]
            )
            pos_id = self._insert_position(symbol, strategy, side, quantity,
                                           fill_price, stop_loss, target)
            return {"trade_id": trade_id, "position_id": pos_id, "fill_price": fill_price, "fees": fees}

        elif self.mode == "paper":
            slippage = self.config.get("paper_trading", {}).get("slippage_pct", 0.05) / 100
            simulate_slip = self.config.get("paper_trading", {}).get("simulate_slippage", True)
            fill_price = self._apply_slippage(signal_price, side, slippage * 100) if simulate_slip else signal_price
            fees = calculate_fees(fill_price, quantity, side, trade_type) if self.simulate_fees else {"total": 0}

            trade_id = self._insert_trade(
                approved_signal, status="FILLED",
                fill_price=fill_price, fees=fees["total"]
            )

            if side == "BUY":
                success = self.virtual_portfolio.execute_buy(
                    symbol, quantity, fill_price, fees, trade_id
                )
            else:
                # For sell signals — calculate P&L from existing position
                pos = self._get_position(symbol, strategy)
                pnl = 0.0
                if pos:
                    entry = pos["entry_price"]
                    pnl = (fill_price - entry) * quantity - fees["total"]
                success = self.virtual_portfolio.execute_sell(
                    symbol, quantity, fill_price, fees, trade_id, pnl
                )
                # Close the existing open position
                if pos:
                    conn = get_connection(self.db_path)
                    try:
                        conn.execute(
                            "UPDATE positions SET status='CLOSED', closed_at=?, close_reason='SELL_SIGNAL' "
                            "WHERE symbol=? AND strategy=? AND status='OPEN'",
                            (self._now(), symbol, strategy)
                        )
                        conn.commit()
                    finally:
                        conn.close()
                    self.risk_manager.record_trade_result(pnl)

            if not success:
                self._update_trade_status(trade_id, "FAILED")
                return None

            # Only open a new position on BUY signals, not on closing SELLs
            pos_id = None
            if side == "BUY":
                pos_id = self._insert_position(symbol, strategy, side, quantity,
                                               fill_price, stop_loss, target)

            # Send Telegram
            if self.telegram:
                import asyncio
                balance = self.virtual_portfolio.get_cash_balance()
                msg = (
                    f"📊 PAPER {side}\n"
                    f"{symbol} × {quantity} @ ₹{fill_price:,.2f}\n"
                    f"Strategy: {strategy}\n"
                    f"Stop Loss: ₹{stop_loss:,.2f} | Target: ₹{target:,.2f}\n"
                    f"Fees: ₹{fees['total']:.2f}\n"
                    f"💰 Virtual Cash: ₹{balance:,.2f}"
                )
                asyncio.create_task(self.telegram.send(msg, priority="trade"))

            return {"trade_id": trade_id, "position_id": pos_id, "fill_price": fill_price, "fees": fees}

        elif self.mode == "live":
            try:
                broker_order_id = self.broker.place_order(
                    symbol=symbol, side=side, quantity=quantity,
                    order_type="MARKET", product=trade_type
                )
                trade_id = self._insert_trade(
                    approved_signal, status="PLACED",
                    broker_order_id=broker_order_id
                )
                return {"trade_id": trade_id, "broker_order_id": broker_order_id}
            except Exception as e:
                logger.error(f"Order placement failed: {e}")
                self._insert_trade(approved_signal, status="FAILED", notes=str(e))
                return None

    def close_position(self, position: dict, reason: str, price: float) -> None:
        symbol = position["symbol"]
        strategy = position["strategy"]
        quantity = position["quantity"]
        entry_price = position["entry_price"]
        side = position["side"]

        strategy_cfg = self.config.get("strategies", {}).get(strategy, {})
        trade_type = strategy_cfg.get("trade_type", "intraday")

        close_side = "SELL" if side == "LONG" else "BUY"

        if self.mode == "paper":
            slippage = self.config.get("paper_trading", {}).get("slippage_pct", 0.05) / 100
            simulate_slip = self.config.get("paper_trading", {}).get("simulate_slippage", True)
            fill_price = self._apply_slippage(price, close_side, slippage * 100) if simulate_slip else price
            fees = calculate_fees(fill_price, quantity, close_side, trade_type) if self.simulate_fees else {"total": 0}

            pnl = (fill_price - float(entry_price)) * quantity
            if side == "SHORT":
                pnl = (float(entry_price) - fill_price) * quantity
            pnl -= fees["total"]

            close_signal = {
                "action": close_side, "symbol": symbol, "price": fill_price,
                "stop_loss": 0, "target": 0, "strategy": strategy,
                "quantity": quantity, "reason": reason
            }
            trade_id = self._insert_trade(close_signal, status="FILLED",
                                          fill_price=fill_price, fees=fees["total"])
            self.virtual_portfolio.execute_sell(
                symbol, quantity, fill_price, fees, trade_id, pnl
            )
            self.risk_manager.record_trade_result(pnl)

        elif self.mode == "backtest":
            fill_price = self._apply_slippage(price, close_side, 0.05)
            fees = calculate_fees(fill_price, quantity, close_side, trade_type)
            pnl = (fill_price - float(entry_price)) * quantity
            if side == "SHORT":
                pnl = (float(entry_price) - fill_price) * quantity
            pnl -= fees["total"]

            close_signal = {
                "action": close_side, "symbol": symbol, "price": fill_price,
                "stop_loss": 0, "target": 0, "strategy": strategy,
                "quantity": quantity, "reason": reason
            }
            self._insert_trade(close_signal, status="FILLED",
                               fill_price=fill_price, fees=fees["total"])
            self.risk_manager.record_trade_result(pnl)

        elif self.mode == "live":
            try:
                self.broker.place_order(
                    symbol=symbol, side=close_side, quantity=quantity,
                    order_type="MARKET", product=trade_type
                )
            except Exception as e:
                logger.error(f"Position close failed: {e}")

        # Update position record
        conn = get_connection(self.db_path)
        try:
            conn.execute(
                "UPDATE positions SET status='CLOSED', closed_at=?, close_reason=? "
                "WHERE symbol=? AND strategy=? AND status='OPEN'",
                (self._now(), reason, symbol, strategy)
            )
            conn.commit()
        finally:
            conn.close()

        logger.info(
            f"Position CLOSED: {symbol} ({strategy}) | Reason: {reason} | "
            f"Price: ₹{price:,.2f}"
        )

    def force_close_all(self, reason: str) -> None:
        positions = self._get_open_positions()
        for pos in positions:
            try:
                price = float(pos.get("current_price") or pos.get("entry_price"))
                self.close_position(pos, reason, price)
            except Exception as e:
                logger.error(f"Failed to force close {pos.get('symbol')}: {e}")

    def monitor_pending_orders(self) -> None:
        if self.mode != "live":
            return
        conn = get_connection(self.db_path)
        try:
            pending = conn.execute(
                "SELECT id, broker_order_id, symbol, strategy, side, quantity, signal_price "
                "FROM trades WHERE status='PLACED' AND broker_order_id IS NOT NULL"
            ).fetchall()
            for trade in pending:
                try:
                    status = self.broker.get_order_status(trade["broker_order_id"])
                    if status["status"] == "FILLED":
                        fill_price = status.get("fill_price") or trade["signal_price"]
                        conn.execute(
                            "UPDATE trades SET status='FILLED', fill_price=?, fill_timestamp=? WHERE id=?",
                            (fill_price, self._now(), trade["id"])
                        )
                        conn.commit()
                        self._insert_position(
                            trade["symbol"], trade["strategy"], trade["side"],
                            trade["quantity"], fill_price, None, None
                        )
                    elif status["status"] in ("REJECTED", "CANCELLED"):
                        conn.execute(
                            "UPDATE trades SET status=? WHERE id=?",
                            (status["status"], trade["id"])
                        )
                        conn.commit()
                except Exception as e:
                    logger.error(f"Failed to check order {trade['broker_order_id']}: {e}")
        finally:
            conn.close()

    def _apply_slippage(self, price: float, side: str, slippage_pct: float) -> float:
        factor = 1 + (slippage_pct / 100)
        if side == "BUY":
            return round(price * factor, 2)
        else:
            return round(price / factor, 2)

    def _insert_trade(self, signal: dict, status: str, fill_price: float = None,
                      fees: float = None, broker_order_id: str = None,
                      notes: str = None) -> int:
        conn = get_connection(self.db_path)
        try:
            cursor = conn.execute(
                "INSERT INTO trades (timestamp, strategy, symbol, side, quantity, "
                "signal_price, fill_price, order_type, mode, broker_order_id, "
                "status, stop_loss, target, fees, notes) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    self._now(),
                    signal.get("strategy", ""),
                    signal.get("symbol", ""),
                    signal.get("action", signal.get("side", "")),
                    signal.get("quantity", 0),
                    signal.get("price", 0),
                    fill_price,
                    "MARKET",
                    self.mode,
                    broker_order_id,
                    status,
                    signal.get("stop_loss"),
                    signal.get("target"),
                    fees,
                    notes or signal.get("reason", ""),
                )
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def _update_trade_status(self, trade_id: int, status: str) -> None:
        conn = get_connection(self.db_path)
        try:
            conn.execute("UPDATE trades SET status=? WHERE id=?", (status, trade_id))
            conn.commit()
        finally:
            conn.close()

    def _insert_position(self, symbol: str, strategy: str, side: str,
                         quantity: int, entry_price: float,
                         stop_loss: float, target: float) -> int:
        pos_side = "LONG" if side == "BUY" else "SHORT"
        conn = get_connection(self.db_path)
        try:
            # Close existing conflicting position first
            conn.execute(
                "UPDATE positions SET status='CLOSED', closed_at=?, close_reason='REPLACED' "
                "WHERE symbol=? AND strategy=? AND status='OPEN'",
                (self._now(), symbol, strategy)
            )
            cursor = conn.execute(
                "INSERT INTO positions "
                "(symbol, strategy, side, quantity, entry_price, entry_time, "
                " current_price, stop_loss, target, highest_since_entry, status) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')",
                (symbol, strategy, pos_side, quantity, entry_price,
                 self._now(), entry_price, stop_loss or 0, target,
                 entry_price)
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def _get_position(self, symbol: str, strategy: str) -> dict | None:
        conn = get_connection(self.db_path)
        try:
            row = conn.execute(
                "SELECT * FROM positions WHERE symbol=? AND strategy=? AND status='OPEN'",
                (symbol, strategy)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
