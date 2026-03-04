"""Virtual wallet and portfolio engine for paper trading (dummy money)."""
import logging
from datetime import datetime

import pytz

from db import get_connection

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")


class VirtualPortfolio:
    """
    Manages the dummy money system for paper trading.
    Behaves exactly like a real brokerage account.
    """

    def __init__(self, config: dict, db_path: str):
        self.config = config
        self.db_path = db_path
        self.initial_capital = config["paper_trading"]["initial_virtual_cash"]
        self.reset_on_restart = config["paper_trading"].get("reset_on_restart", False)

        self.cash_balance: float = 0.0
        self.realized_pnl: float = 0.0
        self.total_fees_paid: float = 0.0

        self._initialize()

    def _now(self) -> str:
        return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

    def _initialize(self) -> None:
        conn = get_connection(self.db_path)
        try:
            row = conn.execute(
                "SELECT balance_after, id FROM virtual_wallet ORDER BY id DESC LIMIT 1"
            ).fetchone()

            if row is None:
                # First run — create initial deposit
                self.cash_balance = self.initial_capital
                conn.execute(
                    "INSERT INTO virtual_wallet (timestamp, event_type, amount, balance_after, notes) "
                    "VALUES (?, 'INITIAL_DEPOSIT', ?, ?, 'Initial virtual capital')",
                    (self._now(), self.initial_capital, self.initial_capital)
                )
                conn.commit()
                logger.info(f"Virtual wallet initialized with ₹{self.initial_capital:,.0f}")
            elif self.reset_on_restart:
                self.cash_balance = self.initial_capital
                conn.execute(
                    "INSERT INTO virtual_wallet (timestamp, event_type, amount, balance_after, notes) "
                    "VALUES (?, 'RESET', ?, ?, 'Reset on restart')",
                    (self._now(), self.initial_capital, self.initial_capital)
                )
                conn.commit()
                logger.info(f"Virtual wallet reset to ₹{self.initial_capital:,.0f}")
            else:
                self.cash_balance = float(row["balance_after"])
                logger.info(f"Virtual wallet resumed. Balance: ₹{self.cash_balance:,.0f}")

            # Load cumulative stats
            stats = conn.execute(
                "SELECT "
                "  SUM(CASE WHEN event_type='FEE_DEDUCTION' THEN ABS(amount) ELSE 0 END) AS fees, "
                "  SUM(CASE WHEN event_type='REALIZED_PNL' THEN amount ELSE 0 END) AS pnl "
                "FROM virtual_wallet"
            ).fetchone()
            self.total_fees_paid = float(stats["fees"] or 0)
            self.realized_pnl = float(stats["pnl"] or 0)
        finally:
            conn.close()

    def get_cash_balance(self) -> float:
        return self.cash_balance

    def get_total_value(self, open_positions: list, live_stream=None) -> float:
        positions_value = self.get_positions_value(open_positions, live_stream)
        return self.cash_balance + positions_value

    def get_positions_value(self, open_positions: list, live_stream=None) -> float:
        total = 0.0
        for pos in open_positions:
            symbol = pos["symbol"] if isinstance(pos, dict) else pos[1]
            qty = pos["quantity"] if isinstance(pos, dict) else pos[4]
            if live_stream:
                price = live_stream.get_ltp(symbol) or 0
            else:
                price = pos.get("current_price") or pos.get("entry_price") or 0
            total += float(qty) * float(price)
        return total

    def execute_buy(self, symbol: str, quantity: int, fill_price: float,
                    fees: dict, trade_id: int) -> bool:
        total_cost = (fill_price * quantity) + fees["total"]
        if total_cost > self.cash_balance:
            logger.warning(
                f"Insufficient virtual funds: need ₹{total_cost:,.2f}, "
                f"have ₹{self.cash_balance:,.2f}"
            )
            return False

        self.cash_balance -= (fill_price * quantity)
        conn = get_connection(self.db_path)
        try:
            conn.execute(
                "INSERT INTO virtual_wallet (timestamp, event_type, amount, balance_after, reference_id, notes) "
                "VALUES (?, 'BUY_DEBIT', ?, ?, ?, ?)",
                (self._now(), -(fill_price * quantity), self.cash_balance,
                 trade_id, f"Buy {quantity} {symbol} @ {fill_price}")
            )
            self.cash_balance -= fees["total"]
            conn.execute(
                "INSERT INTO virtual_wallet (timestamp, event_type, amount, balance_after, reference_id, notes) "
                "VALUES (?, 'FEE_DEDUCTION', ?, ?, ?, ?)",
                (self._now(), -fees["total"], self.cash_balance,
                 trade_id, f"Fees for {symbol} buy")
            )
            conn.commit()
        finally:
            conn.close()

        self.total_fees_paid += fees["total"]
        logger.info(
            f"Virtual BUY: {quantity} {symbol} @ ₹{fill_price} | "
            f"Fees: ₹{fees['total']:.2f} | Cash: ₹{self.cash_balance:,.2f}"
        )
        return True

    def execute_sell(self, symbol: str, quantity: int, fill_price: float,
                     fees: dict, trade_id: int, pnl: float) -> bool:
        gross_credit = fill_price * quantity
        balance_before_fee = self.cash_balance + gross_credit

        conn = get_connection(self.db_path)
        try:
            conn.execute(
                "INSERT INTO virtual_wallet (timestamp, event_type, amount, balance_after, reference_id, notes) "
                "VALUES (?, 'SELL_CREDIT', ?, ?, ?, ?)",
                (self._now(), gross_credit, balance_before_fee,
                 trade_id, f"Sell {quantity} {symbol} @ {fill_price}")
            )
            self.cash_balance = balance_before_fee - fees["total"]
            conn.execute(
                "INSERT INTO virtual_wallet (timestamp, event_type, amount, balance_after, reference_id, notes) "
                "VALUES (?, 'FEE_DEDUCTION', ?, ?, ?, ?)",
                (self._now(), -fees["total"], self.cash_balance,
                 trade_id, f"Fees for {symbol} sell")
            )
            conn.execute(
                "INSERT INTO virtual_wallet (timestamp, event_type, amount, balance_after, reference_id, notes) "
                "VALUES (?, 'REALIZED_PNL', ?, ?, ?, ?)",
                (self._now(), pnl, self.cash_balance,
                 trade_id, f"Realized P&L for {symbol}")
            )
            conn.commit()
        finally:
            conn.close()

        self.realized_pnl += pnl
        self.total_fees_paid += fees["total"]
        logger.info(
            f"Virtual SELL: {quantity} {symbol} @ ₹{fill_price} | "
            f"P&L: ₹{pnl:+.2f} | Fees: ₹{fees['total']:.2f} | Cash: ₹{self.cash_balance:,.2f}"
        )
        return True

    def take_snapshot(self, open_positions: list, live_stream, reason: str) -> None:
        positions_value = self.get_positions_value(open_positions, live_stream)
        total_value = self.cash_balance + positions_value

        unrealized_pnl = 0.0
        for pos in open_positions:
            symbol = pos["symbol"] if isinstance(pos, dict) else pos[1]
            qty = pos["quantity"] if isinstance(pos, dict) else pos[4]
            entry = pos["entry_price"] if isinstance(pos, dict) else pos[5]
            if live_stream:
                current = live_stream.get_ltp(symbol) or float(entry)
            else:
                current = float(entry)
            unrealized_pnl += (current - float(entry)) * float(qty)

        # Calculate day P&L
        today = datetime.now(IST).strftime("%Y-%m-%d")
        conn = get_connection(self.db_path)
        try:
            first_today = conn.execute(
                "SELECT total_value FROM virtual_portfolio_snapshots "
                "WHERE DATE(timestamp) = ? ORDER BY id ASC LIMIT 1",
                (today,)
            ).fetchone()
            day_open_value = float(first_today["total_value"]) if first_today else total_value
            day_pnl = total_value - day_open_value

            conn.execute(
                "INSERT INTO virtual_portfolio_snapshots "
                "(timestamp, cash_balance, positions_value, total_value, unrealized_pnl, "
                " realized_pnl_cumulative, total_fees_cumulative, day_pnl, "
                " num_open_positions, snapshot_reason) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (self._now(), self.cash_balance, positions_value, total_value,
                 unrealized_pnl, self.realized_pnl, self.total_fees_paid,
                 day_pnl, len(open_positions), reason)
            )
            conn.commit()
        finally:
            conn.close()

    def get_equity_curve(self, start_date: str = None, end_date: str = None) -> list:
        conn = get_connection(self.db_path)
        try:
            query = "SELECT timestamp, total_value FROM virtual_portfolio_snapshots WHERE 1=1"
            params = []
            if start_date:
                query += " AND DATE(timestamp) >= ?"
                params.append(start_date)
            if end_date:
                query += " AND DATE(timestamp) <= ?"
                params.append(end_date)
            query += " ORDER BY id ASC"
            rows = conn.execute(query, params).fetchall()
            return [{"timestamp": r["timestamp"], "total_value": r["total_value"]} for r in rows]
        finally:
            conn.close()

    def get_wallet_history(self, limit: int = 50) -> list:
        conn = get_connection(self.db_path)
        try:
            rows = conn.execute(
                "SELECT timestamp, event_type, amount, balance_after, notes "
                "FROM virtual_wallet ORDER BY id DESC LIMIT ?",
                (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def reset(self, initial_amount: float) -> None:
        self.cash_balance = initial_amount
        self.realized_pnl = 0.0
        self.total_fees_paid = 0.0
        self.initial_capital = initial_amount

        conn = get_connection(self.db_path)
        try:
            conn.execute(
                "INSERT INTO virtual_wallet (timestamp, event_type, amount, balance_after, notes) "
                "VALUES (?, 'RESET', ?, ?, 'Manual reset')",
                (self._now(), initial_amount, initial_amount)
            )
            conn.commit()
        finally:
            conn.close()
        logger.info(f"Virtual wallet manually reset to ₹{initial_amount:,.0f}")

    def get_summary(self, open_positions: list = None, live_stream=None) -> dict:
        open_positions = open_positions or []
        positions_value = self.get_positions_value(open_positions, live_stream)
        total_value = self.cash_balance + positions_value

        conn = get_connection(self.db_path)
        try:
            num_trades = conn.execute(
                "SELECT COUNT(*) as cnt FROM trades WHERE mode='paper' AND status='FILLED'"
            ).fetchone()["cnt"]
            first_snapshot = conn.execute(
                "SELECT MIN(DATE(timestamp)) as first_date FROM virtual_portfolio_snapshots"
            ).fetchone()
            first_date = first_snapshot["first_date"]
        finally:
            conn.close()

        days_active = 0
        if first_date:
            from datetime import date
            days_active = (date.today() - date.fromisoformat(first_date)).days

        unrealized_pnl = 0.0
        for pos in open_positions:
            symbol = pos["symbol"] if isinstance(pos, dict) else pos[1]
            qty = pos["quantity"] if isinstance(pos, dict) else pos[4]
            entry = pos["entry_price"] if isinstance(pos, dict) else pos[5]
            if live_stream:
                current = live_stream.get_ltp(symbol) or float(entry)
            else:
                current = float(entry)
            unrealized_pnl += (current - float(entry)) * float(qty)

        return {
            "cash_balance": self.cash_balance,
            "positions_value": positions_value,
            "total_value": total_value,
            "initial_capital": self.initial_capital,
            "total_return_pct": ((total_value - self.initial_capital) / self.initial_capital * 100)
                                if self.initial_capital else 0,
            "total_return_inr": total_value - self.initial_capital,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": unrealized_pnl,
            "total_fees_paid": self.total_fees_paid,
            "num_trades": num_trades,
            "days_active": days_active,
        }
