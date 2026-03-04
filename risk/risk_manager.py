"""Position sizing, risk rules, and kill switch management."""
import logging
from datetime import datetime

import pytz

from db import get_connection
from utils.constants import is_trading_day
from utils.helpers import now_ist

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")


class RiskManager:
    def __init__(self, config: dict, db_path: str, mode: str, virtual_portfolio=None):
        self.config = config
        self.db_path = db_path
        self.mode = mode
        self.virtual_portfolio = virtual_portfolio
        self.capital_cfg = config["capital"]
        self.market_cfg = config["market"]
        self._broker_gateway = None

        # Load system state
        self._kill_switch_active = False
        self._consecutive_losses = 0
        self._daily_loss_total = 0.0
        self._load_state()

    def set_broker_gateway(self, gateway) -> None:
        self._broker_gateway = gateway

    def _load_state(self) -> None:
        conn = get_connection(self.db_path)
        try:
            rows = conn.execute("SELECT key, value FROM system_state").fetchall()
            state = {r["key"]: r["value"] for r in rows}
            self._kill_switch_active = state.get("kill_switch_active", "0") == "1"
            self._consecutive_losses = int(state.get("consecutive_losses", "0"))
            self._daily_loss_total = float(state.get("daily_loss_total", "0"))
        finally:
            conn.close()

    def _save_state(self) -> None:
        conn = get_connection(self.db_path)
        try:
            updates = [
                ("kill_switch_active", "1" if self._kill_switch_active else "0"),
                ("consecutive_losses", str(self._consecutive_losses)),
                ("daily_loss_total", str(self._daily_loss_total)),
                ("last_trade_date", datetime.now(IST).strftime("%Y-%m-%d")),
            ]
            for key, value in updates:
                conn.execute(
                    "INSERT OR REPLACE INTO system_state (key, value) VALUES (?, ?)",
                    (key, value)
                )
            conn.commit()
        finally:
            conn.close()

    def _get_available_capital(self) -> float:
        if self.mode == "paper" and self.virtual_portfolio:
            return self.virtual_portfolio.get_total_value(
                self._get_open_positions()
            )
        elif self.mode == "live" and self._broker_gateway:
            try:
                margins = self._broker_gateway.get_margins()
                return float(margins.get("available_cash", self.capital_cfg["total"]))
            except Exception as e:
                logger.error(f"Failed to get broker margins: {e}")
                return self.capital_cfg["total"]
        else:
            return float(self.capital_cfg["total"])

    def _get_open_positions(self) -> list:
        conn = get_connection(self.db_path)
        try:
            rows = conn.execute(
                "SELECT * FROM positions WHERE status='OPEN'"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def _is_market_open(self) -> bool:
        now = now_ist()
        if not is_trading_day(now.date()):
            return False
        start_str = self.market_cfg.get("trading_start", "09:20")
        end_str = self.market_cfg.get("trading_end", "15:15")
        now_time = now.strftime("%H:%M")
        return start_str <= now_time <= end_str

    def evaluate_signal(self, signal: dict, current_positions: list) -> dict:
        """
        Evaluate a trading signal against all risk rules.
        Returns { "approved": bool, "signal": dict, "reject_reason": str|None }
        """
        symbol = signal["symbol"]
        strategy = signal["strategy"]

        # 1. Kill switch
        if self._kill_switch_active:
            return self._reject(signal, "KILL_SWITCH_ACTIVE")

        # 2. Market hours (skip for backtest)
        if self.mode not in ("backtest",) and not self._is_market_open():
            return self._reject(signal, "OUTSIDE_MARKET_HOURS")

        # 3. Daily loss limit
        if self.check_daily_loss():
            return self._reject(signal, "DAILY_LOSS_LIMIT_HIT")

        # 4. Max open positions
        open_count = len([p for p in current_positions if p.get("status") == "OPEN"])
        if open_count >= self.capital_cfg["max_open_positions"]:
            return self._reject(signal, f"MAX_POSITIONS_REACHED ({open_count})")

        # 5. Duplicate position (same symbol + strategy)
        duplicate = any(
            p.get("symbol") == symbol and p.get("strategy") == strategy
            and p.get("status") == "OPEN"
            for p in current_positions
        )
        if duplicate:
            return self._reject(signal, f"DUPLICATE_POSITION ({symbol}/{strategy})")

        # 6. Max capital per stock
        available = self._get_available_capital()
        max_per_stock = available * self.capital_cfg["max_per_stock_pct"] / 100
        entry_price = signal["price"]
        stop_loss = signal["stop_loss"]

        if not entry_price or entry_price <= 0:
            return self._reject(signal, "INVALID_ENTRY_PRICE")

        # 7. Calculate position size
        qty = self.calculate_position_size(entry_price, stop_loss)
        if qty < 1:
            return self._reject(signal, "POSITION_SIZE_TOO_SMALL")

        order_value = qty * entry_price

        if order_value > max_per_stock:
            qty = max(1, int(max_per_stock // entry_price))
            order_value = qty * entry_price

        # 8. Max capital deployed check
        if self.mode == "paper" and self.virtual_portfolio:
            deployed = self.virtual_portfolio.get_positions_value(
                [p for p in current_positions if p.get("status") == "OPEN"]
            )
        else:
            deployed = sum(
                float(p.get("entry_price", 0)) * float(p.get("quantity", 0))
                for p in current_positions
                if p.get("status") == "OPEN"
            )

        max_deployable = available * self.capital_cfg["max_capital_deployed_pct"] / 100
        if deployed + order_value > max_deployable:
            remaining = max(0, max_deployable - deployed)
            qty = max(1, int(remaining // entry_price))
            order_value = qty * entry_price
            if order_value > max_deployable:
                return self._reject(signal, "MAX_CAPITAL_DEPLOYED")

        # Cash check for paper mode
        if self.mode == "paper" and self.virtual_portfolio:
            cash = self.virtual_portfolio.get_cash_balance()
            if order_value > cash * 0.98:  # keep 2% buffer
                return self._reject(signal, "INSUFFICIENT_VIRTUAL_CASH")

        approved_signal = dict(signal)
        approved_signal["quantity"] = qty
        return {"approved": True, "signal": approved_signal, "reject_reason": None}

    def _reject(self, signal: dict, reason: str) -> dict:
        logger.info(f"Signal REJECTED [{reason}]: {signal['action']} {signal['symbol']}")
        return {"approved": False, "signal": signal, "reject_reason": reason}

    def calculate_position_size(self, entry_price: float, stop_loss: float) -> int:
        if not entry_price or entry_price <= 0:
            return 1
        available = self._get_available_capital()
        risk_amount = available * self.capital_cfg["max_risk_per_trade_pct"] / 100
        risk_per_share = abs(entry_price - stop_loss)
        if risk_per_share <= 0:
            return 1
        qty = risk_amount / risk_per_share

        # Cap by max per stock
        max_qty = (available * self.capital_cfg["max_per_stock_pct"] / 100) / entry_price
        qty = min(qty, max_qty)

        # Cap by max deployed
        max_deploy = (available * self.capital_cfg["max_capital_deployed_pct"] / 100)
        max_qty_deploy = max_deploy / entry_price
        qty = min(qty, max_qty_deploy)

        return max(1, int(qty))

    def check_daily_loss(self) -> bool:
        if self.mode == "paper" and self.virtual_portfolio:
            conn = get_connection(self.db_path)
            try:
                today = datetime.now(IST).strftime("%Y-%m-%d")
                snapshot = conn.execute(
                    "SELECT day_pnl FROM virtual_portfolio_snapshots "
                    "WHERE DATE(timestamp) = ? ORDER BY id DESC LIMIT 1",
                    (today,)
                ).fetchone()
                if snapshot:
                    day_pnl = float(snapshot["day_pnl"])
                    total_val = self.virtual_portfolio.get_total_value([])
                    max_loss = total_val * self.capital_cfg["max_daily_loss_pct"] / 100
                    return day_pnl < -max_loss
            finally:
                conn.close()
            return False

        available = self._get_available_capital()
        max_loss = available * self.capital_cfg["max_daily_loss_pct"] / 100
        return self._daily_loss_total > max_loss

    def record_trade_result(self, pnl: float) -> None:
        if pnl < 0:
            self._consecutive_losses += 1
            self._daily_loss_total += abs(pnl)
        else:
            self._consecutive_losses = 0

        kill_threshold = self.capital_cfg.get("consecutive_loss_kill_switch", 5)
        if self._consecutive_losses >= kill_threshold:
            self._kill_switch_active = True
            logger.warning(
                f"KILL SWITCH ACTIVATED: {self._consecutive_losses} consecutive losses"
            )

        self._save_state()

    def reset_daily(self) -> None:
        self._daily_loss_total = 0.0
        self._save_state()
        logger.info("Daily risk counters reset")

    def reset_kill_switch(self) -> None:
        self._kill_switch_active = False
        self._consecutive_losses = 0
        self._save_state()
        logger.info("Kill switch manually reset")

    @property
    def kill_switch_active(self) -> bool:
        return self._kill_switch_active
