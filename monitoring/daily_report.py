"""Generate end-of-day summary and reports."""
import logging
from datetime import datetime, date

import pytz

from db import get_connection

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")


class DailyReporter:
    def __init__(self, db_path: str, config: dict):
        self.db_path = db_path
        self.config = config

    def generate_daily_summary(self, mode: str, virtual_portfolio=None) -> dict:
        """Generate end-of-day performance summary."""
        today = date.today().isoformat()
        conn = get_connection(self.db_path)
        try:
            trades = conn.execute(
                "SELECT * FROM trades WHERE DATE(timestamp) = ? AND mode = ? AND status = 'FILLED'",
                (today, mode)
            ).fetchall()
            trades = [dict(t) for t in trades]
        finally:
            conn.close()

        total_trades = len(trades)
        wins = [t for t in trades if (t.get("pnl") or 0) > 0]
        losses = [t for t in trades if (t.get("pnl") or 0) <= 0 and t.get("pnl") is not None]
        gross_pnl = sum(t.get("pnl", 0) or 0 for t in trades)
        total_fees = sum(t.get("fees", 0) or 0 for t in trades)
        net_pnl = gross_pnl - total_fees

        best_trade = None
        worst_trade = None
        if trades:
            best = max(trades, key=lambda t: t.get("pnl") or 0, default=None)
            worst = min(trades, key=lambda t: t.get("pnl") or 0, default=None)
            if best:
                best_trade = {"symbol": best["symbol"], "pnl": best.get("pnl", 0)}
            if worst:
                worst_trade = {"symbol": worst["symbol"], "pnl": worst.get("pnl", 0)}

        summary = {
            "date": today,
            "mode": mode,
            "total_trades": total_trades,
            "winning_trades": len(wins),
            "losing_trades": len(losses),
            "gross_pnl": round(gross_pnl, 2),
            "total_fees": round(total_fees, 2),
            "net_pnl": round(net_pnl, 2),
            "net_pnl_pct": 0.0,
            "best_trade": best_trade,
            "worst_trade": worst_trade,
        }

        if virtual_portfolio and mode == "paper":
            vp_summary = virtual_portfolio.get_summary()
            summary["virtual_balance"] = vp_summary["total_value"]
            summary["total_return_pct"] = vp_summary["total_return_pct"]
            capital = vp_summary["initial_capital"]
            if capital:
                summary["net_pnl_pct"] = round(net_pnl / capital * 100, 2)
        else:
            capital = self.config["capital"]["total"]
            if capital:
                summary["net_pnl_pct"] = round(net_pnl / capital * 100, 2)

        # Save to daily_pnl table
        self._save_daily_pnl(summary)
        return summary

    def _save_daily_pnl(self, summary: dict) -> None:
        conn = get_connection(self.db_path)
        try:
            conn.execute(
                "INSERT OR REPLACE INTO daily_pnl "
                "(date, mode, total_trades, winning_trades, losing_trades, "
                " gross_pnl, total_fees, net_pnl) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    summary["date"], summary["mode"],
                    summary["total_trades"], summary["winning_trades"],
                    summary["losing_trades"], summary["gross_pnl"],
                    summary["total_fees"], summary["net_pnl"],
                )
            )
            conn.commit()
        finally:
            conn.close()
