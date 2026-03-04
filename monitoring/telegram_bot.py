"""Telegram bot for trade alerts and daily summaries."""
import asyncio
import logging

logger = logging.getLogger(__name__)


class TelegramAlert:
    def __init__(self, bot_token: str, chat_id: str, enabled: bool):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.enabled = enabled and bool(bot_token) and bool(chat_id)
        self._bot = None

        if self.enabled:
            try:
                from telegram import Bot
                self._bot = Bot(token=self.bot_token)
                logger.info("Telegram bot initialized")
            except ImportError:
                logger.error("python-telegram-bot not installed")
                self.enabled = False
            except Exception as e:
                logger.error(f"Telegram init failed: {e}")
                self.enabled = False

    def _prefix(self, priority: str) -> str:
        return {
            "info": "ℹ️",
            "trade": "📊",
            "warning": "⚠️",
            "critical": "🚨",
            "success": "✅",
        }.get(priority, "ℹ️")

    async def send(self, message: str, priority: str = "info") -> None:
        if not self.enabled:
            return
        try:
            prefix = self._prefix(priority)
            text = f"{prefix} {message}"[:4096]
            await self._bot.send_message(chat_id=self.chat_id, text=text)
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")

    def send_sync(self, message: str, priority: str = "info") -> None:
        """Synchronous wrapper for send()."""
        if not self.enabled:
            return
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(self.send(message, priority))
            else:
                loop.run_until_complete(self.send(message, priority))
        except Exception as e:
            logger.error(f"Telegram send_sync failed: {e}")

    async def send_daily_summary(self, summary: dict) -> None:
        if not self.enabled:
            return
        from datetime import date

        mode = summary.get("mode", "UNKNOWN").upper()
        mode_label = f"PAPER (dummy money)" if mode == "PAPER" else mode
        total_trades = summary.get("total_trades", 0)
        wins = summary.get("winning_trades", 0)
        losses = summary.get("losing_trades", 0)
        net_pnl = summary.get("net_pnl", 0)
        net_pnl_pct = summary.get("net_pnl_pct", 0)
        best = summary.get("best_trade", {})
        worst = summary.get("worst_trade", {})
        virtual_balance = summary.get("virtual_balance")
        total_return_pct = summary.get("total_return_pct")
        gross_pnl = summary.get("gross_pnl", 0)
        fees = summary.get("total_fees", 0)

        pnl_sign = "+" if net_pnl >= 0 else ""

        lines = [
            f"📊 DAILY SUMMARY — {date.today().strftime('%d %b %Y')}",
            f"Mode: {mode_label}",
            "─────────────────",
            f"Trades: {total_trades} ({wins}W / {losses}L)",
            f"Net P&L: {pnl_sign}₹{net_pnl:,.0f} ({pnl_sign}{net_pnl_pct:.2f}%)",
            f"Gross P&L: ₹{gross_pnl:,.0f} | Fees: ₹{fees:.0f}",
        ]
        if best:
            lines.append(f"Best: {best.get('symbol')} +₹{best.get('pnl', 0):,.0f}")
        if worst:
            lines.append(f"Worst: {worst.get('symbol')} ₹{worst.get('pnl', 0):,.0f}")

        if virtual_balance is not None:
            lines.append("─────────────────")
            lines.append(f"💰 Virtual Balance: ₹{virtual_balance:,.0f}")
        if total_return_pct is not None:
            sign = "+" if total_return_pct >= 0 else ""
            lines.append(f"📈 Since Inception: {sign}{total_return_pct:.2f}%")

        await self.send("\n".join(lines), priority="trade")

    async def send_trade_alert(self, trade: dict, mode: str, virtual_balance: float = None) -> None:
        if not self.enabled:
            return

        mode_label = "PAPER" if mode == "paper" else "LIVE"
        side = trade.get("side", trade.get("action", ""))
        symbol = trade.get("symbol", "")
        qty = trade.get("quantity", 0)
        price = trade.get("fill_price", trade.get("price", 0))
        strategy = trade.get("strategy", "")
        stop_loss = trade.get("stop_loss", 0)
        target = trade.get("target", 0)
        fees = trade.get("fees", 0)
        pnl = trade.get("pnl")

        lines = [
            f"📊 {mode_label} {side}",
            f"{symbol} × {qty} @ ₹{price:,.2f}",
            f"Strategy: {strategy}",
        ]
        if stop_loss:
            lines.append(f"Stop Loss: ₹{stop_loss:,.2f}" + (f" | Target: ₹{target:,.2f}" if target else ""))
        if fees:
            lines.append(f"Fees: ₹{fees:.2f}")
        if pnl is not None:
            sign = "+" if pnl >= 0 else ""
            lines.append(f"P&L: {sign}₹{pnl:,.2f}")
        if virtual_balance is not None:
            lines.append(f"💰 Virtual Cash: ₹{virtual_balance:,.2f}")

        await self.send("\n".join(lines), priority="trade")
