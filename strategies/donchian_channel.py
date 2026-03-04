"""Strategy 8: Donchian Channel Breakout (Turtle Trading).

The legendary strategy used by the Turtle Traders. Buy on breakout above
the 20-period high channel, sell on breakdown below 20-period low channel.
Exit at the 10-period channel in opposite direction.
One of the most historically profitable trend-following systems.
"""
import logging

import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class DonchianChannel(BaseStrategy):

    def __init__(self, config: dict):
        super().__init__("donchian_channel", config)
        self.entry_period = config.get("entry_period", 20)
        self.exit_period = config.get("exit_period", 10)
        self.stop_loss_pct = config.get("stop_loss_pct", 2.0)

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        dc = ta.donchian(df["high"], df["low"], lower_length=self.entry_period, upper_length=self.entry_period)
        if dc is not None:
            df = pd.concat([df, dc], axis=1)
        # Exit channel (shorter)
        dc_exit = ta.donchian(df["high"], df["low"], lower_length=self.exit_period, upper_length=self.exit_period)
        if dc_exit is not None:
            for c in dc_exit.columns:
                df[f"exit_{c}"] = dc_exit[c]
        return df

    def _get_dc_cols(self, df: pd.DataFrame):
        upper = lower = mid = None
        for c in df.columns:
            if c.startswith("DCU_") and not c.startswith("exit_"):
                upper = c
            if c.startswith("DCL_") and not c.startswith("exit_"):
                lower = c
            if c.startswith("DCM_") and not c.startswith("exit_"):
                mid = c
        exit_upper = exit_lower = None
        for c in df.columns:
            if c.startswith("exit_DCU_"):
                exit_upper = c
            if c.startswith("exit_DCL_"):
                exit_lower = c
        return upper, lower, mid, exit_upper, exit_lower

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        if len(df) < self.entry_period + 5:
            return None

        df = self.compute_indicators(df)
        upper, lower, mid, _, _ = self._get_dc_cols(df)
        if not upper or not lower:
            return None

        last = df.iloc[-1]
        prev = df.iloc[-2]

        if pd.isna(last[upper]) or pd.isna(prev[upper]):
            return None

        price = float(last["close"])
        prev_price = float(prev["close"])
        upper_val = float(prev[upper])   # Use previous bar's channel (avoid lookahead)
        lower_val = float(prev[lower])

        # BUY: close breaks above previous upper channel
        if price > upper_val and prev_price <= upper_val:
            sl = round(price * (1 - self.stop_loss_pct / 100), 2)
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": None, "strategy": self.name,
                "reason": f"Donchian breakout above {upper_val:.2f}"
            }

        # SELL: close breaks below previous lower channel
        if price < lower_val and prev_price >= lower_val:
            sl = round(price * (1 + self.stop_loss_pct / 100), 2)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": None, "strategy": self.name,
                "reason": f"Donchian breakdown below {lower_val:.2f}"
            }

        return None

    def should_exit(self, position: dict, current_price: float, df: pd.DataFrame) -> dict | None:
        # Check standard SL first
        base = super().should_exit(position, current_price, df)
        if base:
            return base

        if len(df) < self.exit_period + 2:
            return None

        df = self.compute_indicators(df)
        _, _, _, exit_upper, exit_lower = self._get_dc_cols(df)

        if not exit_upper or not exit_lower:
            return None

        prev = df.iloc[-2]
        if pd.isna(prev[exit_upper]):
            return None

        # Exit LONG: price falls below exit channel lower
        if position["side"] == "LONG" and current_price < float(prev[exit_lower]):
            return {
                "action": "SELL", "symbol": position["symbol"],
                "price": current_price, "reason": "DONCHIAN_EXIT_CHANNEL",
                "strategy": self.name
            }

        # Exit SHORT: price rises above exit channel upper
        if position["side"] == "SHORT" and current_price > float(prev[exit_upper]):
            return {
                "action": "BUY", "symbol": position["symbol"],
                "price": current_price, "reason": "DONCHIAN_EXIT_CHANNEL",
                "strategy": self.name
            }

        return None
