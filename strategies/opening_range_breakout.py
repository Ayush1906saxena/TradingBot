"""Opening Range Breakout (ORB) Strategy.

Uses ATR-based proxy for daily data: breakout above/below open ± 0.3×ATR.
Extremely popular in Indian markets. Works properly with intraday data in paper/live mode.
"""
import logging

import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class OpeningRangeBreakout(BaseStrategy):

    def __init__(self, config: dict):
        super().__init__("opening_range_breakout", config)
        self.atr_period = config.get("atr_period", 14)
        self.atr_multiplier = config.get("atr_multiplier", 0.3)
        self.stop_loss_pct = config.get("stop_loss_pct", 1.5)
        self.target_pct = config.get("target_pct", 3.0)

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        atr = ta.atr(df["high"], df["low"], df["close"], length=self.atr_period)
        if atr is not None:
            df["ATR"] = atr
        # Opening range proxy: open ± multiplier * ATR
        df["OR_UPPER"] = df["open"] + self.atr_multiplier * df["ATR"]
        df["OR_LOWER"] = df["open"] - self.atr_multiplier * df["ATR"]
        return df

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        if len(df) < self.atr_period + 5:
            return None

        df = self.compute_indicators(df)
        last = df.iloc[-1]
        prev = df.iloc[-2]

        if pd.isna(last.get("ATR")) or pd.isna(prev.get("ATR")):
            return None

        price = float(last["close"])
        or_upper = float(last["OR_UPPER"])
        or_lower = float(last["OR_LOWER"])
        prev_price = float(prev["close"])
        prev_or_upper = float(prev["OR_UPPER"])
        prev_or_lower = float(prev["OR_LOWER"])

        # Bullish ORB: close breaks above opening range upper
        if price > or_upper and prev_price <= prev_or_upper:
            sl = round(price * (1 - self.stop_loss_pct / 100), 2)
            target = round(price * (1 + self.target_pct / 100), 2)
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": target, "strategy": self.name,
                "reason": f"ORB breakout UP (price={price:.2f} > OR_upper={or_upper:.2f})"
            }

        # Bearish ORB: close breaks below opening range lower
        if price < or_lower and prev_price >= prev_or_lower:
            sl = round(price * (1 + self.stop_loss_pct / 100), 2)
            target = round(price * (1 - self.target_pct / 100), 2)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": target, "strategy": self.name,
                "reason": f"ORB breakout DOWN (price={price:.2f} < OR_lower={or_lower:.2f})"
            }

        return None
