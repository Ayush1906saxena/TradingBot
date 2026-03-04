"""Strategy 5: MACD Crossover — Moving Average Convergence Divergence.

One of the most widely-used momentum indicators globally. Trades the crossover
of the MACD line and the signal line, confirmed by histogram direction.
"""
import logging

import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class MACDCrossover(BaseStrategy):

    def __init__(self, config: dict):
        super().__init__("macd_crossover", config)
        self.fast = config.get("fast_period", 12)
        self.slow = config.get("slow_period", 26)
        self.signal_period = config.get("signal_period", 9)
        self.stop_loss_pct = config.get("stop_loss_pct", 1.5)
        self.target_pct = config.get("target_pct", 3.0)

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        macd = ta.macd(df["close"], fast=self.fast, slow=self.slow, signal=self.signal_period)
        if macd is not None:
            df = pd.concat([df, macd], axis=1)
        return df

    def _get_macd_cols(self, df: pd.DataFrame):
        macd_col = signal_col = hist_col = None
        for c in df.columns:
            if c.startswith("MACD_") and not c.startswith("MACDs") and not c.startswith("MACDh"):
                macd_col = c
            if c.startswith("MACDs_"):
                signal_col = c
            if c.startswith("MACDh_"):
                hist_col = c
        return macd_col, signal_col, hist_col

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        if len(df) < self.slow + self.signal_period + 5:
            return None

        df = self.compute_indicators(df)
        macd_col, signal_col, hist_col = self._get_macd_cols(df)
        if not macd_col or not signal_col:
            return None

        last = df.iloc[-1]
        prev = df.iloc[-2]

        if pd.isna(last[macd_col]) or pd.isna(prev[macd_col]):
            return None

        macd_now = float(last[macd_col])
        signal_now = float(last[signal_col])
        macd_prev = float(prev[macd_col])
        signal_prev = float(prev[signal_col])
        price = float(last["close"])

        # BUY: MACD crosses above signal line
        if macd_prev <= signal_prev and macd_now > signal_now:
            sl = round(price * (1 - self.stop_loss_pct / 100), 2)
            tgt = round(price * (1 + self.target_pct / 100), 2)
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": tgt, "strategy": self.name,
                "reason": f"MACD crossed above signal ({macd_now:.2f} > {signal_now:.2f})"
            }

        # SELL: MACD crosses below signal line
        if macd_prev >= signal_prev and macd_now < signal_now:
            sl = round(price * (1 + self.stop_loss_pct / 100), 2)
            tgt = round(price * (1 - self.target_pct / 100), 2)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": tgt, "strategy": self.name,
                "reason": f"MACD crossed below signal ({macd_now:.2f} < {signal_now:.2f})"
            }

        return None
