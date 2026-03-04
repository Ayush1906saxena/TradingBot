"""Strategy 6: Bollinger Band Mean Reversion.

Buy at the lower band (oversold), sell at the upper band (overbought).
Uses %B indicator and bandwidth for confirmation.
One of the most profitable mean-reversion strategies for range-bound markets.
"""
import logging

import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class BollingerBands(BaseStrategy):

    def __init__(self, config: dict):
        super().__init__("bollinger_bands", config)
        self.bb_period = config.get("bb_period", 20)
        self.bb_std = config.get("bb_std", 2.0)
        self.stop_loss_pct = config.get("stop_loss_pct", 2.0)
        self.target_pct = config.get("target_pct", 3.0)

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        bb = ta.bbands(df["close"], length=self.bb_period, std=self.bb_std)
        if bb is not None:
            df = pd.concat([df, bb], axis=1)
        # %B: (close - lower) / (upper - lower). 0 = at lower band, 1 = at upper band
        return df

    def _get_bb_cols(self, df: pd.DataFrame):
        lower = upper = mid = None
        for c in df.columns:
            if c.startswith("BBL_"):
                lower = c
            if c.startswith("BBU_"):
                upper = c
            if c.startswith("BBM_"):
                mid = c
        return lower, mid, upper

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        if len(df) < self.bb_period + 5:
            return None

        df = self.compute_indicators(df)
        lower_col, mid_col, upper_col = self._get_bb_cols(df)
        if not lower_col or not upper_col:
            return None

        last = df.iloc[-1]
        prev = df.iloc[-2]
        price = float(last["close"])

        if pd.isna(last[lower_col]):
            return None

        lower = float(last[lower_col])
        upper = float(last[upper_col])
        mid = float(last[mid_col])
        prev_price = float(prev["close"])
        prev_lower = float(prev[lower_col])
        prev_upper = float(prev[upper_col])

        # BUY: price crosses below lower band then bounces back (touch-and-reverse)
        if prev_price <= prev_lower and price > lower:
            sl = round(price * (1 - self.stop_loss_pct / 100), 2)
            tgt = round(mid, 2)  # Target = middle band (mean reversion)
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": tgt, "strategy": self.name,
                "reason": f"Bounce off lower BB (price={price:.2f}, lower={lower:.2f})"
            }

        # SELL: price crosses above upper band then falls back
        if prev_price >= prev_upper and price < upper:
            sl = round(price * (1 + self.stop_loss_pct / 100), 2)
            tgt = round(mid, 2)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": tgt, "strategy": self.name,
                "reason": f"Rejection at upper BB (price={price:.2f}, upper={upper:.2f})"
            }

        return None
