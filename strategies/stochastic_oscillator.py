"""Strategy 9: Stochastic Oscillator Momentum.

Uses %K and %D stochastic lines to identify oversold/overbought reversal points.
Combined with trend filter (50-period SMA) to only trade in trend direction.
One of the most reliable momentum reversal indicators used worldwide.
"""
import logging

import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class StochasticOscillator(BaseStrategy):

    def __init__(self, config: dict):
        super().__init__("stochastic_oscillator", config)
        self.k_period = config.get("k_period", 14)
        self.d_period = config.get("d_period", 3)
        self.smooth_k = config.get("smooth_k", 3)
        self.oversold = config.get("oversold_threshold", 20)
        self.overbought = config.get("overbought_threshold", 80)
        self.trend_filter_period = config.get("trend_filter_period", 50)
        self.stop_loss_pct = config.get("stop_loss_pct", 1.5)
        self.target_pct = config.get("target_pct", 3.0)

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        stoch = ta.stoch(df["high"], df["low"], df["close"],
                         k=self.k_period, d=self.d_period, smooth_k=self.smooth_k)
        if stoch is not None:
            df = pd.concat([df, stoch], axis=1)
        df[f"SMA_{self.trend_filter_period}"] = ta.sma(df["close"], length=self.trend_filter_period)
        return df

    def _get_stoch_cols(self, df: pd.DataFrame):
        k_col = d_col = None
        for c in df.columns:
            if c.startswith("STOCHk_"):
                k_col = c
            if c.startswith("STOCHd_"):
                d_col = c
        return k_col, d_col

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        min_bars = max(self.k_period, self.trend_filter_period) + 5
        if len(df) < min_bars:
            return None

        df = self.compute_indicators(df)
        k_col, d_col = self._get_stoch_cols(df)
        if not k_col or not d_col:
            return None

        last = df.iloc[-1]
        prev = df.iloc[-2]
        price = float(last["close"])
        trend_sma = last.get(f"SMA_{self.trend_filter_period}")

        if pd.isna(last[k_col]) or pd.isna(prev[k_col]) or pd.isna(trend_sma):
            return None

        k = float(last[k_col])
        d = float(last[d_col])
        prev_k = float(prev[k_col])
        prev_d = float(prev[d_col])

        # BUY: %K crosses above %D in oversold zone AND price above trend SMA
        if (prev_k <= prev_d and k > d and k < self.oversold + 10
                and price > float(trend_sma)):
            sl = round(price * (1 - self.stop_loss_pct / 100), 2)
            tgt = round(price * (1 + self.target_pct / 100), 2)
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": tgt, "strategy": self.name,
                "reason": f"Stoch %K({k:.0f}) crossed %D({d:.0f}) in oversold, above SMA{self.trend_filter_period}"
            }

        # SELL: %K crosses below %D in overbought zone AND price below trend SMA
        if (prev_k >= prev_d and k < d and k > self.overbought - 10
                and price < float(trend_sma)):
            sl = round(price * (1 + self.stop_loss_pct / 100), 2)
            tgt = round(price * (1 - self.target_pct / 100), 2)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": tgt, "strategy": self.name,
                "reason": f"Stoch %K({k:.0f}) crossed below %D({d:.0f}) in overbought, below SMA{self.trend_filter_period}"
            }

        return None
