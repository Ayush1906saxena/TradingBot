"""RSI Divergence Strategy.

Detects bullish divergence (price makes lower low, RSI makes higher low)
and bearish divergence (price makes higher high, RSI makes lower high).
More sophisticated than simple RSI threshold crossings.
"""
import logging

import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class RSIDivergence(BaseStrategy):

    def __init__(self, config: dict):
        super().__init__("rsi_divergence", config)
        self.rsi_period = config.get("rsi_period", 14)
        self.lookback = config.get("lookback", 20)
        self.swing_window = config.get("swing_window", 5)
        self.stop_loss_pct = config.get("stop_loss_pct", 2.0)
        self.target_pct = config.get("target_pct", 4.0)

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        rsi = ta.rsi(df["close"], length=self.rsi_period)
        if rsi is not None:
            df[f"RSI_{self.rsi_period}"] = rsi
        return df

    def _find_swing_lows(self, series: pd.Series, window: int) -> list[int]:
        """Find indices of swing lows within the series."""
        lows = []
        for i in range(window, len(series) - window):
            if series.iloc[i] == series.iloc[i - window:i + window + 1].min():
                lows.append(i)
        return lows

    def _find_swing_highs(self, series: pd.Series, window: int) -> list[int]:
        """Find indices of swing highs within the series."""
        highs = []
        for i in range(window, len(series) - window):
            if series.iloc[i] == series.iloc[i - window:i + window + 1].max():
                highs.append(i)
        return highs

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        min_bars = self.rsi_period + self.lookback + self.swing_window + 5
        if len(df) < min_bars:
            return None

        df = self.compute_indicators(df)
        rsi_col = f"RSI_{self.rsi_period}"

        if rsi_col not in df.columns or pd.isna(df[rsi_col].iloc[-1]):
            return None

        # Work with the lookback window
        window_df = df.iloc[-(self.lookback + self.swing_window * 2):].copy()
        window_df = window_df.reset_index(drop=True)

        price_series = window_df["close"]
        rsi_series = window_df[rsi_col]

        # Find swing points
        price_lows = self._find_swing_lows(price_series, self.swing_window)
        price_highs = self._find_swing_highs(price_series, self.swing_window)
        rsi_lows = self._find_swing_lows(rsi_series, self.swing_window)
        rsi_highs = self._find_swing_highs(rsi_series, self.swing_window)

        price = float(df.iloc[-1]["close"])

        # Bullish divergence: price lower low + RSI higher low
        if len(price_lows) >= 2 and len(rsi_lows) >= 2:
            pl1, pl2 = price_lows[-2], price_lows[-1]
            rl1, rl2 = rsi_lows[-2], rsi_lows[-1]
            if (float(price_series.iloc[pl2]) < float(price_series.iloc[pl1]) and
                    float(rsi_series.iloc[rl2]) > float(rsi_series.iloc[rl1])):
                sl = round(price * (1 - self.stop_loss_pct / 100), 2)
                target = round(price * (1 + self.target_pct / 100), 2)
                return {
                    "action": "BUY", "symbol": symbol, "price": price,
                    "stop_loss": sl, "target": target, "strategy": self.name,
                    "reason": "Bullish RSI divergence (price lower low, RSI higher low)"
                }

        # Bearish divergence: price higher high + RSI lower high
        if len(price_highs) >= 2 and len(rsi_highs) >= 2:
            ph1, ph2 = price_highs[-2], price_highs[-1]
            rh1, rh2 = rsi_highs[-2], rsi_highs[-1]
            if (float(price_series.iloc[ph2]) > float(price_series.iloc[ph1]) and
                    float(rsi_series.iloc[rh2]) < float(rsi_series.iloc[rh1])):
                sl = round(price * (1 + self.stop_loss_pct / 100), 2)
                target = round(price * (1 - self.target_pct / 100), 2)
                return {
                    "action": "SELL", "symbol": symbol, "price": price,
                    "stop_loss": sl, "target": target, "strategy": self.name,
                    "reason": "Bearish RSI divergence (price higher high, RSI lower high)"
                }

        return None
