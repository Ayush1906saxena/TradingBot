"""Strategy 1: SMA Crossover."""
import logging

import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class SMACrossover(BaseStrategy):
    """
    Buy when fast SMA crosses above slow SMA.
    Sell when fast SMA crosses below slow SMA.
    """

    def __init__(self, config: dict):
        super().__init__("sma_crossover", config)
        self.short_window = config.get("short_window", 9)
        self.long_window = config.get("long_window", 21)
        self.stop_loss_pct = config.get("stop_loss_pct", 1.5)
        self.target_pct = config.get("target_pct", 3.0)

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df[f"SMA_{self.short_window}"] = ta.sma(df["close"], length=self.short_window)
        df[f"SMA_{self.long_window}"] = ta.sma(df["close"], length=self.long_window)

        short = df[f"SMA_{self.short_window}"]
        long_ = df[f"SMA_{self.long_window}"]

        # cross_above: short was below long, now above
        df["cross_above"] = (short > long_) & (short.shift(1) <= long_.shift(1))
        # cross_below: short was above long, now below
        df["cross_below"] = (short < long_) & (short.shift(1) >= long_.shift(1))
        return df

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        if len(df) < self.long_window + 2:
            return None

        df = self.compute_indicators(df)
        last = df.iloc[-1]

        if pd.isna(last[f"SMA_{self.long_window}"]):
            return None

        price = float(last["close"])

        if last["cross_above"]:
            sl = round(price * (1 - self.stop_loss_pct / 100), 2)
            target = round(price * (1 + self.target_pct / 100), 2)
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": target,
                "strategy": self.name,
                "reason": f"SMA{self.short_window} crossed above SMA{self.long_window}"
            }

        if last["cross_below"]:
            sl = round(price * (1 + self.stop_loss_pct / 100), 2)
            target = round(price * (1 - self.target_pct / 100), 2)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": target,
                "strategy": self.name,
                "reason": f"SMA{self.short_window} crossed below SMA{self.long_window}"
            }

        return None
