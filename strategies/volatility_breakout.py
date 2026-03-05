"""Larry Williams Volatility Breakout Strategy.

Entry = previous close ± k_factor × previous day's range.
Classic systematic strategy with strong empirical backing.
"""
import logging

import pandas as pd

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class VolatilityBreakout(BaseStrategy):

    def __init__(self, config: dict):
        super().__init__("volatility_breakout", config)
        self.k_factor = config.get("k_factor", 0.5)
        self.stop_loss_pct = config.get("stop_loss_pct", 2.0)
        self.target_pct = config.get("target_pct", 4.0)

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["prev_close"] = df["close"].shift(1)
        df["prev_range"] = (df["high"].shift(1) - df["low"].shift(1))
        df["upper_trigger"] = df["prev_close"] + self.k_factor * df["prev_range"]
        df["lower_trigger"] = df["prev_close"] - self.k_factor * df["prev_range"]
        return df

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        if len(df) < 10:
            return None

        df = self.compute_indicators(df)
        last = df.iloc[-1]
        prev = df.iloc[-2]

        if pd.isna(last["upper_trigger"]) or pd.isna(prev["upper_trigger"]):
            return None

        price = float(last["close"])
        prev_price = float(prev["close"])
        upper = float(last["upper_trigger"])
        lower = float(last["lower_trigger"])

        # Bullish breakout: price crosses above upper trigger
        if price > upper and prev_price <= float(prev["upper_trigger"]):
            sl = round(price * (1 - self.stop_loss_pct / 100), 2)
            target = round(price * (1 + self.target_pct / 100), 2)
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": target, "strategy": self.name,
                "reason": f"Volatility breakout UP (price={price:.2f} > trigger={upper:.2f})"
            }

        # Bearish breakout: price crosses below lower trigger
        if price < lower and prev_price >= float(prev["lower_trigger"]):
            sl = round(price * (1 + self.stop_loss_pct / 100), 2)
            target = round(price * (1 - self.target_pct / 100), 2)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": target, "strategy": self.name,
                "reason": f"Volatility breakout DOWN (price={price:.2f} < trigger={lower:.2f})"
            }

        return None
