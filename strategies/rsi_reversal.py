"""Strategy 2: RSI Mean Reversion."""
import logging

import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class RSIReversal(BaseStrategy):
    """
    Buy when RSI crosses below oversold threshold.
    Sell when RSI crosses above overbought threshold.
    Exit when RSI returns to 50 (mean).
    """

    def __init__(self, config: dict):
        super().__init__("rsi_reversal", config)
        self.rsi_period = config.get("rsi_period", 14)
        self.oversold = config.get("oversold_threshold", 30)
        self.overbought = config.get("overbought_threshold", 70)
        self.exit_at_mean = config.get("exit_at_mean", True)
        self.stop_loss_pct = config.get("stop_loss_pct", 2.0)
        self.target_pct = config.get("target_pct", 4.0)

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        col = f"RSI_{self.rsi_period}"
        df[col] = ta.rsi(df["close"], length=self.rsi_period)
        df[f"prev_{col}"] = df[col].shift(1)
        return df

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        if len(df) < self.rsi_period + 5:
            return None

        df = self.compute_indicators(df)
        last = df.iloc[-1]
        rsi_col = f"RSI_{self.rsi_period}"

        if pd.isna(last[rsi_col]):
            return None

        rsi = float(last[rsi_col])
        prev_rsi = float(last[f"prev_{rsi_col}"])
        price = float(last["close"])

        # Buy: RSI just crossed below oversold (enters oversold zone)
        if rsi < self.oversold and prev_rsi >= self.oversold:
            sl = round(price * (1 - self.stop_loss_pct / 100), 2)
            target = round(price * (1 + self.target_pct / 100), 2)
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": target,
                "strategy": self.name,
                "reason": f"RSI({rsi:.1f}) crossed below oversold({self.oversold})"
            }

        # Sell: RSI just crossed above overbought (enters overbought zone)
        if rsi > self.overbought and prev_rsi <= self.overbought:
            sl = round(price * (1 + self.stop_loss_pct / 100), 2)
            target = round(price * (1 - self.target_pct / 100), 2)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": target,
                "strategy": self.name,
                "reason": f"RSI({rsi:.1f}) crossed above overbought({self.overbought})"
            }

        return None

    def should_exit(self, position: dict, current_price: float, df: pd.DataFrame) -> dict | None:
        """Override: also exit when RSI returns to 50 (mean reversion complete)."""
        # Check standard SL/target first
        base_exit = super().should_exit(position, current_price, df)
        if base_exit:
            return base_exit

        if not self.exit_at_mean:
            return None

        if len(df) < self.rsi_period + 2:
            return None

        df = self.compute_indicators(df)
        last = df.iloc[-1]
        rsi_col = f"RSI_{self.rsi_period}"

        if pd.isna(last[rsi_col]):
            return None

        rsi = float(last[rsi_col])
        prev_rsi = float(last[f"prev_{rsi_col}"])

        # Exit LONG when RSI returns to 50 from below
        if position["side"] == "LONG" and rsi >= 50 and prev_rsi < 50:
            return {
                "action": "SELL", "symbol": position["symbol"],
                "price": current_price, "reason": "RSI_MEAN_REVERSION",
                "strategy": self.name
            }

        # Exit SHORT when RSI returns to 50 from above
        if position["side"] == "SHORT" and rsi <= 50 and prev_rsi > 50:
            return {
                "action": "BUY", "symbol": position["symbol"],
                "price": current_price, "reason": "RSI_MEAN_REVERSION",
                "strategy": self.name
            }

        return None
