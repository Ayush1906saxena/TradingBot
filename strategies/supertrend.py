"""Strategy 4: Supertrend."""
import logging

import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class SupertrendStrategy(BaseStrategy):
    """
    Buy when Supertrend direction flips from -1 to 1 (bearish to bullish).
    Sell when direction flips from 1 to -1 (bullish to bearish).
    Stop loss is the current Supertrend line value.
    """

    def __init__(self, config: dict):
        super().__init__("supertrend", config)
        self.atr_period = config.get("atr_period", 10)
        self.multiplier = config.get("multiplier", 3.0)

    def _col_names(self):
        mult_str = str(self.multiplier).replace(".", "_") if "." in str(self.multiplier) \
            else f"{self.multiplier}_0"
        # pandas_ta supertrend columns: SUPERT_10_3.0, SUPERTd_10_3.0, SUPERTl_10_3.0, SUPERTs_10_3.0
        return (
            f"SUPERT_{self.atr_period}_{self.multiplier}",
            f"SUPERTd_{self.atr_period}_{self.multiplier}",
        )

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        result = ta.supertrend(
            df["high"], df["low"], df["close"],
            length=self.atr_period,
            multiplier=self.multiplier
        )
        if result is not None:
            df = pd.concat([df, result], axis=1)
        return df

    def _get_supertrend_cols(self, df: pd.DataFrame):
        """Find actual column names in df since pandas_ta naming varies."""
        line_col = None
        dir_col = None
        for col in df.columns:
            if col.startswith("SUPERT_") and not col.startswith("SUPERTd") \
                    and not col.startswith("SUPERTl") and not col.startswith("SUPERTs"):
                line_col = col
            if col.startswith("SUPERTd_"):
                dir_col = col
        return line_col, dir_col

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        min_bars = self.atr_period + 10
        if len(df) < min_bars:
            return None

        df = self.compute_indicators(df)
        line_col, dir_col = self._get_supertrend_cols(df)

        if not line_col or not dir_col:
            return None

        last = df.iloc[-1]
        prev = df.iloc[-2]

        if pd.isna(last[dir_col]) or pd.isna(prev[dir_col]):
            return None

        curr_dir = int(last[dir_col])
        prev_dir = int(prev[dir_col])
        price = float(last["close"])
        st_line = float(last[line_col])

        # BUY: direction flipped from -1 to 1
        if prev_dir == -1 and curr_dir == 1:
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": round(st_line, 2), "target": None,
                "strategy": self.name,
                "reason": "Supertrend flipped bullish"
            }

        # SELL: direction flipped from 1 to -1
        if prev_dir == 1 and curr_dir == -1:
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": round(st_line, 2), "target": None,
                "strategy": self.name,
                "reason": "Supertrend flipped bearish"
            }

        return None

    def should_exit(self, position: dict, current_price: float, df: pd.DataFrame) -> dict | None:
        """Exit when direction flips or price crosses supertrend line."""
        if len(df) < self.atr_period + 5:
            return super().should_exit(position, current_price, df)

        df = self.compute_indicators(df)
        line_col, dir_col = self._get_supertrend_cols(df)

        if not line_col or not dir_col:
            return super().should_exit(position, current_price, df)

        last = df.iloc[-1]
        prev = df.iloc[-2]

        if pd.isna(last[dir_col]):
            return None

        curr_dir = int(last[dir_col])
        prev_dir = int(prev[dir_col]) if not pd.isna(prev[dir_col]) else curr_dir
        st_line = float(last[line_col])

        if position["side"] == "LONG":
            # Exit on direction flip or price below supertrend line
            if (prev_dir == 1 and curr_dir == -1) or current_price < st_line:
                return {
                    "action": "SELL", "symbol": position["symbol"],
                    "price": current_price, "reason": "SUPERTREND_FLIP",
                    "strategy": self.name
                }
        elif position["side"] == "SHORT":
            if (prev_dir == -1 and curr_dir == 1) or current_price > st_line:
                return {
                    "action": "BUY", "symbol": position["symbol"],
                    "price": current_price, "reason": "SUPERTREND_FLIP",
                    "strategy": self.name
                }

        return None
