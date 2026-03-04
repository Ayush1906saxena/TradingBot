"""Strategy 12: Mean Reversion using Z-Score.

Statistical approach: computes Z-score of price relative to its mean.
Buy when Z-score drops below -2 (extremely oversold), sell above +2.
Used extensively by quant funds and statistical arbitrage desks globally.
"""
import logging

import numpy as np
import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class MeanReversionZScore(BaseStrategy):

    def __init__(self, config: dict):
        super().__init__("mean_reversion_zscore", config)
        self.lookback = config.get("lookback_period", 20)
        self.entry_z = config.get("entry_z_score", 2.0)
        self.exit_z = config.get("exit_z_score", 0.0)
        self.stop_loss_pct = config.get("stop_loss_pct", 3.0)

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["sma"] = ta.sma(df["close"], length=self.lookback)
        df["std"] = df["close"].rolling(window=self.lookback).std()
        df["z_score"] = (df["close"] - df["sma"]) / (df["std"] + 1e-10)
        df["prev_z_score"] = df["z_score"].shift(1)
        return df

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        if len(df) < self.lookback + 5:
            return None

        df = self.compute_indicators(df)
        last = df.iloc[-1]

        if pd.isna(last["z_score"]) or pd.isna(last["prev_z_score"]):
            return None

        z = float(last["z_score"])
        prev_z = float(last["prev_z_score"])
        price = float(last["close"])
        mean = float(last["sma"])

        # BUY: Z-score crosses below -entry_z (extremely oversold, expect reversion to mean)
        if z < -self.entry_z and prev_z >= -self.entry_z:
            sl = round(price * (1 - self.stop_loss_pct / 100), 2)
            tgt = round(mean, 2)  # Target = mean
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": tgt, "strategy": self.name,
                "reason": f"Z-score={z:.2f} (below -{self.entry_z}), mean={mean:.2f}"
            }

        # SELL: Z-score crosses above +entry_z (extremely overbought)
        if z > self.entry_z and prev_z <= self.entry_z:
            sl = round(price * (1 + self.stop_loss_pct / 100), 2)
            tgt = round(mean, 2)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": tgt, "strategy": self.name,
                "reason": f"Z-score={z:.2f} (above +{self.entry_z}), mean={mean:.2f}"
            }

        return None

    def should_exit(self, position: dict, current_price: float, df: pd.DataFrame) -> dict | None:
        base = super().should_exit(position, current_price, df)
        if base:
            return base

        if len(df) < self.lookback + 2:
            return None

        df = self.compute_indicators(df)
        last = df.iloc[-1]
        if pd.isna(last["z_score"]):
            return None

        z = float(last["z_score"])

        # Exit LONG when z-score returns to exit_z (mean reversion complete)
        if position["side"] == "LONG" and z >= self.exit_z:
            return {
                "action": "SELL", "symbol": position["symbol"],
                "price": current_price, "reason": f"ZSCORE_MEAN_REVERSION (z={z:.2f})",
                "strategy": self.name
            }

        # Exit SHORT when z-score returns to -exit_z
        if position["side"] == "SHORT" and z <= -self.exit_z:
            return {
                "action": "BUY", "symbol": position["symbol"],
                "price": current_price, "reason": f"ZSCORE_MEAN_REVERSION (z={z:.2f})",
                "strategy": self.name
            }

        return None
