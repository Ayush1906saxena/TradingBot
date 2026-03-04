"""Strategy 13: Momentum / Rate of Change (ROC).

Buys stocks with highest momentum (ROC > threshold) confirmed by volume.
The "momentum factor" is one of the most empirically validated sources of
excess returns in academic finance literature (Jegadeesh & Titman, 1993).
"""
import logging

import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class MomentumROC(BaseStrategy):

    def __init__(self, config: dict):
        super().__init__("momentum_roc", config)
        self.roc_period = config.get("roc_period", 12)
        self.roc_buy_threshold = config.get("roc_buy_threshold", 3.0)
        self.roc_sell_threshold = config.get("roc_sell_threshold", -3.0)
        self.ema_filter_period = config.get("ema_filter_period", 50)
        self.stop_loss_pct = config.get("stop_loss_pct", 2.0)
        self.target_pct = config.get("target_pct", 4.0)

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["ROC"] = ta.roc(df["close"], length=self.roc_period)
        df["prev_ROC"] = df["ROC"].shift(1)
        df[f"EMA_{self.ema_filter_period}"] = ta.ema(df["close"], length=self.ema_filter_period)
        df["volume_sma"] = ta.sma(df["volume"], length=20)
        return df

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        min_bars = max(self.roc_period, self.ema_filter_period) + 5
        if len(df) < min_bars:
            return None

        df = self.compute_indicators(df)
        last = df.iloc[-1]

        if pd.isna(last["ROC"]) or pd.isna(last["prev_ROC"]):
            return None

        roc = float(last["ROC"])
        prev_roc = float(last["prev_ROC"])
        price = float(last["close"])
        ema = float(last[f"EMA_{self.ema_filter_period}"])

        # Volume confirmation
        vol = float(last["volume"])
        avg_vol = float(last["volume_sma"]) if not pd.isna(last["volume_sma"]) else vol
        vol_ok = vol > avg_vol * 1.2

        # BUY: ROC crosses above buy threshold with price above EMA and volume
        if (prev_roc <= self.roc_buy_threshold and roc > self.roc_buy_threshold
                and price > ema and vol_ok):
            sl = round(price * (1 - self.stop_loss_pct / 100), 2)
            tgt = round(price * (1 + self.target_pct / 100), 2)
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": tgt, "strategy": self.name,
                "reason": f"ROC={roc:.2f}% above threshold, EMA{self.ema_filter_period} confirmed"
            }

        # SELL: ROC crosses below sell threshold with price below EMA and volume
        if (prev_roc >= self.roc_sell_threshold and roc < self.roc_sell_threshold
                and price < ema and vol_ok):
            sl = round(price * (1 + self.stop_loss_pct / 100), 2)
            tgt = round(price * (1 - self.target_pct / 100), 2)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": tgt, "strategy": self.name,
                "reason": f"ROC={roc:.2f}% below threshold, EMA{self.ema_filter_period} confirmed"
            }

        return None
