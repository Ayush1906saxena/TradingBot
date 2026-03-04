"""Strategy 7: VWAP (Volume Weighted Average Price).

Institutional-grade strategy. Buy when price crosses above VWAP with volume,
sell when price crosses below VWAP. VWAP acts as dynamic support/resistance.
Used by most professional intraday traders globally.
"""
import logging

import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class VWAPStrategy(BaseStrategy):

    def __init__(self, config: dict):
        super().__init__("vwap_strategy", config)
        self.stop_loss_pct = config.get("stop_loss_pct", 1.0)
        self.target_pct = config.get("target_pct", 2.0)
        self.volume_confirm = config.get("volume_confirm", True)
        self.volume_multiplier = config.get("volume_multiplier", 1.3)

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # Compute VWAP: cumulative(typical_price × volume) / cumulative(volume)
        # pandas_ta vwap needs hlc and volume
        if all(c in df.columns for c in ["high", "low", "close", "volume"]):
            vwap_result = ta.vwap(df["high"], df["low"], df["close"], df["volume"])
            if vwap_result is not None:
                df["VWAP"] = vwap_result
            else:
                # Manual VWAP calculation
                tp = (df["high"] + df["low"] + df["close"]) / 3
                df["VWAP"] = (tp * df["volume"]).cumsum() / df["volume"].cumsum()
        df["volume_sma_20"] = ta.sma(df["volume"], length=20)
        return df

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        if len(df) < 25:
            return None

        df = self.compute_indicators(df)
        if "VWAP" not in df.columns:
            return None

        last = df.iloc[-1]
        prev = df.iloc[-2]

        if pd.isna(last["VWAP"]):
            return None

        price = float(last["close"])
        vwap = float(last["VWAP"])
        prev_price = float(prev["close"])
        prev_vwap = float(prev["VWAP"]) if not pd.isna(prev["VWAP"]) else vwap

        vol_confirm = True
        if self.volume_confirm and "volume_sma_20" in df.columns:
            avg_vol = last.get("volume_sma_20")
            if not pd.isna(avg_vol) and avg_vol > 0:
                vol_confirm = float(last["volume"]) > self.volume_multiplier * float(avg_vol)

        # BUY: price crosses above VWAP with volume
        if prev_price <= prev_vwap and price > vwap and vol_confirm:
            sl = round(price * (1 - self.stop_loss_pct / 100), 2)
            tgt = round(price * (1 + self.target_pct / 100), 2)
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": tgt, "strategy": self.name,
                "reason": f"Price crossed above VWAP ({price:.2f} > {vwap:.2f})"
            }

        # SELL: price crosses below VWAP with volume
        if prev_price >= prev_vwap and price < vwap and vol_confirm:
            sl = round(price * (1 + self.stop_loss_pct / 100), 2)
            tgt = round(price * (1 - self.target_pct / 100), 2)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": tgt, "strategy": self.name,
                "reason": f"Price crossed below VWAP ({price:.2f} < {vwap:.2f})"
            }

        return None
