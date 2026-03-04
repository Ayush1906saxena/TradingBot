"""Strategy 11: Ichimoku Cloud (Ichimoku Kinko Hyo).

The most comprehensive single-indicator system in technical analysis.
Uses 5 lines (Tenkan, Kijun, Senkou A, Senkou B, Chikou) to identify
trend direction, momentum, support/resistance in one glance.
Extremely popular in Japanese and Asian markets.
"""
import logging

import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class IchimokuCloud(BaseStrategy):

    def __init__(self, config: dict):
        super().__init__("ichimoku_cloud", config)
        self.tenkan_period = config.get("tenkan_period", 9)
        self.kijun_period = config.get("kijun_period", 26)
        self.senkou_b_period = config.get("senkou_b_period", 52)
        self.stop_loss_pct = config.get("stop_loss_pct", 2.0)
        self.target_pct = config.get("target_pct", 4.0)

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        ichi = ta.ichimoku(df["high"], df["low"], df["close"],
                           tenkan=self.tenkan_period, kijun=self.kijun_period,
                           senkou=self.senkou_b_period)
        if ichi is not None and len(ichi) == 2:
            # ichimoku returns tuple: (span_df, chikou). We want span_df
            span_df = ichi[0]
            df = pd.concat([df, span_df], axis=1)
        return df

    def _get_ichi_cols(self, df: pd.DataFrame):
        tenkan = kijun = senkou_a = senkou_b = None
        for c in df.columns:
            cl = c.lower()
            if "tenkan" in cl or c.startswith("ITS_"):
                tenkan = c
            elif "kijun" in cl or c.startswith("IKS_"):
                kijun = c
            elif "senkou" in cl and "a" in cl.lower() or c.startswith("ISA_"):
                senkou_a = c
            elif "senkou" in cl and "b" in cl.lower() or c.startswith("ISB_"):
                senkou_b = c
        return tenkan, kijun, senkou_a, senkou_b

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        if len(df) < self.senkou_b_period + 30:
            return None

        df = self.compute_indicators(df)
        tenkan_col, kijun_col, sa_col, sb_col = self._get_ichi_cols(df)
        if not tenkan_col or not kijun_col:
            return None

        last = df.iloc[-1]
        prev = df.iloc[-2]
        price = float(last["close"])

        if pd.isna(last[tenkan_col]) or pd.isna(last[kijun_col]):
            return None

        tenkan = float(last[tenkan_col])
        kijun = float(last[kijun_col])
        prev_tenkan = float(prev[tenkan_col])
        prev_kijun = float(prev[kijun_col])

        # Cloud values (Senkou A & B)
        above_cloud = True
        if sa_col and sb_col and not pd.isna(last.get(sa_col)) and not pd.isna(last.get(sb_col)):
            cloud_top = max(float(last[sa_col]), float(last[sb_col]))
            cloud_bottom = min(float(last[sa_col]), float(last[sb_col]))
            above_cloud = price > cloud_top
            below_cloud = price < cloud_bottom
        else:
            below_cloud = False

        # BUY: Tenkan crosses above Kijun AND price above cloud
        if prev_tenkan <= prev_kijun and tenkan > kijun and above_cloud:
            sl = round(price * (1 - self.stop_loss_pct / 100), 2)
            tgt = round(price * (1 + self.target_pct / 100), 2)
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": tgt, "strategy": self.name,
                "reason": f"Tenkan-Kijun bullish cross above cloud"
            }

        # SELL: Tenkan crosses below Kijun AND price below cloud
        if prev_tenkan >= prev_kijun and tenkan < kijun and below_cloud:
            sl = round(price * (1 + self.stop_loss_pct / 100), 2)
            tgt = round(price * (1 - self.target_pct / 100), 2)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": tgt, "strategy": self.name,
                "reason": f"Tenkan-Kijun bearish cross below cloud"
            }

        return None
