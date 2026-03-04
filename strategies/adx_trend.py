"""Strategy 10: ADX Trend Following.

Uses Average Directional Index to identify strong trends, then enters using
+DI/-DI crossovers. Only trades when ADX > 25 (strong trend confirmed).
Prevents whipsaws in choppy markets — a key edge over simpler moving average systems.
"""
import logging

import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class ADXTrend(BaseStrategy):

    def __init__(self, config: dict):
        super().__init__("adx_trend", config)
        self.adx_period = config.get("adx_period", 14)
        self.adx_threshold = config.get("adx_threshold", 25)
        self.stop_loss_pct = config.get("stop_loss_pct", 2.0)
        self.target_pct = config.get("target_pct", 4.0)

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        adx_result = ta.adx(df["high"], df["low"], df["close"], length=self.adx_period)
        if adx_result is not None:
            df = pd.concat([df, adx_result], axis=1)
        return df

    def _get_adx_cols(self, df: pd.DataFrame):
        adx_col = dmp_col = dmn_col = None
        for c in df.columns:
            if c.startswith("ADX_"):
                adx_col = c
            if c.startswith("DMP_"):
                dmp_col = c
            if c.startswith("DMN_"):
                dmn_col = c
        return adx_col, dmp_col, dmn_col

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        if len(df) < self.adx_period + 10:
            return None

        df = self.compute_indicators(df)
        adx_col, dmp_col, dmn_col = self._get_adx_cols(df)
        if not adx_col or not dmp_col or not dmn_col:
            return None

        last = df.iloc[-1]
        prev = df.iloc[-2]

        if pd.isna(last[adx_col]):
            return None

        adx = float(last[adx_col])
        dmp = float(last[dmp_col])
        dmn = float(last[dmn_col])
        prev_dmp = float(prev[dmp_col])
        prev_dmn = float(prev[dmn_col])
        price = float(last["close"])

        # Only trade when trend is strong
        if adx < self.adx_threshold:
            return None

        # BUY: +DI crosses above -DI in strong trend
        if prev_dmp <= prev_dmn and dmp > dmn:
            sl = round(price * (1 - self.stop_loss_pct / 100), 2)
            tgt = round(price * (1 + self.target_pct / 100), 2)
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": tgt, "strategy": self.name,
                "reason": f"+DI({dmp:.1f}) crossed above -DI({dmn:.1f}), ADX={adx:.1f}"
            }

        # SELL: -DI crosses above +DI in strong trend
        if prev_dmn <= prev_dmp and dmn > dmp:
            sl = round(price * (1 + self.stop_loss_pct / 100), 2)
            tgt = round(price * (1 - self.target_pct / 100), 2)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": tgt, "strategy": self.name,
                "reason": f"-DI({dmn:.1f}) crossed above +DI({dmp:.1f}), ADX={adx:.1f}"
            }

        return None

    def should_exit(self, position: dict, current_price: float, df: pd.DataFrame) -> dict | None:
        base = super().should_exit(position, current_price, df)
        if base:
            return base

        if len(df) < self.adx_period + 5:
            return None

        df = self.compute_indicators(df)
        adx_col, dmp_col, dmn_col = self._get_adx_cols(df)
        if not adx_col:
            return None

        last = df.iloc[-1]
        if pd.isna(last[adx_col]):
            return None

        adx = float(last[adx_col])

        # Exit if trend weakens significantly
        if adx < 15:
            side = "SELL" if position["side"] == "LONG" else "BUY"
            return {
                "action": side, "symbol": position["symbol"],
                "price": current_price, "reason": f"ADX_WEAK ({adx:.1f})",
                "strategy": self.name
            }

        return None
