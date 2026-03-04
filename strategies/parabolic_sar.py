"""Strategy 14: Parabolic SAR (Stop and Reverse).

Developed by J. Welles Wilder Jr., this trend-following indicator provides
potential entry/exit points by plotting dots above or below the price.
When dots flip from above to below price = BUY; below to above = SELL.
Combined with ADX filter to avoid whipsaws in ranging markets.
"""
import logging

import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class ParabolicSAR(BaseStrategy):

    def __init__(self, config: dict):
        super().__init__("parabolic_sar", config)
        self.af_start = config.get("af_start", 0.02)
        self.af_increment = config.get("af_increment", 0.02)
        self.af_max = config.get("af_max", 0.2)
        self.adx_filter_period = config.get("adx_filter_period", 14)
        self.adx_min = config.get("adx_min", 20)
        self.stop_loss_pct = config.get("stop_loss_pct", 2.0)
        self.target_pct = config.get("target_pct", 4.0)

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        psar = ta.psar(df["high"], df["low"], df["close"],
                       af0=self.af_start, af=self.af_increment, max_af=self.af_max)
        if psar is not None:
            df = pd.concat([df, psar], axis=1)

        adx_result = ta.adx(df["high"], df["low"], df["close"], length=self.adx_filter_period)
        if adx_result is not None:
            for c in adx_result.columns:
                if c.startswith("ADX_"):
                    df["psar_adx"] = adx_result[c]
                    break
        return df

    def _get_psar_cols(self, df: pd.DataFrame):
        long_col = short_col = None
        for c in df.columns:
            if "PSARl_" in c:
                long_col = c
            elif "PSARs_" in c:
                short_col = c
        return long_col, short_col

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        if len(df) < max(self.adx_filter_period, 30) + 10:
            return None

        df = self.compute_indicators(df)
        long_col, short_col = self._get_psar_cols(df)
        if not long_col and not short_col:
            return None

        last = df.iloc[-1]
        prev = df.iloc[-2]
        price = float(last["close"])

        # ADX filter: only trade in trending markets
        adx = float(last.get("psar_adx", 0)) if not pd.isna(last.get("psar_adx")) else 0
        if adx < self.adx_min:
            return None

        # Detect SAR flip: long_col has value means bullish, short_col has value means bearish
        curr_bullish = long_col and not pd.isna(last.get(long_col))
        prev_bullish = long_col and not pd.isna(prev.get(long_col))
        curr_bearish = short_col and not pd.isna(last.get(short_col))
        prev_bearish = short_col and not pd.isna(prev.get(short_col))

        # BUY: SAR flips from bearish to bullish (dots move from above to below price)
        if curr_bullish and prev_bearish:
            sar_value = float(last[long_col])
            sl = round(min(sar_value, price * (1 - self.stop_loss_pct / 100)), 2)
            tgt = round(price * (1 + self.target_pct / 100), 2)
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": tgt, "strategy": self.name,
                "reason": f"PSAR flip bullish, SAR={sar_value:.2f}, ADX={adx:.1f}"
            }

        # SELL: SAR flips from bullish to bearish (dots move from below to above price)
        if curr_bearish and prev_bullish:
            sar_value = float(last[short_col])
            sl = round(max(sar_value, price * (1 + self.stop_loss_pct / 100)), 2)
            tgt = round(price * (1 - self.target_pct / 100), 2)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": tgt, "strategy": self.name,
                "reason": f"PSAR flip bearish, SAR={sar_value:.2f}, ADX={adx:.1f}"
            }

        return None

    def should_exit(self, position: dict, current_price: float, df: pd.DataFrame) -> dict | None:
        base = super().should_exit(position, current_price, df)
        if base:
            return base

        if len(df) < 30:
            return None

        df = self.compute_indicators(df)
        long_col, short_col = self._get_psar_cols(df)
        last = df.iloc[-1]

        # Exit LONG when SAR flips bearish
        if position["side"] == "LONG" and short_col and not pd.isna(last.get(short_col)):
            return {
                "action": "SELL", "symbol": position["symbol"],
                "price": current_price, "reason": "PSAR_FLIP_BEARISH",
                "strategy": self.name
            }

        # Exit SHORT when SAR flips bullish
        if position["side"] == "SHORT" and long_col and not pd.isna(last.get(long_col)):
            return {
                "action": "BUY", "symbol": position["symbol"],
                "price": current_price, "reason": "PSAR_FLIP_BULLISH",
                "strategy": self.name
            }

        return None
