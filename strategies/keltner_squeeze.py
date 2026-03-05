"""Keltner Channel Squeeze Strategy.

Detects volatility compression when Bollinger Bands contract inside Keltner Channels.
When the squeeze releases, enters in the direction of momentum (using MACD histogram).
"""
import logging

import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class KeltnerSqueeze(BaseStrategy):

    def __init__(self, config: dict):
        super().__init__("keltner_squeeze", config)
        self.bb_period = config.get("bb_period", 20)
        self.bb_std = config.get("bb_std", 2.0)
        self.kc_period = config.get("kc_period", 20)
        self.kc_atr_mult = config.get("kc_atr_mult", 1.5)
        self.mom_period = config.get("mom_period", 12)
        self.stop_loss_pct = config.get("stop_loss_pct", 2.0)
        self.target_pct = config.get("target_pct", 4.0)

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Bollinger Bands
        bb = ta.bbands(df["close"], length=self.bb_period, std=self.bb_std)
        if bb is not None:
            df = pd.concat([df, bb], axis=1)

        # Keltner Channels: EMA ± mult * ATR
        df["KC_MID"] = ta.ema(df["close"], length=self.kc_period)
        atr = ta.atr(df["high"], df["low"], df["close"], length=self.kc_period)
        if atr is not None:
            df["KC_UPPER"] = df["KC_MID"] + self.kc_atr_mult * atr
            df["KC_LOWER"] = df["KC_MID"] - self.kc_atr_mult * atr

        # Momentum (MACD histogram as momentum proxy)
        macd = ta.macd(df["close"], fast=12, slow=26, signal=9)
        if macd is not None:
            df = pd.concat([df, macd], axis=1)

        return df

    def _get_bb_cols(self, df: pd.DataFrame):
        lower = upper = None
        for c in df.columns:
            if c.startswith("BBL_"):
                lower = c
            if c.startswith("BBU_"):
                upper = c
        return lower, upper

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        if len(df) < max(self.bb_period, self.kc_period, 26) + 5:
            return None

        df = self.compute_indicators(df)
        bbl_col, bbu_col = self._get_bb_cols(df)
        if not bbl_col or not bbu_col:
            return None

        # Need at least 2 bars to detect squeeze release
        if pd.isna(df.iloc[-1].get("KC_UPPER")) or pd.isna(df.iloc[-2].get("KC_UPPER")):
            return None

        last = df.iloc[-1]
        prev = df.iloc[-2]

        if pd.isna(last[bbl_col]) or pd.isna(prev[bbl_col]):
            return None

        # Squeeze: BB is inside KC
        prev_squeeze = (float(prev[bbl_col]) > float(prev["KC_LOWER"])) and \
                        (float(prev[bbu_col]) < float(prev["KC_UPPER"]))
        curr_squeeze = (float(last[bbl_col]) > float(last["KC_LOWER"])) and \
                        (float(last[bbu_col]) < float(last["KC_UPPER"]))

        # Squeeze release: was in squeeze, now not
        if not (prev_squeeze and not curr_squeeze):
            return None

        # Momentum direction from MACD histogram
        hist_col = [c for c in df.columns if c.startswith("MACDh_")]
        if not hist_col or pd.isna(last[hist_col[0]]):
            return None

        momentum = float(last[hist_col[0]])
        price = float(last["close"])

        if momentum > 0:
            sl = round(price * (1 - self.stop_loss_pct / 100), 2)
            target = round(price * (1 + self.target_pct / 100), 2)
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": target, "strategy": self.name,
                "reason": f"Squeeze release BULLISH (momentum={momentum:.2f})"
            }
        elif momentum < 0:
            sl = round(price * (1 + self.stop_loss_pct / 100), 2)
            target = round(price * (1 - self.target_pct / 100), 2)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": target, "strategy": self.name,
                "reason": f"Squeeze release BEARISH (momentum={momentum:.2f})"
            }

        return None
