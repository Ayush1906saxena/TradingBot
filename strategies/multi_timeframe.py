"""Multi-Timeframe Confirmation Strategy.

Weekly trend (50-day SMA as proxy) + daily EMA crossover + RSI filter.
Only enters when higher timeframe agrees with lower timeframe signal.
Filters out counter-trend trades.
"""
import logging

import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class MultiTimeframe(BaseStrategy):

    def __init__(self, config: dict):
        super().__init__("multi_timeframe", config)
        self.trend_sma_period = config.get("trend_sma_period", 50)
        self.ema_fast = config.get("ema_fast", 9)
        self.ema_slow = config.get("ema_slow", 21)
        self.rsi_period = config.get("rsi_period", 14)
        self.rsi_buy_min = config.get("rsi_buy_min", 40)
        self.rsi_buy_max = config.get("rsi_buy_max", 70)
        self.rsi_sell_min = config.get("rsi_sell_min", 30)
        self.rsi_sell_max = config.get("rsi_sell_max", 60)
        self.stop_loss_pct = config.get("stop_loss_pct", 2.0)
        self.target_pct = config.get("target_pct", 4.0)

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # Higher timeframe trend proxy
        df["SMA_TREND"] = ta.sma(df["close"], length=self.trend_sma_period)
        # Lower timeframe EMA crossover
        df["EMA_FAST"] = ta.ema(df["close"], length=self.ema_fast)
        df["EMA_SLOW"] = ta.ema(df["close"], length=self.ema_slow)
        # RSI filter
        rsi = ta.rsi(df["close"], length=self.rsi_period)
        if rsi is not None:
            df["RSI"] = rsi

        # EMA cross signals
        df["ema_cross_up"] = (df["EMA_FAST"] > df["EMA_SLOW"]) & \
                             (df["EMA_FAST"].shift(1) <= df["EMA_SLOW"].shift(1))
        df["ema_cross_down"] = (df["EMA_FAST"] < df["EMA_SLOW"]) & \
                               (df["EMA_FAST"].shift(1) >= df["EMA_SLOW"].shift(1))
        return df

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        min_bars = self.trend_sma_period + 5
        if len(df) < min_bars:
            return None

        df = self.compute_indicators(df)
        last = df.iloc[-1]

        if pd.isna(last.get("SMA_TREND")) or pd.isna(last.get("RSI")):
            return None

        price = float(last["close"])
        trend_sma = float(last["SMA_TREND"])
        rsi = float(last["RSI"])
        uptrend = price > trend_sma
        downtrend = price < trend_sma

        # BUY: uptrend + EMA cross up + RSI in range
        if uptrend and last["ema_cross_up"] and self.rsi_buy_min <= rsi <= self.rsi_buy_max:
            sl = round(price * (1 - self.stop_loss_pct / 100), 2)
            target = round(price * (1 + self.target_pct / 100), 2)
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": target, "strategy": self.name,
                "reason": f"Multi-TF BUY: uptrend + EMA cross + RSI={rsi:.1f}"
            }

        # SELL: downtrend + EMA cross down + RSI in range
        if downtrend and last["ema_cross_down"] and self.rsi_sell_min <= rsi <= self.rsi_sell_max:
            sl = round(price * (1 + self.stop_loss_pct / 100), 2)
            target = round(price * (1 - self.target_pct / 100), 2)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": target, "strategy": self.name,
                "reason": f"Multi-TF SELL: downtrend + EMA cross + RSI={rsi:.1f}"
            }

        return None
