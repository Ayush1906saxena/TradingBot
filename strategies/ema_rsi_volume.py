"""Strategy 3: EMA + RSI + Volume with trailing stop."""
import logging

import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class EMARSIVolume(BaseStrategy):
    """
    Buy when EMA_short > EMA_long AND RSI > threshold AND volume spike.
    Uses a 3-phase trailing stop.
    """

    def __init__(self, config: dict):
        super().__init__("ema_rsi_volume", config)
        self.ema_short = config.get("ema_short", 9)
        self.ema_long = config.get("ema_long", 21)
        self.rsi_period = config.get("rsi_period", 14)
        self.rsi_buy_threshold = config.get("rsi_buy_threshold", 55)
        self.rsi_sell_threshold = config.get("rsi_sell_threshold", 45)
        self.volume_multiplier = config.get("volume_multiplier", 1.5)
        self.stop_loss_pct = config.get("stop_loss_pct", 1.5)
        self.trailing_enabled = config.get("trailing_stop_enabled", True)
        self.breakeven_pct = config.get("trailing_breakeven_at_pct", 2.0)
        self.activate_pct = config.get("trailing_activate_at_pct", 3.0)
        self.trail_dist_pct = config.get("trailing_distance_pct", 1.5)

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df[f"EMA_{self.ema_short}"] = ta.ema(df["close"], length=self.ema_short)
        df[f"EMA_{self.ema_long}"] = ta.ema(df["close"], length=self.ema_long)
        df[f"RSI_{self.rsi_period}"] = ta.rsi(df["close"], length=self.rsi_period)
        df["volume_sma_20"] = ta.sma(df["volume"], length=20)
        df["volume_spike"] = df["volume"] > (self.volume_multiplier * df["volume_sma_20"])
        return df

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        min_bars = max(self.ema_long, 20) + 5
        if len(df) < min_bars:
            return None

        df = self.compute_indicators(df)
        last = df.iloc[-1]

        ema_s = last.get(f"EMA_{self.ema_short}")
        ema_l = last.get(f"EMA_{self.ema_long}")
        rsi = last.get(f"RSI_{self.rsi_period}")
        vol_spike = last.get("volume_spike", False)

        if pd.isna(ema_s) or pd.isna(ema_l) or pd.isna(rsi):
            return None

        price = float(last["close"])

        # BUY: EMA short > EMA long AND RSI > buy threshold AND volume spike
        if float(ema_s) > float(ema_l) and float(rsi) > self.rsi_buy_threshold and vol_spike:
            sl = round(price * (1 - self.stop_loss_pct / 100), 2)
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": None,
                "strategy": self.name,
                "reason": f"EMA{self.ema_short}>EMA{self.ema_long}, RSI={rsi:.1f}, VolumeSpike"
            }

        # SELL: EMA short < EMA long AND RSI < sell threshold AND volume spike
        if float(ema_s) < float(ema_l) and float(rsi) < self.rsi_sell_threshold and vol_spike:
            sl = round(price * (1 + self.stop_loss_pct / 100), 2)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": None,
                "strategy": self.name,
                "reason": f"EMA{self.ema_short}<EMA{self.ema_long}, RSI={rsi:.1f}, VolumeSpike"
            }

        return None

    def should_exit(self, position: dict, current_price: float, df: pd.DataFrame) -> dict | None:
        """3-phase trailing stop logic."""
        entry = float(position["entry_price"])
        side = position["side"]
        highest = float(position.get("highest_since_entry") or entry)

        # Update highest
        if side == "LONG":
            highest = max(highest, current_price)
        else:
            highest = min(highest, current_price)

        # Store update (in-memory; caller should persist)
        position["highest_since_entry"] = highest

        if not self.trailing_enabled:
            return super().should_exit(position, current_price, df)

        if side == "LONG":
            # Use highest price ever reached to determine which phase we're in
            highest_profit_pct = (highest - entry) / entry * 100

            # Phase 3: trailing stop active
            if highest_profit_pct >= self.activate_pct:
                trail_sl = round(highest * (1 - self.trail_dist_pct / 100), 2)
                if current_price <= trail_sl:
                    return {
                        "action": "SELL", "symbol": position["symbol"],
                        "price": current_price, "reason": "TRAILING_STOP",
                        "strategy": self.name
                    }
                return None

            # Phase 2: move to breakeven
            if highest_profit_pct >= self.breakeven_pct:
                if current_price <= entry:
                    return {
                        "action": "SELL", "symbol": position["symbol"],
                        "price": current_price, "reason": "BREAKEVEN_STOP",
                        "strategy": self.name
                    }
                return None

            # Phase 1: initial stop loss
            initial_sl = float(position.get("stop_loss") or entry * (1 - self.stop_loss_pct / 100))
            if current_price <= initial_sl:
                return {
                    "action": "SELL", "symbol": position["symbol"],
                    "price": current_price, "reason": "STOPLOSS",
                    "strategy": self.name
                }

        elif side == "SHORT":
            highest_profit_pct = (entry - highest) / entry * 100

            if highest_profit_pct >= self.activate_pct:
                trail_sl = round(highest * (1 + self.trail_dist_pct / 100), 2)
                if current_price >= trail_sl:
                    return {
                        "action": "BUY", "symbol": position["symbol"],
                        "price": current_price, "reason": "TRAILING_STOP",
                        "strategy": self.name
                    }
                return None

            if highest_profit_pct >= self.breakeven_pct:
                if current_price >= entry:
                    return {
                        "action": "BUY", "symbol": position["symbol"],
                        "price": current_price, "reason": "BREAKEVEN_STOP",
                        "strategy": self.name
                    }
                return None

            initial_sl = float(position.get("stop_loss") or entry * (1 + self.stop_loss_pct / 100))
            if current_price >= initial_sl:
                return {
                    "action": "BUY", "symbol": position["symbol"],
                    "price": current_price, "reason": "STOPLOSS",
                    "strategy": self.name
                }

        return None
