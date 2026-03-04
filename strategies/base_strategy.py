"""Abstract base class for all trading strategies."""
from abc import ABC, abstractmethod
import pandas as pd


class BaseStrategy(ABC):
    def __init__(self, name: str, config: dict):
        self.name = name
        self.config = config
        self.symbols = config["symbols"]
        self.timeframe = config["timeframe"]

    @abstractmethod
    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add indicator columns to OHLCV DataFrame. Return copy, don't mutate."""
        pass

    @abstractmethod
    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        """
        Return signal dict or None:
        { "action": "BUY"|"SELL", "symbol": str, "price": float,
          "stop_loss": float, "target": float|None, "strategy": str, "reason": str }
        """
        pass

    def should_exit(self, position: dict, current_price: float, df: pd.DataFrame) -> dict | None:
        """Check SL/target. Override for trailing stops. Return exit signal or None."""
        sl = position["stop_loss"]
        target = position.get("target")
        if position["side"] == "LONG":
            if current_price <= sl:
                return {
                    "action": "SELL", "symbol": position["symbol"],
                    "price": current_price, "reason": "STOPLOSS", "strategy": self.name
                }
            if target and current_price >= target:
                return {
                    "action": "SELL", "symbol": position["symbol"],
                    "price": current_price, "reason": "TARGET", "strategy": self.name
                }
        elif position["side"] == "SHORT":
            if current_price >= sl:
                return {
                    "action": "BUY", "symbol": position["symbol"],
                    "price": current_price, "reason": "STOPLOSS", "strategy": self.name
                }
            if target and current_price <= target:
                return {
                    "action": "BUY", "symbol": position["symbol"],
                    "price": current_price, "reason": "TARGET", "strategy": self.name
                }
        return None
