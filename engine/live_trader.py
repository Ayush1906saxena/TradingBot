"""Live trading engine — re-exports TradingEngine from paper_trader."""
# Both paper and live modes use the same TradingEngine class.
# Mode is passed as a constructor argument.
from engine.paper_trader import TradingEngine

__all__ = ["TradingEngine"]
