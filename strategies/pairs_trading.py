"""Pairs Trading Strategy — Statistical Arbitrage.

Engle-Granger cointegration test across all symbol pairs.
Trade z-score of spread (entry at ±2, exit at ±0.5).
Multi-symbol strategy — uses generate_signal_multi instead of generate_signal.
"""
import itertools
import logging

import numpy as np
import pandas as pd

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class PairsTrading(BaseStrategy):

    def __init__(self, config: dict):
        super().__init__("pairs_trading", config)
        self.lookback = config.get("lookback", 60)
        self.entry_z = config.get("entry_z_score", 2.0)
        self.exit_z = config.get("exit_z_score", 0.5)
        self.coint_pvalue = config.get("coint_pvalue", 0.05)
        self.stop_loss_pct = config.get("stop_loss_pct", 3.0)
        self.target_pct = config.get("target_pct", 4.0)
        self.recalc_every = config.get("recalc_every", 20)

        self._pairs: list[tuple[str, str, float]] = []  # (sym_a, sym_b, hedge_ratio)
        self._bars_since_calc = 0

    @property
    def is_multi_symbol(self) -> bool:
        return True

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        # Not used directly for pairs — indicators computed in generate_signal_multi
        return df.copy()

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        # Single-symbol generate_signal not used for pairs trading
        return None

    def _find_cointegrated_pairs(self, dfs: dict[str, pd.DataFrame]) -> list[tuple[str, str, float]]:
        """Find cointegrated pairs using Engle-Granger test."""
        try:
            from statsmodels.tsa.stattools import coint
        except ImportError:
            logger.warning("statsmodels not installed — pairs trading disabled")
            return []

        symbols = list(dfs.keys())
        pairs = []

        for sym_a, sym_b in itertools.combinations(symbols, 2):
            df_a = dfs[sym_a]
            df_b = dfs[sym_b]

            # Align on dates
            merged = pd.merge(
                df_a[["timestamp", "close"]].rename(columns={"close": "a"}),
                df_b[["timestamp", "close"]].rename(columns={"close": "b"}),
                on="timestamp", how="inner"
            )

            if len(merged) < self.lookback:
                continue

            # Use last lookback bars
            recent = merged.tail(self.lookback)
            a_prices = recent["a"].values.astype(float)
            b_prices = recent["b"].values.astype(float)

            try:
                _, pvalue, _ = coint(a_prices, b_prices)
                if pvalue < self.coint_pvalue:
                    # Hedge ratio via OLS
                    hedge_ratio = float(np.polyfit(b_prices, a_prices, 1)[0])
                    pairs.append((sym_a, sym_b, hedge_ratio))
            except Exception:
                continue

        return pairs

    def _compute_zscore(self, a_prices: np.ndarray, b_prices: np.ndarray,
                        hedge_ratio: float) -> float:
        """Compute z-score of spread."""
        spread = a_prices - hedge_ratio * b_prices
        if len(spread) < 2:
            return 0.0
        mean = float(np.mean(spread))
        std = float(np.std(spread, ddof=1))
        if std < 1e-10:
            return 0.0
        return float((spread[-1] - mean) / std)

    def generate_signal_multi(self, dfs: dict[str, pd.DataFrame]) -> list[dict]:
        """Generate signals for all cointegrated pairs."""
        signals = []

        self._bars_since_calc += 1
        if not self._pairs or self._bars_since_calc >= self.recalc_every:
            self._pairs = self._find_cointegrated_pairs(dfs)
            self._bars_since_calc = 0
            if self._pairs:
                logger.info(f"Pairs trading: found {len(self._pairs)} cointegrated pairs")

        for sym_a, sym_b, hedge_ratio in self._pairs:
            if sym_a not in dfs or sym_b not in dfs:
                continue

            df_a = dfs[sym_a]
            df_b = dfs[sym_b]

            if len(df_a) < self.lookback or len(df_b) < self.lookback:
                continue

            a_prices = df_a["close"].tail(self.lookback).values.astype(float)
            b_prices = df_b["close"].tail(self.lookback).values.astype(float)

            zscore = self._compute_zscore(a_prices, b_prices, hedge_ratio)

            price_a = float(df_a.iloc[-1]["close"])
            price_b = float(df_b.iloc[-1]["close"])

            # Spread too high: short A, long B (mean reversion)
            if zscore > self.entry_z:
                signals.append({
                    "action": "SELL", "symbol": sym_a, "price": price_a,
                    "stop_loss": round(price_a * (1 + self.stop_loss_pct / 100), 2),
                    "target": round(price_a * (1 - self.target_pct / 100), 2),
                    "strategy": self.name,
                    "reason": f"Pairs: short {sym_a} (z={zscore:.2f}, pair={sym_b})"
                })
                signals.append({
                    "action": "BUY", "symbol": sym_b, "price": price_b,
                    "stop_loss": round(price_b * (1 - self.stop_loss_pct / 100), 2),
                    "target": round(price_b * (1 + self.target_pct / 100), 2),
                    "strategy": self.name,
                    "reason": f"Pairs: long {sym_b} (z={zscore:.2f}, pair={sym_a})"
                })

            # Spread too low: long A, short B
            elif zscore < -self.entry_z:
                signals.append({
                    "action": "BUY", "symbol": sym_a, "price": price_a,
                    "stop_loss": round(price_a * (1 - self.stop_loss_pct / 100), 2),
                    "target": round(price_a * (1 + self.target_pct / 100), 2),
                    "strategy": self.name,
                    "reason": f"Pairs: long {sym_a} (z={zscore:.2f}, pair={sym_b})"
                })
                signals.append({
                    "action": "SELL", "symbol": sym_b, "price": price_b,
                    "stop_loss": round(price_b * (1 + self.stop_loss_pct / 100), 2),
                    "target": round(price_b * (1 - self.target_pct / 100), 2),
                    "strategy": self.name,
                    "reason": f"Pairs: short {sym_b} (z={zscore:.2f}, pair={sym_a})"
                })

        return signals
