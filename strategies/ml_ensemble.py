"""ML Ensemble Strategy — Random Forest.

Features: RSI(7,14), MACD, BB width, ATR, returns(1,5,10), volume ratio, range.
Rolling train window (200 bars), retrain every 20 bars, no look-ahead bias.
Predict: 5-day forward return > 0? BUY if prob > 0.6, SELL if prob < 0.4.
"""
import logging

import numpy as np
import pandas as pd
import pandas_ta as ta

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class MLEnsemble(BaseStrategy):

    def __init__(self, config: dict):
        super().__init__("ml_ensemble", config)
        self.train_window = config.get("train_window", 200)
        self.retrain_every = config.get("retrain_every", 20)
        self.forward_days = config.get("forward_days", 5)
        self.buy_threshold = config.get("buy_threshold", 0.6)
        self.sell_threshold = config.get("sell_threshold", 0.4)
        self.stop_loss_pct = config.get("stop_loss_pct", 2.5)
        self.target_pct = config.get("target_pct", 5.0)

        self._model = None
        self._bars_since_train = 0
        self._feature_cols = []

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # RSI at two periods
        rsi7 = ta.rsi(df["close"], length=7)
        rsi14 = ta.rsi(df["close"], length=14)
        if rsi7 is not None:
            df["RSI_7"] = rsi7
        if rsi14 is not None:
            df["RSI_14"] = rsi14

        # MACD
        macd = ta.macd(df["close"], fast=12, slow=26, signal=9)
        if macd is not None:
            df = pd.concat([df, macd], axis=1)

        # Bollinger Band width
        bb = ta.bbands(df["close"], length=20, std=2.0)
        if bb is not None:
            bbu = [c for c in bb.columns if c.startswith("BBU_")]
            bbl = [c for c in bb.columns if c.startswith("BBL_")]
            bbm = [c for c in bb.columns if c.startswith("BBM_")]
            if bbu and bbl and bbm:
                df["BB_WIDTH"] = (bb[bbu[0]] - bb[bbl[0]]) / bb[bbm[0]]

        # ATR
        atr = ta.atr(df["high"], df["low"], df["close"], length=14)
        if atr is not None:
            df["ATR_14"] = atr
            df["ATR_NORM"] = df["ATR_14"] / df["close"]

        # Returns
        df["RET_1"] = df["close"].pct_change(1)
        df["RET_5"] = df["close"].pct_change(5)
        df["RET_10"] = df["close"].pct_change(10)

        # Volume ratio
        df["VOL_RATIO"] = df["volume"] / df["volume"].rolling(20).mean()

        # Daily range normalized
        df["RANGE_NORM"] = (df["high"] - df["low"]) / df["close"]

        return df

    def _get_features(self, df: pd.DataFrame) -> list[str]:
        candidates = [
            "RSI_7", "RSI_14", "BB_WIDTH", "ATR_NORM",
            "RET_1", "RET_5", "RET_10", "VOL_RATIO", "RANGE_NORM",
        ]
        # Add any MACD columns
        macd_cols = [c for c in df.columns if c.startswith("MACD") and not c.startswith("MACDs")]
        candidates.extend(macd_cols[:2])  # MACD line + histogram
        return [c for c in candidates if c in df.columns]

    def _build_training_data(self, df: pd.DataFrame):
        """Build features and labels with no look-ahead bias."""
        feature_cols = self._get_features(df)
        if not feature_cols:
            return None, None, feature_cols

        # Label: 5-day forward return > 0
        df = df.copy()
        df["_target"] = (df["close"].shift(-self.forward_days) / df["close"] - 1) > 0
        df["_target"] = df["_target"].astype(float)

        # Drop rows with NaN in features or target
        valid = df.dropna(subset=feature_cols + ["_target"])
        if len(valid) < 50:
            return None, None, feature_cols

        X = valid[feature_cols].values
        y = valid["_target"].values
        return X, y, feature_cols

    def _train_model(self, df: pd.DataFrame) -> bool:
        """Train Random Forest on the training window."""
        try:
            from sklearn.ensemble import RandomForestClassifier
        except ImportError:
            logger.warning("scikit-learn not installed — ML Ensemble disabled")
            return False

        X, y, feature_cols = self._build_training_data(df)
        if X is None or len(X) < 50:
            return False

        self._feature_cols = feature_cols
        self._model = RandomForestClassifier(
            n_estimators=100, max_depth=5, min_samples_leaf=10,
            random_state=42, n_jobs=-1
        )
        self._model.fit(X, y)
        self._bars_since_train = 0
        return True

    def generate_signal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        min_bars = self.train_window + self.forward_days + 10
        if len(df) < min_bars:
            return None

        df = self.compute_indicators(df)

        # Retrain periodically
        self._bars_since_train += 1
        if self._model is None or self._bars_since_train >= self.retrain_every:
            train_df = df.iloc[-(self.train_window + self.forward_days):].copy()
            if not self._train_model(train_df):
                return None

        if not self._feature_cols or self._model is None:
            return None

        # Predict on latest bar
        last = df.iloc[-1:]
        features = last[self._feature_cols]
        if features.isna().any(axis=1).iloc[0]:
            return None

        prob = self._model.predict_proba(features.values)
        # prob shape: (1, 2) — [prob_class_0, prob_class_1]
        if prob.shape[1] < 2:
            return None

        buy_prob = float(prob[0][1])
        price = float(last.iloc[0]["close"])

        if buy_prob > self.buy_threshold:
            sl = round(price * (1 - self.stop_loss_pct / 100), 2)
            target = round(price * (1 + self.target_pct / 100), 2)
            return {
                "action": "BUY", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": target, "strategy": self.name,
                "reason": f"ML ensemble BUY (prob={buy_prob:.2f})"
            }

        if buy_prob < self.sell_threshold:
            sl = round(price * (1 + self.stop_loss_pct / 100), 2)
            target = round(price * (1 - self.target_pct / 100), 2)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "stop_loss": sl, "target": target, "strategy": self.name,
                "reason": f"ML ensemble SELL (prob={buy_prob:.2f})"
            }

        return None
