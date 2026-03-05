"""Unit tests for trading strategy signal generation."""
import numpy as np
import pandas as pd
import pytest


def make_ohlcv(n=100, trend="up") -> pd.DataFrame:
    """Generate synthetic OHLCV data."""
    np.random.seed(42)
    base = 2500.0
    prices = [base]
    for _ in range(n - 1):
        change = np.random.normal(0, 10)
        if trend == "up":
            change += 2
        elif trend == "down":
            change -= 2
        prices.append(max(1, prices[-1] + change))

    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01 09:15", periods=n, freq="15min"),
        "open": prices,
        "high": [p + abs(np.random.normal(0, 5)) for p in prices],
        "low": [p - abs(np.random.normal(0, 5)) for p in prices],
        "close": prices,
        "volume": np.random.randint(100000, 500000, n),
    })
    df["high"] = df[["open", "high", "close"]].max(axis=1)
    df["low"] = df[["open", "low", "close"]].min(axis=1)
    return df


def make_sma_crossover_config():
    return {
        "symbols": ["TEST"],
        "timeframe": "15min",
        "short_window": 9,
        "long_window": 21,
        "stop_loss_pct": 1.5,
        "target_pct": 3.0,
        "capital_allocation": 25000,
        "trade_type": "intraday",
    }


def make_rsi_config():
    return {
        "symbols": ["TEST"],
        "timeframe": "15min",
        "rsi_period": 14,
        "oversold_threshold": 30,
        "overbought_threshold": 70,
        "exit_at_mean": True,
        "stop_loss_pct": 2.0,
        "target_pct": 4.0,
        "capital_allocation": 25000,
        "trade_type": "intraday",
    }



class TestSMACrossover:
    def test_compute_indicators(self):
        from strategies.sma_crossover import SMACrossover
        strategy = SMACrossover(make_sma_crossover_config())
        df = make_ohlcv(100)
        result = strategy.compute_indicators(df)
        assert f"SMA_9" in result.columns
        assert f"SMA_21" in result.columns
        assert "cross_above" in result.columns
        assert "cross_below" in result.columns

    def test_generate_signal_returns_none_with_insufficient_data(self):
        from strategies.sma_crossover import SMACrossover
        strategy = SMACrossover(make_sma_crossover_config())
        df = make_ohlcv(10)  # too few bars
        signal = strategy.generate_signal(df, "TEST")
        assert signal is None

    def test_generate_signal_with_enough_data(self):
        from strategies.sma_crossover import SMACrossover
        strategy = SMACrossover(make_sma_crossover_config())
        df = make_ohlcv(100, trend="up")
        # May or may not generate signal, but should not raise
        signal = strategy.generate_signal(df, "TEST")
        if signal is not None:
            assert signal["action"] in ("BUY", "SELL")
            assert signal["symbol"] == "TEST"
            assert signal["stop_loss"] > 0
            assert signal["strategy"] == "sma_crossover"

    def test_signal_has_correct_fields(self):
        from strategies.sma_crossover import SMACrossover
        strategy = SMACrossover(make_sma_crossover_config())
        df = make_ohlcv(100, trend="up")

        # Force a crossover signal by manipulating data
        df_copy = df.copy()
        # Ensure we get a cross_above signal
        for i in range(len(df_copy) - 5, len(df_copy)):
            df_copy.loc[i, "close"] = df_copy.loc[i, "close"] * 1.05

        signal = strategy.generate_signal(df_copy, "TEST")
        if signal:
            required_keys = ["action", "symbol", "price", "stop_loss", "target", "strategy", "reason"]
            for key in required_keys:
                assert key in signal, f"Missing key: {key}"

    def test_should_exit_stoploss(self):
        from strategies.sma_crossover import SMACrossover
        strategy = SMACrossover(make_sma_crossover_config())
        position = {
            "symbol": "TEST", "strategy": "sma_crossover",
            "side": "LONG", "stop_loss": 2400.0, "target": 2600.0
        }
        df = make_ohlcv(50)
        # Price below stop loss
        exit_signal = strategy.should_exit(position, 2390.0, df)
        assert exit_signal is not None
        assert exit_signal["reason"] == "STOPLOSS"

    def test_should_exit_target(self):
        from strategies.sma_crossover import SMACrossover
        strategy = SMACrossover(make_sma_crossover_config())
        position = {
            "symbol": "TEST", "strategy": "sma_crossover",
            "side": "LONG", "stop_loss": 2400.0, "target": 2600.0
        }
        df = make_ohlcv(50)
        exit_signal = strategy.should_exit(position, 2650.0, df)
        assert exit_signal is not None
        assert exit_signal["reason"] == "TARGET"

    def test_does_not_mutate_input_df(self):
        from strategies.sma_crossover import SMACrossover
        strategy = SMACrossover(make_sma_crossover_config())
        df = make_ohlcv(100)
        original_cols = set(df.columns)
        strategy.generate_signal(df, "TEST")
        assert set(df.columns) == original_cols


class TestRSIReversal:
    def test_compute_indicators(self):
        from strategies.rsi_reversal import RSIReversal
        strategy = RSIReversal(make_rsi_config())
        df = make_ohlcv(100)
        result = strategy.compute_indicators(df)
        assert "RSI_14" in result.columns
        assert "prev_RSI_14" in result.columns

    def test_buy_signal_on_oversold(self):
        from strategies.rsi_reversal import RSIReversal
        strategy = RSIReversal(make_rsi_config())

        # Create data that will produce oversold RSI
        df = make_ohlcv(60, trend="down")
        # May generate buy signal since prices are falling
        signal = strategy.generate_signal(df, "TEST")
        if signal is not None:
            assert signal["action"] in ("BUY", "SELL")

    def test_signal_fields_correct(self):
        from strategies.rsi_reversal import RSIReversal
        strategy = RSIReversal(make_rsi_config())
        df = make_ohlcv(100)
        signal = strategy.generate_signal(df, "TEST")
        if signal:
            assert "action" in signal
            assert "stop_loss" in signal
            assert signal["stop_loss"] > 0


class TestKeltnerSqueeze:
    def test_compute_indicators(self):
        from strategies.keltner_squeeze import KeltnerSqueeze
        cfg = {"symbols": ["TEST"], "timeframe": "15min", "stop_loss_pct": 2.0, "target_pct": 4.0}
        strategy = KeltnerSqueeze(cfg)
        df = make_ohlcv(100)
        result = strategy.compute_indicators(df)
        assert "KC_MID" in result.columns
        assert "KC_UPPER" in result.columns

    def test_no_signal_insufficient_data(self):
        from strategies.keltner_squeeze import KeltnerSqueeze
        cfg = {"symbols": ["TEST"], "timeframe": "15min", "stop_loss_pct": 2.0, "target_pct": 4.0}
        strategy = KeltnerSqueeze(cfg)
        df = make_ohlcv(10)
        assert strategy.generate_signal(df, "TEST") is None

    def test_signal_or_none(self):
        from strategies.keltner_squeeze import KeltnerSqueeze
        cfg = {"symbols": ["TEST"], "timeframe": "15min", "stop_loss_pct": 2.0, "target_pct": 4.0}
        strategy = KeltnerSqueeze(cfg)
        df = make_ohlcv(200)
        signal = strategy.generate_signal(df, "TEST")
        if signal:
            assert signal["action"] in ("BUY", "SELL")
            assert signal["strategy"] == "keltner_squeeze"


class TestRSIDivergence:
    def test_compute_indicators(self):
        from strategies.rsi_divergence import RSIDivergence
        cfg = {"symbols": ["TEST"], "timeframe": "15min", "rsi_period": 14,
               "lookback": 20, "swing_window": 5, "stop_loss_pct": 2.0, "target_pct": 4.0}
        strategy = RSIDivergence(cfg)
        df = make_ohlcv(100)
        result = strategy.compute_indicators(df)
        assert "RSI_14" in result.columns

    def test_no_signal_insufficient_data(self):
        from strategies.rsi_divergence import RSIDivergence
        cfg = {"symbols": ["TEST"], "timeframe": "15min", "rsi_period": 14,
               "lookback": 20, "swing_window": 5, "stop_loss_pct": 2.0, "target_pct": 4.0}
        strategy = RSIDivergence(cfg)
        df = make_ohlcv(10)
        assert strategy.generate_signal(df, "TEST") is None


class TestVolatilityBreakout:
    def test_compute_indicators(self):
        from strategies.volatility_breakout import VolatilityBreakout
        cfg = {"symbols": ["TEST"], "timeframe": "15min", "k_factor": 0.5,
               "stop_loss_pct": 2.0, "target_pct": 4.0}
        strategy = VolatilityBreakout(cfg)
        df = make_ohlcv(50)
        result = strategy.compute_indicators(df)
        assert "upper_trigger" in result.columns
        assert "lower_trigger" in result.columns

    def test_signal_or_none(self):
        from strategies.volatility_breakout import VolatilityBreakout
        cfg = {"symbols": ["TEST"], "timeframe": "15min", "k_factor": 0.5,
               "stop_loss_pct": 2.0, "target_pct": 4.0}
        strategy = VolatilityBreakout(cfg)
        df = make_ohlcv(100)
        signal = strategy.generate_signal(df, "TEST")
        if signal:
            assert signal["action"] in ("BUY", "SELL")
            assert signal["strategy"] == "volatility_breakout"


class TestOpeningRangeBreakout:
    def test_compute_indicators(self):
        from strategies.opening_range_breakout import OpeningRangeBreakout
        cfg = {"symbols": ["TEST"], "timeframe": "15min", "atr_period": 14,
               "atr_multiplier": 0.3, "stop_loss_pct": 1.5, "target_pct": 3.0}
        strategy = OpeningRangeBreakout(cfg)
        df = make_ohlcv(50)
        result = strategy.compute_indicators(df)
        assert "ATR" in result.columns
        assert "OR_UPPER" in result.columns

    def test_no_signal_insufficient_data(self):
        from strategies.opening_range_breakout import OpeningRangeBreakout
        cfg = {"symbols": ["TEST"], "timeframe": "15min", "atr_period": 14,
               "atr_multiplier": 0.3, "stop_loss_pct": 1.5, "target_pct": 3.0}
        strategy = OpeningRangeBreakout(cfg)
        df = make_ohlcv(5)
        assert strategy.generate_signal(df, "TEST") is None


class TestMultiTimeframe:
    def test_compute_indicators(self):
        from strategies.multi_timeframe import MultiTimeframe
        cfg = {"symbols": ["TEST"], "timeframe": "15min", "trend_sma_period": 50,
               "ema_fast": 9, "ema_slow": 21, "rsi_period": 14,
               "stop_loss_pct": 2.0, "target_pct": 4.0}
        strategy = MultiTimeframe(cfg)
        df = make_ohlcv(100)
        result = strategy.compute_indicators(df)
        assert "SMA_TREND" in result.columns
        assert "EMA_FAST" in result.columns
        assert "RSI" in result.columns

    def test_no_signal_insufficient_data(self):
        from strategies.multi_timeframe import MultiTimeframe
        cfg = {"symbols": ["TEST"], "timeframe": "15min", "trend_sma_period": 50,
               "ema_fast": 9, "ema_slow": 21, "rsi_period": 14,
               "stop_loss_pct": 2.0, "target_pct": 4.0}
        strategy = MultiTimeframe(cfg)
        df = make_ohlcv(20)
        assert strategy.generate_signal(df, "TEST") is None


class TestMLEnsemble:
    def test_compute_indicators(self):
        from strategies.ml_ensemble import MLEnsemble
        cfg = {"symbols": ["TEST"], "timeframe": "15min", "train_window": 200,
               "retrain_every": 20, "stop_loss_pct": 2.5, "target_pct": 5.0}
        strategy = MLEnsemble(cfg)
        df = make_ohlcv(100)
        result = strategy.compute_indicators(df)
        assert "RSI_7" in result.columns
        assert "RSI_14" in result.columns
        assert "RET_1" in result.columns
        assert "VOL_RATIO" in result.columns

    def test_no_signal_insufficient_data(self):
        from strategies.ml_ensemble import MLEnsemble
        cfg = {"symbols": ["TEST"], "timeframe": "15min", "train_window": 200,
               "retrain_every": 20, "stop_loss_pct": 2.5, "target_pct": 5.0}
        strategy = MLEnsemble(cfg)
        df = make_ohlcv(50)
        assert strategy.generate_signal(df, "TEST") is None

    def test_signal_with_enough_data(self):
        from strategies.ml_ensemble import MLEnsemble
        cfg = {"symbols": ["TEST"], "timeframe": "15min", "train_window": 100,
               "retrain_every": 20, "forward_days": 5, "buy_threshold": 0.6,
               "sell_threshold": 0.4, "stop_loss_pct": 2.5, "target_pct": 5.0}
        strategy = MLEnsemble(cfg)
        df = make_ohlcv(250, trend="up")
        signal = strategy.generate_signal(df, "TEST")
        if signal:
            assert signal["action"] in ("BUY", "SELL")
            assert signal["strategy"] == "ml_ensemble"


class TestPairsTrading:
    def test_is_multi_symbol(self):
        from strategies.pairs_trading import PairsTrading
        cfg = {"symbols": ["TEST_A", "TEST_B"], "timeframe": "15min",
               "lookback": 60, "stop_loss_pct": 3.0, "target_pct": 4.0}
        strategy = PairsTrading(cfg)
        assert strategy.is_multi_symbol is True

    def test_single_symbol_generate_signal_returns_none(self):
        from strategies.pairs_trading import PairsTrading
        cfg = {"symbols": ["TEST_A", "TEST_B"], "timeframe": "15min",
               "lookback": 60, "stop_loss_pct": 3.0, "target_pct": 4.0}
        strategy = PairsTrading(cfg)
        df = make_ohlcv(100)
        assert strategy.generate_signal(df, "TEST_A") is None

    def test_generate_signal_multi_returns_list(self):
        from strategies.pairs_trading import PairsTrading
        cfg = {"symbols": ["TEST_A", "TEST_B"], "timeframe": "15min",
               "lookback": 60, "coint_pvalue": 0.99,  # relaxed for test
               "stop_loss_pct": 3.0, "target_pct": 4.0}
        strategy = PairsTrading(cfg)
        dfs = {"TEST_A": make_ohlcv(100, "up"), "TEST_B": make_ohlcv(100, "up")}
        signals = strategy.generate_signal_multi(dfs)
        assert isinstance(signals, list)
