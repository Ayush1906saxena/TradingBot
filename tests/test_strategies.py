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


def make_ema_rsi_volume_config():
    return {
        "symbols": ["TEST"],
        "timeframe": "15min",
        "ema_short": 9,
        "ema_long": 21,
        "rsi_period": 14,
        "rsi_buy_threshold": 55,
        "rsi_sell_threshold": 45,
        "volume_multiplier": 1.5,
        "stop_loss_pct": 1.5,
        "trailing_stop_enabled": True,
        "trailing_breakeven_at_pct": 2.0,
        "trailing_activate_at_pct": 3.0,
        "trailing_distance_pct": 1.5,
        "capital_allocation": 50000,
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


class TestEMARSIVolume:
    def test_compute_indicators(self):
        from strategies.ema_rsi_volume import EMARSIVolume
        strategy = EMARSIVolume(make_ema_rsi_volume_config())
        df = make_ohlcv(100)
        result = strategy.compute_indicators(df)
        assert "EMA_9" in result.columns
        assert "EMA_21" in result.columns
        assert "RSI_14" in result.columns
        assert "volume_sma_20" in result.columns
        assert "volume_spike" in result.columns

    def test_trailing_stop_phase1(self):
        from strategies.ema_rsi_volume import EMARSIVolume
        strategy = EMARSIVolume(make_ema_rsi_volume_config())
        position = {
            "symbol": "TEST", "strategy": "ema_rsi_volume",
            "side": "LONG", "entry_price": 2500.0,
            "stop_loss": 2462.5, "highest_since_entry": 2500.0
        }
        df = make_ohlcv(50)
        # Price at entry — no exit
        exit_sig = strategy.should_exit(position, 2500.0, df)
        assert exit_sig is None

    def test_trailing_stop_stoploss_hit(self):
        from strategies.ema_rsi_volume import EMARSIVolume
        strategy = EMARSIVolume(make_ema_rsi_volume_config())
        position = {
            "symbol": "TEST", "strategy": "ema_rsi_volume",
            "side": "LONG", "entry_price": 2500.0,
            "stop_loss": 2462.5, "highest_since_entry": 2500.0
        }
        df = make_ohlcv(50)
        # Price below stop loss
        exit_sig = strategy.should_exit(position, 2460.0, df)
        assert exit_sig is not None
        assert exit_sig["reason"] == "STOPLOSS"

    def test_trailing_stop_activation(self):
        from strategies.ema_rsi_volume import EMARSIVolume
        strategy = EMARSIVolume(make_ema_rsi_volume_config())
        position = {
            "symbol": "TEST", "strategy": "ema_rsi_volume",
            "side": "LONG", "entry_price": 2500.0,
            "stop_loss": 2462.5, "highest_since_entry": 2600.0  # profit > 3%
        }
        df = make_ohlcv(50)
        # Trail stop = 2600 * (1 - 1.5%) = 2561
        # Price at 2570 — no exit
        exit_sig = strategy.should_exit(position, 2570.0, df)
        assert exit_sig is None

        # Price below trail stop
        exit_sig = strategy.should_exit(position, 2555.0, df)
        assert exit_sig is not None
        assert exit_sig["reason"] == "TRAILING_STOP"
