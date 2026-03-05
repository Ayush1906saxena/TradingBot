"""Unit tests for backtesting engine accuracy."""
import os
import tempfile

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from db import init_db
    init_db(db_path)
    yield db_path
    os.unlink(db_path)


@pytest.fixture
def backtest_config():
    return {
        "capital": {
            "total": 100000,
            "max_risk_per_trade_pct": 2.0,
            "max_daily_loss_pct": 5.0,
            "max_capital_deployed_pct": 80.0,
            "max_per_stock_pct": 20.0,
            "max_open_positions": 5,
            "consecutive_loss_kill_switch": 10,
        },
        "market": {
            "timezone": "Asia/Kolkata",
            "trading_start": "00:00",
            "trading_end": "23:59",
        },
        "backtest": {
            "start_date": "2024-01-01",
            "end_date": "2024-06-30",
            "initial_capital": 100000,
            "slippage_pct": 0.05,
            "commission_per_order": 20,
            "walk_forward_splits": 2,
            "walk_forward_train_days": 60,
            "walk_forward_test_days": 30,
        },
        "paper_trading": {
            "slippage_pct": 0.05,
            "simulate_slippage": True,
            "simulate_fees": True,
        },
        "strategies": {
            "sma_crossover": {
                "enabled": True,
                "symbols": ["RELIANCE"],
                "timeframe": "15min",
                "short_window": 9,
                "long_window": 21,
                "stop_loss_pct": 1.5,
                "target_pct": 3.0,
                "capital_allocation": 100000,
                "trade_type": "intraday",
            },
            "rsi_reversal": {"enabled": False, "symbols": [], "timeframe": "15min",
                              "rsi_period": 14, "oversold_threshold": 30,
                              "overbought_threshold": 70, "exit_at_mean": True,
                              "stop_loss_pct": 2.0, "target_pct": 4.0,
                              "capital_allocation": 25000, "trade_type": "intraday"},
            "supertrend": {"enabled": False, "symbols": [], "timeframe": "15min",
                            "atr_period": 10, "multiplier": 3.0,
                            "capital_allocation": 50000, "trade_type": "intraday"},
        }
    }


def seed_daily_ohlcv(db_path: str, symbol: str = "RELIANCE", n_days: int = 200):
    """Insert synthetic daily OHLCV data into DB for testing."""
    from db import get_connection
    import random
    random.seed(42)

    conn = get_connection(db_path)
    price = 2500.0
    from datetime import date, timedelta
    current = date(2023, 6, 1)
    rows = []
    while len(rows) < n_days:
        if current.weekday() < 5:  # weekdays only
            change = random.gauss(0, 15)
            open_p = price
            close_p = max(1, price + change)
            high_p = max(open_p, close_p) + abs(random.gauss(0, 5))
            low_p = min(open_p, close_p) - abs(random.gauss(0, 5))
            volume = random.randint(500000, 2000000)
            rows.append((symbol, current.isoformat(), open_p, high_p, low_p, close_p, volume))
            price = close_p
        current += timedelta(days=1)

    conn.executemany(
        "INSERT OR IGNORE INTO daily_ohlcv (symbol, date, open, high, low, close, volume) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows
    )
    conn.commit()
    conn.close()


class TestBacktester:
    def test_run_returns_results_dict(self, temp_db, backtest_config):
        from engine.backtester import Backtester
        seed_daily_ohlcv(temp_db)

        bt = Backtester(backtest_config, temp_db)
        # Override timeframe to daily for test data
        backtest_config["strategies"]["sma_crossover"]["timeframe"] = "1d"
        results = bt.run("sma_crossover")

        assert isinstance(results, dict)
        assert "sma_crossover" in results

    def test_metrics_have_required_keys(self, temp_db, backtest_config):
        from engine.backtester import Backtester
        seed_daily_ohlcv(temp_db)
        backtest_config["strategies"]["sma_crossover"]["timeframe"] = "1d"

        bt = Backtester(backtest_config, temp_db)
        results = bt.run("sma_crossover")

        if "sma_crossover" in results:
            metrics = results["sma_crossover"]["metrics"]
            required = [
                "total_return_pct", "win_rate_pct", "max_drawdown_pct",
                "sharpe_ratio", "total_trades", "total_fees_inr"
            ]
            for key in required:
                assert key in metrics, f"Missing metric: {key}"

    def test_calculate_metrics_empty(self, temp_db, backtest_config):
        from engine.backtester import Backtester
        bt = Backtester(backtest_config, temp_db)
        metrics = bt.calculate_metrics([], 100000)
        assert metrics["total_trades"] == 0
        assert metrics["total_return_pct"] == 0

    def test_calculate_metrics_profitable(self, temp_db, backtest_config):
        from engine.backtester import Backtester
        bt = Backtester(backtest_config, temp_db)

        trades = [
            {"pnl": 1000, "fees": 50, "symbol": "RELIANCE", "strategy": "sma_crossover",
             "side": "SELL", "quantity": 10, "entry_price": 2500, "exit_price": 2600, "timestamp": "2024-01-15"},
            {"pnl": -500, "fees": 30, "symbol": "RELIANCE", "strategy": "sma_crossover",
             "side": "SELL", "quantity": 5, "entry_price": 2600, "exit_price": 2560, "timestamp": "2024-01-20"},
            {"pnl": 800, "fees": 40, "symbol": "RELIANCE", "strategy": "sma_crossover",
             "side": "SELL", "quantity": 8, "entry_price": 2550, "exit_price": 2650, "timestamp": "2024-01-25"},
        ]
        metrics = bt.calculate_metrics(trades, 100000)
        assert metrics["total_trades"] == 3
        assert metrics["win_rate_pct"] > 50  # 2 wins out of 3
        assert metrics["total_return_inr"] == pytest.approx(1300, rel=0.01)

    def test_equity_curve_increases_with_profitable_trades(self, temp_db, backtest_config):
        from engine.backtester import Backtester
        bt = Backtester(backtest_config, temp_db)

        trades = [
            {"pnl": 1000, "fees": 20, "symbol": "RELIANCE", "strategy": "test",
             "side": "SELL", "quantity": 10, "entry_price": 2500, "exit_price": 2600, "timestamp": "2024-01-15"},
            {"pnl": 2000, "fees": 20, "symbol": "RELIANCE", "strategy": "test",
             "side": "SELL", "quantity": 10, "entry_price": 2600, "exit_price": 2800, "timestamp": "2024-01-20"},
        ]
        curve = bt._build_equity_curve(trades, 100000)
        assert curve[-1]["equity"] > curve[0]["equity"]

    def test_generate_report_creates_file(self, temp_db, backtest_config, tmp_path):
        from engine.backtester import Backtester
        import os

        # Change to tmp_path directory to write logs there
        original_dir = os.getcwd()
        os.chdir(tmp_path)
        os.makedirs("logs", exist_ok=True)

        try:
            bt = Backtester(backtest_config, temp_db)
            metrics = bt.calculate_metrics(
                [{"pnl": 500, "fees": 20, "symbol": "RELIANCE", "strategy": "test",
                  "side": "SELL", "quantity": 5, "entry_price": 2500, "exit_price": 2600,
                  "timestamp": "2024-01-15"}],
                100000
            )
            curve = [{"timestamp": "start", "equity": 100000},
                     {"timestamp": "end", "equity": 100500, "pnl": 500, "symbol": "RELIANCE"}]

            path = bt.generate_report("sma_crossover", [], metrics, curve)
            assert os.path.exists(path)
        finally:
            os.chdir(original_dir)
