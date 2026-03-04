"""Unit tests for order manager."""
import os
import tempfile

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
def config():
    return {
        "capital": {
            "total": 100000,
            "max_risk_per_trade_pct": 2.0,
            "max_daily_loss_pct": 5.0,
            "max_capital_deployed_pct": 80.0,
            "max_per_stock_pct": 20.0,
            "max_open_positions": 5,
            "consecutive_loss_kill_switch": 5,
        },
        "market": {
            "timezone": "Asia/Kolkata",
            "trading_start": "00:00",
            "trading_end": "23:59",
        },
        "paper_trading": {
            "slippage_pct": 0.05,
            "simulate_slippage": True,
            "simulate_fees": True,
        },
        "strategies": {
            "sma_crossover": {
                "trade_type": "intraday",
                "enabled": True,
                "symbols": ["RELIANCE"],
                "timeframe": "15min",
            }
        }
    }


class TestCalculateFees:
    def test_intraday_buy_fees(self):
        from orders.order_manager import calculate_fees
        fees = calculate_fees(2500.0, 40, "BUY", "intraday")
        assert fees["total"] > 0
        assert fees["stt"] == 0  # No STT on intraday buy
        assert fees["stamp_duty"] > 0
        assert fees["brokerage"] > 0
        assert fees["gst"] > 0

    def test_intraday_sell_fees(self):
        from orders.order_manager import calculate_fees
        fees = calculate_fees(2500.0, 40, "SELL", "intraday")
        assert fees["stt"] > 0  # STT on intraday sell
        assert fees["stamp_duty"] == 0  # No stamp duty on sell

    def test_brokerage_capped_at_20(self):
        from orders.order_manager import calculate_fees
        # Large order: turnover = 2500 * 1000 = 2,500,000
        # 0.03% = 750, but capped at 20
        fees = calculate_fees(2500.0, 1000, "BUY", "intraday")
        assert fees["brokerage"] == 20.0

    def test_small_order_brokerage(self):
        from orders.order_manager import calculate_fees
        # Small order: turnover = 100, brokerage = 100 * 0.03% = 0.03 (< 20)
        fees = calculate_fees(100.0, 1, "BUY", "intraday")
        assert fees["brokerage"] < 20.0


class TestOrderManager:
    def test_process_signal_backtest(self, temp_db, config):
        from risk.risk_manager import RiskManager
        from orders.order_manager import OrderManager

        rm = RiskManager(config, temp_db, "backtest")
        om = OrderManager(
            broker_gateway=None,
            risk_manager=rm,
            config=config,
            db_path=temp_db,
            mode="backtest",
        )

        signal = {
            "action": "BUY", "symbol": "RELIANCE", "price": 2500.0,
            "stop_loss": 2450.0, "target": 2600.0, "strategy": "sma_crossover"
        }
        result = om.process_signal(signal)
        assert result is not None
        assert result["fill_price"] > 0
        assert result["trade_id"] > 0

    def test_rejected_signal_logged(self, temp_db, config):
        from risk.risk_manager import RiskManager
        from orders.order_manager import OrderManager
        from db import get_connection

        # Activate kill switch
        conn = get_connection(temp_db)
        conn.execute(
            "INSERT OR REPLACE INTO system_state (key, value) VALUES ('kill_switch_active', '1')"
        )
        conn.commit()
        conn.close()

        rm = RiskManager(config, temp_db, "backtest")
        om = OrderManager(
            broker_gateway=None,
            risk_manager=rm,
            config=config,
            db_path=temp_db,
            mode="backtest",
        )

        signal = {
            "action": "BUY", "symbol": "RELIANCE", "price": 2500.0,
            "stop_loss": 2450.0, "target": 2600.0, "strategy": "sma_crossover"
        }
        result = om.process_signal(signal)
        assert result is None

        # Check trade was logged as REJECTED
        conn = get_connection(temp_db)
        row = conn.execute(
            "SELECT status FROM trades WHERE status='REJECTED' LIMIT 1"
        ).fetchone()
        conn.close()
        assert row is not None

    def test_slippage_applied_on_buy(self, temp_db, config):
        from orders.order_manager import OrderManager

        class MockRM:
            def evaluate_signal(self, signal, positions):
                s = dict(signal)
                s["quantity"] = 10
                return {"approved": True, "signal": s, "reject_reason": None}

        om = OrderManager(
            broker_gateway=None,
            risk_manager=MockRM(),
            config=config,
            db_path=temp_db,
            mode="backtest",
        )

        signal = {
            "action": "BUY", "symbol": "RELIANCE", "price": 2500.0,
            "stop_loss": 2450.0, "target": 2600.0, "strategy": "sma_crossover",
            "quantity": 10
        }
        result = om.process_signal(signal)
        assert result is not None
        # Fill price should be slightly higher than signal price (slippage)
        assert result["fill_price"] >= 2500.0
