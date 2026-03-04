"""Unit tests for risk manager."""
import os
import tempfile

import pytest


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from db import init_db
    init_db(db_path)
    yield db_path

    os.unlink(db_path)


@pytest.fixture
def base_config():
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
            "trading_start": "00:00",  # Always open for tests
            "trading_end": "23:59",
        },
    }


class TestRiskManager:
    def test_evaluate_signal_approved(self, temp_db, base_config):
        from risk.risk_manager import RiskManager
        rm = RiskManager(base_config, temp_db, "backtest")
        signal = {
            "action": "BUY", "symbol": "RELIANCE", "price": 2500.0,
            "stop_loss": 2450.0, "target": 2600.0, "strategy": "sma_crossover"
        }
        result = rm.evaluate_signal(signal, [])
        assert result["approved"] is True
        assert result["signal"]["quantity"] >= 1

    def test_evaluate_signal_rejected_kill_switch(self, temp_db, base_config):
        from risk.risk_manager import RiskManager
        from db import get_connection
        conn = get_connection(temp_db)
        conn.execute(
            "INSERT OR REPLACE INTO system_state (key, value) VALUES ('kill_switch_active', '1')"
        )
        conn.commit()
        conn.close()

        rm = RiskManager(base_config, temp_db, "backtest")
        signal = {
            "action": "BUY", "symbol": "RELIANCE", "price": 2500.0,
            "stop_loss": 2450.0, "target": 2600.0, "strategy": "sma_crossover"
        }
        result = rm.evaluate_signal(signal, [])
        assert result["approved"] is False
        assert result["reject_reason"] == "KILL_SWITCH_ACTIVE"

    def test_evaluate_signal_rejected_max_positions(self, temp_db, base_config):
        from risk.risk_manager import RiskManager
        rm = RiskManager(base_config, temp_db, "backtest")
        signal = {
            "action": "BUY", "symbol": "TCS", "price": 3500.0,
            "stop_loss": 3450.0, "target": 3600.0, "strategy": "sma_crossover"
        }
        # Create 5 open positions
        open_positions = [
            {"symbol": f"SYM{i}", "strategy": "sma_crossover", "status": "OPEN",
             "entry_price": 1000, "quantity": 1}
            for i in range(5)
        ]
        result = rm.evaluate_signal(signal, open_positions)
        assert result["approved"] is False
        assert "MAX_POSITIONS" in result["reject_reason"]

    def test_calculate_position_size(self, temp_db, base_config):
        from risk.risk_manager import RiskManager
        rm = RiskManager(base_config, temp_db, "backtest")
        # Risk = 2% of 100000 = 2000. Risk per share = |2500 - 2450| = 50
        # qty from risk = 2000 / 50 = 40
        # But capped by max_per_stock_pct: 20% of 100000 / 2500 = 8
        qty = rm.calculate_position_size(2500.0, 2450.0)
        assert qty == 8

    def test_calculate_position_size_minimum_1(self, temp_db, base_config):
        from risk.risk_manager import RiskManager
        rm = RiskManager(base_config, temp_db, "backtest")
        qty = rm.calculate_position_size(2500.0, 2499.0)
        assert qty >= 1

    def test_consecutive_loss_kill_switch(self, temp_db, base_config):
        from risk.risk_manager import RiskManager
        rm = RiskManager(base_config, temp_db, "backtest")

        for _ in range(5):
            rm.record_trade_result(-1000.0)

        assert rm.kill_switch_active is True

    def test_consecutive_loss_reset_on_win(self, temp_db, base_config):
        from risk.risk_manager import RiskManager
        rm = RiskManager(base_config, temp_db, "backtest")

        rm.record_trade_result(-500.0)
        rm.record_trade_result(-500.0)
        rm.record_trade_result(1000.0)  # Win resets consecutive losses

        assert rm._consecutive_losses == 0

    def test_reset_kill_switch(self, temp_db, base_config):
        from risk.risk_manager import RiskManager
        rm = RiskManager(base_config, temp_db, "backtest")

        for _ in range(5):
            rm.record_trade_result(-1000.0)

        assert rm.kill_switch_active is True
        rm.reset_kill_switch()
        assert rm.kill_switch_active is False

    def test_duplicate_position_rejected(self, temp_db, base_config):
        from risk.risk_manager import RiskManager
        rm = RiskManager(base_config, temp_db, "backtest")
        signal = {
            "action": "BUY", "symbol": "RELIANCE", "price": 2500.0,
            "stop_loss": 2450.0, "target": 2600.0, "strategy": "sma_crossover"
        }
        existing_position = {
            "symbol": "RELIANCE", "strategy": "sma_crossover",
            "status": "OPEN", "entry_price": 2480, "quantity": 10
        }
        result = rm.evaluate_signal(signal, [existing_position])
        assert result["approved"] is False
        assert "DUPLICATE" in result["reject_reason"]
