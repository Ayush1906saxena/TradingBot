"""
Algo Trading System — Entry Point

Usage:
    python main.py --mode backtest
    python main.py --mode paper
    python main.py --mode live
    python main.py --mode dashboard
    python main.py --mode backtest --reset-kill-switch
    python main.py --mode paper --reset-virtual-wallet
    python main.py --mode paper --virtual-cash 200000
"""
import argparse
import os
import subprocess
import sys

import yaml
from dotenv import load_dotenv

from db import init_db, get_connection, DB_PATH
from utils.logger import setup_logging, get_logger


def load_config(config_path: str = "configs/config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def apply_env_overrides(config: dict) -> dict:
    """Override config values with .env secrets if present."""
    load_dotenv()

    def _set(keys: list, env_var: str):
        val = os.getenv(env_var)
        if val:
            d = config
            for k in keys[:-1]:
                d = d.setdefault(k, {})
            d[keys[-1]] = val

    _set(["broker", "dhan", "client_id"], "DHAN_CLIENT_ID")
    _set(["broker", "dhan", "access_token"], "DHAN_ACCESS_TOKEN")
    _set(["broker", "zerodha", "api_key"], "ZERODHA_API_KEY")
    _set(["broker", "zerodha", "api_secret"], "ZERODHA_API_SECRET")
    _set(["broker", "zerodha", "totp_secret"], "ZERODHA_TOTP_SECRET")
    _set(["telegram", "bot_token"], "TELEGRAM_BOT_TOKEN")
    _set(["telegram", "chat_id"], "TELEGRAM_CHAT_ID")
    return config


def reset_kill_switch(db_path: str) -> None:
    conn = get_connection(db_path)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO system_state (key, value) VALUES ('kill_switch_active', '0')"
        )
        conn.execute(
            "INSERT OR REPLACE INTO system_state (key, value) VALUES ('consecutive_losses', '0')"
        )
        conn.commit()
        print("Kill switch reset.")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Algo Trading System")
    parser.add_argument(
        "--mode", required=True,
        choices=["backtest", "paper", "live", "dashboard"]
    )
    parser.add_argument(
        "--reset-kill-switch", action="store_true",
        help="Reset kill switch before starting"
    )
    parser.add_argument(
        "--reset-virtual-wallet", action="store_true",
        help="Reset virtual wallet to initial amount"
    )
    parser.add_argument(
        "--virtual-cash", type=float, default=None,
        help="Override initial virtual cash (e.g., --virtual-cash 200000)"
    )
    parser.add_argument(
        "--config", default="configs/config.yaml",
        help="Path to config file"
    )
    args = parser.parse_args()

    # 1. Load config
    config = load_config(args.config)

    # 2. Apply .env overrides
    config = apply_env_overrides(config)

    # 3. Initialize logging
    setup_logging(config)
    logger = get_logger("main")
    logger.info(f"Starting Algo Trading System | Mode: {args.mode.upper()}")

    # 4. Initialize database
    db_path = DB_PATH
    init_db(db_path)
    logger.info(f"Database initialized at {db_path}")

    # 5. Reset kill switch if requested
    if args.reset_kill_switch:
        reset_kill_switch(db_path)

    # 6. Override virtual cash if provided
    if args.virtual_cash is not None:
        config["paper_trading"]["initial_virtual_cash"] = args.virtual_cash
        logger.info(f"Virtual cash overridden to ₹{args.virtual_cash:,.0f}")

    # 7-8. Execute based on mode
    if args.mode == "backtest":
        _run_backtest(config, db_path, logger)

    elif args.mode == "paper":
        _run_paper(config, db_path, args.reset_virtual_wallet, logger)

    elif args.mode == "live":
        _run_live(config, db_path, logger)

    elif args.mode == "dashboard":
        _run_dashboard(config, logger)


def _run_backtest(config: dict, db_path: str, logger) -> None:
    from data.historical import HistoricalDataFetcher
    from engine.backtester import Backtester

    logger.info("=== BACKTEST MODE ===")

    # Download historical data if needed
    fetcher = HistoricalDataFetcher(db_path, config)
    symbols = _get_all_symbols(config)
    start = config["backtest"]["start_date"]
    end = config["backtest"]["end_date"]

    logger.info(f"Downloading historical daily data for {symbols} ({start} to {end})...")
    fetcher.download_daily(symbols, start, end)

    backtester = Backtester(config, db_path)
    results = backtester.run()

    print("\n" + "=" * 60)
    print("BACKTEST RESULTS")
    print("=" * 60)
    for strategy_name, result in results.items():
        metrics = result.get("metrics", {})
        print(f"\nStrategy: {strategy_name}")
        print(f"  Total Return: {metrics.get('total_return_pct', 0):.2f}% (₹{metrics.get('total_return_inr', 0):,.0f})")
        print(f"  Win Rate:     {metrics.get('win_rate_pct', 0):.1f}%")
        print(f"  Max Drawdown: {metrics.get('max_drawdown_pct', 0):.2f}%")
        print(f"  Sharpe Ratio: {metrics.get('sharpe_ratio', 0):.2f}")
        print(f"  Total Trades: {metrics.get('total_trades', 0)}")
        print(f"  Total Fees:   ₹{metrics.get('total_fees_inr', 0):,.2f}")
        report_path = result.get("report_path")
        if report_path:
            print(f"  Report:       {report_path}")
    print("=" * 60)


def _run_paper(config: dict, db_path: str, reset_wallet: bool, logger) -> None:
    from risk.virtual_portfolio import VirtualPortfolio
    from engine.paper_trader import TradingEngine

    logger.info("=== PAPER MODE (DUMMY MONEY) ===")

    # Check broker credentials (needed for WebSocket data)
    broker_name = config["broker"]["name"]
    broker_cfg = config["broker"][broker_name]
    if not broker_cfg.get("client_id") and not broker_cfg.get("api_key"):
        print(
            "\nERROR: Paper mode requires broker API credentials for live market data.\n"
            f"Please add your {broker_name.upper()} credentials to configs/config.yaml\n"
            "or the .env file. See config.yaml for instructions.\n"
        )
        sys.exit(1)

    virtual_portfolio = VirtualPortfolio(config, db_path)

    if reset_wallet:
        initial = config["paper_trading"]["initial_virtual_cash"]
        virtual_portfolio.reset(initial)
        logger.info(f"Virtual wallet reset to ₹{initial:,.0f}")

    balance = virtual_portfolio.get_cash_balance()
    print(f"\nPaper mode: Virtual wallet loaded. Balance: ₹{balance:,.0f}")

    engine = TradingEngine(mode="paper", config=config, db_path=db_path,
                           virtual_portfolio=virtual_portfolio)
    engine.start()


def _run_live(config: dict, db_path: str, logger) -> None:
    from engine.live_trader import TradingEngine

    logger.info("=== LIVE MODE (REAL MONEY) ===")
    print("\n⚠️  LIVE MODE: Real orders will be placed with real money.")
    print("   Press Ctrl+C within 5 seconds to abort...\n")

    import time
    time.sleep(5)

    engine = TradingEngine(mode="live", config=config, db_path=db_path)
    engine.start()


def _run_dashboard(config: dict, logger) -> None:
    logger.info("Launching Streamlit dashboard...")
    port = config.get("dashboard", {}).get("port", 8501)
    auto_open = config.get("dashboard", {}).get("auto_open_browser", True)

    cmd = [
        sys.executable, "-m", "streamlit", "run",
        "dashboard/app.py",
        f"--server.port={port}",
        f"--server.headless={'false' if auto_open else 'true'}",
        "--server.fileWatcherType=none",
    ]
    logger.info(f"Dashboard URL: http://localhost:{port}")
    proc = subprocess.Popen(cmd)
    try:
        proc.wait()
    except KeyboardInterrupt:
        proc.terminate()


def _get_all_symbols(config: dict) -> list:
    """Collect all unique symbols from all enabled strategies."""
    symbols = set()
    for strategy_cfg in config.get("strategies", {}).values():
        if strategy_cfg.get("enabled", False):
            for s in strategy_cfg.get("symbols", []):
                symbols.add(s)
    return sorted(symbols)


if __name__ == "__main__":
    main()
