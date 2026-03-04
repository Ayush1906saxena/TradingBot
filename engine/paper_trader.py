"""Trading Engine — handles both paper and live trading modes."""
import asyncio
import logging
import time
from datetime import datetime

import pandas as pd
import pytz
from apscheduler.schedulers.background import BackgroundScheduler

from data.instruments import InstrumentManager
from db import get_connection
from monitoring.daily_report import DailyReporter
from monitoring.telegram_bot import TelegramAlert
from orders.order_manager import OrderManager
from risk.risk_manager import RiskManager
from utils.constants import is_trading_day
from utils.helpers import now_ist

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")


class TradingEngine:
    """Unified trading engine for paper and live modes."""

    def __init__(self, mode: str, config: dict, db_path: str,
                 virtual_portfolio=None):
        self.mode = mode
        self.config = config
        self.db_path = db_path
        self.virtual_portfolio = virtual_portfolio

        self._trading_enabled = False
        self._scheduler = BackgroundScheduler(timezone=IST)
        self._strategies = []
        self._live_stream = None
        self._broker_gateway = None

        # Initialize components
        self._instrument_mgr = InstrumentManager(config)
        self._init_telegram()
        self._init_risk_manager()
        self._init_order_manager()
        self._init_strategies()
        self._reporter = DailyReporter(db_path, config)

    def _init_telegram(self):
        tg_cfg = self.config.get("telegram", {})
        self._telegram = TelegramAlert(
            bot_token=tg_cfg.get("bot_token", ""),
            chat_id=tg_cfg.get("chat_id", ""),
            enabled=tg_cfg.get("enabled", False),
        )

    def _init_risk_manager(self):
        self._risk_mgr = RiskManager(
            config=self.config,
            db_path=self.db_path,
            mode=self.mode,
            virtual_portfolio=self.virtual_portfolio if self.mode == "paper" else None,
        )

    def _init_order_manager(self):
        self._order_mgr = OrderManager(
            broker_gateway=None,  # set after broker init for live mode
            risk_manager=self._risk_mgr,
            config=self.config,
            db_path=self.db_path,
            mode=self.mode,
            virtual_portfolio=self.virtual_portfolio if self.mode == "paper" else None,
            telegram_alert=self._telegram,
        )

    def _init_broker(self):
        if self.mode == "live":
            broker_name = self.config["broker"]["name"]
            if broker_name == "dhan":
                from broker.dhan_gateway import DhanGateway
                self._broker_gateway = DhanGateway(self.config, self._instrument_mgr)
            elif broker_name == "zerodha":
                from broker.zerodha_gateway import ZerodhaGateway
                self._broker_gateway = ZerodhaGateway(self.config, self._instrument_mgr)

            connected = self._broker_gateway.connect()
            if not connected:
                raise RuntimeError("Failed to connect to broker")
            self._risk_mgr.set_broker_gateway(self._broker_gateway)
            self._order_mgr.broker = self._broker_gateway
        elif self.mode == "paper":
            # Paper mode: broker for WebSocket data only, not orders
            broker_name = self.config["broker"]["name"]
            if broker_name == "dhan":
                from broker.dhan_gateway import DhanGateway
                self._broker_gateway = DhanGateway(self.config, self._instrument_mgr)
                self._broker_gateway.connect()

    def _init_live_stream(self):
        # Use free polling-based stream (Market Quote API) instead of paid WebSocket
        from data.polling_stream import PollingDataStream
        self._live_stream = PollingDataStream(self.config, self._instrument_mgr)
        self._live_stream.register_candle_callback(self._on_candle_close)

    def _init_strategies(self):
        from strategies.sma_crossover import SMACrossover
        from strategies.rsi_reversal import RSIReversal
        from strategies.ema_rsi_volume import EMARSIVolume
        from strategies.supertrend import SupertrendStrategy
        from strategies.macd_crossover import MACDCrossover
        from strategies.bollinger_bands import BollingerBands
        from strategies.vwap_strategy import VWAPStrategy
        from strategies.donchian_channel import DonchianChannel
        from strategies.stochastic_oscillator import StochasticOscillator
        from strategies.adx_trend import ADXTrend
        from strategies.ichimoku_cloud import IchimokuCloud
        from strategies.mean_reversion_zscore import MeanReversionZScore
        from strategies.momentum_roc import MomentumROC
        from strategies.parabolic_sar import ParabolicSAR

        strategy_map = {
            "sma_crossover": SMACrossover,
            "rsi_reversal": RSIReversal,
            "ema_rsi_volume": EMARSIVolume,
            "supertrend": SupertrendStrategy,
            "macd_crossover": MACDCrossover,
            "bollinger_bands": BollingerBands,
            "vwap_strategy": VWAPStrategy,
            "donchian_channel": DonchianChannel,
            "stochastic_oscillator": StochasticOscillator,
            "adx_trend": ADXTrend,
            "ichimoku_cloud": IchimokuCloud,
            "mean_reversion_zscore": MeanReversionZScore,
            "momentum_roc": MomentumROC,
            "parabolic_sar": ParabolicSAR,
        }

        for name, cls in strategy_map.items():
            cfg = self.config.get("strategies", {}).get(name, {})
            if cfg.get("enabled", False):
                self._strategies.append(cls(cfg))
                logger.info(f"Strategy loaded: {name}")

    def start(self) -> None:
        """Schedule all market day tasks and run."""
        logger.info(f"Trading engine starting in {self.mode.upper()} mode")

        # Schedule daily tasks (IST)
        self._scheduler.add_job(self._pre_market, "cron", hour=8, minute=45, id="pre_market")
        self._scheduler.add_job(self._connect_data_stream, "cron", hour=9, minute=15, id="connect_ws")
        self._scheduler.add_job(self._enable_trading, "cron", hour=9, minute=20, id="enable_trading")
        self._scheduler.add_job(self._disable_trading, "cron", hour=15, minute=15, id="disable_trading")
        self._scheduler.add_job(self._force_exit_intraday, "cron", hour=15, minute=20, id="force_exit")
        self._scheduler.add_job(self._end_of_day, "cron", hour=15, minute=30, id="eod")
        self._scheduler.add_job(self._post_market, "cron", hour=15, minute=35, id="post_market")
        self._scheduler.add_job(self._update_data, "cron", hour=16, minute=0, id="update_data")

        if self.mode == "paper":
            self._scheduler.add_job(
                self._take_periodic_snapshot, "interval", minutes=5, id="snapshot"
            )

        self._scheduler.start()

        # If started outside market hours, check if today is a trading day and wait
        logger.info("Scheduler started. Waiting for market hours...")
        logger.info("Press Ctrl+C to stop.")

        # Run immediately if within market hours
        now = now_ist()
        now_str = now.strftime("%H:%M")
        if is_trading_day(now.date()):
            if "08:45" <= now_str <= "09:14":
                self._pre_market()
            elif "09:15" <= now_str <= "09:19":
                self._pre_market()
                self._connect_data_stream()
            elif "09:20" <= now_str <= "15:14":
                self._pre_market()
                self._connect_data_stream()
                self._enable_trading()

        try:
            while True:
                time.sleep(30)
        except KeyboardInterrupt:
            logger.info("Shutting down trading engine...")
            self._scheduler.shutdown()
            if self._live_stream:
                self._live_stream.disconnect()

    def _pre_market(self) -> None:
        """08:45 — Pre-market setup."""
        now = now_ist()
        if not is_trading_day(now.date()):
            logger.info(f"Today ({now.date()}) is not a trading day. Skipping.")
            return

        logger.info("=== PRE-MARKET: Initializing for today's session ===")
        self._risk_mgr.reset_daily()

        try:
            self._init_broker()
        except Exception as e:
            logger.error(f"Broker init failed: {e}")

        if self.mode == "paper" and self.virtual_portfolio:
            balance = self.virtual_portfolio.get_cash_balance()
            msg = f"Mode: PAPER (dummy money) | Virtual Balance: ₹{balance:,.0f}"
            logger.info(msg)
            self._telegram.send_sync(f"🟢 System starting\n{msg}", "info")
        else:
            self._telegram.send_sync("🟢 LIVE trading system starting", "critical")

    def _connect_data_stream(self) -> None:
        """09:15 — Connect polling data stream."""
        logger.info("Connecting polling data stream...")
        self._init_live_stream()

        # Download latest daily data and warmup candle history
        from data.historical import HistoricalDataFetcher
        fetcher = HistoricalDataFetcher(self.db_path, self.config)
        symbols = list({s for cfg in self.config.get("strategies", {}).values()
                        if cfg.get("enabled") for s in cfg.get("symbols", [])})
        fetcher.download_daily(symbols, "2024-01-01",
                               datetime.now().strftime("%Y-%m-%d"))
        self._live_stream.warmup_from_daily(self.db_path)

        self._live_stream.connect()

    def _enable_trading(self) -> None:
        """09:20 — Enable signal processing."""
        self._trading_enabled = True
        logger.info("Trading ENABLED")

    def _disable_trading(self) -> None:
        """15:15 — Stop new signals."""
        self._trading_enabled = False
        logger.info("Trading DISABLED — no new signals after 15:15")

    def _force_exit_intraday(self) -> None:
        """15:20 — Force close all intraday positions."""
        logger.info("Force closing all intraday positions...")
        self._order_mgr.force_close_all("INTRADAY_FORCE_CLOSE")

    def _end_of_day(self) -> None:
        """15:30 — EOD snapshot and report."""
        if self.mode == "paper" and self.virtual_portfolio:
            open_positions = self._get_open_positions()
            self.virtual_portfolio.take_snapshot(
                open_positions, self._live_stream, "EOD"
            )

        summary = self._reporter.generate_daily_summary(
            self.mode, self.virtual_portfolio
        )
        logger.info(
            f"EOD Summary: Trades={summary['total_trades']} | "
            f"Net P&L=₹{summary['net_pnl']:+,.0f}"
        )
        asyncio.run(self._telegram.send_daily_summary(summary))

    def _post_market(self) -> None:
        """15:35 — Disconnect."""
        if self._live_stream:
            self._live_stream.disconnect()
        if self._broker_gateway:
            self._broker_gateway.disconnect()
        logger.info("Market session ended. Connections closed.")

    def _update_data(self) -> None:
        """16:00 — Update historical data."""
        from data.historical import HistoricalDataFetcher
        fetcher = HistoricalDataFetcher(self.db_path, self.config)
        fetcher.update_daily()

    def _take_periodic_snapshot(self) -> None:
        """Every 5 min — Paper mode portfolio snapshot."""
        if self.mode == "paper" and self.virtual_portfolio and self._live_stream:
            open_positions = self._get_open_positions()
            self.virtual_portfolio.take_snapshot(
                open_positions, self._live_stream, "PERIODIC"
            )

    def _on_candle_close(self, symbol: str, timeframe: str, candle: pd.Series) -> None:
        """HEARTBEAT — fires on every candle close."""
        try:
            for strategy in self._strategies:
                if symbol not in strategy.symbols:
                    continue
                if strategy.timeframe != timeframe:
                    continue

                # Get candle history
                history = self._live_stream.get_candle_history(symbol, timeframe, 200)
                if history.empty:
                    continue

                # Check open position exit first
                position = self._get_open_position(symbol, strategy.name)
                if position:
                    current_price = self._live_stream.get_ltp(symbol) or float(candle["close"])
                    exit_signal = strategy.should_exit(position, current_price, history)
                    if exit_signal:
                        self._order_mgr.close_position(position, exit_signal["reason"], current_price)
                        if self.mode == "paper" and self.virtual_portfolio:
                            open_positions = self._get_open_positions()
                            self.virtual_portfolio.take_snapshot(
                                open_positions, self._live_stream, "TRADE"
                            )
                        continue

                # Generate new entry signal
                if self._trading_enabled and not position:
                    signal = strategy.generate_signal(history, symbol)
                    if signal:
                        result = self._order_mgr.process_signal(signal)
                        if result and self.mode == "paper" and self.virtual_portfolio:
                            open_positions = self._get_open_positions()
                            self.virtual_portfolio.take_snapshot(
                                open_positions, self._live_stream, "TRADE"
                            )

            # Daily loss check
            self._check_daily_loss()

        except Exception as e:
            logger.error(f"on_candle_close error for {symbol}/{timeframe}: {e}")

    def _check_daily_loss(self) -> None:
        if self._risk_mgr.check_daily_loss():
            logger.warning("Daily loss limit hit — force closing all positions")
            self._order_mgr.force_close_all("DAILY_LOSS_LIMIT")
            self._trading_enabled = False

    def _get_open_positions(self) -> list:
        conn = get_connection(self.db_path)
        try:
            rows = conn.execute(
                "SELECT * FROM positions WHERE status='OPEN'"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def _get_open_position(self, symbol: str, strategy: str) -> dict | None:
        conn = get_connection(self.db_path)
        try:
            row = conn.execute(
                "SELECT * FROM positions WHERE symbol=? AND strategy=? AND status='OPEN'",
                (symbol, strategy)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
