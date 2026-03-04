"""Free polling-based market data feed using Dhan's Market Quote API (no ₹499 Data API needed).

Polls LTP every second via the free Trading API's Market Quote endpoint,
builds OHLCV candles in-memory, and fires callbacks on candle close —
drop-in replacement for the WebSocket-based LiveDataStream.
"""
import logging
import threading
import time
from datetime import datetime

import pandas as pd
import pytz

from data.live_stream import get_candle_bucket, timeframe_to_minutes

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")


class PollingDataStream:
    """Polls Dhan Market Quote API for LTP and builds candles — 100% free."""

    def __init__(self, config: dict, instrument_manager):
        self.config = config
        self.instrument_manager = instrument_manager

        broker_cfg = config["broker"]["dhan"]
        self.client_id = broker_cfg.get("client_id", "")
        self.access_token = broker_cfg.get("access_token", "")

        self._poll_interval = config.get("polling", {}).get("interval_seconds", 1)

        # Collect all unique timeframes from enabled strategies
        self._active_timeframes = set()
        for s_cfg in config.get("strategies", {}).values():
            if s_cfg.get("enabled", False):
                self._active_timeframes.add(s_cfg.get("timeframe", "15min"))

        self._timeframe_minutes = {
            tf: timeframe_to_minutes(tf) for tf in self._active_timeframes
        }

        # symbol → security_id mapping for all subscribed symbols
        self._symbols: list[str] = []
        self._security_ids: dict[str, int] = {}  # symbol → security_id
        self._id_to_symbol: dict[int, str] = {}  # security_id → symbol

        # candle history per symbol per timeframe
        self._candles: dict = {}
        # current in-progress candle
        self._current_candle: dict = {}
        # last LTP per symbol
        self._last_ltp: dict = {}

        self._callbacks: list = []
        self._connected = False
        self._poll_thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._dhan = None

    def connect(self) -> None:
        """Start polling in a background thread."""
        if not self.client_id or not self.access_token:
            logger.error("Dhan credentials not set — cannot start polling data stream")
            return

        try:
            from dhanhq import dhanhq
            self._dhan = dhanhq(self.client_id, self.access_token)
        except ImportError:
            logger.error("dhanhq not installed. Run: pip install dhanhq")
            return
        except Exception as e:
            logger.error(f"Failed to initialize Dhan client: {e}")
            return

        # Build symbol → security_id mappings
        self._symbols = self._get_all_symbols()
        for sym in self._symbols:
            try:
                sec_id = int(self.instrument_manager.get_security_id(sym))
                self._security_ids[sym] = sec_id
                self._id_to_symbol[sec_id] = sym
            except Exception as e:
                logger.warning(f"Could not get security ID for {sym}: {e}")

        if not self._security_ids:
            logger.error("No instruments resolved — cannot start polling")
            return

        self._connected = True
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()
        logger.info(
            f"Polling data stream started for {len(self._security_ids)} symbols "
            f"(every {self._poll_interval}s)"
        )

    def _poll_loop(self) -> None:
        """Main polling loop — runs in background thread."""
        sec_ids = list(self._security_ids.values())
        securities = {"NSE_EQ": sec_ids}

        while self._connected:
            try:
                response = self._dhan.ticker_data(securities=securities)

                if response and response.get("status") == "success":
                    data = response.get("data", {}).get("NSE_EQ", {})
                    current_time = datetime.now(IST)

                    for sec_id_str, quote in data.items():
                        sec_id = int(sec_id_str)
                        symbol = self._id_to_symbol.get(sec_id)
                        if not symbol:
                            continue

                        ltp = quote.get("last_price")
                        if ltp is None:
                            continue
                        ltp = float(ltp)

                        self._process_tick(symbol, ltp, current_time)
                else:
                    status = response.get("status") if response else "no response"
                    remarks = response.get("remarks", "") if response else ""
                    if remarks:
                        logger.debug(f"Quote API: {status} — {remarks}")

            except Exception as e:
                logger.error(f"Polling error: {e}")

            time.sleep(self._poll_interval)

    def _process_tick(self, symbol: str, ltp: float, current_time: datetime) -> None:
        """Process a single LTP update — same candle-building logic as WebSocket version."""
        with self._lock:
            self._last_ltp[symbol] = ltp

            if symbol not in self._candles:
                self._candles[symbol] = {
                    tf: pd.DataFrame(
                        columns=["timestamp", "open", "high", "low", "close", "volume"]
                    ) for tf in self._active_timeframes
                }
                self._current_candle[symbol] = {}

            for tf, tf_minutes in self._timeframe_minutes.items():
                bucket = get_candle_bucket(current_time, tf_minutes)
                curr = self._current_candle[symbol].get(tf)

                if curr is None or curr.get("bucket") != bucket:
                    # Finalize previous candle
                    if curr is not None:
                        candle_row = {
                            "timestamp": curr["bucket"],
                            "open": curr["open"], "high": curr["high"],
                            "low": curr["low"], "close": curr["close"],
                            "volume": curr["volume"],
                        }
                        new_row = pd.DataFrame([candle_row])
                        self._candles[symbol][tf] = pd.concat(
                            [self._candles[symbol][tf], new_row], ignore_index=True
                        )
                        self._candles[symbol][tf] = self._candles[symbol][tf].tail(500)

                        # Fire callbacks
                        candle_series = pd.Series(candle_row)
                        for cb in self._callbacks:
                            try:
                                cb(symbol, tf, candle_series)
                            except Exception as e:
                                logger.error(f"Callback error: {e}")

                    # Start new candle
                    self._current_candle[symbol][tf] = {
                        "bucket": bucket,
                        "open": ltp, "high": ltp, "low": ltp, "close": ltp,
                        "volume": 0,  # No volume from LTP endpoint
                    }
                else:
                    # Update current candle
                    curr["high"] = max(curr["high"], ltp)
                    curr["low"] = min(curr["low"], ltp)
                    curr["close"] = ltp

    def register_candle_callback(self, callback) -> None:
        """Register function called on candle close. Signature: callback(symbol, timeframe, candle_series)"""
        self._callbacks.append(callback)

    def get_latest_candle(self, symbol: str, timeframe: str) -> pd.Series | None:
        with self._lock:
            df = self._candles.get(symbol, {}).get(timeframe)
            if df is None or df.empty:
                return None
            return df.iloc[-1]

    def get_candle_history(self, symbol: str, timeframe: str, n_candles: int) -> pd.DataFrame:
        with self._lock:
            df = self._candles.get(symbol, {}).get(timeframe, pd.DataFrame())
            if df.empty:
                return df
            return df.tail(n_candles).copy()

    def get_ltp(self, symbol: str) -> float | None:
        return self._last_ltp.get(symbol)

    def subscribe(self, symbols: list) -> None:
        """Add more symbols to polling."""
        for sym in symbols:
            if sym not in self._security_ids:
                try:
                    sec_id = int(self.instrument_manager.get_security_id(sym))
                    self._security_ids[sym] = sec_id
                    self._id_to_symbol[sec_id] = sym
                    self._symbols.append(sym)
                except Exception as e:
                    logger.warning(f"Could not subscribe to {sym}: {e}")

    def disconnect(self) -> None:
        self._connected = False
        logger.info("Polling data stream stopped")

    @property
    def is_connected(self) -> bool:
        return self._connected

    def warmup_from_daily(self, db_path: str) -> None:
        """Pre-load candle history from daily OHLCV in DB so strategies have enough
        bars to compute indicators from the very first candle close."""
        from data.historical import HistoricalDataFetcher
        fetcher = HistoricalDataFetcher(db_path, self.config)

        for symbol in self._symbols:
            df = fetcher.get_daily_df(symbol)
            if df.empty:
                continue
            # Use last 200 daily candles as warmup history for each active timeframe
            df = df.tail(200).copy()
            df = df.rename(columns={"date": "timestamp"})

            with self._lock:
                if symbol not in self._candles:
                    self._candles[symbol] = {}
                for tf in self._active_timeframes:
                    self._candles[symbol][tf] = df[
                        ["timestamp", "open", "high", "low", "close", "volume"]
                    ].reset_index(drop=True)

            logger.info(f"Warmup: loaded {len(df)} daily candles for {symbol}")

    def _get_all_symbols(self) -> list:
        symbols = set()
        for s_cfg in self.config.get("strategies", {}).values():
            if s_cfg.get("enabled", False):
                symbols.update(s_cfg.get("symbols", []))
        return sorted(symbols)
