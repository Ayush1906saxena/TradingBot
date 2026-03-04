"""Real-time WebSocket tick data and candle aggregation."""
import logging
import threading
from datetime import datetime

import pandas as pd
import pytz

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")


def get_candle_bucket(timestamp: datetime, timeframe_minutes: int) -> datetime:
    """
    tick at 10:17:34 with 15min tf → 10:15:00
    tick at 10:30:01 with 15min tf → 10:30:00
    """
    minutes_since_midnight = timestamp.hour * 60 + timestamp.minute
    bucket_minutes = (minutes_since_midnight // timeframe_minutes) * timeframe_minutes
    return timestamp.replace(
        hour=bucket_minutes // 60,
        minute=bucket_minutes % 60,
        second=0, microsecond=0
    )


def timeframe_to_minutes(timeframe: str) -> int:
    """Convert "1min"→1, "5min"→5, "15min"→15, "1h"→60, "1d"→375"""
    mapping = {"1min": 1, "5min": 5, "15min": 15, "1h": 60, "1d": 375}
    return mapping.get(timeframe, 15)


class LiveDataStream:
    """Connects to Dhan WebSocket for real-time tick data and aggregates into candles."""

    def __init__(self, config: dict, instrument_manager):
        self.config = config
        self.instrument_manager = instrument_manager

        broker_cfg = config["broker"]["dhan"]
        self.client_id = broker_cfg.get("client_id", "")
        self.access_token = broker_cfg.get("access_token", "")

        # Collect all unique timeframes from enabled strategies
        self._active_timeframes = set()
        for s_cfg in config.get("strategies", {}).values():
            if s_cfg.get("enabled", False):
                self._active_timeframes.add(s_cfg.get("timeframe", "15min"))

        self._timeframe_minutes = {
            tf: timeframe_to_minutes(tf) for tf in self._active_timeframes
        }

        # candle history per symbol per timeframe
        self._candles: dict = {}
        # current in-progress candle
        self._current_candle: dict = {}
        # last LTP per symbol
        self._last_ltp: dict = {}

        self._callbacks: list = []
        self._feed = None
        self._connected = False
        self._lock = threading.Lock()

    def connect(self) -> None:
        """Connect to Dhan WebSocket in a daemon thread."""
        if not self.client_id or not self.access_token:
            logger.error("Dhan credentials not set — cannot connect to live data stream")
            return

        try:
            from dhanhq import marketfeed

            # Build instrument list for initial subscription
            symbols = self._get_all_symbols()
            instruments = []
            for sym in symbols:
                try:
                    sec_id = self.instrument_manager.get_security_id(sym)
                    instruments.append((marketfeed.NSE, sec_id, marketfeed.Quote))
                except Exception as e:
                    logger.warning(f"Could not get security ID for {sym}: {e}")

            if not instruments:
                logger.error("No instruments to subscribe to")
                return

            self._feed = marketfeed.DhanFeed(
                client_id=self.client_id,
                access_token=self.access_token,
                instruments=instruments,
                on_ticks=self._on_raw_tick,
                on_close=self._on_disconnect,
            )

            thread = threading.Thread(target=self._run_feed, daemon=True)
            thread.start()
            self._connected = True
            logger.info(f"WebSocket data stream connecting for {len(symbols)} symbols...")

        except ImportError:
            logger.error("dhanhq not installed. Run: pip install dhanhq")
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")

    def _run_feed(self) -> None:
        """Run feed with auto-reconnect."""
        import time
        while True:
            try:
                if self._feed:
                    self._feed.run_forever()
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
            logger.info("WebSocket disconnected. Reconnecting in 5 seconds...")
            time.sleep(5)
            if not self._connected:
                break

    def subscribe(self, symbols: list) -> None:
        """Subscribe to additional symbols."""
        if not self._feed:
            return
        try:
            from dhanhq import marketfeed
            instruments = []
            for sym in symbols:
                try:
                    sec_id = self.instrument_manager.get_security_id(sym)
                    instruments.append((marketfeed.NSE, sec_id, marketfeed.Quote))
                except Exception as e:
                    logger.warning(f"Could not subscribe to {sym}: {e}")
            if instruments:
                self._feed.subscribe(instruments)
        except Exception as e:
            logger.error(f"Subscribe failed: {e}")

    def _on_raw_tick(self, tick: dict) -> None:
        """Internal callback from WebSocket."""
        try:
            security_id = tick.get("security_id") or tick.get("securityId")
            ltp = tick.get("LTP") or tick.get("last_price")

            if not security_id or not ltp:
                return

            symbol = self.instrument_manager.get_symbol(str(security_id))
            ltp = float(ltp)
            ltq = float(tick.get("LTQ", tick.get("last_quantity", 1)) or 1)

            with self._lock:
                self._last_ltp[symbol] = ltp

                # Initialize data structures if needed
                if symbol not in self._candles:
                    self._candles[symbol] = {tf: pd.DataFrame(
                        columns=["timestamp", "open", "high", "low", "close", "volume"]
                    ) for tf in self._active_timeframes}
                    self._current_candle[symbol] = {}

                current_time = datetime.now(IST)

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
                            # Keep last 500 candles in memory
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
                            "volume": ltq,
                        }
                    else:
                        # Update current candle
                        curr["high"] = max(curr["high"], ltp)
                        curr["low"] = min(curr["low"], ltp)
                        curr["close"] = ltp
                        curr["volume"] += ltq

        except Exception as e:
            logger.error(f"Tick processing error: {e}")

    def _on_disconnect(self, *args) -> None:
        logger.warning("WebSocket connection closed")
        self._connected = False

    def register_candle_callback(self, callback) -> None:
        """Register function called on candle close. Signature: callback(symbol, timeframe, candle_series)"""
        self._callbacks.append(callback)

    def get_latest_candle(self, symbol: str, timeframe: str) -> pd.Series | None:
        """Return the most recently completed candle."""
        with self._lock:
            df = self._candles.get(symbol, {}).get(timeframe)
            if df is None or df.empty:
                return None
            return df.iloc[-1]

    def get_candle_history(self, symbol: str, timeframe: str, n_candles: int) -> pd.DataFrame:
        """Return last N completed candles."""
        with self._lock:
            df = self._candles.get(symbol, {}).get(timeframe, pd.DataFrame())
            if df.empty:
                return df
            return df.tail(n_candles).copy()

    def get_ltp(self, symbol: str) -> float | None:
        """Return last traded price."""
        return self._last_ltp.get(symbol)

    def disconnect(self) -> None:
        self._connected = False
        if self._feed:
            try:
                self._feed.disconnect()
            except Exception:
                pass
        logger.info("Live data stream disconnected")

    def _get_all_symbols(self) -> list:
        symbols = set()
        for s_cfg in self.config.get("strategies", {}).values():
            if s_cfg.get("enabled", False):
                symbols.update(s_cfg.get("symbols", []))
        return sorted(symbols)

    @property
    def is_connected(self) -> bool:
        return self._connected
