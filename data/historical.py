"""Historical OHLCV data fetcher using yfinance (daily) and Dhan API (minute)."""
import logging
import sqlite3
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

from db import get_connection

logger = logging.getLogger(__name__)


class HistoricalDataFetcher:
    """Downloads and stores historical OHLCV data in SQLite."""

    def __init__(self, db_path: str, config: dict):
        self.db_path = db_path
        self.config = config
        self._dhan = None

    def _get_dhan(self):
        if self._dhan is None:
            broker_cfg = self.config["broker"]["dhan"]
            client_id = broker_cfg.get("client_id", "")
            access_token = broker_cfg.get("access_token", "")
            if client_id and access_token:
                from dhanhq import DhanHQ
                self._dhan = DhanHQ(client_id=client_id, access_token=access_token)
        return self._dhan

    def download_daily(self, symbols: list, start_date: str, end_date: str) -> None:
        """
        Download daily OHLCV data using yfinance.
        Converts "RELIANCE" → "RELIANCE.NS" for yfinance, stores as "RELIANCE" in DB.
        """
        conn = get_connection(self.db_path)
        try:
            for symbol in symbols:
                yf_symbol = f"{symbol}.NS"
                logger.info(f"Downloading daily data for {symbol} ({start_date} to {end_date})")
                try:
                    ticker = yf.Ticker(yf_symbol)
                    df = ticker.history(start=start_date, end=end_date, interval="1d")
                    if df.empty:
                        logger.warning(f"No daily data returned for {symbol}")
                        continue

                    df = df.reset_index()
                    df.columns = [c.lower() for c in df.columns]
                    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
                    df["symbol"] = symbol

                    rows = df[["symbol", "date", "open", "high", "low", "close", "volume"]].values.tolist()
                    conn.executemany(
                        "INSERT OR IGNORE INTO daily_ohlcv (symbol, date, open, high, low, close, volume) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        rows
                    )
                    conn.commit()
                    logger.info(f"Stored {len(rows)} daily candles for {symbol}")
                except Exception as e:
                    logger.error(f"Failed to download daily data for {symbol}: {e}")
        finally:
            conn.close()

    def download_minute(self, symbols: list, timeframe: str, days_back: int = 60) -> None:
        """
        Download minute-level data using Dhan API.
        Resamples to requested timeframe and stores in minute_ohlcv table.
        """
        dhan = self._get_dhan()
        if dhan is None:
            logger.warning("Dhan credentials not set — skipping minute data download")
            return

        from data.instruments import InstrumentManager
        instrument_mgr = InstrumentManager(self.config)

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        conn = get_connection(self.db_path)
        try:
            for symbol in symbols:
                try:
                    security_id = instrument_mgr.get_security_id(symbol)
                    logger.info(f"Downloading minute data for {symbol} (security_id={security_id})")

                    response = dhan.historical_minute_charts(
                        security_id=security_id,
                        exchange_segment="NSE_EQ",
                        instrument_type="EQUITY",
                        from_date=start_date.strftime("%Y-%m-%d"),
                        to_date=end_date.strftime("%Y-%m-%d")
                    )

                    if not response or "data" not in response:
                        logger.warning(f"No minute data for {symbol}")
                        continue

                    data = response["data"]
                    df = pd.DataFrame({
                        "timestamp": pd.to_datetime(data["timestamp"]),
                        "open": data["open"],
                        "high": data["high"],
                        "low": data["low"],
                        "close": data["close"],
                        "volume": data["volume"],
                    })
                    df = df.set_index("timestamp").sort_index()

                    resampled = self._resample(df, timeframe)
                    resampled["symbol"] = symbol
                    resampled["timeframe"] = timeframe
                    resampled = resampled.reset_index()
                    resampled["timestamp"] = resampled["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

                    rows = resampled[["symbol", "timestamp", "timeframe", "open", "high", "low", "close", "volume"]].values.tolist()
                    conn.executemany(
                        "INSERT OR IGNORE INTO minute_ohlcv "
                        "(symbol, timestamp, timeframe, open, high, low, close, volume) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        rows
                    )
                    conn.commit()
                    logger.info(f"Stored {len(rows)} {timeframe} candles for {symbol}")
                except Exception as e:
                    logger.error(f"Failed to download minute data for {symbol}: {e}")
        finally:
            conn.close()

    def _resample(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """Resample 1-minute OHLCV DataFrame to a higher timeframe."""
        tf_map = {"1min": "1min", "5min": "5min", "15min": "15min", "1h": "1h", "1d": "1D"}
        rule = tf_map.get(timeframe, "15min")
        resampled = df.resample(rule, origin="start_day").agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum"
        }).dropna()
        return resampled

    def update_daily(self) -> None:
        """Called daily at 4:00 PM IST. Downloads today's daily candle for all watchlist symbols."""
        symbols = self._get_all_symbols()
        today = datetime.now().strftime("%Y-%m-%d")
        self.download_daily(symbols, today, today)

    def get_daily_df(self, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """Read daily OHLCV from SQLite into a pandas DataFrame."""
        conn = get_connection(self.db_path)
        try:
            query = "SELECT date, open, high, low, close, volume FROM daily_ohlcv WHERE symbol = ?"
            params = [symbol]
            if start_date:
                query += " AND date >= ?"
                params.append(start_date)
            if end_date:
                query += " AND date <= ?"
                params.append(end_date)
            query += " ORDER BY date ASC"
            df = pd.read_sql_query(query, conn, params=params)
            if not df.empty:
                df["date"] = pd.to_datetime(df["date"])
            return df
        finally:
            conn.close()

    def get_minute_df(self, symbol: str, timeframe: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """Read minute OHLCV from SQLite into a pandas DataFrame."""
        conn = get_connection(self.db_path)
        try:
            query = (
                "SELECT timestamp, open, high, low, close, volume FROM minute_ohlcv "
                "WHERE symbol = ? AND timeframe = ?"
            )
            params = [symbol, timeframe]
            if start_date:
                query += " AND timestamp >= ?"
                params.append(start_date)
            if end_date:
                query += " AND timestamp <= ?"
                params.append(end_date)
            query += " ORDER BY timestamp ASC"
            df = pd.read_sql_query(query, conn, params=params)
            if not df.empty:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            return df
        finally:
            conn.close()

    def _get_all_symbols(self) -> list:
        symbols = set()
        for s_cfg in self.config.get("strategies", {}).values():
            if s_cfg.get("enabled", False):
                symbols.update(s_cfg.get("symbols", []))
        return sorted(symbols)
