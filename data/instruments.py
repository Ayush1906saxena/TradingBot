"""Symbol to security ID mapping using Dhan's instrument master CSV."""
import json
import logging
import os
from datetime import date

import requests

logger = logging.getLogger(__name__)

INSTRUMENT_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
CACHE_FILE = "data/instruments_cache.json"


class InstrumentManager:
    """Maps human-readable symbol names to broker-specific security IDs."""

    def __init__(self, config: dict):
        self.config = config
        self._instruments: dict = {}
        self._load()

    def _load(self) -> None:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "r") as f:
                data = json.load(f)
            cache_date = data.get("_cache_date", "")
            if cache_date == date.today().isoformat():
                self._instruments = data.get("instruments", {})
                logger.debug(f"Loaded {len(self._instruments)} instruments from cache")
                return
        self.refresh()

    def refresh(self) -> None:
        """Re-download instrument master CSV."""
        try:
            import pandas as pd

            logger.info("Downloading Dhan instrument master CSV...")
            resp = requests.get(INSTRUMENT_MASTER_URL, timeout=30)
            resp.raise_for_status()

            from io import StringIO
            df = pd.read_csv(StringIO(resp.text), low_memory=False)

            # Filter NSE equity instruments
            nse_eq = df[
                (df["SEM_EXM_EXCH_ID"] == "NSE") &
                (df["SEM_INSTRUMENT_NAME"] == "EQUITY")
            ].copy()

            instruments = {}
            for _, row in nse_eq.iterrows():
                symbol = str(row["SEM_TRADING_SYMBOL"]).strip()
                instruments[symbol] = {
                    "security_id": str(int(row["SEM_SMST_SECURITY_ID"])),
                    "lot_size": int(row.get("SEM_LOT_UNITS", 1) or 1),
                    "tick_size": float(row.get("SEM_TICK_SIZE", 0.05) or 0.05),
                }

            self._instruments = instruments
            os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
            with open(CACHE_FILE, "w") as f:
                json.dump({"_cache_date": date.today().isoformat(), "instruments": instruments}, f)

            logger.info(f"Instrument master loaded: {len(instruments)} NSE equities")

        except Exception as e:
            logger.error(f"Failed to download instrument master: {e}")
            if not self._instruments:
                # Fallback: hardcoded common securities
                self._instruments = {
                    "RELIANCE":  {"security_id": "2885",  "lot_size": 1, "tick_size": 0.05},
                    "TCS":       {"security_id": "11536", "lot_size": 1, "tick_size": 0.05},
                    "HDFCBANK":  {"security_id": "1333",  "lot_size": 1, "tick_size": 0.05},
                    "INFY":      {"security_id": "10999", "lot_size": 1, "tick_size": 0.05},
                    "ICICIBANK": {"security_id": "4963",  "lot_size": 1, "tick_size": 0.05},
                    "NIFTY":     {"security_id": "13",    "lot_size": 50, "tick_size": 0.05},
                }
                logger.warning("Using hardcoded fallback instrument IDs")

    def get_security_id(self, symbol: str) -> str:
        info = self._instruments.get(symbol)
        if not info:
            raise ValueError(f"Security ID not found for symbol: {symbol}")
        return info["security_id"]

    def get_symbol(self, security_id: str) -> str:
        for sym, info in self._instruments.items():
            if info["security_id"] == str(security_id):
                return sym
        return str(security_id)

    def get_lot_size(self, symbol: str) -> int:
        return self._instruments.get(symbol, {}).get("lot_size", 1)

    def get_tick_size(self, symbol: str) -> float:
        return self._instruments.get(symbol, {}).get("tick_size", 0.05)
