from datetime import date

MARKET_OPEN = "09:15"
MARKET_CLOSE = "15:30"
TIMEZONE = "Asia/Kolkata"

NSE_HOLIDAYS = [
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10", "2025-04-14",
    "2025-04-18", "2025-05-01", "2025-08-15", "2025-08-27", "2025-10-02",
    "2025-10-21", "2025-10-22", "2025-11-05", "2025-12-25", "2026-01-26", "2026-03-17",
]

BROKERAGE_FLAT = 20
BROKERAGE_PCT = 0.0003
STT_DELIVERY_PCT = 0.001
STT_INTRADAY_SELL_PCT = 0.00025
NSE_TRANSACTION_PCT = 0.0000345
GST_PCT = 0.18
SEBI_CHARGES_PCT = 0.000001
STAMP_DUTY_BUY_PCT = 0.00003


def is_trading_day(d) -> bool:
    """True if weekday and not in NSE_HOLIDAYS."""
    if isinstance(d, str):
        d = date.fromisoformat(d)
    if hasattr(d, 'date'):
        d = d.date()
    if d.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    return d.isoformat() not in NSE_HOLIDAYS
