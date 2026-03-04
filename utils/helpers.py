from datetime import datetime, date
import pytz
import math


IST = pytz.timezone("Asia/Kolkata")


def now_ist() -> datetime:
    """Return current datetime in IST."""
    return datetime.now(IST)


def to_ist(dt: datetime) -> datetime:
    """Convert naive or UTC datetime to IST."""
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def round_to_tick(price: float, tick_size: float = 0.05) -> float:
    """Round price to nearest tick size."""
    return round(round(price / tick_size) * tick_size, 2)


def format_inr(amount: float) -> str:
    """Format number as Indian Rupee string. E.g., 123456.78 → '₹1,23,456.78'"""
    if amount < 0:
        return f"-₹{format_inr_abs(-amount)}"
    return f"₹{format_inr_abs(amount)}"


def format_inr_abs(amount: float) -> str:
    """Format absolute value in Indian number system."""
    amount = round(amount, 2)
    integer_part = int(amount)
    decimal_part = f"{amount - integer_part:.2f}"[1:]  # e.g., ".50"
    s = str(integer_part)
    if len(s) <= 3:
        return s + decimal_part
    result = s[-3:]
    s = s[:-3]
    while s:
        result = s[-2:] + "," + result
        s = s[:-2]
    return result.lstrip(",") + decimal_part


def pct_change(old: float, new: float) -> float:
    """Calculate percentage change from old to new."""
    if old == 0:
        return 0.0
    return ((new - old) / old) * 100


def date_range_str(start: str, end: str) -> list[str]:
    """Return list of date strings from start to end (inclusive)."""
    from datetime import timedelta
    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    dates = []
    current = start_d
    while current <= end_d:
        dates.append(current.isoformat())
        current += timedelta(days=1)
    return dates


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Division that returns default instead of raising ZeroDivisionError."""
    if denominator == 0:
        return default
    return numerator / denominator


def clamp(value: float, min_val: float, max_val: float) -> float:
    """Clamp value between min and max."""
    return max(min_val, min(max_val, value))
