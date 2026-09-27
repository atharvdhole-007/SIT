"""Time helpers. Canonical timestamps are int nanoseconds since the UNIX epoch."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo

    NEW_YORK = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover - tz database missing
    NEW_YORK = timezone(timedelta(hours=-4), "EDT")

NS = 1_000_000_000


def midnight_ns(day: date, tz=NEW_YORK) -> int:
    """Epoch ns of local midnight on `day` in `tz`."""
    dt = datetime(day.year, day.month, day.day, tzinfo=tz)
    return int(dt.timestamp()) * NS


def to_ms(ns: int) -> int:
    return ns // 1_000_000


def clock(ns: int, tz=NEW_YORK, millis: bool = False) -> str:
    dt = datetime.fromtimestamp(ns / NS, tz)
    return dt.strftime("%H:%M:%S.%f")[:-3] if millis else dt.strftime("%H:%M:%S")


def day_str(ns: int, tz=NEW_YORK) -> str:
    return datetime.fromtimestamp(ns / NS, tz).strftime("%Y-%m-%d")


def parse_clock(day: date, hhmmss: str, tz=NEW_YORK) -> int:
    """'10:30' or '10:30:15' on `day` -> epoch ns."""
    parts = [int(p) for p in hhmmss.split(":")]
    while len(parts) < 3:
        parts.append(0)
    h, m, s = parts
    return midnight_ns(day, tz) + ((h * 60 + m) * 60 + s) * NS
