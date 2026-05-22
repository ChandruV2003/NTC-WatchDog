"""Schedule parsing and evaluation helpers for NTC WebCall."""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


DAY_INDEX = {
    "MON": 0,
    "TUE": 1,
    "WED": 2,
    "THU": 3,
    "FRI": 4,
    "SAT": 5,
    "SUN": 6,
}


def normalize_day(value: str) -> str:
    """Normalize a day label into a three-letter uppercase key."""

    key = value.strip().upper()[:3]
    if key not in DAY_INDEX:
        raise ValueError(f"Unsupported day value: {value!r}")
    return key


def normalize_time(value: str) -> str:
    """Normalize an HH:MM time string."""

    pieces = value.strip().split(":")
    if len(pieces) != 2:
        raise ValueError(f"Unsupported time value: {value!r}")

    hour = int(pieces[0])
    minute = int(pieces[1])
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"Unsupported time value: {value!r}")
    return f"{hour:02d}:{minute:02d}"


def _minutes_since_midnight(value: str) -> int:
    normalized = normalize_time(value)
    hour, minute = normalized.split(":")
    return int(hour) * 60 + int(minute)


def normalize_schedule_rows(rows):
    """Normalize and sort schedule rows."""

    normalized = []
    for row in rows:
        normalized.append(
            {
                "day": normalize_day(row["day"]),
                "start": normalize_time(row["start"]),
                "end": normalize_time(row["end"]),
                "enabled": bool(row.get("enabled", True)),
            }
        )

    return sorted(
        normalized,
        key=lambda row: (DAY_INDEX[row["day"]], row["start"], row["end"]),
    )


def is_schedule_active(rows, when: datetime | None = None, timezone: str = "America/New_York") -> bool:
    """Return True when the supplied schedule is active at the given time."""

    tz = ZoneInfo(timezone)
    current = when.astimezone(tz) if when else datetime.now(tz)
    weekday = current.weekday()
    minutes = current.hour * 60 + current.minute

    for row in normalize_schedule_rows(rows):
        if not row["enabled"]:
            continue
        start = _minutes_since_midnight(row["start"])
        end = _minutes_since_midnight(row["end"])
        day_index = DAY_INDEX[row["day"]]

        if start <= end:
            if weekday == day_index and start <= minutes < end:
                return True
            continue

        # Overnight window, e.g. 23:00-01:00.
        next_day = (day_index + 1) % 7
        if weekday == day_index and minutes >= start:
            return True
        if weekday == next_day and minutes < end:
            return True

    return False


def is_schedule_hold_active(
    rows,
    when: datetime | None = None,
    timezone: str = "America/New_York",
    grace_minutes: int = 240,
) -> bool:
    """Return True when we are shortly after a scheduled end time."""

    tz = ZoneInfo(timezone)
    current = when.astimezone(tz) if when else datetime.now(tz)
    window_start = current - timedelta(minutes=grace_minutes)
    today = current.date()

    for row in normalize_schedule_rows(rows):
        if not row["enabled"]:
            continue

        day_index = DAY_INDEX[row["day"]]
        start = _minutes_since_midnight(row["start"])
        end = _minutes_since_midnight(row["end"])

        for offset in range(-8, 1):
            candidate_date = today + timedelta(days=offset)
            if candidate_date.weekday() != day_index:
                continue

            end_date = candidate_date if start <= end else candidate_date + timedelta(days=1)
            end_at = datetime(
                end_date.year,
                end_date.month,
                end_date.day,
                end // 60,
                end % 60,
                tzinfo=tz,
            )
            if window_start <= end_at <= current:
                return True

    return False


def next_schedule_change(rows, when: datetime | None = None, timezone: str = "America/New_York"):
    """Return the next datetime where the schedule flips state, or None."""

    tz = ZoneInfo(timezone)
    current = when.astimezone(tz) if when else datetime.now(tz)
    today = current.date()
    candidates = []

    for row in normalize_schedule_rows(rows):
        if not row["enabled"]:
            continue
        base_day = DAY_INDEX[row["day"]]
        start = _minutes_since_midnight(row["start"])
        end = _minutes_since_midnight(row["end"])

        for offset in range(0, 8):
            candidate_date = today + timedelta(days=offset)
            if candidate_date.weekday() != base_day:
                continue

            start_at = datetime(
                candidate_date.year,
                candidate_date.month,
                candidate_date.day,
                start // 60,
                start % 60,
                tzinfo=tz,
            )
            candidates.append(start_at)

            end_date = candidate_date if start <= end else candidate_date + timedelta(days=1)
            end_at = datetime(
                end_date.year,
                end_date.month,
                end_date.day,
                end // 60,
                end % 60,
                tzinfo=tz,
            )
            candidates.append(end_at)

    future_candidates = sorted(candidate for candidate in candidates if candidate > current)
    return future_candidates[0] if future_candidates else None


def format_schedule_rows(rows, *, include_disabled: bool = True) -> str:
    """Render schedule rows as editable text."""

    normalized = normalize_schedule_rows(rows)
    rendered = []
    for row in normalized:
        if not include_disabled and not row["enabled"]:
            continue
        prefix = "[off] " if not row["enabled"] else ""
        rendered.append(f"{prefix}{row['day']} {row['start']}-{row['end']}")
    return "\n".join(rendered)


def parse_schedule_text(value: str):
    """Parse editable schedule text into normalized rows."""

    rows = []
    for raw_line in (value or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        enabled = True
        lowered = line.casefold()
        if lowered.startswith("[off] "):
            enabled = False
            line = line[6:].strip()
        elif lowered.startswith("off "):
            enabled = False
            line = line[4:].strip()
        day_part, _, time_part = line.partition(" ")
        if not day_part or not time_part or "-" not in time_part:
            raise ValueError(f"Invalid schedule row: {raw_line!r}")
        start, end = time_part.split("-", 1)
        rows.append({"day": day_part, "start": start, "end": end, "enabled": enabled})
    return normalize_schedule_rows(rows)
