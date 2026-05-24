"""Default host inventory and schedules for the NTC WebCall control panel."""

from copy import deepcopy

DEFAULT_TIMEZONE = "America/New_York"

_DEFAULT_HOSTS = [
    {
        "slug": "hp-pavilion-14m-ba1xx",
        "label": "HP Pavilion x360 14m-ba1xx",
        "room": "Room B",
        "enabled": True,
        "manual_mode": "auto",
        "capture_mode": "stereo",
        "capture_sample_rate_hz": 96000,
        "notes": "Primary Saturday meeting laptop.",
        "timezone": DEFAULT_TIMEZONE,
        "schedules": [
            {"day": "MON", "start": "10:20", "end": "13:00", "enabled": False},
            {"day": "MON", "start": "18:50", "end": "21:00", "enabled": False},
            {"day": "TUE", "start": "10:20", "end": "13:00", "enabled": False},
            {"day": "TUE", "start": "18:50", "end": "21:00", "enabled": False},
            {"day": "WED", "start": "10:20", "end": "13:00", "enabled": False},
            {"day": "WED", "start": "18:50", "end": "21:00", "enabled": False},
            {"day": "THU", "start": "10:20", "end": "13:00", "enabled": False},
            {"day": "THU", "start": "18:50", "end": "21:00", "enabled": True},
            {"day": "FRI", "start": "10:20", "end": "13:00", "enabled": False},
            {"day": "FRI", "start": "18:50", "end": "21:00", "enabled": True},
            {"day": "SAT", "start": "10:20", "end": "13:00", "enabled": False},
            {"day": "SAT", "start": "17:50", "end": "20:00", "enabled": True},
            {"day": "SUN", "start": "10:20", "end": "13:00", "enabled": False},
        ],
    },
    {
        "slug": "hp-envy-16-ad0xx",
        "label": "HP Envy 16-ad0xx",
        "room": "Room A",
        "enabled": True,
        "manual_mode": "auto",
        "capture_mode": "stereo",
        "capture_sample_rate_hz": 48000,
        "notes": "Used for Sunday meetings and Wednesday Bible study.",
        "timezone": DEFAULT_TIMEZONE,
        "schedules": [
            {"day": "WED", "start": "18:50", "end": "21:00", "enabled": True},
            {"day": "SUN", "start": "10:20", "end": "14:00", "enabled": True},
        ],
    },
    {
        "slug": "leonovo-laptop-mv23gfqd",
        "label": "Lenovo laptop MV23GFQD",
        "room": "Spare / diagnostics host",
        "enabled": False,
        "manual_mode": "force_off",
        "capture_mode": "mono",
        "capture_sample_rate_hz": 48000,
        "notes": "Reachable over SSH, but not currently part of the default WebCall schedule.",
        "timezone": DEFAULT_TIMEZONE,
        "schedules": [],
    },
]


def get_default_hosts():
    """Return a deep copy of the seeded control-panel host configuration."""

    return deepcopy(_DEFAULT_HOSTS)
