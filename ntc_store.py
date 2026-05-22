"""SQLite-backed configuration and runtime state for NTC."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from host_defaults import get_default_hosts
from ntc_env import install_legacy_env_aliases
from schedule_engine import (
    is_schedule_active,
    is_schedule_hold_active,
    next_schedule_change,
    normalize_schedule_rows,
    parse_schedule_text,
)

install_legacy_env_aliases()


DEFAULT_TIMEZONE = "America/New_York"
DEFAULT_PIN = os.getenv("NTC_DEFAULT_PIN", "7070")
SCHEDULE_HOLD_GRACE_MINUTES = 240
POST_END_SILENCE_GRACE_SECONDS = int(os.getenv("NTC_POST_END_SILENCE_GRACE_SECONDS", "300"))

ROOM_SEEDS = {
    "hp-pavilion-14m-ba1xx": {
        "slug": "room-b",
        "label": "Room B",
        "description": "Tarry Meeting Hall.",
        "enabled": True,
    },
    "hp-envy-16-ad0xx": {
        "slug": "room-a",
        "label": "Room A",
        "description": "Main Sanctuary.",
        "enabled": True,
    },
    "leonovo-laptop-mv23gfqd": {
        "slug": "diagnostics",
        "label": "Diagnostics",
        "description": "Spare room for testing and fallback validation.",
        "enabled": False,
    },
}

ROOM_SLUG_MIGRATIONS = {
    "study-room": "room-a",
    "meeting-hall": "room-b",
}

ROOM_REFERENCE_TABLES = (
    "hosts",
    "listener_sessions",
    "meeting_sessions",
    "meeting_incidents",
    "audio_level_samples",
    "transcript_segments",
    "translation_audio_jobs",
    "room_events",
)

LEGACY_AUDIO_DEFAULTS = {
    "hp-pavilion-14m-ba1xx": ("CQ", "__disabled__"),
    "hp-envy-16-ad0xx": ("SQ", "__disabled__"),
    "leonovo-laptop-mv23gfqd": ("Microphone", ""),
}

HOST_PRIORITY = {
    "hp-envy-16-ad0xx": 20,
    "hp-pavilion-14m-ba1xx": 10,
    "leonovo-laptop-mv23gfqd": 0,
}

SILENCE_WARNING_PREFIX = "No program audio detected for "


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso8601_like(value: str | None):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _hash_pin(pin: str) -> str:
    return hashlib.sha256(pin.encode("utf-8")).hexdigest()


def _bool_from_int(value) -> bool:
    return bool(int(value)) if value is not None else False


def _json_list(value: str | None):
    try:
        parsed = json.loads(value or "[]")
    except json.JSONDecodeError:
        parsed = []
    if not isinstance(parsed, list):
        return []
    deduped = []
    for item in parsed:
        text = str(item or "").strip()
        if text and text not in deduped:
            deduped.append(text)
    return deduped


def _dedupe_list(values):
    deduped = []
    for item in values or []:
        text = str(item or "").strip()
        if text and text not in deduped:
            deduped.append(text)
    return deduped


def _remembered_device_list(previous_devices, current_devices):
    ignored = {"test-tone"}
    remembered = []
    for device in _dedupe_list(list(previous_devices or []) + list(current_devices or [])):
        normalized = device.casefold()
        if normalized in ignored or normalized.startswith("synthetic "):
            continue
        remembered.append(device)
    return remembered


def _is_silence_warning(message: str | None) -> bool:
    return (message or "").strip().startswith(SILENCE_WARNING_PREFIX)


def _silence_warning_expired(runtime: dict | None) -> bool:
    if not runtime or not _is_silence_warning(runtime.get("last_error")):
        return False
    started_at = _parse_iso8601_like(runtime.get("last_error_changed_at"))
    if not started_at:
        return False
    return (datetime.now(timezone.utc) - started_at).total_seconds() >= POST_END_SILENCE_GRACE_SECONDS


def _post_end_primary_input_present(host_row: sqlite3.Row, runtime: dict | None) -> bool:
    """Only extend a scheduled meeting after end time while the main mixer feed is present."""

    if not runtime or not runtime.get("is_ingesting"):
        return False

    current_device = (runtime.get("current_device") or "").strip()
    if not current_device:
        return False

    configured_order = _json_list(host_row["device_order_json"])
    primary_device = configured_order[0] if configured_order else ""
    preferred_pattern = (host_row["preferred_audio_pattern"] or "").strip()

    if primary_device and current_device == primary_device:
        return True
    if preferred_pattern and preferred_pattern != "__disabled__" and preferred_pattern.casefold() in current_device.casefold():
        return True

    # Hosts without a configured primary input keep the old behavior.
    return not primary_device and not preferred_pattern


class ClosingSQLiteConnection(sqlite3.Connection):
    """Commit or roll back like sqlite3.Connection, then close the handle."""

    def __exit__(self, exc_type, exc_value, traceback):
        suppress = super().__exit__(exc_type, exc_value, traceback)
        self.close()
        return suppress


class NTCStore:
    """Persist rooms, source hosts, schedules, and source heartbeat state."""

    def __init__(self, db_path: str | None = None):
        default_path = Path(__file__).resolve().parent / "data" / "ntccast.db"
        configured_path = Path(db_path or os.getenv("NTC_DB_PATH", default_path))
        self.db_path = configured_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self._seed_defaults()

    def _connect(self):
        connection = sqlite3.connect(self.db_path, timeout=30.0, factory=ClosingSQLiteConnection)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = NORMAL")
        return connection

    def _init_db(self):
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS rooms (
                    slug TEXT PRIMARY KEY,
                    label TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    transcription_enabled INTEGER NOT NULL DEFAULT 0,
                    pin_hash TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS hosts (
                    slug TEXT PRIMARY KEY,
                    label TEXT NOT NULL,
                    room_slug TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    manual_mode TEXT NOT NULL DEFAULT 'auto',
                    capture_mode TEXT NOT NULL DEFAULT 'auto',
                    capture_sample_rate_hz INTEGER NOT NULL DEFAULT 48000,
                    notes TEXT NOT NULL DEFAULT '',
                    timezone TEXT NOT NULL DEFAULT 'America/New_York',
                    preferred_audio_pattern TEXT NOT NULL DEFAULT 'Scarlett',
                    fallback_audio_pattern TEXT NOT NULL DEFAULT 'Microphone',
                    device_order_json TEXT NOT NULL DEFAULT '[]',
                    translation_output_enabled INTEGER NOT NULL DEFAULT 0,
                    translation_target_language TEXT NOT NULL DEFAULT 'zh-CN',
                    heartbeat_token TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(room_slug) REFERENCES rooms(slug) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS schedules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host_slug TEXT NOT NULL,
                    day TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    FOREIGN KEY(host_slug) REFERENCES hosts(slug) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS source_runtime (
                    host_slug TEXT PRIMARY KEY,
                    current_device TEXT NOT NULL DEFAULT '',
                    device_list_json TEXT NOT NULL DEFAULT '[]',
                    is_ingesting INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    last_error_changed_at TEXT NOT NULL DEFAULT '',
                    desired_active INTEGER NOT NULL DEFAULT 0,
                    stream_profile TEXT NOT NULL DEFAULT '',
                    stream_channels INTEGER NOT NULL DEFAULT 1,
                    sample_rate_hz INTEGER NOT NULL DEFAULT 48000,
                    sample_bits INTEGER NOT NULL DEFAULT 0,
                    last_seen_at TEXT NOT NULL,
                    FOREIGN KEY(host_slug) REFERENCES hosts(slug) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS listener_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    room_slug TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    participant_label TEXT NOT NULL,
                    participant_key TEXT NOT NULL,
                    ip_address TEXT NOT NULL DEFAULT '',
                    user_agent TEXT NOT NULL DEFAULT '',
                    joined_at TEXT NOT NULL,
                    left_at TEXT,
                    close_reason TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY(room_slug) REFERENCES rooms(slug) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_listener_sessions_room_joined
                ON listener_sessions(room_slug, joined_at DESC);

                CREATE TABLE IF NOT EXISTS meeting_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    room_slug TEXT NOT NULL,
                    host_slug TEXT,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    trigger_mode TEXT NOT NULL DEFAULT 'system',
                    started_by TEXT NOT NULL DEFAULT '',
                    ended_by TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY(room_slug) REFERENCES rooms(slug) ON DELETE CASCADE,
                    FOREIGN KEY(host_slug) REFERENCES hosts(slug) ON DELETE SET NULL
                );

                CREATE INDEX IF NOT EXISTS idx_meeting_sessions_room_started
                ON meeting_sessions(room_slug, started_at DESC);

                CREATE TABLE IF NOT EXISTS meeting_incidents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    room_slug TEXT NOT NULL,
                    host_slug TEXT,
                    occurred_at TEXT NOT NULL,
                    severity TEXT NOT NULL DEFAULT 'warn',
                    message TEXT NOT NULL,
                    FOREIGN KEY(room_slug) REFERENCES rooms(slug) ON DELETE CASCADE,
                    FOREIGN KEY(host_slug) REFERENCES hosts(slug) ON DELETE SET NULL
                );

                CREATE INDEX IF NOT EXISTS idx_meeting_incidents_room_time
                ON meeting_incidents(room_slug, occurred_at DESC);

                CREATE TABLE IF NOT EXISTS audio_level_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    room_slug TEXT NOT NULL,
                    host_slug TEXT,
                    sampled_at TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'watchdog',
                    signal_level_db REAL,
                    signal_peak_db REAL,
                    signal_level_percent REAL,
                    signal_peak_percent REAL,
                    listener_count INTEGER NOT NULL DEFAULT 0,
                    broadcasting INTEGER NOT NULL DEFAULT 0,
                    is_ingesting INTEGER NOT NULL DEFAULT 0,
                    desired_active INTEGER NOT NULL DEFAULT 0,
                    current_device TEXT NOT NULL DEFAULT '',
                    stream_transport TEXT NOT NULL DEFAULT '',
                    connection_quality_percent REAL,
                    connection_quality_label TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY(room_slug) REFERENCES rooms(slug) ON DELETE CASCADE,
                    FOREIGN KEY(host_slug) REFERENCES hosts(slug) ON DELETE SET NULL
                );

                CREATE INDEX IF NOT EXISTS idx_audio_level_samples_room_time
                ON audio_level_samples(room_slug, sampled_at DESC);

                CREATE TABLE IF NOT EXISTS transcript_segments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    room_slug TEXT NOT NULL,
                    host_slug TEXT,
                    provider TEXT NOT NULL DEFAULT '',
                    model TEXT NOT NULL DEFAULT '',
                    started_at TEXT NOT NULL,
                    ended_at TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    text TEXT NOT NULL,
                    is_final INTEGER NOT NULL DEFAULT 1,
                    source TEXT NOT NULL DEFAULT 'transcriber',
                    FOREIGN KEY(room_slug) REFERENCES rooms(slug) ON DELETE CASCADE,
                    FOREIGN KEY(host_slug) REFERENCES hosts(slug) ON DELETE SET NULL
                );

                CREATE INDEX IF NOT EXISTS idx_transcript_segments_room_time
                ON transcript_segments(room_slug, received_at DESC);

                CREATE TABLE IF NOT EXISTS translation_audio_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host_slug TEXT NOT NULL,
                    room_slug TEXT NOT NULL,
                    target_language TEXT NOT NULL DEFAULT 'zh-CN',
                    source_text TEXT NOT NULL DEFAULT '',
                    translated_text TEXT NOT NULL DEFAULT '',
                    audio_filename TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    created_at TEXT NOT NULL,
                    played_at TEXT,
                    FOREIGN KEY(host_slug) REFERENCES hosts(slug) ON DELETE CASCADE,
                    FOREIGN KEY(room_slug) REFERENCES rooms(slug) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_translation_audio_jobs_host_id
                ON translation_audio_jobs(host_slug, id DESC);

                CREATE TABLE IF NOT EXISTS room_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at TEXT NOT NULL,
                    component TEXT NOT NULL,
                    level TEXT NOT NULL DEFAULT 'info',
                    event_type TEXT NOT NULL,
                    room_slug TEXT,
                    host_slug TEXT,
                    listener_session_id INTEGER,
                    meeting_session_id INTEGER,
                    message TEXT NOT NULL,
                    details_json TEXT NOT NULL DEFAULT '{}',
                    FOREIGN KEY(room_slug) REFERENCES rooms(slug) ON DELETE SET NULL,
                    FOREIGN KEY(host_slug) REFERENCES hosts(slug) ON DELETE SET NULL,
                    FOREIGN KEY(listener_session_id) REFERENCES listener_sessions(id) ON DELETE SET NULL,
                    FOREIGN KEY(meeting_session_id) REFERENCES meeting_sessions(id) ON DELETE SET NULL
                );

                CREATE INDEX IF NOT EXISTS idx_room_events_time
                ON room_events(occurred_at DESC);

                CREATE INDEX IF NOT EXISTS idx_room_events_room_time
                ON room_events(room_slug, occurred_at DESC);

                CREATE INDEX IF NOT EXISTS idx_room_events_host_time
                ON room_events(host_slug, occurred_at DESC);
                """
            )
            room_columns = {row["name"] for row in connection.execute("PRAGMA table_info(rooms)").fetchall()}
            if "transcription_enabled" not in room_columns:
                connection.execute(
                    "ALTER TABLE rooms ADD COLUMN transcription_enabled INTEGER NOT NULL DEFAULT 0"
                )

            host_columns = {row["name"] for row in connection.execute("PRAGMA table_info(hosts)").fetchall()}
            if "capture_mode" not in host_columns:
                connection.execute(
                    "ALTER TABLE hosts ADD COLUMN capture_mode TEXT NOT NULL DEFAULT 'auto'"
                )
            if "capture_sample_rate_hz" not in host_columns:
                connection.execute(
                    "ALTER TABLE hosts ADD COLUMN capture_sample_rate_hz INTEGER NOT NULL DEFAULT 48000"
                )
            if "device_order_json" not in host_columns:
                connection.execute(
                    "ALTER TABLE hosts ADD COLUMN device_order_json TEXT NOT NULL DEFAULT '[]'"
                )
            if "translation_output_enabled" not in host_columns:
                connection.execute(
                    "ALTER TABLE hosts ADD COLUMN translation_output_enabled INTEGER NOT NULL DEFAULT 0"
                )
            if "translation_target_language" not in host_columns:
                connection.execute(
                    "ALTER TABLE hosts ADD COLUMN translation_target_language TEXT NOT NULL DEFAULT 'zh-CN'"
                )

            transcript_columns = {row["name"] for row in connection.execute("PRAGMA table_info(transcript_segments)").fetchall()}
            if transcript_columns and "source" not in transcript_columns:
                connection.execute(
                    "ALTER TABLE transcript_segments ADD COLUMN source TEXT NOT NULL DEFAULT 'transcriber'"
                )

            runtime_columns = {row["name"] for row in connection.execute("PRAGMA table_info(source_runtime)").fetchall()}
            if "stream_profile" not in runtime_columns:
                connection.execute(
                    "ALTER TABLE source_runtime ADD COLUMN stream_profile TEXT NOT NULL DEFAULT ''"
                )
            if "stream_channels" not in runtime_columns:
                connection.execute(
                    "ALTER TABLE source_runtime ADD COLUMN stream_channels INTEGER NOT NULL DEFAULT 1"
                )
            if "sample_rate_hz" not in runtime_columns:
                connection.execute(
                    "ALTER TABLE source_runtime ADD COLUMN sample_rate_hz INTEGER NOT NULL DEFAULT 48000"
                )
            if "sample_bits" not in runtime_columns:
                connection.execute(
                    "ALTER TABLE source_runtime ADD COLUMN sample_bits INTEGER NOT NULL DEFAULT 0"
                )
            if "last_error_changed_at" not in runtime_columns:
                connection.execute(
                    "ALTER TABLE source_runtime ADD COLUMN last_error_changed_at TEXT NOT NULL DEFAULT ''"
                )

            schedule_columns = {row["name"] for row in connection.execute("PRAGMA table_info(schedules)").fetchall()}
            if "enabled" not in schedule_columns:
                connection.execute(
                    "ALTER TABLE schedules ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1"
                )

            listener_columns = {row["name"] for row in connection.execute("PRAGMA table_info(listener_sessions)").fetchall()}
            if "close_reason" not in listener_columns:
                connection.execute(
                    "ALTER TABLE listener_sessions ADD COLUMN close_reason TEXT NOT NULL DEFAULT ''"
                )
            self._migrate_room_slugs(connection)

    def _migrate_room_slugs(self, connection):
        timestamp = _utc_now()
        for old_slug, new_slug in ROOM_SLUG_MIGRATIONS.items():
            old_room = connection.execute(
                """
                SELECT slug, label, description, enabled, pin_hash, updated_at, transcription_enabled
                FROM rooms
                WHERE slug = ?
                """,
                (old_slug,),
            ).fetchone()
            if not old_room:
                continue

            new_room = connection.execute("SELECT slug FROM rooms WHERE slug = ?", (new_slug,)).fetchone()
            if not new_room:
                connection.execute(
                    """
                    INSERT INTO rooms (slug, label, description, enabled, pin_hash, updated_at, transcription_enabled)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_slug,
                        old_room["label"],
                        old_room["description"],
                        old_room["enabled"],
                        old_room["pin_hash"],
                        timestamp,
                        old_room["transcription_enabled"],
                    ),
                )

            for table in ROOM_REFERENCE_TABLES:
                connection.execute(
                    f"UPDATE {table} SET room_slug = ? WHERE room_slug = ?",
                    (new_slug, old_slug),
                )

            connection.execute("DELETE FROM rooms WHERE slug = ?", (old_slug,))

    def _seed_defaults(self):
        with self._connect() as connection:
            existing_hosts = connection.execute("SELECT COUNT(*) FROM hosts").fetchone()[0]
            existing_rooms = connection.execute("SELECT COUNT(*) FROM rooms").fetchone()[0]
            if existing_hosts or existing_rooms:
                self._sync_seed_metadata(connection)
                return

            timestamp = _utc_now()
            pin_hash = _hash_pin(DEFAULT_PIN)
            inserted_rooms = set()

            for host in get_default_hosts():
                room_seed = ROOM_SEEDS.get(
                    host["slug"],
                    {
                        "slug": host["slug"],
                        "label": host["label"],
                        "description": host["room"],
                        "enabled": host["enabled"],
                    },
                )
                room_slug = room_seed["slug"]
                if room_slug not in inserted_rooms:
                    connection.execute(
                        """
                        INSERT INTO rooms (slug, label, description, enabled, pin_hash, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            room_slug,
                            room_seed["label"],
                            room_seed["description"],
                            1 if room_seed["enabled"] else 0,
                            pin_hash,
                            timestamp,
                        ),
                    )
                    inserted_rooms.add(room_slug)

                preferred_audio, fallback_audio = LEGACY_AUDIO_DEFAULTS.get(host["slug"], ("Scarlett", "Microphone"))
                connection.execute(
                    """
                    INSERT INTO hosts (
                        slug,
                        label,
                        room_slug,
                        enabled,
                        manual_mode,
                        capture_mode,
                        capture_sample_rate_hz,
                        notes,
                        timezone,
                        preferred_audio_pattern,
                        fallback_audio_pattern,
                        device_order_json,
                        translation_output_enabled,
                        translation_target_language,
                        heartbeat_token,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        host["slug"],
                        host["label"],
                        room_slug,
                        1 if host["enabled"] else 0,
                        host["manual_mode"],
                        host.get("capture_mode", "auto"),
                        max(8000, int(host.get("capture_sample_rate_hz") or 48000)),
                        host["notes"],
                        host["timezone"],
                        preferred_audio,
                        fallback_audio,
                        json.dumps([]),
                        0,
                        "zh-CN",
                        secrets.token_urlsafe(24),
                        timestamp,
                    ),
                )

                for schedule in normalize_schedule_rows(host["schedules"]):
                    connection.execute(
                        """
                        INSERT INTO schedules (host_slug, day, start_time, end_time, enabled)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            host["slug"],
                            schedule["day"],
                            schedule["start"],
                            schedule["end"],
                            1 if schedule["enabled"] else 0,
                        ),
                    )
            self._sync_seed_metadata(connection)

    def _sync_seed_metadata(self, connection):
        for host in get_default_hosts():
            room_seed = ROOM_SEEDS.get(
                host["slug"],
                {
                    "slug": host["slug"],
                    "label": host["label"],
                    "description": host["room"],
                    "enabled": host["enabled"],
                },
            )
            connection.execute(
                """
                UPDATE rooms
                SET label = ?, description = ?, enabled = ?, pin_hash = ?
                WHERE slug = ?
                """,
                (
                    room_seed["label"],
                    room_seed["description"],
                    1 if room_seed["enabled"] else 0,
                    _hash_pin(DEFAULT_PIN),
                    room_seed["slug"],
                ),
            )
            connection.execute(
                """
                UPDATE hosts
                SET label = ?,
                    room_slug = ?,
                    capture_mode = ?,
                    capture_sample_rate_hz = ?,
                    preferred_audio_pattern = ?,
                    fallback_audio_pattern = ?
                WHERE slug = ?
                """,
                (
                    host["label"],
                    room_seed["slug"],
                    host.get("capture_mode", "auto"),
                    max(8000, int(host.get("capture_sample_rate_hz") or 48000)),
                    LEGACY_AUDIO_DEFAULTS.get(host["slug"], ("Scarlett", "Microphone"))[0],
                    LEGACY_AUDIO_DEFAULTS.get(host["slug"], ("Scarlett", "Microphone"))[1],
                    host["slug"],
                ),
            )

    def _insert_event(
        self,
        connection,
        *,
        component: str,
        event_type: str,
        message: str,
        level: str = "info",
        room_slug: str | None = None,
        host_slug: str | None = None,
        listener_session_id: int | None = None,
        meeting_session_id: int | None = None,
        details: dict | None = None,
        occurred_at: str | None = None,
    ) -> int:
        cursor = connection.execute(
            """
            INSERT INTO room_events (
                occurred_at,
                component,
                level,
                event_type,
                room_slug,
                host_slug,
                listener_session_id,
                meeting_session_id,
                message,
                details_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                occurred_at or _utc_now(),
                (component or "").strip() or "system",
                (level or "info").strip() or "info",
                (event_type or "").strip() or "event",
                room_slug,
                host_slug,
                listener_session_id,
                meeting_session_id,
                (message or "").strip(),
                json.dumps(details or {}, sort_keys=True),
            ),
        )
        return int(cursor.lastrowid)

    def record_event(
        self,
        *,
        component: str,
        event_type: str,
        message: str,
        level: str = "info",
        room_slug: str | None = None,
        host_slug: str | None = None,
        listener_session_id: int | None = None,
        meeting_session_id: int | None = None,
        details: dict | None = None,
    ) -> int:
        with self._connect() as connection:
            return self._insert_event(
                connection,
                component=component,
                event_type=event_type,
                message=message,
                level=level,
                room_slug=room_slug,
                host_slug=host_slug,
                listener_session_id=listener_session_id,
                meeting_session_id=meeting_session_id,
                details=details,
            )

    def list_recent_events(
        self,
        *,
        limit: int = 100,
        room_slug: str | None = None,
        host_slug: str | None = None,
        component: str | None = None,
    ):
        query = """
            SELECT id, occurred_at, component, level, event_type, room_slug, host_slug,
                   listener_session_id, meeting_session_id, message, details_json
            FROM room_events
            WHERE 1 = 1
        """
        params: list[object] = []
        if room_slug:
            query += " AND room_slug = ?"
            params.append(room_slug)
        if host_slug:
            query += " AND host_slug = ?"
            params.append(host_slug)
        if component:
            query += " AND component = ?"
            params.append(component)
        query += " ORDER BY occurred_at DESC LIMIT ?"
        params.append(limit)

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
            return [
                {
                    "id": row["id"],
                    "occurred_at": row["occurred_at"],
                    "component": row["component"],
                    "level": row["level"],
                    "event_type": row["event_type"],
                    "room_slug": row["room_slug"],
                    "host_slug": row["host_slug"],
                    "listener_session_id": row["listener_session_id"],
                    "meeting_session_id": row["meeting_session_id"],
                    "message": row["message"],
                    "details": json.loads(row["details_json"] or "{}"),
                }
                for row in rows
            ]

    def _schedules_for_host(self, connection, slug: str):
        rows = connection.execute(
            """
            SELECT day, start_time, end_time, enabled
            FROM schedules
            WHERE host_slug = ?
            ORDER BY
                CASE day
                    WHEN 'MON' THEN 0
                    WHEN 'TUE' THEN 1
                    WHEN 'WED' THEN 2
                    WHEN 'THU' THEN 3
                    WHEN 'FRI' THEN 4
                    WHEN 'SAT' THEN 5
                    WHEN 'SUN' THEN 6
                END,
                start_time,
                end_time
            """,
            (slug,),
        ).fetchall()
        return [
            {
                "day": row["day"],
                "start": row["start_time"],
                "end": row["end_time"],
                "enabled": _bool_from_int(row["enabled"]),
            }
            for row in rows
        ]

    def _runtime_for_host(self, connection, slug: str):
        row = connection.execute(
            """
            SELECT current_device, device_list_json, is_ingesting, last_error, last_error_changed_at, desired_active, stream_profile, stream_channels, sample_rate_hz, sample_bits, last_seen_at
            FROM source_runtime
            WHERE host_slug = ?
            """,
            (slug,),
        ).fetchone()
        if not row:
            return None

        return {
            "current_device": row["current_device"],
            "devices": _json_list(row["device_list_json"]),
            "is_ingesting": _bool_from_int(row["is_ingesting"]),
            "last_error": row["last_error"],
            "last_error_changed_at": row["last_error_changed_at"],
            "desired_active": _bool_from_int(row["desired_active"]),
            "stream_profile": row["stream_profile"] or "",
            "stream_channels": int(row["stream_channels"] or 1),
            "sample_rate_hz": int(row["sample_rate_hz"] or 48000),
            "sample_bits": int(row["sample_bits"] or 0),
            "last_seen_at": row["last_seen_at"],
        }

    def _enrich_host(self, connection, row: sqlite3.Row, include_secret: bool = False):
        schedules = self._schedules_for_host(connection, row["slug"])
        runtime = self._runtime_for_host(connection, row["slug"])
        schedule_active = is_schedule_active(schedules, timezone=row["timezone"])
        schedule_hold_active = is_schedule_hold_active(
            schedules,
            timezone=row["timezone"],
            grace_minutes=SCHEDULE_HOLD_GRACE_MINUTES,
        )
        desired_active = False
        if row["manual_mode"] == "force_off":
            desired_active = False
        elif _bool_from_int(row["enabled"]):
            if row["manual_mode"] == "force_on":
                desired_active = True
            elif schedule_active:
                desired_active = True
            elif (
                schedule_hold_active
                and _post_end_primary_input_present(row, runtime)
                and not _silence_warning_expired(runtime)
            ):
                desired_active = True

        next_change = next_schedule_change(schedules, timezone=row["timezone"])
        payload = {
            "slug": row["slug"],
            "label": row["label"],
            "room_slug": row["room_slug"],
            "room_label": row["room_label"],
            "room_description": row["room_description"],
            "room_enabled": _bool_from_int(row["room_enabled"]),
            "enabled": _bool_from_int(row["enabled"]),
            "manual_mode": row["manual_mode"],
            "capture_mode": row["capture_mode"],
            "capture_sample_rate_hz": max(8000, int(row["capture_sample_rate_hz"] or 48000)),
            "notes": row["notes"],
            "timezone": row["timezone"],
            "preferred_audio_pattern": row["preferred_audio_pattern"],
            "fallback_audio_pattern": row["fallback_audio_pattern"],
            "device_order": _json_list(row["device_order_json"]),
            "translation_output_enabled": _bool_from_int(row["translation_output_enabled"]),
            "translation_target_language": row["translation_target_language"] or "zh-CN",
            "priority": HOST_PRIORITY.get(row["slug"], 0),
            "schedules": schedules,
            "schedule_active": schedule_active,
            "desired_active": desired_active,
            "next_change": next_change.isoformat() if next_change else None,
            "runtime": runtime,
        }
        if include_secret:
            payload["heartbeat_token"] = row["heartbeat_token"]
        return payload

    def list_rooms(self):
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT slug, label, description, enabled, transcription_enabled, updated_at
                FROM rooms
                ORDER BY label
                """
            ).fetchall()
            return [
                {
                    "slug": row["slug"],
                    "label": row["label"],
                    "description": row["description"],
                    "enabled": _bool_from_int(row["enabled"]),
                    "transcription_enabled": _bool_from_int(row["transcription_enabled"]),
                    "updated_at": row["updated_at"],
                }
                for row in rows
            ]

    def get_room(self, slug: str, *, include_secret: bool = False):
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT slug, label, description, enabled, transcription_enabled, pin_hash, updated_at
                FROM rooms
                WHERE slug = ?
                """,
                (slug,),
            ).fetchone()
            if not row:
                return None

            payload = {
                "slug": row["slug"],
                "label": row["label"],
                "description": row["description"],
                "enabled": _bool_from_int(row["enabled"]),
                "transcription_enabled": _bool_from_int(row["transcription_enabled"]),
                "updated_at": row["updated_at"],
            }
            if include_secret:
                payload["pin_hash"] = row["pin_hash"]
            return payload

    def set_room_transcription_enabled(self, slug: str, enabled: bool) -> bool:
        timestamp = _utc_now()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE rooms
                SET transcription_enabled = ?, updated_at = ?
                WHERE slug = ?
                """,
                (1 if enabled else 0, timestamp, slug),
            )
            if not cursor.rowcount:
                return False
            room = connection.execute("SELECT label FROM rooms WHERE slug = ?", (slug,)).fetchone()
            room_label = room["label"] if room else slug
            self._insert_event(
                connection,
                component="transcription",
                event_type="transcription-config-updated",
                message=f"Transcription {'enabled' if enabled else 'disabled'} for {room_label}.",
                room_slug=slug,
                details={"transcription_enabled": bool(enabled)},
            )
            return True

    def verify_room_pin(self, slug: str, pin: str) -> bool:
        room = self.get_room(slug, include_secret=True)
        if not room:
            return False
        provided_hash = _hash_pin(pin.strip())
        return hmac.compare_digest(room["pin_hash"], provided_hash)

    def list_hosts(self, *, include_secret: bool = False):
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    hosts.slug,
                    hosts.label,
                    hosts.room_slug,
                    hosts.enabled,
                    hosts.manual_mode,
                    hosts.capture_mode,
                    hosts.capture_sample_rate_hz,
                    hosts.notes,
                    hosts.timezone,
                    hosts.preferred_audio_pattern,
                    hosts.fallback_audio_pattern,
                    hosts.device_order_json,
                    hosts.translation_output_enabled,
                    hosts.translation_target_language,
                    hosts.heartbeat_token,
                    rooms.label AS room_label,
                    rooms.description AS room_description,
                    rooms.enabled AS room_enabled
                FROM hosts
                JOIN rooms ON rooms.slug = hosts.room_slug
                ORDER BY hosts.label
                """
            ).fetchall()
            return [self._enrich_host(connection, row, include_secret=include_secret) for row in rows]

    def get_host(self, slug: str, *, include_secret: bool = False):
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    hosts.slug,
                    hosts.label,
                    hosts.room_slug,
                    hosts.enabled,
                    hosts.manual_mode,
                    hosts.capture_mode,
                    hosts.capture_sample_rate_hz,
                    hosts.notes,
                    hosts.timezone,
                    hosts.preferred_audio_pattern,
                    hosts.fallback_audio_pattern,
                    hosts.device_order_json,
                    hosts.translation_output_enabled,
                    hosts.translation_target_language,
                    hosts.heartbeat_token,
                    rooms.label AS room_label,
                    rooms.description AS room_description,
                    rooms.enabled AS room_enabled
                FROM hosts
                JOIN rooms ON rooms.slug = hosts.room_slug
                WHERE hosts.slug = ?
                """,
                (slug,),
            ).fetchone()
            if not row:
                return None
            return self._enrich_host(connection, row, include_secret=include_secret)

    def update_host_controls(
        self,
        slug: str,
        *,
        enabled: bool,
        manual_mode: str,
        capture_mode: str | None = None,
        capture_sample_rate_hz: int | None = None,
        notes: str,
        device_order=None,
        translation_output_enabled: bool | None = None,
    ):
        if manual_mode not in {"auto", "force_on", "force_off"}:
            raise ValueError("manual_mode must be auto, force_on, or force_off")

        with self._connect() as connection:
            existing = connection.execute(
                """
                SELECT preferred_audio_pattern, fallback_audio_pattern, device_order_json, capture_mode, capture_sample_rate_hz, translation_output_enabled
                FROM hosts
                WHERE slug = ?
                """,
                (slug,),
            ).fetchone()
            if not existing:
                raise ValueError(f"Unknown host: {slug}")

            next_device_order = _json_list(existing["device_order_json"])
            next_capture_mode = (capture_mode or existing["capture_mode"] or "auto").strip() or "auto"
            next_capture_sample_rate_hz = max(
                8000,
                int(
                    capture_sample_rate_hz
                    if capture_sample_rate_hz is not None
                    else (existing["capture_sample_rate_hz"] or 48000)
                ),
            )
            if device_order is not None:
                next_device_order = []
                for item in device_order:
                    text = str(item or "").strip()
                    if text and text not in next_device_order:
                        next_device_order.append(text)
            next_translation_output_enabled = (
                _bool_from_int(existing["translation_output_enabled"])
                if translation_output_enabled is None
                else bool(translation_output_enabled)
            )

            cursor = connection.execute(
                """
                UPDATE hosts
                SET enabled = ?, manual_mode = ?, capture_mode = ?, capture_sample_rate_hz = ?, notes = ?, preferred_audio_pattern = ?, fallback_audio_pattern = ?, device_order_json = ?, translation_output_enabled = ?, updated_at = ?
                WHERE slug = ?
                """,
                (
                    1 if enabled else 0,
                    manual_mode,
                    next_capture_mode,
                    next_capture_sample_rate_hz,
                    (notes or "").strip(),
                    existing["preferred_audio_pattern"],
                    existing["fallback_audio_pattern"],
                    json.dumps(next_device_order),
                    1 if next_translation_output_enabled else 0,
                    _utc_now(),
                    slug,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError(f"Unknown host: {slug}")

    def set_host_translation_output_enabled(self, slug: str, enabled: bool):
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE hosts
                SET translation_output_enabled = ?, updated_at = ?
                WHERE slug = ?
                """,
                (1 if enabled else 0, _utc_now(), slug),
            )
            if cursor.rowcount != 1:
                raise ValueError(f"Unknown host: {slug}")

    def set_host_translation_target_language(self, slug: str, target_language: str):
        normalized = (target_language or "zh-CN").strip() or "zh-CN"
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE hosts
                SET translation_target_language = ?, updated_at = ?
                WHERE slug = ?
                """,
                (normalized, _utc_now(), slug),
            )
            if cursor.rowcount != 1:
                raise ValueError(f"Unknown host: {slug}")

    def enqueue_translation_audio_job(
        self,
        host_slug: str,
        *,
        room_slug: str,
        target_language: str,
        audio_filename: str,
        source_text: str = "",
        translated_text: str = "",
    ) -> int:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO translation_audio_jobs
                    (host_slug, room_slug, target_language, source_text, translated_text, audio_filename, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, 'queued', ?)
                """,
                (
                    host_slug,
                    room_slug,
                    (target_language or "zh-CN").strip() or "zh-CN",
                    (source_text or "").strip(),
                    (translated_text or "").strip(),
                    audio_filename,
                    _utc_now(),
                ),
            )
            return int(cursor.lastrowid)

    def list_translation_audio_jobs_after(self, host_slug: str, *, after_id: int = 0, limit: int = 8):
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, host_slug, room_slug, target_language, source_text, translated_text, audio_filename, status, created_at, played_at
                FROM translation_audio_jobs
                WHERE host_slug = ? AND id > ? AND status = 'queued'
                ORDER BY id ASC
                LIMIT ?
                """,
                (host_slug, max(0, int(after_id or 0)), max(1, min(50, int(limit or 8)))),
            ).fetchall()
            return [dict(row) for row in rows]

    def list_recent_translation_audio_jobs(self, room_slug: str, *, limit: int = 8):
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, host_slug, room_slug, target_language, source_text, translated_text, audio_filename, status, created_at, played_at
                FROM translation_audio_jobs
                WHERE room_slug = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (room_slug, max(1, min(50, int(limit or 8)))),
            ).fetchall()
            return [dict(row) for row in rows]

    def mark_translation_audio_job_played(self, host_slug: str, job_id: int):
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE translation_audio_jobs
                SET status = 'played', played_at = ?
                WHERE host_slug = ? AND id = ?
                """,
                (_utc_now(), host_slug, int(job_id)),
            )
            if cursor.rowcount != 1:
                raise ValueError(f"Unknown translation audio job: {job_id}")

    def replace_host_schedule(self, slug: str, schedule_rows):
        if isinstance(schedule_rows, str):
            rows = parse_schedule_text(schedule_rows)
        else:
            rows = normalize_schedule_rows(schedule_rows)
        with self._connect() as connection:
            cursor = connection.execute(
                "SELECT 1 FROM hosts WHERE slug = ?",
                (slug,),
            ).fetchone()
            if not cursor:
                raise ValueError(f"Unknown host: {slug}")

            connection.execute("DELETE FROM schedules WHERE host_slug = ?", (slug,))
            for row in rows:
                connection.execute(
                    """
                    INSERT INTO schedules (host_slug, day, start_time, end_time, enabled)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        slug,
                        row["day"],
                        row["start"],
                        row["end"],
                        1 if row.get("enabled", True) else 0,
                    ),
                )

    def begin_listener_session(
        self,
        room_slug: str,
        *,
        channel: str,
        participant_label: str,
        participant_key: str,
        ip_address: str,
        user_agent: str,
    ) -> int:
        timestamp = _utc_now()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO listener_sessions (
                    room_slug,
                    channel,
                    participant_label,
                    participant_key,
                    ip_address,
                    user_agent,
                    joined_at,
                    left_at,
                    close_reason
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL, '')
                """,
                (
                    room_slug,
                    channel,
                    (participant_label or "").strip() or "Anonymous listener",
                    (participant_key or "").strip(),
                    (ip_address or "").strip(),
                    (user_agent or "").strip(),
                    timestamp,
                ),
            )
            session_id = int(cursor.lastrowid)
            if not self._is_internal_listener_user_agent(user_agent):
                self._insert_event(
                    connection,
                    component="listener",
                    event_type="listener-joined",
                    message=f"{(participant_label or '').strip() or 'Anonymous listener'} joined via {channel}.",
                    room_slug=room_slug,
                    listener_session_id=session_id,
                    details={
                        "channel": (channel or "").strip(),
                        "participant_label": (participant_label or "").strip() or "Anonymous listener",
                        "participant_key": (participant_key or "").strip(),
                        "ip_address": (ip_address or "").strip(),
                        "user_agent": (user_agent or "").strip(),
                    },
                    occurred_at=timestamp,
                )
            return session_id

    def end_listener_session(self, session_id: int, *, reason: str = ""):
        close_reason = (reason or "").strip()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT room_slug, channel, participant_label, participant_key, ip_address, user_agent
                FROM listener_sessions
                WHERE id = ?
                """,
                (session_id,),
            ).fetchone()
            cursor = connection.execute(
                """
                UPDATE listener_sessions
                SET left_at = ?,
                    close_reason = CASE WHEN close_reason = '' THEN ? ELSE close_reason END
                WHERE id = ? AND left_at IS NULL
                """,
                (_utc_now(), close_reason, session_id),
            )
            if row and cursor.rowcount and not self._is_internal_listener_user_agent(row["user_agent"]):
                self._insert_event(
                    connection,
                    component="listener",
                    event_type="listener-left",
                    message=f"{row['participant_label']} left {row['channel']}.",
                    room_slug=row["room_slug"],
                    listener_session_id=session_id,
                    details={
                        "channel": row["channel"],
                        "participant_label": row["participant_label"],
                        "participant_key": row["participant_key"],
                        "ip_address": row["ip_address"],
                        "user_agent": row["user_agent"],
                        "close_reason": close_reason,
                    },
                )

    def _end_active_listener_sessions(self, connection, room_slug: str, ended_at: str, *, reason: str = "meeting-ended"):
        connection.execute(
            """
            UPDATE listener_sessions
            SET left_at = ?,
                close_reason = CASE WHEN close_reason = '' THEN ? ELSE close_reason END
            WHERE room_slug = ? AND left_at IS NULL
            """,
            (ended_at, (reason or "").strip(), room_slug),
        )

    def end_active_listener_sessions(self, room_slug: str, *, reason: str = "meeting-ended"):
        timestamp = _utc_now()
        with self._connect() as connection:
            self._end_active_listener_sessions(connection, room_slug, timestamp, reason=reason)

    def close_orphaned_listener_sessions(self):
        timestamp = _utc_now()
        with self._connect() as connection:
            # Listener sockets are process-local. If the server process starts
            # with open listener rows, those rows belong to a previous process
            # and cannot still represent live clients.
            connection.execute(
                """
                UPDATE listener_sessions
                SET left_at = ?,
                    close_reason = CASE WHEN close_reason = '' THEN 'server-restarted' ELSE close_reason END
                WHERE left_at IS NULL
                """,
                (timestamp,),
            )

    def list_listener_sessions(self, room_slug: str, *, active_only: bool = False, limit: int = 12):
        query = """
            SELECT id, room_slug, channel, participant_label, participant_key, ip_address, user_agent, joined_at, left_at, close_reason
            FROM listener_sessions
            WHERE room_slug = ?
        """
        params: list[object] = [room_slug]
        if active_only:
            query += " AND left_at IS NULL"
        query += " ORDER BY joined_at DESC LIMIT ?"
        params.append(limit)

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
            return [
                {
                    "id": row["id"],
                    "room_slug": row["room_slug"],
                    "channel": row["channel"],
                    "participant_label": row["participant_label"],
                    "participant_key": row["participant_key"],
                    "ip_address": row["ip_address"],
                    "user_agent": row["user_agent"],
                    "joined_at": row["joined_at"],
                    "left_at": row["left_at"],
                    "close_reason": row["close_reason"],
                }
                for row in rows
            ]

    @staticmethod
    def _listener_identity_key(row) -> str:
        channel = (row["channel"] or "").strip().lower()
        participant_label = (row["participant_label"] or "").strip().lower()
        ip_address = (row["ip_address"] or "").strip().lower()
        user_agent = (row["user_agent"] or "").strip().lower()
        if channel == "phone":
            return f"phone:{participant_label}"
        if ip_address or user_agent:
            return f"{channel or 'web'}:{ip_address}:{user_agent[:160]}"
        return f"{channel or 'web'}:{participant_label}"

    @staticmethod
    def _is_internal_listener(row) -> bool:
        user_agent = (row["user_agent"] or "").strip()
        return user_agent.startswith(("NTCWatchdog/", "NTC-WebCall-LoadTest/"))

    @staticmethod
    def _is_internal_listener_user_agent(user_agent: str | None) -> bool:
        value = (user_agent or "").strip()
        return value.startswith(("NTCWatchdog/", "NTC-WebCall-LoadTest/"))

    @classmethod
    def _collapse_listener_rows(cls, rows) -> list[dict]:
        collapsed: dict[str, dict] = {}
        for row in rows:
            if cls._is_internal_listener(row):
                continue
            identity_key = cls._listener_identity_key(row)
            existing = collapsed.get(identity_key)
            if not existing:
                collapsed[identity_key] = {
                    "id": row["id"],
                    "room_slug": row["room_slug"],
                    "channel": row["channel"],
                    "participant_label": row["participant_label"],
                    "participant_key": row["participant_key"],
                    "ip_address": row["ip_address"],
                    "user_agent": row["user_agent"],
                    "joined_at": row["joined_at"],
                    "left_at": row["left_at"],
                    "close_reason": row["close_reason"],
                    "session_count": 1,
                }
                continue

            existing["session_count"] += 1
            if row["joined_at"] < existing["joined_at"]:
                existing["joined_at"] = row["joined_at"]
            if not row["left_at"]:
                existing["left_at"] = None
            elif existing["left_at"] and row["left_at"] > existing["left_at"]:
                existing["left_at"] = row["left_at"]
                existing["close_reason"] = row["close_reason"]
        return sorted(collapsed.values(), key=lambda item: item["joined_at"], reverse=True)

    def list_listener_identities(
        self,
        room_slug: str,
        *,
        active_only: bool = False,
        started_at: str | None = None,
        ended_at: str | None = None,
        limit: int | None = 50,
    ):
        query = """
            SELECT id, room_slug, channel, participant_label, participant_key, ip_address, user_agent, joined_at, left_at, close_reason
            FROM listener_sessions
            WHERE room_slug = ?
        """
        params: list[object] = [room_slug]
        if active_only:
            query += " AND left_at IS NULL"
        if started_at and ended_at:
            query += " AND joined_at <= ? AND COALESCE(left_at, ?) >= ?"
            params.extend([ended_at, ended_at, started_at])
        query += " ORDER BY joined_at DESC"

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        identities = self._collapse_listener_rows(rows)
        if limit is not None:
            return identities[: max(0, int(limit))]
        return identities

    def count_active_listener_sessions(self, room_slug: str) -> int:
        return len(self.list_listener_identities(room_slug, active_only=True, limit=None))

    def record_heartbeat(
        self,
        host_slug: str,
        *,
        current_device: str,
        devices,
        is_ingesting: bool,
        last_error: str,
        desired_active: bool,
        stream_profile: str = "",
        stream_channels: int = 1,
        sample_rate_hz: int = 48000,
        sample_bits: int = 0,
    ):
        timestamp = _utc_now()
        with self._connect() as connection:
            prior = connection.execute(
                """
                SELECT rooms.slug AS room_slug,
                       hosts.device_order_json,
                       hosts.preferred_audio_pattern,
                       hosts.fallback_audio_pattern,
                       source_runtime.current_device,
                       source_runtime.device_list_json,
                       source_runtime.is_ingesting,
                       source_runtime.last_error,
                       source_runtime.last_error_changed_at,
                       source_runtime.stream_profile,
                       source_runtime.stream_channels,
                       source_runtime.sample_rate_hz,
                       source_runtime.sample_bits
                FROM hosts
                JOIN rooms ON rooms.slug = hosts.room_slug
                LEFT JOIN source_runtime ON source_runtime.host_slug = hosts.slug
                WHERE hosts.slug = ?
                """,
                (host_slug,),
            ).fetchone()

            normalized_error = (last_error or "").strip()
            previous_error = (prior["last_error"] or "") if prior else ""
            previous_error_changed_at = (prior["last_error_changed_at"] or "") if prior else ""
            error_changed_at = previous_error_changed_at if normalized_error == previous_error and previous_error_changed_at else timestamp
            if not normalized_error:
                error_changed_at = ""

            current_devices = _dedupe_list(devices)
            previous_devices = _json_list(prior["device_list_json"]) if prior else []
            remembered_devices = _remembered_device_list(previous_devices, current_devices)
            selection_context = {
                "available_devices": current_devices,
                "remembered_devices": remembered_devices,
                "configured_device_order": _json_list(prior["device_order_json"]) if prior else [],
                "preferred_audio_pattern": (prior["preferred_audio_pattern"] or "") if prior else "",
                "fallback_audio_pattern": (prior["fallback_audio_pattern"] or "") if prior else "",
                "desired_active": bool(desired_active),
                "is_ingesting": bool(is_ingesting),
            }

            connection.execute(
                """
                INSERT INTO source_runtime (
                    host_slug,
                    current_device,
                    device_list_json,
                    is_ingesting,
                    last_error,
                    last_error_changed_at,
                    desired_active,
                    stream_profile,
                    stream_channels,
                    sample_rate_hz,
                    sample_bits,
                    last_seen_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(host_slug) DO UPDATE SET
                    current_device = excluded.current_device,
                    device_list_json = excluded.device_list_json,
                    is_ingesting = excluded.is_ingesting,
                    last_error = excluded.last_error,
                    last_error_changed_at = excluded.last_error_changed_at,
                    desired_active = excluded.desired_active,
                    stream_profile = excluded.stream_profile,
                    stream_channels = excluded.stream_channels,
                    sample_rate_hz = excluded.sample_rate_hz,
                    sample_bits = excluded.sample_bits,
                    last_seen_at = excluded.last_seen_at
                """,
                (
                    host_slug,
                    current_device or "",
                    json.dumps(remembered_devices),
                    1 if is_ingesting else 0,
                    (last_error or "").strip(),
                    error_changed_at,
                    1 if desired_active else 0,
                    (stream_profile or "").strip(),
                    max(1, int(stream_channels or 1)),
                    max(8000, int(sample_rate_hz or 48000)),
                    max(0, int(sample_bits or 0)),
                    timestamp,
                ),
            )

            if prior and remembered_devices != previous_devices:
                self._insert_event(
                    connection,
                    component="agent",
                    event_type="device-list-changed",
                    message=f"{host_slug} updated its remembered input list.",
                    room_slug=prior["room_slug"],
                    host_slug=host_slug,
                    details=selection_context,
                    occurred_at=timestamp,
                )

            previous_device = (prior["current_device"] or "") if prior else ""
            current_device_value = (current_device or "").strip()
            if prior and current_device_value != previous_device:
                event_type = "source-device-cleared" if not current_device_value else "source-device-changed"
                message = (
                    f"{host_slug} cleared its current input device."
                    if not current_device_value
                    else f"{host_slug} switched to {current_device_value}."
                )
                self._insert_event(
                    connection,
                    component="agent",
                    event_type=event_type,
                    message=message,
                    room_slug=prior["room_slug"],
                    host_slug=host_slug,
                    details={
                        **selection_context,
                        "previous_device": previous_device,
                        "current_device": current_device_value,
                    },
                    occurred_at=timestamp,
                )

            previous_ingesting = bool(prior["is_ingesting"]) if prior and prior["is_ingesting"] is not None else None
            if prior and previous_ingesting != bool(is_ingesting):
                self._insert_event(
                    connection,
                    component="agent",
                    event_type="ingest-started" if is_ingesting else "ingest-stopped",
                    message=(
                        f"{host_slug} started publishing audio."
                        if is_ingesting
                        else f"{host_slug} stopped publishing audio."
                    ),
                    room_slug=prior["room_slug"],
                    host_slug=host_slug,
                    details={
                        **selection_context,
                        "current_device": current_device_value,
                    },
                    occurred_at=timestamp,
                )

            if prior and any(
                [
                    (prior["stream_profile"] or "") != (stream_profile or "").strip(),
                    int(prior["stream_channels"] or 0) != max(1, int(stream_channels or 1)),
                    int(prior["sample_rate_hz"] or 0) != max(8000, int(sample_rate_hz or 48000)),
                    int(prior["sample_bits"] or 0) != max(0, int(sample_bits or 0)),
                ]
            ):
                self._insert_event(
                    connection,
                    component="agent",
                    event_type="stream-format-changed",
                    message=f"{host_slug} changed its stream format.",
                    room_slug=prior["room_slug"],
                    host_slug=host_slug,
                    details={
                        "stream_profile": (stream_profile or "").strip(),
                        "stream_channels": max(1, int(stream_channels or 1)),
                        "sample_rate_hz": max(8000, int(sample_rate_hz or 48000)),
                        "sample_bits": max(0, int(sample_bits or 0)),
                    },
                    occurred_at=timestamp,
                )

            if prior and normalized_error and normalized_error != previous_error:
                connection.execute(
                    """
                    INSERT INTO meeting_incidents (room_slug, host_slug, occurred_at, severity, message)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (prior["room_slug"], host_slug, timestamp, "warn", normalized_error),
                )
                self._insert_event(
                    connection,
                    component="agent",
                    event_type="runtime-error",
                    message=normalized_error,
                    level="warn",
                    room_slug=prior["room_slug"],
                    host_slug=host_slug,
                    details=selection_context,
                    occurred_at=timestamp,
                )
            elif prior and previous_error and not normalized_error:
                self._insert_event(
                    connection,
                    component="agent",
                    event_type="runtime-error-cleared",
                    message=f"{host_slug} cleared its last runtime error.",
                    room_slug=prior["room_slug"],
                    host_slug=host_slug,
                    details={"previous_error": previous_error},
                    occurred_at=timestamp,
                )

    def record_incident(
        self,
        room_slug: str,
        *,
        host_slug: str | None = None,
        severity: str = "warn",
        message: str,
        dedupe_window_seconds: int = 600,
    ):
        timestamp = _utc_now()
        normalized_message = (message or "").strip()
        with self._connect() as connection:
            latest = connection.execute(
                """
                SELECT occurred_at
                FROM meeting_incidents
                WHERE room_slug = ?
                  AND COALESCE(host_slug, '') = COALESCE(?, '')
                  AND severity = ?
                  AND message = ?
                ORDER BY occurred_at DESC
                LIMIT 1
                """,
                (room_slug, host_slug, severity, normalized_message),
            ).fetchone()
            if latest and dedupe_window_seconds > 0:
                previous = _parse_iso8601_like(latest["occurred_at"])
                if previous is not None:
                    delta = datetime.now(timezone.utc) - previous
                    if delta.total_seconds() < dedupe_window_seconds:
                        return

            connection.execute(
                """
                INSERT INTO meeting_incidents (room_slug, host_slug, occurred_at, severity, message)
                VALUES (?, ?, ?, ?, ?)
                """,
                (room_slug, host_slug, timestamp, severity, normalized_message),
            )
            self._insert_event(
                connection,
                component="watchdog" if normalized_message.startswith("[watchdog:") else "incident",
                event_type="incident-recorded",
                message=normalized_message,
                level=severity,
                room_slug=room_slug,
                host_slug=host_slug,
                occurred_at=timestamp,
            )

    def record_audio_level_sample(
        self,
        room_slug: str,
        *,
        host_slug: str | None = None,
        source: str = "watchdog",
        signal_level_db: float | None = None,
        signal_peak_db: float | None = None,
        signal_level_percent: float | None = None,
        signal_peak_percent: float | None = None,
        listener_count: int = 0,
        broadcasting: bool = False,
        is_ingesting: bool = False,
        desired_active: bool = False,
        current_device: str = "",
        stream_transport: str = "",
        connection_quality_percent: float | None = None,
        connection_quality_label: str = "",
        sampled_at: str | None = None,
    ) -> int:
        timestamp = sampled_at or _utc_now()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO audio_level_samples (
                    room_slug,
                    host_slug,
                    sampled_at,
                    source,
                    signal_level_db,
                    signal_peak_db,
                    signal_level_percent,
                    signal_peak_percent,
                    listener_count,
                    broadcasting,
                    is_ingesting,
                    desired_active,
                    current_device,
                    stream_transport,
                    connection_quality_percent,
                    connection_quality_label
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    room_slug,
                    host_slug,
                    timestamp,
                    (source or "watchdog").strip() or "watchdog",
                    signal_level_db,
                    signal_peak_db,
                    signal_level_percent,
                    signal_peak_percent,
                    max(0, int(listener_count or 0)),
                    1 if broadcasting else 0,
                    1 if is_ingesting else 0,
                    1 if desired_active else 0,
                    (current_device or "").strip(),
                    (stream_transport or "").strip(),
                    connection_quality_percent,
                    (connection_quality_label or "").strip(),
                ),
            )
            return int(cursor.lastrowid)

    def audio_level_summary(self, room_slug: str, *, window_seconds: int = 300):
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=max(1, int(window_seconds)))).isoformat()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS sample_count,
                       AVG(signal_level_db) AS avg_signal_level_db,
                       MIN(signal_level_db) AS min_signal_level_db,
                       MAX(signal_level_db) AS max_signal_level_db,
                       AVG(signal_peak_db) AS avg_signal_peak_db,
                       MAX(signal_peak_db) AS max_signal_peak_db
                FROM audio_level_samples
                WHERE room_slug = ?
                  AND sampled_at >= ?
                  AND signal_level_db IS NOT NULL
                """,
                (room_slug, cutoff),
            ).fetchone()
            latest = connection.execute(
                """
                SELECT sampled_at, host_slug, signal_level_db, signal_peak_db,
                       listener_count, broadcasting, is_ingesting, desired_active,
                       current_device, connection_quality_label
                FROM audio_level_samples
                WHERE room_slug = ?
                ORDER BY sampled_at DESC
                LIMIT 1
                """,
                (room_slug,),
            ).fetchone()

        summary = {
            "sample_count": int((row or {})["sample_count"] or 0),
            "avg_signal_level_db": (row or {})["avg_signal_level_db"],
            "min_signal_level_db": (row or {})["min_signal_level_db"],
            "max_signal_level_db": (row or {})["max_signal_level_db"],
            "avg_signal_peak_db": (row or {})["avg_signal_peak_db"],
            "max_signal_peak_db": (row or {})["max_signal_peak_db"],
            "latest": dict(latest) if latest else None,
        }
        return summary

    def prune_audio_level_samples(self, *, retain_days: int = 14):
        cutoff = (datetime.now(timezone.utc) - timedelta(days=max(1, int(retain_days)))).isoformat()
        with self._connect() as connection:
            cursor = connection.execute(
                "DELETE FROM audio_level_samples WHERE sampled_at < ?",
                (cutoff,),
            )
            return int(cursor.rowcount or 0)

    def record_transcript_segment(
        self,
        room_slug: str,
        *,
        host_slug: str | None = None,
        provider: str = "",
        model: str = "",
        started_at: str | None = None,
        ended_at: str | None = None,
        received_at: str | None = None,
        text: str = "",
        is_final: bool = True,
        source: str = "transcriber",
    ) -> int:
        normalized_text = (text or "").strip()
        if not normalized_text:
            return 0
        now = _utc_now()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO transcript_segments (
                    room_slug,
                    host_slug,
                    provider,
                    model,
                    started_at,
                    ended_at,
                    received_at,
                    text,
                    is_final,
                    source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    room_slug,
                    host_slug,
                    (provider or "").strip(),
                    (model or "").strip(),
                    started_at or now,
                    ended_at or now,
                    received_at or now,
                    normalized_text,
                    1 if is_final else 0,
                    (source or "transcriber").strip() or "transcriber",
                ),
            )
            segment_id = int(cursor.lastrowid)
            self._insert_event(
                connection,
                component="transcription",
                event_type="transcript-segment",
                message=normalized_text[:160],
                level="info",
                room_slug=room_slug,
                host_slug=host_slug,
                occurred_at=received_at or now,
                details={
                    "provider": (provider or "").strip(),
                    "model": (model or "").strip(),
                    "segment_id": segment_id,
                },
            )
            return segment_id

    def list_transcript_segments(self, room_slug: str, *, limit: int = 25):
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, room_slug, host_slug, provider, model, started_at,
                       ended_at, received_at, text, is_final, source
                FROM transcript_segments
                WHERE room_slug = ?
                ORDER BY received_at DESC, id DESC
                LIMIT ?
                """,
                (room_slug, max(1, min(500, int(limit or 25)))),
            ).fetchall()
        return [
            {
                "id": row["id"],
                "room_slug": row["room_slug"],
                "host_slug": row["host_slug"],
                "provider": row["provider"],
                "model": row["model"],
                "started_at": row["started_at"],
                "ended_at": row["ended_at"],
                "received_at": row["received_at"],
                "text": row["text"],
                "is_final": bool(row["is_final"]),
                "source": row["source"],
            }
            for row in rows
        ]

    def list_transcript_segments_after(self, room_slug: str, *, after_id: int = 0, limit: int = 50):
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, room_slug, host_slug, provider, model, started_at,
                       ended_at, received_at, text, is_final, source
                FROM transcript_segments
                WHERE room_slug = ?
                  AND id > ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (room_slug, max(0, int(after_id or 0)), max(1, min(500, int(limit or 50)))),
            ).fetchall()
        return [
            {
                "id": row["id"],
                "room_slug": row["room_slug"],
                "host_slug": row["host_slug"],
                "provider": row["provider"],
                "model": row["model"],
                "started_at": row["started_at"],
                "ended_at": row["ended_at"],
                "received_at": row["received_at"],
                "text": row["text"],
                "is_final": bool(row["is_final"]),
                "source": row["source"],
            }
            for row in rows
        ]

    def sync_meeting_state(self, room_slug: str, *, active: bool, host_slug: str | None = None, trigger_mode: str = "system", actor: str = ""):
        timestamp = _utc_now()
        with self._connect() as connection:
            def room_label(value: str) -> str:
                row = connection.execute("SELECT label FROM rooms WHERE slug = ?", (value,)).fetchone()
                return row["label"] if row else value

            active_session = connection.execute(
                """
                SELECT id, room_slug
                FROM meeting_sessions
                WHERE ended_at IS NULL
                ORDER BY started_at DESC
                """
            ).fetchall()

            current = next((row for row in active_session if row["room_slug"] == room_slug), None)

            if active:
                for row in active_session:
                    if row["room_slug"] != room_slug:
                        connection.execute(
                            """
                            UPDATE meeting_sessions
                            SET ended_at = ?, ended_by = ?
                            WHERE id = ?
                            """,
                            (timestamp, actor or trigger_mode, row["id"]),
                        )
                        self._end_active_listener_sessions(connection, row["room_slug"], timestamp, reason="meeting-preempted")
                        self._insert_event(
                            connection,
                            component="meeting",
                            event_type="meeting-preempted",
                            message=f"{room_label(row['room_slug'])} was ended because another room became active.",
                            room_slug=row["room_slug"],
                            meeting_session_id=int(row["id"]),
                            details={
                                "next_room_slug": room_slug,
                                "trigger_mode": trigger_mode,
                                "actor": actor or trigger_mode,
                            },
                            occurred_at=timestamp,
                        )
                if current:
                    return int(current["id"])

                cursor = connection.execute(
                    """
                    INSERT INTO meeting_sessions (room_slug, host_slug, started_at, trigger_mode, started_by)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (room_slug, host_slug, timestamp, trigger_mode, actor or trigger_mode),
                )
                meeting_id = int(cursor.lastrowid)
                self._insert_event(
                    connection,
                    component="meeting",
                    event_type="meeting-started",
                    message=f"{room_label(room_slug)} became active.",
                    room_slug=room_slug,
                    host_slug=host_slug,
                    meeting_session_id=meeting_id,
                    details={"trigger_mode": trigger_mode, "actor": actor or trigger_mode},
                    occurred_at=timestamp,
                )
                return meeting_id

            if current:
                connection.execute(
                    """
                    UPDATE meeting_sessions
                    SET ended_at = ?, ended_by = ?
                    WHERE id = ?
                    """,
                    (timestamp, actor or trigger_mode, current["id"]),
                )
                self._end_active_listener_sessions(connection, room_slug, timestamp, reason="meeting-stopped")
                self._insert_event(
                    connection,
                    component="meeting",
                    event_type="meeting-stopped",
                    message=f"{room_label(room_slug)} was ended.",
                    room_slug=room_slug,
                    host_slug=host_slug,
                    meeting_session_id=int(current["id"]),
                    details={"trigger_mode": trigger_mode, "actor": actor or trigger_mode},
                    occurred_at=timestamp,
                )
                return int(current["id"])

            return None

    def get_active_meeting(self):
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT meeting_sessions.id, meeting_sessions.room_slug, meeting_sessions.host_slug,
                       meeting_sessions.started_at, meeting_sessions.trigger_mode,
                       rooms.label AS room_label
                FROM meeting_sessions
                JOIN rooms ON rooms.slug = meeting_sessions.room_slug
                WHERE meeting_sessions.ended_at IS NULL
                ORDER BY meeting_sessions.started_at DESC
                LIMIT 1
                """
            ).fetchone()
            if not row:
                return None
            return {
                "id": row["id"],
                "room_slug": row["room_slug"],
                "host_slug": row["host_slug"],
                "room_label": row["room_label"],
                "started_at": row["started_at"],
                "trigger_mode": row["trigger_mode"],
            }

    def list_meeting_sessions(self, *, limit: int = 20):
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT meeting_sessions.id, meeting_sessions.room_slug, meeting_sessions.host_slug,
                       meeting_sessions.started_at, meeting_sessions.ended_at,
                       meeting_sessions.trigger_mode, meeting_sessions.started_by, meeting_sessions.ended_by,
                       rooms.label AS room_label
                FROM meeting_sessions
                JOIN rooms ON rooms.slug = meeting_sessions.room_slug
                ORDER BY meeting_sessions.started_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

            sessions = []
            for row in rows:
                sessions.append(self._meeting_summary(connection, row))
            return sessions

    def _meeting_summary(self, connection, row: sqlite3.Row):
        started_at = row["started_at"]
        ended_at = row["ended_at"] or _utc_now()
        listener_rows = connection.execute(
            """
            SELECT id, room_slug, channel, participant_label, participant_key, ip_address, user_agent, joined_at, left_at, close_reason
            FROM listener_sessions
            WHERE room_slug = ?
              AND joined_at <= ?
              AND COALESCE(left_at, ?) >= ?
            ORDER BY joined_at DESC
            """,
            (row["room_slug"], ended_at, ended_at, started_at),
        ).fetchall()
        listener_rows = [row for row in listener_rows if not self._is_internal_listener(row)]
        listener_count = len(self._collapse_listener_rows(listener_rows))
        incident_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM meeting_incidents
            WHERE room_slug = ?
              AND occurred_at >= ?
              AND occurred_at <= ?
            """,
            (row["room_slug"], started_at, ended_at),
        ).fetchone()[0]
        return {
            "id": row["id"],
            "room_slug": row["room_slug"],
            "room_label": row["room_label"],
            "host_slug": row["host_slug"],
            "started_at": started_at,
            "ended_at": row["ended_at"],
            "trigger_mode": row["trigger_mode"],
            "started_by": row["started_by"],
            "ended_by": row["ended_by"],
            "listener_count": int(listener_count or 0),
            "listener_join_count": len(listener_rows),
            "incident_count": int(incident_count or 0),
        }

    def get_meeting_report(self, meeting_id: int):
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT meeting_sessions.id, meeting_sessions.room_slug, meeting_sessions.host_slug,
                       meeting_sessions.started_at, meeting_sessions.ended_at,
                       meeting_sessions.trigger_mode, meeting_sessions.started_by, meeting_sessions.ended_by,
                       rooms.label AS room_label
                FROM meeting_sessions
                JOIN rooms ON rooms.slug = meeting_sessions.room_slug
                WHERE meeting_sessions.id = ?
                """,
                (meeting_id,),
            ).fetchone()
            if not row:
                return None

            summary = self._meeting_summary(connection, row)
            ended_at = row["ended_at"] or _utc_now()
            listener_rows = connection.execute(
                """
                SELECT id, room_slug, channel, participant_label, participant_key, ip_address, user_agent, joined_at, left_at, close_reason
                FROM listener_sessions
                WHERE room_slug = ?
                  AND joined_at <= ?
                  AND COALESCE(left_at, ?) >= ?
                ORDER BY joined_at DESC
                """,
                (row["room_slug"], ended_at, ended_at, row["started_at"]),
            ).fetchall()
            listener_rows = [row for row in listener_rows if not self._is_internal_listener(row)]
            listeners = self._collapse_listener_rows(listener_rows)
            incidents = connection.execute(
                """
                SELECT host_slug, occurred_at, severity, message
                FROM meeting_incidents
                WHERE room_slug = ?
                  AND occurred_at >= ?
                  AND occurred_at <= ?
                ORDER BY occurred_at
                """,
                (row["room_slug"], row["started_at"], ended_at),
            ).fetchall()
            audio_levels = connection.execute(
                """
                SELECT sampled_at, host_slug, signal_level_db, signal_peak_db,
                       listener_count, broadcasting, is_ingesting, desired_active,
                       current_device, connection_quality_label
                FROM audio_level_samples
                WHERE room_slug = ?
                  AND sampled_at >= ?
                  AND sampled_at <= ?
                ORDER BY sampled_at
                """,
                (row["room_slug"], row["started_at"], ended_at),
            ).fetchall()
            transcripts = connection.execute(
                """
                SELECT host_slug, provider, model, started_at, ended_at, received_at, text
                FROM transcript_segments
                WHERE room_slug = ?
                  AND received_at >= ?
                  AND received_at <= ?
                ORDER BY received_at
                """,
                (row["room_slug"], row["started_at"], ended_at),
            ).fetchall()

            summary["listeners"] = [
                {
                    "participant_label": listener["participant_label"],
                    "channel": listener["channel"],
                    "ip_address": listener["ip_address"],
                    "user_agent": listener["user_agent"],
                    "joined_at": listener["joined_at"],
                    "left_at": listener["left_at"],
                    "close_reason": listener["close_reason"],
                    "session_count": listener.get("session_count", 1),
                }
                for listener in listeners
            ]
            summary["listener_join_count"] = len(listener_rows)
            summary["incidents"] = [
                {
                    "host_slug": incident["host_slug"],
                    "occurred_at": incident["occurred_at"],
                    "severity": incident["severity"],
                    "message": incident["message"],
                }
                for incident in incidents
            ]
            summary["audio_levels"] = [
                {
                    "sampled_at": sample["sampled_at"],
                    "host_slug": sample["host_slug"],
                    "signal_level_db": sample["signal_level_db"],
                    "signal_peak_db": sample["signal_peak_db"],
                    "listener_count": sample["listener_count"],
                    "broadcasting": bool(sample["broadcasting"]),
                    "is_ingesting": bool(sample["is_ingesting"]),
                    "desired_active": bool(sample["desired_active"]),
                    "current_device": sample["current_device"],
                    "connection_quality_label": sample["connection_quality_label"],
                }
                for sample in audio_levels
            ]
            summary["transcripts"] = [
                {
                    "host_slug": transcript["host_slug"],
                    "provider": transcript["provider"],
                    "model": transcript["model"],
                    "started_at": transcript["started_at"],
                    "ended_at": transcript["ended_at"],
                    "received_at": transcript["received_at"],
                    "text": transcript["text"],
                }
                for transcript in transcripts
            ]
            return summary
