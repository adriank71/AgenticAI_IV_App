import json
import os
import uuid
from calendar import monthrange
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    from ..storage import _connect_postgres, sanitize_profile_id
except ImportError:
    from storage import _connect_postgres, sanitize_profile_id


DEFAULT_TIMEZONE = os.environ.get("IV_AGENT_CALENDAR_DEFAULT_TIMEZONE", "Europe/Berlin").strip() or "Europe/Berlin"
VALID_CATEGORIES = {"assistant", "transport", "other"}
ASSISTANT_HOUR_FIELDS = (
    "koerperpflege",
    "mahlzeiten_eingeben",
    "mahlzeiten_zubereiten",
    "begleitung_therapie",
)
_CALENDAR_STORE_CACHE: dict[tuple[str, str], Any] = {}


def normalize_timezone(timezone_name: str | None = None) -> str:
    candidate = str(timezone_name or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    try:
        ZoneInfo(candidate)
        return candidate
    except ZoneInfoNotFoundError:
        return DEFAULT_TIMEZONE


def normalize_user_id(user_id: str | None = None) -> str:
    return sanitize_profile_id(user_id or "default")


def _zone(timezone_name: str | None = None) -> ZoneInfo:
    return ZoneInfo(normalize_timezone(timezone_name))


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return list(parsed) if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _normalize_category(value: Any) -> str:
    category = str(value or "other").strip().lower()
    if category == "therapy":
        category = "other"
    return category if category in VALID_CATEGORIES else "other"


def normalize_assistant_hours(raw_hours: Any, fallback_hours: float = 0.0) -> dict[str, float]:
    source = _json_dict(raw_hours)
    normalized = {field: 0.0 for field in ASSISTANT_HOUR_FIELDS}
    for field in ASSISTANT_HOUR_FIELDS:
        try:
            normalized[field] = max(0.0, float(source.get(field, 0.0) or 0.0))
        except (TypeError, ValueError):
            normalized[field] = 0.0
    if not any(normalized.values()) and fallback_hours > 0:
        normalized[ASSISTANT_HOUR_FIELDS[0]] = round(float(fallback_hours), 2)
    return {field: round(value, 2) for field, value in normalized.items()}


def assistant_total_hours(event: dict[str, Any]) -> float:
    metadata = _json_dict(event.get("metadata"))
    assistant_hours = event.get("assistant_hours") or metadata.get("assistant_hours")
    normalized = normalize_assistant_hours(assistant_hours)
    total = round(sum(normalized.values()), 2)
    if total > 0:
        return total
    try:
        return round(float(metadata.get("hours", event.get("hours", 0.0)) or 0.0), 2)
    except (TypeError, ValueError):
        return 0.0


def _parse_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return datetime.strptime(str(value or "").strip(), "%Y-%m-%d").date()


def _parse_time(value: Any, fallback: str = "00:00") -> time:
    raw = str(value or fallback).strip() or fallback
    return datetime.strptime(raw, "%H:%M").time()


def parse_datetime(value: Any, timezone_name: str | None = None) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError("datetime is required")
        if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
            parsed = datetime.combine(_parse_date(raw), time.min)
        else:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_zone(timezone_name))
    return parsed


def _combine_local(date_value: Any, time_value: Any, timezone_name: str | None = None) -> datetime:
    return datetime.combine(_parse_date(date_value), _parse_time(time_value), tzinfo=_zone(timezone_name))


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _month_range(month: str, timezone_name: str | None = None) -> tuple[datetime, datetime]:
    start_date = datetime.strptime(month, "%Y-%m").date().replace(day=1)
    next_month_index = start_date.month
    next_year = start_date.year + next_month_index // 12
    next_month = next_month_index % 12 + 1
    end_date = date(next_year, next_month, 1)
    tz = _zone(timezone_name)
    return datetime.combine(start_date, time.min, tzinfo=tz), datetime.combine(end_date, time.min, tzinfo=tz)


def _months_between(start_at: datetime, end_at: datetime) -> list[str]:
    cursor = date(start_at.year, start_at.month, 1)
    end_cursor = date(end_at.year, end_at.month, 1)
    months = []
    while cursor <= end_cursor:
        months.append(cursor.strftime("%Y-%m"))
        next_month = cursor.month % 12 + 1
        next_year = cursor.year + (cursor.month // 12)
        cursor = date(next_year, next_month, 1)
    return months


def _event_end_for_overlap(event: dict[str, Any], timezone_name: str | None = None) -> datetime:
    end_at = event.get("end_at")
    if end_at:
        return parse_datetime(end_at, timezone_name)
    start_at = parse_datetime(event["start_at"], timezone_name)
    return start_at + (timedelta(days=1) if event.get("all_day") else timedelta(hours=1))


def _overlaps(event: dict[str, Any], start_at: datetime, end_at: datetime, timezone_name: str | None = None) -> bool:
    event_start = parse_datetime(event["start_at"], timezone_name)
    event_end = _event_end_for_overlap(event, timezone_name)
    return event_start < end_at and event_end > start_at


def _matches_query(event: dict[str, Any], query: str) -> bool:
    normalized_query = str(query or "").strip().lower()
    if not normalized_query:
        return True
    metadata = _json_dict(event.get("metadata"))
    haystack = " ".join(
        str(value or "")
        for value in (
            event.get("title"),
            event.get("description"),
            event.get("location"),
            event.get("category"),
            metadata.get("notes"),
            metadata.get("transport_address"),
        )
    ).lower()
    return normalized_query in haystack


def _coerce_uuid(value: Any | None = None) -> str:
    raw = str(value or "").strip()
    if raw:
        try:
            return str(uuid.UUID(raw))
        except ValueError:
            pass
    return str(uuid.uuid4())


def _normalize_metadata(payload: dict[str, Any], category: str, existing: dict[str, Any] | None = None) -> dict[str, Any]:
    metadata = _json_dict(existing.get("metadata") if existing else {})
    metadata.update(_json_dict(payload.get("metadata")))
    notes = str(payload.get("notes", payload.get("description", metadata.get("notes", ""))) or "").strip()
    if notes:
        metadata["notes"] = notes

    assistant_hours = normalize_assistant_hours(
        payload.get("assistant_hours", metadata.get("assistant_hours")),
        float(payload.get("hours", metadata.get("hours", 0.0)) or 0.0),
    )
    metadata["assistant_hours"] = assistant_hours if category == "assistant" else {field: 0.0 for field in ASSISTANT_HOUR_FIELDS}
    metadata["hours"] = round(sum(metadata["assistant_hours"].values()), 2) if category == "assistant" else 0.0

    transport_mode = str(payload.get("transport_mode", metadata.get("transport_mode", "")) or "").strip().lower()
    transport_address = str(payload.get("transport_address", metadata.get("transport_address", "")) or "").strip()
    try:
        transport_kilometers = max(
            0.0,
            float(payload.get("transport_kilometers", metadata.get("transport_kilometers", 0.0)) or 0.0),
        )
    except (TypeError, ValueError):
        transport_kilometers = 0.0

    metadata["transport_mode"] = transport_mode if category == "transport" else ""
    metadata["transport_kilometers"] = round(transport_kilometers, 2) if category == "transport" else 0.0
    metadata["transport_address"] = transport_address if category == "transport" else ""
    return metadata


def normalize_event_payload(
    payload: dict[str, Any],
    *,
    user_id: str | None = None,
    timezone_name: str | None = None,
    existing: dict[str, Any] | None = None,
    default_duration_minutes: int = 60,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("calendar event payload must be an object")

    tz_name = normalize_timezone(timezone_name or payload.get("timezone"))
    category = _normalize_category(payload.get("category", existing.get("category") if existing else "other"))
    all_day = bool(payload.get("all_day", existing.get("all_day") if existing else False))

    if payload.get("start_at"):
        start_at = parse_datetime(payload["start_at"], tz_name)
    elif payload.get("date"):
        if all_day:
            start_at = datetime.combine(_parse_date(payload["date"]), time.min, tzinfo=_zone(tz_name))
        else:
            start_at = _combine_local(payload["date"], payload.get("time", "09:00"), tz_name)
    elif existing and payload.get("time"):
        existing_local_date = parse_datetime(existing["start_at"], tz_name).astimezone(_zone(tz_name)).date()
        start_at = datetime.combine(existing_local_date, _parse_time(payload["time"]), tzinfo=_zone(tz_name))
    elif existing:
        start_at = parse_datetime(existing["start_at"], tz_name)
    else:
        raise ValueError("start_at or date is required")

    if payload.get("end_at"):
        end_at = parse_datetime(payload["end_at"], tz_name)
    elif all_day:
        end_at = datetime.combine(start_at.astimezone(_zone(tz_name)).date() + timedelta(days=1), time.min, tzinfo=_zone(tz_name))
    elif payload.get("date") and payload.get("end_time"):
        end_at = _combine_local(payload["date"], payload["end_time"], tz_name)
    elif existing and payload.get("end_time"):
        end_at = datetime.combine(start_at.astimezone(_zone(tz_name)).date(), _parse_time(payload["end_time"]), tzinfo=_zone(tz_name))
    elif existing and not any(key in payload for key in ("start_at", "date", "time")):
        end_at = parse_datetime(existing["end_at"], tz_name) if existing.get("end_at") else None
    else:
        end_at = start_at + timedelta(minutes=max(1, int(default_duration_minutes or 60)))

    if end_at and end_at <= start_at:
        raise ValueError("end_at must be later than start_at")

    title = str(payload.get("title", existing.get("title") if existing else "") or "").strip()
    if not title:
        raise ValueError("title is required")

    metadata = _normalize_metadata(payload, category, existing)
    description = str(payload.get("description", payload.get("notes", existing.get("description") if existing else "")) or "").strip()
    location = str(
        payload.get("location")
        or payload.get("transport_address")
        or (existing.get("location") if existing else "")
        or ""
    ).strip()
    color = str(payload.get("color", existing.get("color") if existing else "") or "").strip()

    return {
        "id": _coerce_uuid(payload.get("id") or payload.get("event_id") or (existing.get("id") if existing else None)),
        "user_id": normalize_user_id(payload.get("user_id") or user_id or (existing.get("user_id") if existing else "default")),
        "title": title,
        "description": description,
        "start_at": start_at,
        "end_at": end_at,
        "all_day": all_day,
        "category": category,
        "location": location,
        "color": color,
        "metadata": metadata,
    }


def calendar_event_to_legacy(event: dict[str, Any], timezone_name: str | None = None) -> dict[str, Any]:
    tz_name = normalize_timezone(timezone_name)
    local_start = parse_datetime(event["start_at"], tz_name).astimezone(_zone(tz_name))
    local_end = _event_end_for_overlap(event, tz_name).astimezone(_zone(tz_name))
    metadata = _json_dict(event.get("metadata"))
    category = _normalize_category(event.get("category"))
    assistant_hours = normalize_assistant_hours(metadata.get("assistant_hours"), metadata.get("hours", 0.0))

    legacy = {
        "id": str(event["id"]),
        "user_id": normalize_user_id(event.get("user_id")),
        "date": local_start.date().isoformat(),
        "time": "" if event.get("all_day") else local_start.strftime("%H:%M"),
        "end_time": "" if event.get("all_day") else local_end.strftime("%H:%M"),
        "all_day": bool(event.get("all_day")),
        "category": category,
        "title": str(event.get("title") or "").strip(),
        "notes": str(event.get("description") or metadata.get("notes") or "").strip(),
        "description": str(event.get("description") or "").strip(),
        "hours": round(sum(assistant_hours.values()), 2) if category == "assistant" else 0.0,
        "assistant_hours": assistant_hours if category == "assistant" else {field: 0.0 for field in ASSISTANT_HOUR_FIELDS},
        "transport_mode": str(metadata.get("transport_mode") or "").strip(),
        "transport_kilometers": round(float(metadata.get("transport_kilometers", 0.0) or 0.0), 2),
        "transport_address": str(metadata.get("transport_address") or event.get("location") or "").strip(),
        "location": str(event.get("location") or "").strip(),
        "color": str(event.get("color") or "").strip(),
        "metadata": metadata,
        "start_at": _iso(parse_datetime(event["start_at"], tz_name)),
        "end_at": _iso(parse_datetime(event["end_at"], tz_name)) if event.get("end_at") else None,
    }
    return legacy


class CalendarEventsPostgresStore:
    def __init__(self, database_url: str, connection_factory: Callable[[], Any] | None = None):
        self._database_url = database_url
        self._connection_factory = connection_factory or (lambda: _connect_postgres(database_url))
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS calendar_events (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        user_id TEXT NOT NULL,
                        title TEXT NOT NULL,
                        description TEXT,
                        start_at TIMESTAMPTZ NOT NULL,
                        end_at TIMESTAMPTZ,
                        all_day BOOLEAN NOT NULL DEFAULT FALSE,
                        category TEXT,
                        location TEXT,
                        color TEXT,
                        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute("CREATE INDEX IF NOT EXISTS calendar_events_user_start_idx ON calendar_events (user_id, start_at)")
                cursor.execute("CREATE INDEX IF NOT EXISTS calendar_events_user_title_idx ON calendar_events (user_id, lower(title))")
                cursor.execute("ALTER TABLE calendar_events ENABLE ROW LEVEL SECURITY")
                self._backfill_legacy_events(cursor)

    def _backfill_legacy_events(self, cursor) -> None:
        cursor.execute("SELECT to_regclass('public.events') AS table_name")
        row = cursor.fetchone()
        if not row or not row.get("table_name"):
            return
        cursor.execute(
            """
            INSERT INTO calendar_events (
                id,
                user_id,
                title,
                description,
                start_at,
                end_at,
                all_day,
                category,
                location,
                color,
                metadata,
                created_at,
                updated_at
            )
            SELECT
                gen_random_uuid(),
                'default',
                title,
                NULLIF(notes, ''),
                (
                    event_date::timestamp
                    + CASE
                        WHEN all_day OR NULLIF(start_time, '') IS NULL THEN time '00:00'
                        ELSE start_time::time
                      END
                ) AT TIME ZONE %s,
                CASE
                    WHEN all_day THEN (event_date::timestamp + interval '1 day') AT TIME ZONE %s
                    WHEN NULLIF(end_time, '') IS NOT NULL THEN (event_date::timestamp + end_time::time) AT TIME ZONE %s
                    WHEN NULLIF(start_time, '') IS NOT NULL THEN (event_date::timestamp + start_time::time + interval '30 minutes') AT TIME ZONE %s
                    ELSE NULL
                END,
                all_day,
                category,
                NULLIF(transport_address, ''),
                NULL,
                jsonb_build_object(
                    'legacy_event_id', event_id,
                    'notes', notes,
                    'hours', hours,
                    'assistant_hours', COALESCE(assistant_hours, '{}'::jsonb),
                    'transport_mode', transport_mode,
                    'transport_kilometers', transport_kilometers,
                    'transport_address', transport_address
                ),
                created_at,
                updated_at
            FROM events legacy_events
            WHERE NOT EXISTS (
                SELECT 1
                FROM calendar_events existing_events
                WHERE existing_events.metadata->>'legacy_event_id' = legacy_events.event_id
            )
            """,
            (DEFAULT_TIMEZONE, DEFAULT_TIMEZONE, DEFAULT_TIMEZONE, DEFAULT_TIMEZONE),
        )

    def _row_to_event(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": str(row["id"]),
            "user_id": normalize_user_id(row.get("user_id")),
            "title": row.get("title") or "",
            "description": row.get("description") or "",
            "start_at": _iso(parse_datetime(row["start_at"])),
            "end_at": _iso(parse_datetime(row["end_at"])) if row.get("end_at") else None,
            "all_day": bool(row.get("all_day")),
            "category": _normalize_category(row.get("category")),
            "location": row.get("location") or "",
            "color": row.get("color") or "",
            "metadata": _json_dict(row.get("metadata")),
            "created_at": _iso(parse_datetime(row["created_at"])) if row.get("created_at") else None,
            "updated_at": _iso(parse_datetime(row["updated_at"])) if row.get("updated_at") else None,
        }

    def list_events(
        self,
        *,
        user_id: str,
        start_at: datetime,
        end_at: datetime,
        query: str = "",
    ) -> list[dict[str, Any]]:
        normalized_user_id = normalize_user_id(user_id)
        query_text = str(query or "").strip()
        sql = """
            SELECT *
            FROM calendar_events
            WHERE user_id = %s
              AND start_at < %s
              AND COALESCE(end_at, start_at + interval '1 hour') > %s
        """
        params: list[Any] = [normalized_user_id, end_at, start_at]
        if query_text:
            sql += """
              AND (
                title ILIKE %s
                OR COALESCE(description, '') ILIKE %s
                OR COALESCE(location, '') ILIKE %s
                OR COALESCE(category, '') ILIKE %s
              )
            """
            pattern = f"%{query_text}%"
            params.extend([pattern, pattern, pattern, pattern])
        sql += " ORDER BY start_at ASC, title ASC"
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                rows = cursor.fetchall()
        return [self._row_to_event(row) for row in rows]

    def get_event(self, *, user_id: str, event_id: str) -> dict[str, Any] | None:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM calendar_events WHERE user_id = %s AND id = %s::uuid LIMIT 1",
                    (normalize_user_id(user_id), event_id),
                )
                row = cursor.fetchone()
        return self._row_to_event(row) if row else None

    def create_event(self, payload: dict[str, Any], *, timezone_name: str | None = None) -> dict[str, Any]:
        event = normalize_event_payload(payload, timezone_name=timezone_name)
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO calendar_events (
                        id,
                        user_id,
                        title,
                        description,
                        start_at,
                        end_at,
                        all_day,
                        category,
                        location,
                        color,
                        metadata,
                        created_at,
                        updated_at
                    )
                    VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW())
                    RETURNING *
                    """,
                    (
                        event["id"],
                        event["user_id"],
                        event["title"],
                        event["description"],
                        event["start_at"],
                        event["end_at"],
                        event["all_day"],
                        event["category"],
                        event["location"],
                        event["color"],
                        json.dumps(event["metadata"]),
                    ),
                )
                row = cursor.fetchone()
        return self._row_to_event(row)

    def update_event(
        self,
        *,
        user_id: str,
        event_id: str,
        updates: dict[str, Any],
        timezone_name: str | None = None,
    ) -> dict[str, Any] | None:
        existing = self.get_event(user_id=user_id, event_id=event_id)
        if not existing:
            return None
        event = normalize_event_payload(
            {**updates, "id": event_id, "user_id": user_id},
            user_id=user_id,
            timezone_name=timezone_name,
            existing=existing,
        )
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE calendar_events
                    SET
                        title = %s,
                        description = %s,
                        start_at = %s,
                        end_at = %s,
                        all_day = %s,
                        category = %s,
                        location = %s,
                        color = %s,
                        metadata = %s::jsonb,
                        updated_at = NOW()
                    WHERE user_id = %s
                      AND id = %s::uuid
                    RETURNING *
                    """,
                    (
                        event["title"],
                        event["description"],
                        event["start_at"],
                        event["end_at"],
                        event["all_day"],
                        event["category"],
                        event["location"],
                        event["color"],
                        json.dumps(event["metadata"]),
                        normalize_user_id(user_id),
                        event_id,
                    ),
                )
                row = cursor.fetchone()
        return self._row_to_event(row) if row else None

    def delete_event(self, *, user_id: str, event_id: str) -> bool:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM calendar_events WHERE user_id = %s AND id = %s::uuid",
                    (normalize_user_id(user_id), event_id),
                )
                return bool(getattr(cursor, "rowcount", 0))

    def replace_events(self, *, user_id: str, events: list[dict[str, Any]], timezone_name: str | None = None) -> int:
        normalized_user_id = normalize_user_id(user_id)
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM calendar_events WHERE user_id = %s", (normalized_user_id,))
                for raw_event in events:
                    event = normalize_event_payload(
                        raw_event,
                        user_id=normalized_user_id,
                        timezone_name=timezone_name,
                        default_duration_minutes=30,
                    )
                    cursor.execute(
                        """
                        INSERT INTO calendar_events (
                            id, user_id, title, description, start_at, end_at, all_day,
                            category, location, color, metadata, created_at, updated_at
                        )
                        VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW())
                        """,
                        (
                            event["id"],
                            event["user_id"],
                            event["title"],
                            event["description"],
                            event["start_at"],
                            event["end_at"],
                            event["all_day"],
                            event["category"],
                            event["location"],
                            event["color"],
                            json.dumps(event["metadata"]),
                        ),
                    )
        return len(events)


class LegacyCalendarManagerServiceStore:
    def _calendar_manager(self):
        try:
            from .. import calendar_manager
        except ImportError:
            import calendar_manager
        return calendar_manager

    def _events_for_range(self, user_id: str, start_at: datetime, end_at: datetime, timezone_name: str) -> list[dict[str, Any]]:
        calendar_manager = self._calendar_manager()
        events = []
        for month in _months_between(start_at.astimezone(_zone(timezone_name)), end_at.astimezone(_zone(timezone_name))):
            for legacy_event in calendar_manager.get_events(month):
                event = legacy_event_to_calendar_event(legacy_event, user_id=user_id, timezone_name=timezone_name)
                if _overlaps(event, start_at, end_at, timezone_name):
                    events.append(event)
        return sorted(events, key=lambda item: (item["start_at"], item["title"]))

    def list_events(
        self,
        *,
        user_id: str,
        start_at: datetime,
        end_at: datetime,
        query: str = "",
    ) -> list[dict[str, Any]]:
        timezone_name = DEFAULT_TIMEZONE
        return [event for event in self._events_for_range(user_id, start_at, end_at, timezone_name) if _matches_query(event, query)]

    def get_event(self, *, user_id: str, event_id: str) -> dict[str, Any] | None:
        calendar_manager = self._calendar_manager()
        for legacy_event in calendar_manager.get_event_store().load_all_events():
            if str(legacy_event.get("id")) == str(event_id):
                return legacy_event_to_calendar_event(legacy_event, user_id=user_id, timezone_name=DEFAULT_TIMEZONE)
        return None

    def create_event(self, payload: dict[str, Any], *, timezone_name: str | None = None) -> dict[str, Any]:
        calendar_manager = self._calendar_manager()
        legacy_payload = calendar_payload_to_legacy_payload(payload, timezone_name=timezone_name, default_duration_minutes=60)
        allowed_payload = {
            key: legacy_payload[key]
            for key in (
                "date",
                "time",
                "category",
                "title",
                "all_day",
                "end_time",
                "notes",
                "hours",
                "assistant_hours",
                "transport_mode",
                "transport_kilometers",
                "transport_address",
            )
            if key in legacy_payload
        }
        return legacy_event_to_calendar_event(
            calendar_manager.add_events(**allowed_payload)[0],
            user_id=payload.get("user_id"),
            timezone_name=timezone_name,
        )

    def update_event(
        self,
        *,
        user_id: str,
        event_id: str,
        updates: dict[str, Any],
        timezone_name: str | None = None,
    ) -> dict[str, Any] | None:
        calendar_manager = self._calendar_manager()
        existing = self.get_event(user_id=user_id, event_id=event_id)
        if not existing:
            return None
        merged = normalize_event_payload(updates, user_id=user_id, timezone_name=timezone_name, existing=existing)
        legacy_payload = calendar_event_to_legacy(merged, timezone_name=timezone_name)
        updated = calendar_manager.update_event(
            event_id=event_id,
            date=legacy_payload["date"],
            time=legacy_payload["time"],
            end_time=legacy_payload["end_time"],
            all_day=legacy_payload["all_day"],
            category=legacy_payload["category"],
            title=legacy_payload["title"],
            notes=legacy_payload["notes"],
            hours=legacy_payload["hours"],
            assistant_hours=legacy_payload["assistant_hours"],
            transport_mode=legacy_payload["transport_mode"],
            transport_kilometers=legacy_payload["transport_kilometers"],
            transport_address=legacy_payload["transport_address"],
        )
        return legacy_event_to_calendar_event(updated, user_id=user_id, timezone_name=timezone_name) if updated else None

    def delete_event(self, *, user_id: str, event_id: str) -> bool:
        return bool(self._calendar_manager().delete_event(event_id))


class CalendarEventCompatibilityStore:
    def __init__(self, database_url: str, connection_factory: Callable[[], Any] | None = None):
        self._store = CalendarEventsPostgresStore(database_url, connection_factory=connection_factory)

    def add_events(
        self,
        date,
        time,
        category,
        title,
        all_day=False,
        end_time="",
        notes="",
        hours=0.0,
        assistant_hours=None,
        transport_mode="",
        transport_kilometers=0.0,
        transport_address="",
        recurrence="none",
        repeat_count=0,
        user_id: str | None = None,
    ):
        events = []
        for occurrence_date in _build_occurrence_dates(str(date), str(recurrence or "none"), int(repeat_count or 0)):
            payload = {
                "user_id": normalize_user_id(user_id),
                "date": occurrence_date,
                "time": time,
                "end_time": end_time,
                "all_day": all_day,
                "category": category,
                "title": title,
                "notes": notes,
                "hours": hours,
                "assistant_hours": assistant_hours or {},
                "transport_mode": transport_mode,
                "transport_kilometers": transport_kilometers,
                "transport_address": transport_address,
            }
            created = self._store.create_event(
                normalize_event_payload(
                    payload,
                    user_id=user_id,
                    timezone_name=DEFAULT_TIMEZONE,
                    default_duration_minutes=30,
                ),
                timezone_name=DEFAULT_TIMEZONE,
            )
            events.append(calendar_event_to_legacy(created, DEFAULT_TIMEZONE))
        return sorted(events, key=lambda item: (item["date"], 0 if item.get("all_day") else 1, item.get("time", ""), item["title"]))

    def get_events(self, month: str, user_id: str | None = None):
        start_at, end_at = _month_range(month, DEFAULT_TIMEZONE)
        events = self._store.list_events(user_id=normalize_user_id(user_id), start_at=start_at, end_at=end_at)
        return [calendar_event_to_legacy(event, DEFAULT_TIMEZONE) for event in events]

    def update_event(
        self,
        event_id: str,
        date: str,
        time: str,
        category: str,
        title: str,
        all_day=False,
        end_time="",
        notes="",
        hours=0.0,
        assistant_hours=None,
        transport_mode="",
        transport_kilometers=0.0,
        transport_address="",
        user_id: str | None = None,
    ):
        updates = {
            "date": date,
            "time": time,
            "end_time": end_time,
            "all_day": all_day,
            "category": category,
            "title": title,
            "notes": notes,
            "hours": hours,
            "assistant_hours": assistant_hours or {},
            "transport_mode": transport_mode,
            "transport_kilometers": transport_kilometers,
            "transport_address": transport_address,
        }
        updated = self._store.update_event(
            user_id=normalize_user_id(user_id),
            event_id=event_id,
            updates=updates,
            timezone_name=DEFAULT_TIMEZONE,
        )
        return calendar_event_to_legacy(updated, DEFAULT_TIMEZONE) if updated else None

    def delete_event(self, event_id: str, user_id: str | None = None):
        return self._store.delete_event(user_id=normalize_user_id(user_id), event_id=event_id)

    def load_all_events(self):
        start_at = datetime(1900, 1, 1, tzinfo=_zone(DEFAULT_TIMEZONE))
        end_at = datetime(2100, 1, 1, tzinfo=_zone(DEFAULT_TIMEZONE))
        events = self._store.list_events(user_id="default", start_at=start_at, end_at=end_at)
        return [calendar_event_to_legacy(event, DEFAULT_TIMEZONE) for event in events]

    def replace_all_events(self, events):
        payloads = []
        for event in events:
            payload = dict(event)
            payload.setdefault("user_id", "default")
            payloads.append(payload)
        return self._store.replace_events(user_id="default", events=payloads, timezone_name=DEFAULT_TIMEZONE)


def _build_occurrence_dates(date_value: str, recurrence: str, repeat_count: int) -> list[str]:
    base_date = datetime.strptime(date_value, "%Y-%m-%d").date()
    normalized_recurrence = str(recurrence or "none").strip().lower()
    dates = [base_date]
    for occurrence_index in range(1, max(0, int(repeat_count or 0)) + 1):
        if normalized_recurrence == "weekly":
            dates.append(base_date + timedelta(weeks=occurrence_index))
        elif normalized_recurrence == "biweekly":
            dates.append(base_date + timedelta(weeks=2 * occurrence_index))
        elif normalized_recurrence == "monthly":
            month_index = base_date.month - 1 + occurrence_index
            year = base_date.year + month_index // 12
            month = month_index % 12 + 1
            day = min(base_date.day, monthrange(year, month)[1])
            dates.append(date(year, month, day))
    return [item.isoformat() for item in dates]


def legacy_event_to_calendar_event(
    legacy_event: dict[str, Any],
    *,
    user_id: str | None = None,
    timezone_name: str | None = None,
) -> dict[str, Any]:
    tz_name = normalize_timezone(timezone_name)
    all_day = bool(legacy_event.get("all_day")) or not str(legacy_event.get("time", "")).strip()
    if all_day:
        start_at = datetime.combine(_parse_date(legacy_event["date"]), time.min, tzinfo=_zone(tz_name))
        end_at = start_at + timedelta(days=1)
    else:
        start_at = _combine_local(legacy_event["date"], legacy_event.get("time", "09:00"), tz_name)
        end_time = str(legacy_event.get("end_time") or "").strip()
        end_at = _combine_local(legacy_event["date"], end_time, tz_name) if end_time else start_at + timedelta(minutes=30)
    category = _normalize_category(legacy_event.get("category"))
    metadata = _normalize_metadata(legacy_event, category)
    return {
        "id": str(legacy_event.get("id") or uuid.uuid4()),
        "user_id": normalize_user_id(user_id or legacy_event.get("user_id")),
        "title": str(legacy_event.get("title") or "").strip(),
        "description": str(legacy_event.get("description") or legacy_event.get("notes") or "").strip(),
        "start_at": _iso(start_at),
        "end_at": _iso(end_at),
        "all_day": all_day,
        "category": category,
        "location": str(legacy_event.get("location") or legacy_event.get("transport_address") or "").strip(),
        "color": str(legacy_event.get("color") or "").strip(),
        "metadata": metadata,
    }


def calendar_payload_to_legacy_payload(
    payload: dict[str, Any],
    *,
    timezone_name: str | None = None,
    default_duration_minutes: int = 60,
) -> dict[str, Any]:
    event = normalize_event_payload(
        payload,
        user_id=payload.get("user_id"),
        timezone_name=timezone_name,
        default_duration_minutes=default_duration_minutes,
    )
    return calendar_event_to_legacy(event, timezone_name=timezone_name)


def get_calendar_event_store():
    backend = str(os.environ.get("IV_AGENT_STORAGE_BACKEND", "auto") or "auto").strip().lower()
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if backend == "local" or not database_url:
        cache_key = ("local", "")
        if cache_key not in _CALENDAR_STORE_CACHE:
            _CALENDAR_STORE_CACHE[cache_key] = LegacyCalendarManagerServiceStore()
        return _CALENDAR_STORE_CACHE[cache_key]

    cache_key = ("postgres", database_url)
    if cache_key not in _CALENDAR_STORE_CACHE:
        _CALENDAR_STORE_CACHE[cache_key] = CalendarEventsPostgresStore(database_url)
    return _CALENDAR_STORE_CACHE[cache_key]


def list_calendar_events(
    *,
    user_id: str | None = None,
    start_at: str,
    end_at: str,
    query: str = "",
    timezone_name: str | None = None,
) -> dict[str, Any]:
    tz_name = normalize_timezone(timezone_name)
    start_dt = parse_datetime(start_at, tz_name)
    end_dt = parse_datetime(end_at, tz_name)
    if end_dt <= start_dt:
        raise ValueError("end_at must be later than start_at")
    events = get_calendar_event_store().list_events(
        user_id=normalize_user_id(user_id),
        start_at=start_dt,
        end_at=end_dt,
        query=query,
    )
    legacy_events = [calendar_event_to_legacy(event, tz_name) for event in events]
    return {
        "user_id": normalize_user_id(user_id),
        "start_at": _iso(start_dt),
        "end_at": _iso(end_dt),
        "timezone": tz_name,
        "events": legacy_events,
        "calendar_events": events,
        "count": len(events),
    }


def create_calendar_event(
    payload: dict[str, Any],
    *,
    user_id: str | None = None,
    timezone_name: str | None = None,
    default_duration_minutes: int = 60,
) -> dict[str, Any]:
    event_payload = normalize_event_payload(
        {**payload, "user_id": user_id or payload.get("user_id")},
        user_id=user_id,
        timezone_name=timezone_name,
        default_duration_minutes=default_duration_minutes,
    )
    created = get_calendar_event_store().create_event(event_payload, timezone_name=timezone_name)
    return {
        "event": calendar_event_to_legacy(created, timezone_name),
        "calendar_event": created,
    }


def update_calendar_event(
    event_id: str,
    updates: dict[str, Any],
    *,
    user_id: str | None = None,
    timezone_name: str | None = None,
) -> dict[str, Any] | None:
    updated = get_calendar_event_store().update_event(
        user_id=normalize_user_id(user_id),
        event_id=str(event_id or "").strip(),
        updates=updates,
        timezone_name=timezone_name,
    )
    if not updated:
        return None
    return {
        "event": calendar_event_to_legacy(updated, timezone_name),
        "calendar_event": updated,
    }


def delete_calendar_event(event_id: str, *, user_id: str | None = None) -> bool:
    return bool(get_calendar_event_store().delete_event(user_id=normalize_user_id(user_id), event_id=str(event_id or "").strip()))


def count_calendar_events(
    *,
    user_id: str | None = None,
    start_at: str,
    end_at: str,
    query: str = "",
    timezone_name: str | None = None,
) -> dict[str, Any]:
    listed = list_calendar_events(
        user_id=user_id,
        start_at=start_at,
        end_at=end_at,
        query=query,
        timezone_name=timezone_name,
    )
    return {
        "user_id": listed["user_id"],
        "start_at": listed["start_at"],
        "end_at": listed["end_at"],
        "timezone": listed["timezone"],
        "query": query,
        "count": listed["count"],
        "events": listed["events"],
    }


def check_availability(
    *,
    user_id: str | None = None,
    start_at: str,
    end_at: str,
    timezone_name: str | None = None,
) -> dict[str, Any]:
    listed = list_calendar_events(
        user_id=user_id,
        start_at=start_at,
        end_at=end_at,
        timezone_name=timezone_name,
    )
    return {
        "user_id": listed["user_id"],
        "start_at": listed["start_at"],
        "end_at": listed["end_at"],
        "timezone": listed["timezone"],
        "available": listed["count"] == 0,
        "conflicts": listed["events"],
    }


def find_matching_calendar_events(
    *,
    user_id: str | None = None,
    start_at: str,
    end_at: str,
    query: str,
    timezone_name: str | None = None,
) -> list[dict[str, Any]]:
    return list_calendar_events(
        user_id=user_id,
        start_at=start_at,
        end_at=end_at,
        query=query,
        timezone_name=timezone_name,
    )["events"]
