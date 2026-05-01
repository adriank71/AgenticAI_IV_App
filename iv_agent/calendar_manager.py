import json
import os
import uuid
from calendar import monthrange
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Protocol

try:
    from .storage import _connect_postgres
except ImportError:
    from storage import _connect_postgres


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
CALENDAR_PATH = os.path.join(DATA_DIR, "calendar.json")
VALID_CATEGORIES = {"assistant", "transport", "other"}
CATEGORY_ALIASES = {
    "tixi": "transport",
    "therapy": "other",
}
ASSISTANT_HOUR_FIELDS = (
    "koerperpflege",
    "mahlzeiten_eingeben",
    "mahlzeiten_zubereiten",
    "begleitung_therapie",
)
VALID_RECURRENCE_PATTERNS = {"none", "weekly", "biweekly", "monthly"}
_EVENT_STORE_CACHE: dict[tuple[str, str, str, str], Any] = {}


class EventStore(Protocol):
    def add_events(
        self,
        date: str,
        time: str,
        category: str,
        title: str,
        all_day: bool = False,
        end_time: str = "",
        notes: str = "",
        hours: float = 0.0,
        assistant_hours: Dict[str, float] | None = None,
        transport_mode: str = "",
        transport_kilometers: float = 0.0,
        transport_address: str = "",
        recurrence: str = "none",
        repeat_count: int = 0,
    ) -> List[Dict]:
        ...

    def get_events(self, month: str) -> List[Dict]:
        ...

    def update_event(
        self,
        event_id: str,
        date: str,
        time: str,
        category: str,
        title: str,
        all_day: bool = False,
        end_time: str = "",
        notes: str = "",
        hours: float = 0.0,
        assistant_hours: Dict[str, float] | None = None,
        transport_mode: str = "",
        transport_kilometers: float = 0.0,
        transport_address: str = "",
    ) -> Dict | None:
        ...

    def delete_event(self, event_id: str) -> bool:
        ...

    def load_all_events(self) -> List[Dict]:
        ...

    def replace_all_events(self, events: List[Dict]) -> int:
        ...


def _default_end_time(start_time: str) -> str:
    start_dt = datetime.strptime(start_time, "%H:%M")
    return (start_dt + timedelta(minutes=30)).strftime("%H:%M")


def _normalize_end_time(start_time: str, end_time: str | None = None) -> str:
    normalized_end_time = str(end_time or "").strip()
    if not normalized_end_time:
        return _default_end_time(start_time)
    return normalized_end_time


def _normalize_category(category: str) -> str:
    normalized = str(category or "").strip().lower()
    return CATEGORY_ALIASES.get(normalized, normalized)


def _normalize_assistant_hours(raw_hours: Dict | None, fallback_hours: float = 0.0) -> Dict[str, float]:
    normalized = {field: 0.0 for field in ASSISTANT_HOUR_FIELDS}
    if isinstance(raw_hours, dict):
        for field in ASSISTANT_HOUR_FIELDS:
            value = raw_hours.get(field, 0.0)
            normalized[field] = float(value or 0.0)
    elif fallback_hours > 0:
        normalized[ASSISTANT_HOUR_FIELDS[0]] = float(fallback_hours)
    return normalized


def _assistant_total_hours(event: Dict) -> float:
    assistant_hours = event.get("assistant_hours")
    if isinstance(assistant_hours, dict):
        total = sum(float(assistant_hours.get(field, 0.0) or 0.0) for field in ASSISTANT_HOUR_FIELDS)
        if total > 0:
            return round(total, 2)
    return round(float(event.get("hours", 0.0) or 0.0), 2)


def _normalize_event(event: Dict) -> Dict:
    normalized = dict(event)
    normalized["category"] = _normalize_category(normalized.get("category", "other"))
    normalized["all_day"] = bool(normalized.get("all_day")) or not str(normalized.get("time", "")).strip()
    if normalized["all_day"]:
        normalized["time"] = ""
        normalized["end_time"] = ""
    else:
        normalized["end_time"] = _normalize_end_time(normalized.get("time", "00:00"), normalized.get("end_time"))

    assistant_hours = normalized.get("assistant_hours")
    if isinstance(assistant_hours, str):
        try:
            assistant_hours = json.loads(assistant_hours)
        except json.JSONDecodeError:
            assistant_hours = {}

    normalized["assistant_hours"] = _normalize_assistant_hours(
        assistant_hours,
        float(normalized.get("hours", 0.0) or 0.0) if normalized["category"] == "assistant" else 0.0,
    )
    normalized["transport_mode"] = str(normalized.get("transport_mode") or "").strip().lower()
    normalized["transport_kilometers"] = round(float(normalized.get("transport_kilometers", 0.0) or 0.0), 2)
    normalized["transport_address"] = str(normalized.get("transport_address") or "").strip()
    normalized["hours"] = _assistant_total_hours(normalized) if normalized["category"] == "assistant" else 0.0
    normalized["title"] = str(normalized.get("title") or "").strip()
    normalized["notes"] = str(normalized.get("notes") or "").strip()
    return normalized


def _validate_event_inputs(
    date: str,
    time: str,
    end_time: str,
    all_day: bool,
    category: str,
    hours: float,
    assistant_hours: Dict[str, float],
    transport_kilometers: float,
    recurrence: str,
    repeat_count: int,
) -> None:
    datetime.strptime(date, "%Y-%m-%d")
    if not all_day:
        start_dt = datetime.strptime(time, "%H:%M")
        end_dt = datetime.strptime(end_time, "%H:%M")
        if end_dt <= start_dt:
            raise ValueError("End time must be later than start time")
    if category not in VALID_CATEGORIES:
        raise ValueError(f"Unsupported category: {category}")
    if hours < 0:
        raise ValueError("Hours cannot be negative")
    if transport_kilometers < 0:
        raise ValueError("Transport kilometers cannot be negative")
    for field, value in assistant_hours.items():
        if value < 0:
            raise ValueError(f"Assistant hours cannot be negative: {field}")
    if recurrence not in VALID_RECURRENCE_PATTERNS:
        raise ValueError(f"Unsupported recurrence pattern: {recurrence}")
    if repeat_count < 0:
        raise ValueError("repeat_count cannot be negative")


def _add_months(date_value, months: int):
    target_month_index = date_value.month - 1 + months
    target_year = date_value.year + target_month_index // 12
    target_month = target_month_index % 12 + 1
    target_day = min(date_value.day, monthrange(target_year, target_month)[1])
    return date_value.replace(year=target_year, month=target_month, day=target_day)


def _build_occurrence_dates(date_value: str, recurrence: str, repeat_count: int) -> List[str]:
    base_date = datetime.strptime(date_value, "%Y-%m-%d").date()
    dates = [base_date]

    for occurrence_index in range(1, repeat_count + 1):
        if recurrence == "weekly":
            dates.append(base_date + timedelta(weeks=occurrence_index))
        elif recurrence == "biweekly":
            dates.append(base_date + timedelta(weeks=2 * occurrence_index))
        elif recurrence == "monthly":
            dates.append(_add_months(base_date, occurrence_index))

    return [item.isoformat() for item in dates]


def _event_sort_key(item: Dict):
    return item["date"], 0 if item.get("all_day") else 1, item.get("time", ""), item["title"]


def _build_event_record(
    *,
    event_id: str | None,
    date: str,
    time: str,
    category: str,
    title: str,
    all_day: bool = False,
    end_time: str = "",
    notes: str = "",
    hours: float = 0.0,
    assistant_hours: Dict[str, float] | None = None,
    transport_mode: str = "",
    transport_kilometers: float = 0.0,
    transport_address: str = "",
    recurrence: str = "none",
    repeat_count: int = 0,
) -> Dict:
    normalized_category = _normalize_category(category)
    normalized_all_day = bool(all_day)
    normalized_time = "" if normalized_all_day else str(time or "").strip()
    normalized_end_time = "" if normalized_all_day else _normalize_end_time(normalized_time, end_time)
    normalized_assistant_hours = _normalize_assistant_hours(assistant_hours, float(hours or 0.0))
    normalized_transport_mode = str(transport_mode or "").strip().lower()
    normalized_transport_kilometers = round(float(transport_kilometers or 0.0), 2)
    normalized_transport_address = str(transport_address or "").strip()
    normalized_recurrence = str(recurrence or "none").strip().lower()
    normalized_repeat_count = int(repeat_count or 0)
    total_hours = round(sum(normalized_assistant_hours.values()), 2) if normalized_category == "assistant" else 0.0

    _validate_event_inputs(
        date,
        normalized_time,
        normalized_end_time,
        normalized_all_day,
        normalized_category,
        total_hours,
        normalized_assistant_hours,
        normalized_transport_kilometers,
        normalized_recurrence,
        normalized_repeat_count,
    )

    return {
        "id": event_id or str(uuid.uuid4()),
        "date": date,
        "time": normalized_time,
        "end_time": normalized_end_time,
        "all_day": normalized_all_day,
        "category": normalized_category,
        "title": str(title or "").strip(),
        "notes": str(notes or "").strip(),
        "hours": total_hours,
        "assistant_hours": normalized_assistant_hours,
        "transport_mode": normalized_transport_mode,
        "transport_kilometers": normalized_transport_kilometers,
        "transport_address": normalized_transport_address,
    }


class JsonEventStore:
    def __init__(self, data_dir: str, calendar_path: str):
        self._data_dir = data_dir
        self._calendar_path = calendar_path

    def _ensure_storage(self) -> None:
        os.makedirs(self._data_dir, exist_ok=True)
        if not os.path.exists(self._calendar_path):
            with open(self._calendar_path, "w", encoding="utf-8") as file:
                json.dump([], file)

    def load_all_events(self) -> List[Dict]:
        self._ensure_storage()
        with open(self._calendar_path, "r", encoding="utf-8") as file:
            return [_normalize_event(event) for event in json.load(file)]

    def replace_all_events(self, events: List[Dict]) -> int:
        normalized_events = [_normalize_event(event) for event in events]
        normalized_events.sort(key=_event_sort_key)
        self._ensure_storage()
        with open(self._calendar_path, "w", encoding="utf-8") as file:
            json.dump(normalized_events, file, indent=2)
        return len(normalized_events)

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
    ):
        events = self.load_all_events()
        created_events = []

        for occurrence_date in _build_occurrence_dates(date, str(recurrence or "none").strip().lower(), int(repeat_count or 0)):
            event = _build_event_record(
                event_id=None,
                date=occurrence_date,
                time=time,
                category=category,
                title=title,
                all_day=all_day,
                end_time=end_time,
                notes=notes,
                hours=hours,
                assistant_hours=assistant_hours,
                transport_mode=transport_mode,
                transport_kilometers=transport_kilometers,
                transport_address=transport_address,
            )
            created_events.append(event)
            events.append(event)

        events.sort(key=_event_sort_key)
        self.replace_all_events(events)
        return created_events

    def get_events(self, month: str):
        datetime.strptime(month, "%Y-%m")
        events = [event for event in self.load_all_events() if event["date"].startswith(month)]
        return sorted(events, key=_event_sort_key)

    def delete_event(self, event_id: str):
        events = self.load_all_events()
        updated_events = [event for event in events if event["id"] != event_id]
        if len(updated_events) == len(events):
            return False
        self.replace_all_events(updated_events)
        return True

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
    ) -> Dict | None:
        events = self.load_all_events()
        target_index = next((index for index, event in enumerate(events) if event["id"] == event_id), None)
        if target_index is None:
            return None

        updated_event = _build_event_record(
            event_id=event_id,
            date=date,
            time=time,
            category=category,
            title=title,
            all_day=all_day,
            end_time=end_time,
            notes=notes,
            hours=hours,
            assistant_hours=assistant_hours,
            transport_mode=transport_mode,
            transport_kilometers=transport_kilometers,
            transport_address=transport_address,
        )
        events[target_index] = updated_event
        events.sort(key=_event_sort_key)
        self.replace_all_events(events)
        return updated_event


class PostgresEventStore:
    def __init__(
        self,
        database_url: str,
        connection_factory: Callable[[], Any] | None = None,
    ):
        self._database_url = database_url
        self._connection_factory = connection_factory or (lambda: _connect_postgres(database_url))
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS events (
                        event_id TEXT PRIMARY KEY,
                        event_date DATE NOT NULL,
                        start_time TEXT NOT NULL DEFAULT '',
                        end_time TEXT NOT NULL DEFAULT '',
                        all_day BOOLEAN NOT NULL DEFAULT FALSE,
                        category TEXT NOT NULL,
                        title TEXT NOT NULL,
                        notes TEXT NOT NULL DEFAULT '',
                        hours DOUBLE PRECISION NOT NULL DEFAULT 0,
                        assistant_hours JSONB NOT NULL DEFAULT '{}'::jsonb,
                        transport_mode TEXT NOT NULL DEFAULT '',
                        transport_kilometers DOUBLE PRECISION NOT NULL DEFAULT 0,
                        transport_address TEXT NOT NULL DEFAULT '',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS events_month_idx ON events (event_date, start_time)"
                )

    def _row_to_event(self, row: Dict[str, Any]) -> Dict:
        event_date = row["event_date"]
        return _normalize_event(
            {
                "id": row["event_id"],
                "date": event_date.isoformat() if hasattr(event_date, "isoformat") else str(event_date),
                "time": row.get("start_time", "") or "",
                "end_time": row.get("end_time", "") or "",
                "all_day": bool(row.get("all_day")),
                "category": row.get("category", "other"),
                "title": row.get("title", ""),
                "notes": row.get("notes", ""),
                "hours": float(row.get("hours", 0.0) or 0.0),
                "assistant_hours": row.get("assistant_hours") or {},
                "transport_mode": row.get("transport_mode", "") or "",
                "transport_kilometers": float(row.get("transport_kilometers", 0.0) or 0.0),
                "transport_address": row.get("transport_address", "") or "",
            }
        )

    def _upsert_event(self, cursor, event: Dict[str, Any]) -> None:
        cursor.execute(
            """
            INSERT INTO events (
                event_id,
                event_date,
                start_time,
                end_time,
                all_day,
                category,
                title,
                notes,
                hours,
                assistant_hours,
                transport_mode,
                transport_kilometers,
                transport_address,
                created_at,
                updated_at
            )
            VALUES (
                %s,
                %s::date,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s::jsonb,
                %s,
                %s,
                %s,
                NOW(),
                NOW()
            )
            ON CONFLICT (event_id)
            DO UPDATE SET
                event_date = EXCLUDED.event_date,
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time,
                all_day = EXCLUDED.all_day,
                category = EXCLUDED.category,
                title = EXCLUDED.title,
                notes = EXCLUDED.notes,
                hours = EXCLUDED.hours,
                assistant_hours = EXCLUDED.assistant_hours,
                transport_mode = EXCLUDED.transport_mode,
                transport_kilometers = EXCLUDED.transport_kilometers,
                transport_address = EXCLUDED.transport_address,
                updated_at = NOW()
            """,
            (
                event["id"],
                event["date"],
                event["time"],
                event["end_time"],
                event["all_day"],
                event["category"],
                event["title"],
                event["notes"],
                event["hours"],
                json.dumps(event["assistant_hours"]),
                event["transport_mode"],
                event["transport_kilometers"],
                event["transport_address"],
            ),
        )

    def load_all_events(self) -> List[Dict]:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        event_id,
                        event_date,
                        start_time,
                        end_time,
                        all_day,
                        category,
                        title,
                        notes,
                        hours,
                        assistant_hours,
                        transport_mode,
                        transport_kilometers,
                        transport_address
                    FROM events
                    ORDER BY
                        event_date ASC,
                        CASE WHEN all_day THEN 0 ELSE 1 END ASC,
                        start_time ASC,
                        title ASC
                    """
                )
                rows = cursor.fetchall()
        return [self._row_to_event(row) for row in rows]

    def replace_all_events(self, events: List[Dict]) -> int:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM events")
                for event in sorted([_normalize_event(event) for event in events], key=_event_sort_key):
                    self._upsert_event(cursor, event)
        return len(events)

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
    ):
        created_events = []
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                for occurrence_date in _build_occurrence_dates(
                    date,
                    str(recurrence or "none").strip().lower(),
                    int(repeat_count or 0),
                ):
                    event = _build_event_record(
                        event_id=None,
                        date=occurrence_date,
                        time=time,
                        category=category,
                        title=title,
                        all_day=all_day,
                        end_time=end_time,
                        notes=notes,
                        hours=hours,
                        assistant_hours=assistant_hours,
                        transport_mode=transport_mode,
                        transport_kilometers=transport_kilometers,
                        transport_address=transport_address,
                    )
                    self._upsert_event(cursor, event)
                    created_events.append(event)
        return sorted(created_events, key=_event_sort_key)

    def get_events(self, month: str):
        datetime.strptime(month, "%Y-%m")
        start_date = f"{month}-01"
        next_month = _add_months(datetime.strptime(start_date, "%Y-%m-%d").date(), 1).isoformat()
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        event_id,
                        event_date,
                        start_time,
                        end_time,
                        all_day,
                        category,
                        title,
                        notes,
                        hours,
                        assistant_hours,
                        transport_mode,
                        transport_kilometers,
                        transport_address
                    FROM events
                    WHERE event_date >= %s::date
                      AND event_date < %s::date
                    ORDER BY
                        event_date ASC,
                        CASE WHEN all_day THEN 0 ELSE 1 END ASC,
                        start_time ASC,
                        title ASC
                    """,
                    (start_date, next_month),
                )
                rows = cursor.fetchall()
        return [self._row_to_event(row) for row in rows]

    def delete_event(self, event_id: str):
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM events WHERE event_id = %s", (event_id,))
                return bool(getattr(cursor, "rowcount", 0))

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
    ) -> Dict | None:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT event_id FROM events WHERE event_id = %s", (event_id,))
                if not cursor.fetchone():
                    return None

                updated_event = _build_event_record(
                    event_id=event_id,
                    date=date,
                    time=time,
                    category=category,
                    title=title,
                    all_day=all_day,
                    end_time=end_time,
                    notes=notes,
                    hours=hours,
                    assistant_hours=assistant_hours,
                    transport_mode=transport_mode,
                    transport_kilometers=transport_kilometers,
                    transport_address=transport_address,
                )
                self._upsert_event(cursor, updated_event)
        return updated_event


def get_event_store() -> EventStore:
    backend = str(os.environ.get("IV_AGENT_STORAGE_BACKEND", "auto") or "auto").strip().lower()
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if backend == "local" or not database_url:
        cache_key = ("local", "", DATA_DIR, CALENDAR_PATH)
        if cache_key not in _EVENT_STORE_CACHE:
            _EVENT_STORE_CACHE[cache_key] = JsonEventStore(DATA_DIR, CALENDAR_PATH)
        return _EVENT_STORE_CACHE[cache_key]

    cache_key = ("postgres", database_url, DATA_DIR, CALENDAR_PATH)
    if cache_key not in _EVENT_STORE_CACHE:
        _EVENT_STORE_CACHE[cache_key] = PostgresEventStore(database_url)
    return _EVENT_STORE_CACHE[cache_key]


def add_events(
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
):
    return get_event_store().add_events(
        date=date,
        time=time,
        category=category,
        title=title,
        all_day=all_day,
        end_time=end_time,
        notes=notes,
        hours=hours,
        assistant_hours=assistant_hours,
        transport_mode=transport_mode,
        transport_kilometers=transport_kilometers,
        transport_address=transport_address,
        recurrence=recurrence,
        repeat_count=repeat_count,
    )


def add_event(
    date,
    time,
    category,
    title,
    notes="",
    hours=0.0,
    assistant_hours=None,
    end_time="",
    all_day=False,
    transport_mode="",
    transport_kilometers=0.0,
    transport_address="",
):
    return add_events(
        date=date,
        time=time,
        all_day=all_day,
        end_time=end_time,
        category=category,
        title=title,
        notes=notes,
        hours=hours,
        assistant_hours=assistant_hours,
        transport_mode=transport_mode,
        transport_kilometers=transport_kilometers,
        transport_address=transport_address,
        recurrence="none",
        repeat_count=0,
    )[0]


def get_events(month: str):
    return get_event_store().get_events(month)


def get_assistant_hours_breakdown(month: str) -> Dict[str, float]:
    return get_assistant_hours_breakdown_for_events(get_events(month))


def get_assistant_hours_breakdown_for_events(events: List[Dict]) -> Dict[str, float]:
    totals = {field: 0.0 for field in ASSISTANT_HOUR_FIELDS}
    for event in events:
        if event["category"] != "assistant":
            continue
        assistant_hours = _normalize_assistant_hours(event.get("assistant_hours"), event.get("hours", 0.0))
        for field in ASSISTANT_HOUR_FIELDS:
            totals[field] += assistant_hours.get(field, 0.0)
    return {field: round(value, 2) for field, value in totals.items()}


def get_assistant_hours(month: str):
    return get_assistant_hours_for_events(get_events(month))


def get_assistant_hours_for_events(events: List[Dict]):
    return round(sum(_assistant_total_hours(event) for event in events if event["category"] == "assistant"), 2)


def _assistant_breakdown_suffix(event: Dict) -> str:
    if event["category"] != "assistant":
        return ""

    assistant_hours = event.get("assistant_hours") or {}
    entries = []
    for field in ASSISTANT_HOUR_FIELDS:
        value = float(assistant_hours.get(field, 0.0) or 0.0)
        if value > 0:
            entries.append(f"{field}: {value:.2f}")

    if not entries and event.get("hours", 0.0):
        entries.append(f"legacy total: {float(event['hours']):.2f}")

    total_hours = _assistant_total_hours(event)
    if not entries:
        return f" | hours: {total_hours:.2f}"
    return f" | hours: {total_hours:.2f} ({', '.join(entries)})"


def display_month(month: str):
    events = get_events(month)
    if not events:
        print(f"No events found for {month}")
        return

    grouped_events = defaultdict(list)
    for event in events:
        grouped_events[event["date"]].append(event)

    print(f"\nMonth view for {month}")
    for date in sorted(grouped_events):
        day_label = datetime.strptime(date, "%Y-%m-%d").strftime("%A, %Y-%m-%d")
        print(day_label)
        for event in sorted(grouped_events[date], key=_event_sort_key):
            hours_suffix = _assistant_breakdown_suffix(event)
            notes_suffix = f" | notes: {event['notes']}" if event.get("notes") else ""
            time_label = "All day" if event.get("all_day") else f"{event['time']}-{event['end_time']}"
            print(
                f"  {time_label} | {event['category']} | {event['title']}"
                f" | id: {event['id']}{hours_suffix}{notes_suffix}"
            )


def delete_event(event_id: str):
    return get_event_store().delete_event(event_id)


def update_event(
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
) -> Dict | None:
    return get_event_store().update_event(
        event_id=event_id,
        date=date,
        time=time,
        category=category,
        title=title,
        all_day=all_day,
        end_time=end_time,
        notes=notes,
        hours=hours,
        assistant_hours=assistant_hours,
        transport_mode=transport_mode,
        transport_kilometers=transport_kilometers,
        transport_address=transport_address,
    )


def export_month_plan(month: str):
    events = get_events(month)
    total_hours = get_assistant_hours_for_events(events)
    lines = [f"Monthly plan for {month}", f"Assistant hours: {total_hours:.2f}", ""]

    if not events:
        lines.append("No events scheduled.")
        return "\n".join(lines)

    current_date = None
    for event in events:
        if event["date"] != current_date:
            current_date = event["date"]
            lines.append(current_date)
        time_label = "All day" if event.get("all_day") else f"{event['time']}-{event['end_time']}"
        line = f"- {time_label} [{event['category']}] {event['title']}"
        if event["category"] == "assistant":
            line += _assistant_breakdown_suffix(event)
        if event.get("notes"):
            line += f" - {event['notes']}"
        lines.append(line)

    return "\n".join(lines)
