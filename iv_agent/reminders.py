import json
import os
import uuid
from calendar import monthrange
from datetime import date, datetime, time, timedelta
from typing import Any, Callable, Dict, List, Optional, Protocol

try:
    from .storage import _connect_postgres
except ImportError:
    from storage import _connect_postgres

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore
    ZoneInfoNotFoundError = Exception  # type: ignore


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
REMINDERS_PATH = os.path.join(DATA_DIR, "reminders.json")

VALID_ACTIONS = {"notify", "generate_assistenzbeitrag"}
VALID_SCHEDULES = {"month_end", "weekly_sun", "weekly_mon", "daily", "once"}
DEFAULT_TIMEZONE = "Europe/Berlin"


class ReminderStore(Protocol):
    def load_all(self) -> List[Dict[str, Any]]:
        ...

    def replace_all(self, items: List[Dict[str, Any]]) -> int:
        ...


def _now(tz_name: str | None = None) -> datetime:
    tz = _resolve_tz(tz_name)
    return datetime.now(tz)


def _resolve_tz(tz_name: str | None):
    candidate = str(tz_name or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    if ZoneInfo is None:
        return None
    try:
        return ZoneInfo(candidate)
    except ZoneInfoNotFoundError:
        return ZoneInfo(DEFAULT_TIMEZONE)


def _ensure_json_storage() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(REMINDERS_PATH):
        with open(REMINDERS_PATH, "w", encoding="utf-8") as file:
            json.dump([], file)


def _load_json_all() -> List[Dict[str, Any]]:
    _ensure_json_storage()
    try:
        with open(REMINDERS_PATH, "r", encoding="utf-8") as file:
            data = json.load(file)
            return data if isinstance(data, list) else []
    except (json.JSONDecodeError, OSError):
        return []


def _save_json_all(items: List[Dict[str, Any]]) -> None:
    _ensure_json_storage()
    with open(REMINDERS_PATH, "w", encoding="utf-8") as file:
        json.dump(items, file, indent=2)


class JsonReminderStore:
    def load_all(self) -> List[Dict[str, Any]]:
        return _load_json_all()

    def replace_all(self, items: List[Dict[str, Any]]) -> int:
        _save_json_all(items)
        return len(items)


class PostgresReminderStore:
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
                    CREATE TABLE IF NOT EXISTS reminders (
                        reminder_id TEXT PRIMARY KEY,
                        title TEXT NOT NULL,
                        action TEXT NOT NULL,
                        schedule TEXT NOT NULL,
                        note TEXT NOT NULL DEFAULT '',
                        run_time TEXT NOT NULL DEFAULT '09:00',
                        run_date TEXT NOT NULL DEFAULT '',
                        timezone TEXT NOT NULL DEFAULT 'Europe/Berlin',
                        status TEXT NOT NULL DEFAULT 'active',
                        last_run_at TIMESTAMPTZ,
                        next_run_at TIMESTAMPTZ,
                        last_run_status TEXT,
                        last_run_message TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS reminders_next_run_idx ON reminders (status, next_run_at)"
                )

    def _row_to_reminder(self, row: Dict[str, Any]) -> Dict[str, Any]:
        def iso(value: Any) -> str | None:
            if value is None:
                return None
            if hasattr(value, "isoformat"):
                return value.isoformat()
            return str(value)

        return {
            "id": row["reminder_id"],
            "created_at": iso(row.get("created_at")),
            "last_run_at": iso(row.get("last_run_at")),
            "next_run_at": iso(row.get("next_run_at")),
            "status": row.get("status") or "active",
            "title": row.get("title") or "",
            "action": row.get("action") or "notify",
            "schedule": row.get("schedule") or "month_end",
            "note": row.get("note") or "",
            "run_time": row.get("run_time") or "09:00",
            "run_date": row.get("run_date") or "",
            "timezone": row.get("timezone") or DEFAULT_TIMEZONE,
            "last_run_status": row.get("last_run_status"),
            "last_run_message": row.get("last_run_message"),
        }

    def _upsert_reminder(self, cursor, item: Dict[str, Any]) -> None:
        cursor.execute(
            """
            INSERT INTO reminders (
                reminder_id,
                title,
                action,
                schedule,
                note,
                run_time,
                run_date,
                timezone,
                status,
                last_run_at,
                next_run_at,
                last_run_status,
                last_run_message,
                created_at,
                updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s::timestamptz,
                %s::timestamptz,
                %s, %s,
                %s::timestamptz,
                NOW()
            )
            ON CONFLICT (reminder_id)
            DO UPDATE SET
                title = EXCLUDED.title,
                action = EXCLUDED.action,
                schedule = EXCLUDED.schedule,
                note = EXCLUDED.note,
                run_time = EXCLUDED.run_time,
                run_date = EXCLUDED.run_date,
                timezone = EXCLUDED.timezone,
                status = EXCLUDED.status,
                last_run_at = EXCLUDED.last_run_at,
                next_run_at = EXCLUDED.next_run_at,
                last_run_status = EXCLUDED.last_run_status,
                last_run_message = EXCLUDED.last_run_message,
                updated_at = NOW()
            """,
            (
                item["id"],
                item.get("title") or "",
                item.get("action") or "notify",
                item.get("schedule") or "month_end",
                item.get("note") or "",
                item.get("run_time") or "09:00",
                item.get("run_date") or "",
                item.get("timezone") or DEFAULT_TIMEZONE,
                item.get("status") or "active",
                item.get("last_run_at"),
                item.get("next_run_at"),
                item.get("last_run_status"),
                item.get("last_run_message"),
                item.get("created_at") or _now(item.get("timezone")).isoformat(),
            ),
        )

    def load_all(self) -> List[Dict[str, Any]]:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        reminder_id,
                        title,
                        action,
                        schedule,
                        note,
                        run_time,
                        run_date,
                        timezone,
                        status,
                        last_run_at,
                        next_run_at,
                        last_run_status,
                        last_run_message,
                        created_at,
                        updated_at
                    FROM reminders
                    ORDER BY created_at ASC
                    """
                )
                rows = cursor.fetchall()
        return [self._row_to_reminder(row) for row in rows]

    def replace_all(self, items: List[Dict[str, Any]]) -> int:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM reminders")
                for item in items:
                    self._upsert_reminder(cursor, item)
        return len(items)


def get_reminder_store() -> ReminderStore:
    backend = str(os.environ.get("IV_AGENT_STORAGE_BACKEND", "auto") or "auto").strip().lower()
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if backend == "local" or not database_url:
        return JsonReminderStore()
    return PostgresReminderStore(database_url)


def _load_all() -> List[Dict[str, Any]]:
    return get_reminder_store().load_all()


def _save_all(items: List[Dict[str, Any]]) -> None:
    get_reminder_store().replace_all(items)


def _last_day_of_month(year: int, month: int) -> int:
    return monthrange(year, month)[1]


def _add_months(value: date, months: int) -> date:
    target_index = value.month - 1 + months
    target_year = value.year + target_index // 12
    target_month = target_index % 12 + 1
    target_day = min(value.day, _last_day_of_month(target_year, target_month))
    return value.replace(year=target_year, month=target_month, day=target_day)


def compute_next_run(
    schedule: str,
    run_time: str = "09:00",
    run_date: str | None = None,
    after: datetime | None = None,
    tz_name: str | None = None,
) -> Optional[datetime]:
    tz = _resolve_tz(tz_name)
    after_dt = after or _now(tz_name)
    if tz and after_dt.tzinfo is None:
        after_dt = after_dt.replace(tzinfo=tz)
    try:
        run_hour, run_minute = (int(part) for part in run_time.split(":"))
    except (ValueError, AttributeError):
        run_hour, run_minute = 9, 0

    today = after_dt.date()

    if schedule == "once":
        if not run_date:
            return None
        try:
            target_date = datetime.strptime(run_date, "%Y-%m-%d").date()
        except ValueError:
            return None
        candidate = datetime.combine(target_date, time(run_hour, run_minute), tzinfo=tz)
        return candidate if candidate >= after_dt else None

    if schedule == "month_end":
        last_day = _last_day_of_month(today.year, today.month)
        candidate_date = today.replace(day=last_day)
        candidate = datetime.combine(candidate_date, time(run_hour, run_minute), tzinfo=tz)
        if candidate < after_dt:
            next_month = _add_months(candidate_date.replace(day=1), 1)
            last_day_next = _last_day_of_month(next_month.year, next_month.month)
            candidate = datetime.combine(next_month.replace(day=last_day_next), time(run_hour, run_minute), tzinfo=tz)
        return candidate

    if schedule == "daily":
        candidate = datetime.combine(today, time(run_hour, run_minute), tzinfo=tz)
        if candidate < after_dt:
            candidate += timedelta(days=1)
        return candidate

    if schedule in {"weekly_sun", "weekly_mon"}:
        target_weekday = 6 if schedule == "weekly_sun" else 0
        days_ahead = (target_weekday - today.weekday()) % 7
        candidate_date = today + timedelta(days=days_ahead)
        candidate = datetime.combine(candidate_date, time(run_hour, run_minute), tzinfo=tz)
        if candidate < after_dt:
            candidate += timedelta(days=7)
        return candidate

    return None


def _normalize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    title = str(payload.get("title") or "").strip()
    if not title:
        raise ValueError("title is required")
    action = str(payload.get("action") or "notify").strip().lower()
    if action not in VALID_ACTIONS:
        raise ValueError(f"Unsupported action: {action}")
    schedule = str(payload.get("schedule") or "month_end").strip().lower()
    if schedule not in VALID_SCHEDULES:
        raise ValueError(f"Unsupported schedule: {schedule}")
    note = str(payload.get("note") or "").strip()
    run_time = str(payload.get("run_time") or "09:00").strip() or "09:00"
    run_date = str(payload.get("run_date") or "").strip()
    if schedule == "once" and not run_date:
        raise ValueError("run_date is required for one-time automations")
    timezone_name = str(payload.get("timezone") or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    return {
        "title": title,
        "action": action,
        "schedule": schedule,
        "note": note,
        "run_time": run_time,
        "run_date": run_date,
        "timezone": timezone_name,
    }


def list_reminders() -> List[Dict[str, Any]]:
    return _load_all()


def create_reminder(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_payload(payload)
    next_run = compute_next_run(
        normalized["schedule"],
        run_time=normalized["run_time"],
        run_date=normalized["run_date"],
        tz_name=normalized["timezone"],
    )
    record = {
        "id": str(uuid.uuid4()),
        "created_at": _now(normalized["timezone"]).isoformat(),
        "last_run_at": None,
        "next_run_at": next_run.isoformat() if next_run else None,
        "status": "active",
        **normalized,
    }
    items = _load_all()
    items.append(record)
    _save_all(items)
    return record


def delete_reminder(reminder_id: str) -> bool:
    items = _load_all()
    remaining = [item for item in items if item.get("id") != reminder_id]
    if len(remaining) == len(items):
        return False
    _save_all(remaining)
    return True


def get_reminder(reminder_id: str) -> Optional[Dict[str, Any]]:
    for item in _load_all():
        if item.get("id") == reminder_id:
            return item
    return None


def mark_run(reminder_id: str, *, success: bool = True, message: str = "") -> Optional[Dict[str, Any]]:
    items = _load_all()
    target = next((item for item in items if item.get("id") == reminder_id), None)
    if not target:
        return None
    now_iso = _now(target.get("timezone")).isoformat()
    target["last_run_at"] = now_iso
    target["last_run_status"] = "ok" if success else "error"
    if message:
        target["last_run_message"] = message
    if target.get("schedule") == "once":
        target["status"] = "completed" if success else "active"
        target["next_run_at"] = None
    else:
        next_run = compute_next_run(
            target["schedule"],
            run_time=target.get("run_time", "09:00"),
            run_date=target.get("run_date") or None,
            after=_now(target.get("timezone")) + timedelta(minutes=1),
            tz_name=target.get("timezone"),
        )
        target["next_run_at"] = next_run.isoformat() if next_run else None
    _save_all(items)
    return target


def due_reminders(now_value: datetime | None = None) -> List[Dict[str, Any]]:
    due = []
    for item in _load_all():
        if item.get("status") != "active":
            continue
        next_run = item.get("next_run_at")
        if not next_run:
            continue
        try:
            next_run_dt = datetime.fromisoformat(next_run)
        except ValueError:
            continue
        reference = now_value or _now(item.get("timezone"))
        if next_run_dt.tzinfo and reference.tzinfo is None:
            reference = reference.replace(tzinfo=next_run_dt.tzinfo)
        if next_run_dt.tzinfo is None and reference.tzinfo:
            next_run_dt = next_run_dt.replace(tzinfo=reference.tzinfo)
        if next_run_dt <= reference:
            due.append(item)
    return due
