import json
import os
import uuid
from calendar import monthrange
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List


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


def _default_end_time(start_time: str) -> str:
    start_dt = datetime.strptime(start_time, "%H:%M")
    return (start_dt + timedelta(minutes=30)).strftime("%H:%M")


def _normalize_end_time(start_time: str, end_time: str | None = None) -> str:
    normalized_end_time = str(end_time or "").strip()
    if not normalized_end_time:
        return _default_end_time(start_time)
    return normalized_end_time


def _ensure_storage() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(CALENDAR_PATH):
        with open(CALENDAR_PATH, "w", encoding="utf-8") as file:
            json.dump([], file)


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
    normalized["assistant_hours"] = _normalize_assistant_hours(
        normalized.get("assistant_hours"),
        float(normalized.get("hours", 0.0) or 0.0) if normalized["category"] == "assistant" else 0.0,
    )
    normalized["transport_mode"] = str(normalized.get("transport_mode") or "").strip().lower()
    normalized["transport_kilometers"] = round(float(normalized.get("transport_kilometers", 0.0) or 0.0), 2)
    normalized["transport_address"] = str(normalized.get("transport_address") or "").strip()
    normalized["hours"] = _assistant_total_hours(normalized) if normalized["category"] == "assistant" else 0.0
    return normalized


def _load_events() -> List[Dict]:
    _ensure_storage()
    with open(CALENDAR_PATH, "r", encoding="utf-8") as file:
        return [_normalize_event(event) for event in json.load(file)]


def _save_events(events: List[Dict]) -> None:
    _ensure_storage()
    with open(CALENDAR_PATH, "w", encoding="utf-8") as file:
        json.dump(events, file, indent=2)


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
    normalized_category = _normalize_category(category)
    normalized_all_day = bool(all_day)
    normalized_time = "" if normalized_all_day else time
    normalized_end_time = "" if normalized_all_day else _normalize_end_time(time, end_time)
    normalized_assistant_hours = _normalize_assistant_hours(assistant_hours, float(hours or 0.0))
    normalized_transport_mode = str(transport_mode or "").strip().lower()
    normalized_transport_kilometers = round(float(transport_kilometers or 0.0), 2)
    normalized_transport_address = str(transport_address or "").strip()
    total_hours = (
        round(sum(normalized_assistant_hours.values()), 2)
        if normalized_category == "assistant"
        else 0.0
    )
    normalized_recurrence = str(recurrence or "none").strip().lower()
    normalized_repeat_count = int(repeat_count or 0)

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

    events = _load_events()
    created_events = []

    for occurrence_date in _build_occurrence_dates(date, normalized_recurrence, normalized_repeat_count):
        event = {
            "id": str(uuid.uuid4()),
            "date": occurrence_date,
            "time": normalized_time,
            "end_time": normalized_end_time,
            "all_day": normalized_all_day,
            "category": normalized_category,
            "title": title,
            "notes": notes,
            "hours": total_hours,
            "assistant_hours": normalized_assistant_hours,
            "transport_mode": normalized_transport_mode,
            "transport_kilometers": normalized_transport_kilometers,
            "transport_address": normalized_transport_address,
        }
        created_events.append(event)
        events.append(event)

    events.sort(key=_event_sort_key)
    _save_events(events)
    return created_events


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
    datetime.strptime(month, "%Y-%m")
    events = [event for event in _load_events() if event["date"].startswith(month)]
    return sorted(events, key=_event_sort_key)


def get_assistant_hours_breakdown(month: str) -> Dict[str, float]:
    totals = {field: 0.0 for field in ASSISTANT_HOUR_FIELDS}
    for event in get_events(month):
        if event["category"] != "assistant":
            continue
        assistant_hours = _normalize_assistant_hours(event.get("assistant_hours"), event.get("hours", 0.0))
        for field in ASSISTANT_HOUR_FIELDS:
            totals[field] += assistant_hours.get(field, 0.0)
    return {field: round(value, 2) for field, value in totals.items()}


def get_assistant_hours(month: str):
    return round(
        sum(_assistant_total_hours(event) for event in get_events(month) if event["category"] == "assistant"),
        2,
    )


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
    events = _load_events()
    updated_events = [event for event in events if event["id"] != event_id]
    if len(updated_events) == len(events):
        return False
    _save_events(updated_events)
    return True


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
    events = _load_events()
    target_index = next((index for index, event in enumerate(events) if event["id"] == event_id), None)
    if target_index is None:
        return None

    normalized_category = _normalize_category(category)
    normalized_all_day = bool(all_day)
    normalized_time = "" if normalized_all_day else time
    normalized_end_time = "" if normalized_all_day else _normalize_end_time(time, end_time)
    normalized_assistant_hours = _normalize_assistant_hours(assistant_hours, float(hours or 0.0))
    normalized_transport_mode = str(transport_mode or "").strip().lower()
    normalized_transport_kilometers = round(float(transport_kilometers or 0.0), 2)
    normalized_transport_address = str(transport_address or "").strip()
    total_hours = (
        round(sum(normalized_assistant_hours.values()), 2)
        if normalized_category == "assistant"
        else 0.0
    )

    _validate_event_inputs(
        date,
        normalized_time,
        normalized_end_time,
        normalized_all_day,
        normalized_category,
        total_hours,
        normalized_assistant_hours,
        normalized_transport_kilometers,
        "none",
        0,
    )

    updated_event = {
        "id": event_id,
        "date": date,
        "time": normalized_time,
        "end_time": normalized_end_time,
        "all_day": normalized_all_day,
        "category": normalized_category,
        "title": title,
        "notes": notes,
        "hours": total_hours,
        "assistant_hours": normalized_assistant_hours,
        "transport_mode": normalized_transport_mode,
        "transport_kilometers": normalized_transport_kilometers,
        "transport_address": normalized_transport_address,
    }
    events[target_index] = updated_event
    events.sort(key=_event_sort_key)
    _save_events(events)
    return updated_event


def export_month_plan(month: str):
    events = get_events(month)
    total_hours = get_assistant_hours(month)
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
