import json
import re
from datetime import datetime, timedelta
from typing import Any

try:
    from .. import reminders as reminders_module
except ImportError:
    import reminders as reminders_module


VALID_REPORT_TYPES = {"assistenzbeitrag", "transportkostenabrechnung"}
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def parse_report_types(value: Any) -> list[str]:
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = [item.strip() for item in raw.split(",")]
    elif isinstance(value, list):
        parsed = value
    else:
        parsed = []

    normalized = []
    for item in parsed:
        report_type = str(item or "").strip().lower()
        if not report_type:
            continue
        if report_type not in VALID_REPORT_TYPES:
            raise ValueError(f"Unsupported report type: {report_type}")
        if report_type not in normalized:
            normalized.append(report_type)
    return normalized


def validate_email(value: Any) -> str:
    email = str(value or "").strip()
    if not email or not EMAIL_PATTERN.match(email):
        raise ValueError("A valid recipient email is required")
    return email


def validate_month(value: Any) -> str:
    month = str(value or "").strip()
    if not re.match(r"^\d{4}-\d{2}$", month):
        raise ValueError("month must be YYYY-MM")
    year, month_number = month.split("-")
    if int(month_number) < 1 or int(month_number) > 12:
        raise ValueError("month must be YYYY-MM")
    return f"{int(year):04d}-{int(month_number):02d}"


def build_generate_report_action_payload(
    *,
    month: str,
    report_types: Any,
    user_id: str,
    timezone: str,
) -> dict[str, Any]:
    selected_types = parse_report_types(report_types) or ["assistenzbeitrag"]
    return {
        "month": validate_month(month),
        "report_types": selected_types,
        "user_id": user_id,
        "profile_id": user_id,
        "timezone": timezone,
    }


def build_report_reminder_payload(
    *,
    title: str,
    to_email: str,
    subject: str,
    month: str,
    report_types: Any,
    schedule: str = "once",
    run_date: str = "",
    run_time: str = "09:00",
    timezone: str = "Europe/Berlin",
    note: str = "",
    body: str = "",
) -> dict[str, Any]:
    reminder_title = str(title or "").strip() or "Report per Mail vorbereiten"
    selected_types = parse_report_types(report_types) or ["assistenzbeitrag"]
    normalized_schedule = str(schedule or "once").strip().lower()
    payload = {
        "title": reminder_title,
        "action": "send_report_reminder_email",
        "schedule": normalized_schedule,
        "run_date": str(run_date or "").strip(),
        "run_time": str(run_time or "09:00").strip() or "09:00",
        "timezone": str(timezone or "Europe/Berlin").strip() or "Europe/Berlin",
        "note": str(note or "").strip(),
        "payload": {
            "to_email": validate_email(to_email),
            "subject": str(subject or "").strip() or reminder_title,
            "body": str(body or "").strip(),
            "target_month": validate_month(month),
            "report_types": selected_types,
            "link_context": {
                "panel": "automations",
                "reportModal": "1",
            },
        },
    }
    if normalized_schedule == "once" and not payload["run_date"]:
        raise ValueError("run_date is required for one-time report reminders")
    return payload


def create_report_reminder(payload: dict[str, Any]) -> dict[str, Any]:
    return reminders_module.create_reminder(payload)


def parse_relative_once(
    phrase: str,
    *,
    now_value: datetime,
) -> dict[str, str]:
    """Small deterministic helper for simple relative German/English reminder phrases."""
    raw = str(phrase or "").strip().lower()
    if not raw:
        raise ValueError("relative phrase is required")

    match = re.search(r"in\s+(\d+)\s*(stunden|stunde|hours|hour|h)\b", raw)
    if match:
        target = now_value + timedelta(hours=int(match.group(1)))
        return {
            "schedule": "once",
            "run_date": target.strftime("%Y-%m-%d"),
            "run_time": target.strftime("%H:%M"),
        }

    if "heute abend" in raw or "tonight" in raw:
        target = now_value.replace(hour=19, minute=0, second=0, microsecond=0)
        if target <= now_value:
            target = target + timedelta(days=1)
        return {
            "schedule": "once",
            "run_date": target.strftime("%Y-%m-%d"),
            "run_time": target.strftime("%H:%M"),
        }

    if "ende des monats" in raw or "month end" in raw:
        return {
            "schedule": "month_end",
            "run_date": "",
            "run_time": "09:00",
        }

    raise ValueError("Unsupported relative reminder phrase")

