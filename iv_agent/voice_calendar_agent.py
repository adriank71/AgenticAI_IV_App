import io
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_TIMEZONE = "Europe/Berlin"
DEFAULT_TRANSCRIPTION_MODEL = "whisper-1"
DEFAULT_EVENT_AGENT_MODEL = os.environ.get("OPENAI_CALENDAR_AGENT_MODEL", "gpt-5.4-mini").strip() or "gpt-5.4-mini"
MAX_AUDIO_BYTES = 25 * 1024 * 1024
ASSISTANT_HOUR_FIELDS = (
    "koerperpflege",
    "mahlzeiten_eingeben",
    "mahlzeiten_zubereiten",
    "begleitung_therapie",
)


def _resolve_openai_api_key() -> str:
    for key_name in ("OPENAI_API_KEY", "OPEN_AI_KEY", "OPENAI_KEY"):
        value = os.environ.get(key_name, "").strip()
        if value:
            return value
    return ""


EVENT_DRAFT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["draft", "missing_fields", "confidence", "warnings"],
    "properties": {
        "draft": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "date",
                "time",
                "end_time",
                "all_day",
                "category",
                "title",
                "notes",
                "hours",
                "assistant_hours",
                "transport_mode",
                "transport_kilometers",
                "transport_address",
                "recurrence",
                "repeat_count",
            ],
            "properties": {
                "date": {"type": "string"},
                "time": {"type": "string"},
                "end_time": {"type": "string"},
                "all_day": {"type": "boolean"},
                "category": {"type": "string", "enum": ["assistant", "transport", "other"]},
                "title": {"type": "string"},
                "notes": {"type": "string"},
                "hours": {"type": "number"},
                "assistant_hours": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": list(ASSISTANT_HOUR_FIELDS),
                    "properties": {
                        "koerperpflege": {"type": "number"},
                        "mahlzeiten_eingeben": {"type": "number"},
                        "mahlzeiten_zubereiten": {"type": "number"},
                        "begleitung_therapie": {"type": "number"},
                    },
                },
                "transport_mode": {"type": "string", "enum": ["", "bus_bahn", "privatauto", "taxi", "fahrdienst"]},
                "transport_kilometers": {"type": "number"},
                "transport_address": {"type": "string"},
                "recurrence": {"type": "string", "enum": ["none", "weekly", "biweekly", "monthly"]},
                "repeat_count": {"type": "integer"},
            },
        },
        "missing_fields": {
            "type": "array",
            "items": {"type": "string"},
        },
        "confidence": {"type": "number"},
        "warnings": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
}


def _resolve_timezone(timezone_name: str | None) -> str:
    candidate = str(timezone_name or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    try:
        ZoneInfo(candidate)
        return candidate
    except ZoneInfoNotFoundError:
        return DEFAULT_TIMEZONE


def _resolve_now(now_value: str | None, timezone_name: str) -> str:
    if now_value:
        try:
            parsed = datetime.fromisoformat(str(now_value).replace("Z", "+00:00"))
            return parsed.isoformat()
        except ValueError:
            pass

    return datetime.now(ZoneInfo(timezone_name)).isoformat()


def _get_openai_client(client=None):
    if client is not None:
        return client

    api_key = _resolve_openai_api_key()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set on the server")

    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RuntimeError("OpenAI Python package is not installed") from exc

    return OpenAI(api_key=api_key)


def _extract_text_response(response) -> str:
    output_text = getattr(response, "output_text", "")
    if output_text:
        return str(output_text).strip()

    if isinstance(response, dict):
        output_text = response.get("output_text")
        if output_text:
            return str(output_text).strip()
        output_items = response.get("output") or []
    else:
        output_items = getattr(response, "output", []) or []

    parts = []
    for output_item in output_items:
        content_items = output_item.get("content", []) if isinstance(output_item, dict) else getattr(output_item, "content", []) or []
        for content_item in content_items:
            if isinstance(content_item, dict):
                text = content_item.get("text") or content_item.get("value") or ""
            else:
                text = getattr(content_item, "text", "") or getattr(content_item, "value", "")
            if text:
                parts.append(str(text))

    return "".join(parts).strip()


def _normalize_number(value, default=0.0) -> float:
    try:
        return float(value or default)
    except (TypeError, ValueError):
        return float(default)


def _normalize_int(value, default=0) -> int:
    try:
        return int(value or default)
    except (TypeError, ValueError):
        return int(default)


def _normalize_agent_payload(payload: dict, transcript: str) -> dict:
    draft = payload.get("draft") if isinstance(payload, dict) else {}
    if not isinstance(draft, dict):
        draft = {}

    category = str(draft.get("category") or "other").strip().lower()
    if category not in {"assistant", "transport", "other"}:
        category = "other"

    all_day = bool(draft.get("all_day"))
    assistant_hours = draft.get("assistant_hours") if isinstance(draft.get("assistant_hours"), dict) else {}
    normalized_assistant_hours = {
        field: max(0.0, _normalize_number(assistant_hours.get(field), 0.0))
        for field in ASSISTANT_HOUR_FIELDS
    }
    if category != "assistant":
        normalized_assistant_hours = {field: 0.0 for field in ASSISTANT_HOUR_FIELDS}

    transport_mode = str(draft.get("transport_mode") or "").strip().lower()
    if transport_mode not in {"", "bus_bahn", "privatauto", "taxi", "fahrdienst"}:
        transport_mode = ""
    if category != "transport":
        transport_mode = ""

    recurrence = str(draft.get("recurrence") or "none").strip().lower()
    if recurrence not in {"none", "weekly", "biweekly", "monthly"}:
        recurrence = "none"

    notes = str(draft.get("notes") or "").strip()
    if transcript and "Transcript:" not in notes:
        notes = f"{notes}\n\nTranscript: {transcript}".strip()

    normalized_draft = {
        "date": str(draft.get("date") or "").strip(),
        "time": "" if all_day else str(draft.get("time") or "").strip(),
        "end_time": "" if all_day else str(draft.get("end_time") or "").strip(),
        "all_day": all_day,
        "category": category,
        "title": str(draft.get("title") or "").strip(),
        "notes": notes,
        "hours": round(sum(normalized_assistant_hours.values()), 2) if category == "assistant" else 0.0,
        "assistant_hours": normalized_assistant_hours,
        "transport_mode": transport_mode,
        "transport_kilometers": max(0.0, _normalize_number(draft.get("transport_kilometers"), 0.0)) if category == "transport" else 0.0,
        "transport_address": str(draft.get("transport_address") or "").strip() if category == "transport" else "",
        "recurrence": recurrence,
        "repeat_count": max(0, _normalize_int(draft.get("repeat_count"), 0)),
    }

    missing_fields = payload.get("missing_fields") if isinstance(payload, dict) else []
    if not isinstance(missing_fields, list):
        missing_fields = []
    missing_fields = [str(field).strip() for field in missing_fields if str(field).strip()]

    for required_field in ("date", "title"):
        if not normalized_draft[required_field] and required_field not in missing_fields:
            missing_fields.append(required_field)
    if not normalized_draft["all_day"]:
        for required_field in ("time", "end_time"):
            if not normalized_draft[required_field] and required_field not in missing_fields:
                missing_fields.append(required_field)

    warnings = payload.get("warnings") if isinstance(payload, dict) else []
    if not isinstance(warnings, list):
        warnings = []

    return {
        "transcript": transcript,
        "draft": normalized_draft,
        "missing_fields": missing_fields,
        "confidence": max(0.0, min(1.0, _normalize_number(payload.get("confidence") if isinstance(payload, dict) else 0.0, 0.0))),
        "warnings": [str(warning).strip() for warning in warnings if str(warning).strip()],
    }


def transcribe_audio(audio_bytes: bytes, filename: str, client=None) -> str:
    openai_client = _get_openai_client(client)
    audio_file = io.BytesIO(audio_bytes)
    audio_file.name = filename or "calendar-voice.webm"
    transcription = openai_client.audio.transcriptions.create(
        model=DEFAULT_TRANSCRIPTION_MODEL,
        file=audio_file,
        response_format="json",
    )
    transcript = getattr(transcription, "text", "")
    if not transcript and isinstance(transcription, dict):
        transcript = transcription.get("text", "")
    transcript = str(transcript or "").strip()
    if not transcript:
        raise RuntimeError("OpenAI returned an empty transcription")
    return transcript


def extract_event_draft(transcript: str, timezone_name: str, now_value: str, client=None) -> dict:
    openai_client = _get_openai_client(client)
    response = openai_client.responses.create(
        model=DEFAULT_EVENT_AGENT_MODEL,
        input=[
            {
                "role": "system",
                "content": (
                    "You are a calendar entry extraction agent. Return JSON only. "
                    "Convert the user's transcript into one draft event for an IV-Helper calendar. "
                    "Use category assistant for care/support blocks, transport for travel or taxi rides, "
                    "and other for general reminders or appointments. Use ISO date YYYY-MM-DD and 24-hour HH:MM. "
                    "If a required field is ambiguous, leave that field empty and include its name in missing_fields. "
                    "Do not invent assistant hour breakdowns unless the transcript states them clearly."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Timezone: {timezone_name}\n"
                    f"Current local datetime: {now_value}\n"
                    f"Transcript: {transcript}\n"
                    "Required timed event fields are date, time, end_time, category, and title. "
                    "For all-day reminders, time and end_time must be empty."
                ),
            },
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": "calendar_event_draft",
                "strict": True,
                "schema": EVENT_DRAFT_SCHEMA,
            }
        },
    )
    raw_text = _extract_text_response(response)
    if not raw_text:
        raise RuntimeError("OpenAI returned an empty calendar draft")

    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("OpenAI returned an invalid calendar draft") from exc

    return _normalize_agent_payload(payload, transcript)


def build_voice_calendar_draft(
    audio_bytes: bytes,
    filename: str,
    *,
    timezone_name: str | None = None,
    now_value: str | None = None,
    client=None,
) -> dict:
    if not audio_bytes:
        raise ValueError("audio is required")
    if len(audio_bytes) > MAX_AUDIO_BYTES:
        raise ValueError("audio must be 25 MB or smaller")

    resolved_timezone = _resolve_timezone(timezone_name)
    resolved_now = _resolve_now(now_value, resolved_timezone)
    transcript = transcribe_audio(audio_bytes, filename, client=client)
    return extract_event_draft(transcript, resolved_timezone, resolved_now, client=client)
