import json
import os
from datetime import datetime
from typing import Any, Dict, Optional

try:
    from .reminders import VALID_ACTIONS, VALID_SCHEDULES, DEFAULT_TIMEZONE
    from .voice_calendar_agent import transcribe_audio, MAX_AUDIO_BYTES, _extract_text_response, _get_openai_client
except ImportError:
    from reminders import VALID_ACTIONS, VALID_SCHEDULES, DEFAULT_TIMEZONE
    from voice_calendar_agent import transcribe_audio, MAX_AUDIO_BYTES, _extract_text_response, _get_openai_client


OPENAI_AUTOMATION_MODEL = (
    os.environ.get("OPENAI_AUTOMATION_MODEL")
    or os.environ.get("OPENAI_CALENDAR_AGENT_MODEL")
    or "gpt-5.4-mini"
).strip() or "gpt-5.4-mini"


CREATE_REMINDER_TOOL = {
    "type": "function",
    "name": "create_reminder",
    "description": (
        "Create a calendar automation that fires on a recurring or one-time schedule. "
        "Use schedule=month_end for last day of every month. Use generate_assistenzbeitrag "
        "as action when the user wants the monthly Assistenzbeitrag PDF prepared automatically."
    ),
    "parameters": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "title": {
                "type": "string",
                "description": "Short title for the automation (max ~80 chars).",
            },
            "action": {
                "type": "string",
                "enum": sorted(VALID_ACTIONS),
                "description": "notify = reminder only; generate_assistenzbeitrag = auto-fill the Assistenzbeitrag PDF.",
            },
            "schedule": {
                "type": "string",
                "enum": sorted(VALID_SCHEDULES),
                "description": "When the automation runs.",
            },
            "run_time": {
                "type": "string",
                "description": "24-hour HH:MM, defaults to 09:00",
            },
            "run_date": {
                "type": "string",
                "description": "ISO date YYYY-MM-DD, required only if schedule == once",
            },
            "note": {
                "type": "string",
                "description": "Optional message to show when the reminder fires.",
            },
        },
        "required": ["title", "action", "schedule"],
    },
}


def _system_prompt(now_value: str, timezone_name: str) -> str:
    return (
        "You are an automation-creation agent for an IV-Helper calendar app. "
        "Convert the user's transcript into exactly one create_reminder tool call. "
        f"Current local datetime: {now_value}. Timezone: {timezone_name}. "
        "If the user says 'end of month' / 'Monatsende' use schedule=month_end. "
        "If they ask the system to prepare/fill/generate the Assistenzbeitrag, "
        "set action=generate_assistenzbeitrag and a clear title like 'Generate Assistenzbeitrag at month-end'. "
        "Only use schedule=once with an explicit calendar date. "
        "Always emit a tool call - never plain text."
    )


def _coerce_tool_arguments(arguments: Any) -> Optional[Dict[str, Any]]:
    if isinstance(arguments, dict):
        return arguments
    if isinstance(arguments, str) and arguments.strip():
        try:
            parsed = json.loads(arguments)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def _parse_openai_tool_response(response: Any) -> Optional[Dict[str, Any]]:
    output_items = response.get("output", []) if isinstance(response, dict) else getattr(response, "output", []) or []
    for item in output_items:
        item_type = item.get("type") if isinstance(item, dict) else getattr(item, "type", "")
        name = item.get("name") if isinstance(item, dict) else getattr(item, "name", "")
        if item_type == "function_call" and name == "create_reminder":
            arguments = item.get("arguments") if isinstance(item, dict) else getattr(item, "arguments", None)
            coerced = _coerce_tool_arguments(arguments)
            if coerced:
                return coerced

    raw_text = _extract_text_response(response)
    if raw_text:
        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def _call_openai_with_tools(
    transcript: str,
    now_value: str,
    timezone_name: str,
    *,
    client=None,
) -> Optional[Dict[str, Any]]:
    openai_client = _get_openai_client(client)
    response = openai_client.responses.create(
        model=OPENAI_AUTOMATION_MODEL,
        input=[
            {
                "role": "system",
                "content": _system_prompt(now_value, timezone_name),
            },
            {
                "role": "user",
                "content": (
                    f"Transcript: {transcript}\n"
                    "Pick the closest matching schedule and action and emit the tool call."
                ),
            },
        ],
        tools=[CREATE_REMINDER_TOOL],
        tool_choice={"type": "function", "name": "create_reminder"},
        max_output_tokens=600,
    )
    return _parse_openai_tool_response(response)


def build_reminder_draft_from_audio(
    audio_bytes: bytes,
    filename: str,
    *,
    timezone_name: str | None = None,
    now_value: str | None = None,
    client=None,
) -> Dict[str, Any]:
    if not audio_bytes:
        raise ValueError("audio is required")
    if len(audio_bytes) > MAX_AUDIO_BYTES:
        raise ValueError("audio must be 25 MB or smaller")

    resolved_timezone = (timezone_name or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    resolved_now = (now_value or "").strip() or datetime.now().isoformat()

    transcript = transcribe_audio(audio_bytes, filename, client=client)
    tool_input = _call_openai_with_tools(transcript, resolved_now, resolved_timezone, client=client)
    if not tool_input:
        return {
            "transcript": transcript,
            "draft": None,
            "error": "OpenAI did not produce a reminder draft.",
        }

    tool_input.setdefault("note", "")
    tool_input.setdefault("run_time", "09:00")
    tool_input["timezone"] = resolved_timezone
    return {
        "transcript": transcript,
        "draft": tool_input,
    }


def build_reminder_draft_from_text(
    text: str,
    *,
    timezone_name: str | None = None,
    now_value: str | None = None,
    client=None,
) -> Dict[str, Any]:
    transcript = str(text or "").strip()
    if not transcript:
        raise ValueError("text is required")
    resolved_timezone = (timezone_name or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    resolved_now = (now_value or "").strip() or datetime.now().isoformat()
    tool_input = _call_openai_with_tools(transcript, resolved_now, resolved_timezone, client=client)
    if not tool_input:
        return {"transcript": transcript, "draft": None, "error": "OpenAI did not produce a reminder draft."}
    tool_input.setdefault("note", "")
    tool_input.setdefault("run_time", "09:00")
    tool_input["timezone"] = resolved_timezone
    return {"transcript": transcript, "draft": tool_input}
