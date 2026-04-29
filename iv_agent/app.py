import logging
import json
import io
import os
import socket
import base64
import binascii
import urllib.error
import urllib.request
from contextlib import ExitStack
from datetime import datetime, timezone

from flask import Flask, jsonify, redirect, request, send_file, send_from_directory, url_for
from flask_cors import CORS

try:
    from .calendar_manager import (
        ASSISTANT_HOUR_FIELDS,
        add_events,
        delete_event,
        export_month_plan,
        get_assistant_hours,
        get_assistant_hours_breakdown,
        get_events,
        update_event,
    )
    from .form_pilot import (
        DUAL_REPORT_HOURLY_RATE,
        STANDARD_RATE,
        fill_assistenz_dual_form_auto_bytes,
        fill_assistenz_form_auto_bytes,
    )
    from .storage import (
        make_profile_store,
        make_report_store,
        make_invoice_capture_store,
        materialize_binary_reference,
        resolve_profile_file_path,
    )
    from .voice_calendar_agent import (
        MAX_AUDIO_BYTES,
        MissingOpenAIConfigurationError,
        build_voice_calendar_draft,
        openai_configuration_status,
        _extract_text_response,
        _get_openai_client,
    )
    from . import reminders as reminders_module
    from .reminders_agent import build_reminder_draft_from_audio, build_reminder_draft_from_text
except ImportError:
    from calendar_manager import (
        ASSISTANT_HOUR_FIELDS,
        add_events,
        delete_event,
        export_month_plan,
        get_assistant_hours,
        get_assistant_hours_breakdown,
        get_events,
        update_event,
    )
    from form_pilot import (
        DUAL_REPORT_HOURLY_RATE,
        STANDARD_RATE,
        fill_assistenz_dual_form_auto_bytes,
        fill_assistenz_form_auto_bytes,
    )
    from storage import (
        make_profile_store,
        make_report_store,
        make_invoice_capture_store,
        materialize_binary_reference,
        resolve_profile_file_path,
    )
    from voice_calendar_agent import (
        MAX_AUDIO_BYTES,
        MissingOpenAIConfigurationError,
        build_voice_calendar_draft,
        openai_configuration_status,
        _extract_text_response,
        _get_openai_client,
    )
    import reminders as reminders_module
    from reminders_agent import build_reminder_draft_from_audio, build_reminder_draft_from_text


app = Flask(__name__, static_folder="static", static_url_path="/static")
CORS(app)
VALID_CATEGORIES = {"assistant", "transport", "other"}
VALID_REPORT_TYPES = {"assistenzbeitrag", "transportkostenabrechnung"}
VALID_RECURRENCE_PATTERNS = {"none", "weekly", "biweekly", "monthly"}
VALID_TRANSPORT_MODES = {"bus_bahn", "privatauto", "taxi", "fahrdienst", ""}
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
PROFILE_DIR = os.path.join(BASE_DIR, "data", "profiles")
DEFAULT_PROFILE_PATH = os.path.join(BASE_DIR, "data", "profile.json")
DEFAULT_STUNDENBLATT_TEMPLATE_CANDIDATES = (
    os.environ.get("IV_AGENT_STUNDENBLATT_PDF", "").strip(),
    os.path.join(PROJECT_ROOT, "Stundenblatt.pdf"),
    r"C:\Users\trxqz\Desktop\Stundenblatt.pdf",
)
DEFAULT_RECHNUNG_TEMPLATE_CANDIDATES = (
    os.environ.get("IV_AGENT_RECHNUNG_PDF", "").strip(),
    os.path.join(PROJECT_ROOT, "Rechnungsvorlage_aL_elektronisch (1).pdf"),
    r"C:\Users\trxqz\Desktop\Rechnungsvorlage_aL_elektronisch (1).pdf",
)
DEFAULT_TEMPLATE_CANDIDATES = (
    os.environ.get("IV_AGENT_TEMPLATE_PDF", "").strip(),
    os.path.join(PROJECT_ROOT, "318.536_D_Rechnung_AB_01_2025_V1.pdf"),
    r"C:\Users\trxqz\Desktop\318.536_D_Rechnung_AB_01_2025_V1.pdf",
)
N8N_WEBHOOK_URL = os.environ.get(
    "IV_AGENT_N8N_WEBHOOK_URL",
    "https://adrx.app.n8n.cloud/webhook/da1ab6f3-73d4-4eaa-9063-ebf8d0e6226f",
).strip()
N8N_CHAT_WEBHOOK_PATH = "da1ab6f3-73d4-4eaa-9063-ebf8d0e6226f"
N8N_CHAT_BASE_URL = os.environ.get("IV_AGENT_CHAT_BASE_URL", "https://adrx.app.n8n.cloud").strip().rstrip("/")
N8N_CHAT_WEBHOOK_MODE = os.environ.get("IV_AGENT_CHAT_WEBHOOK_MODE", "production").strip().lower()
N8N_CHAT_PRODUCTION_URL = f"{N8N_CHAT_BASE_URL}/webhook/{N8N_CHAT_WEBHOOK_PATH}"
N8N_CHAT_TEST_URL = f"{N8N_CHAT_BASE_URL}/webhook-test/{N8N_CHAT_WEBHOOK_PATH}"
N8N_CHAT_TIMEOUT_SECONDS = max(5, int(os.environ.get("IV_AGENT_CHAT_TIMEOUT_SECONDS", "90").strip() or "90"))


def resolve_chat_webhook_url() -> str:
    explicit_url = os.environ.get("IV_AGENT_CHAT_WEBHOOK_URL", "").strip()
    if explicit_url:
        return explicit_url

    if N8N_CHAT_WEBHOOK_MODE == "test":
        return N8N_CHAT_TEST_URL

    return N8N_CHAT_PRODUCTION_URL


N8N_CHAT_WEBHOOK_URL = resolve_chat_webhook_url()


def json_error(message: str, status_code: int):
    response = jsonify({"error": message})
    response.status_code = status_code
    return response


def ai_configuration_error():
    return json_error(
        "AI is not configured on this server. Add OPENAI_API_KEY in Vercel Project Settings -> Environment Variables, then redeploy.",
        503,
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_timestamp() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def get_json_payload(*, required: bool = False) -> dict:
    payload = request.get_json(silent=True)
    if payload is None:
        if required:
            raise ValueError("JSON body is required")
        return {}
    if not isinstance(payload, dict):
        raise ValueError("JSON body is required")
    return payload


def format_webhook_error_detail(raw_detail: bytes | str) -> str:
    if isinstance(raw_detail, bytes):
        detail = raw_detail.decode("utf-8", errors="replace").strip()
    else:
        detail = str(raw_detail or "").strip()

    if not detail:
        return ""

    try:
        parsed = json.loads(detail)
    except json.JSONDecodeError:
        return detail[:1500]

    if isinstance(parsed, dict):
        preferred_keys = ("message", "error", "reason", "description", "details", "hint")
        parts = [str(parsed[key]).strip() for key in preferred_keys if str(parsed.get(key, "")).strip()]
        if parts:
            return " | ".join(parts)[:1500]

    return json.dumps(parsed, ensure_ascii=True)[:1500]


def make_json_safe(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")

    if isinstance(value, dict):
        return {str(key): make_json_safe(item) for key, item in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [make_json_safe(item) for item in value]

    return str(value)


def parse_month(value: str) -> str:
    if not value:
        raise ValueError("Month is required")
    datetime.strptime(value, "%Y-%m")
    return value


def resolve_profile_path(profile_id: str | None) -> str:
    return resolve_profile_file_path(DEFAULT_PROFILE_PATH, PROFILE_DIR, profile_id)


def resolve_existing_path(candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        normalized_candidate = str(candidate or "").strip()
        if not normalized_candidate:
            continue
        if os.path.exists(normalized_candidate):
            return normalized_candidate
        if normalized_candidate.startswith(("http://", "https://")):
            return normalized_candidate
    return None


def resolve_configured_reference(candidate: str) -> str | None:
    normalized_candidate = str(candidate or "").strip()
    if not normalized_candidate:
        return None
    if os.path.exists(normalized_candidate):
        return normalized_candidate
    if normalized_candidate.startswith(("http://", "https://")):
        return normalized_candidate
    if not os.path.isabs(normalized_candidate):
        return normalized_candidate
    return None


def resolve_template_path() -> str:
    configured_reference = resolve_configured_reference(DEFAULT_TEMPLATE_CANDIDATES[0])
    if configured_reference:
        return configured_reference

    resolved_path = resolve_existing_path(DEFAULT_TEMPLATE_CANDIDATES[1:])
    if resolved_path:
        return resolved_path
    raise FileNotFoundError(
        "PDF template not found. Set IV_AGENT_TEMPLATE_PDF or place template in project root."
    )


def resolve_dual_template_paths() -> tuple[str, str] | None:
    stundenblatt_path = resolve_configured_reference(DEFAULT_STUNDENBLATT_TEMPLATE_CANDIDATES[0]) or resolve_existing_path(
        DEFAULT_STUNDENBLATT_TEMPLATE_CANDIDATES[1:]
    )
    rechnung_path = resolve_configured_reference(DEFAULT_RECHNUNG_TEMPLATE_CANDIDATES[0]) or resolve_existing_path(
        DEFAULT_RECHNUNG_TEMPLATE_CANDIDATES[1:]
    )
    if stundenblatt_path and rechnung_path:
        return stundenblatt_path, rechnung_path
    return None


def load_profile_payload(profile_id: str | None) -> dict:
    profile_store = make_profile_store(DEFAULT_PROFILE_PATH, PROFILE_DIR)
    profile_payload = profile_store.get_profile(profile_id)
    if profile_payload is not None:
        return profile_payload

    fallback_profile_path = resolve_profile_path(profile_id)
    if os.path.exists(fallback_profile_path):
        with open(fallback_profile_path, "r", encoding="utf-8") as file:
            return json.load(file)

    raise FileNotFoundError("Profile not found")


def build_report_download_path(report_record: dict) -> str:
    return f"/api/reports/download/{report_record['report_id']}/{report_record['file_name']}"


def build_report_preview_path(report_record: dict) -> str:
    return f"/api/reports/view/{report_record['report_id']}/{report_record['file_name']}"


def get_report_store():
    return make_report_store(OUTPUT_DIR)


def get_invoice_store():
    return make_invoice_capture_store(OUTPUT_DIR)


def resolve_report_record(
    *,
    report_id: str | None = None,
    file_name: str | None = None,
    month: str | None = None,
) -> dict | None:
    return get_report_store().get_report(report_id=report_id, file_name=file_name, month=month)


def serve_report_response(report_record: dict, *, as_attachment: bool):
    report_store = get_report_store()
    report_bytes, content_type = report_store.read_report_bytes(report_record)
    return send_file(
        io.BytesIO(report_bytes),
        mimetype=content_type or "application/pdf",
        as_attachment=as_attachment,
        download_name=report_record["file_name"],
    )


def trigger_n8n_webhook(payload: dict) -> None:
    if not N8N_WEBHOOK_URL:
        raise RuntimeError("n8n webhook is not configured. Set IV_AGENT_N8N_WEBHOOK_URL.")

    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        N8N_WEBHOOK_URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as response:
        if response.status < 200 or response.status >= 300:
            raise RuntimeError(f"n8n webhook failed with status {response.status}")


def trigger_chat_webhook(payload: dict) -> dict:
    if not N8N_CHAT_WEBHOOK_URL:
        raise RuntimeError("chat webhook is not configured. Set IV_AGENT_CHAT_WEBHOOK_URL.")

    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        N8N_CHAT_WEBHOOK_URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=N8N_CHAT_TIMEOUT_SECONDS) as response:
            if response.status < 200 or response.status >= 300:
                raise RuntimeError(f"chat webhook failed with status {response.status}")
            response_body = response.read()
    except urllib.error.HTTPError as exc:
        detail = format_webhook_error_detail(exc.read())
        if "webhook-test" in N8N_CHAT_WEBHOOK_URL:
            raise RuntimeError(
                "n8n test webhook is not listening. In n8n click 'Listen for test event' or switch the app to the production webhook URL."
            ) from exc
        if exc.code == 404:
            raise RuntimeError(
                "chat webhook returned 404. The production n8n webhook URL is "
                f"{N8N_CHAT_PRODUCTION_URL}. In n8n either activate the workflow for the production URL "
                "or switch the app to test mode with IV_AGENT_CHAT_WEBHOOK_MODE=test and click "
                "'Listen for test event'."
                + (f" n8n detail: {detail}" if detail else "")
            ) from exc
        logger.error(
            "chat webhook HTTP error status=%s url=%s detail=%s payload=%s",
            exc.code,
            N8N_CHAT_WEBHOOK_URL,
            detail or "<empty>",
            json.dumps(payload, ensure_ascii=True),
        )
        raise RuntimeError(
            f"chat webhook failed with status {exc.code}"
            + (f": {detail}" if detail else ". n8n returned an empty error body.")
        ) from exc
    except socket.timeout as exc:
        raise RuntimeError(
            "chat webhook timed out after "
            f"{N8N_CHAT_TIMEOUT_SECONDS} seconds. "
            "n8n is taking too long to finish before the Respond to Webhook step."
        ) from exc
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, socket.timeout):
            raise RuntimeError(
                "chat webhook timed out after "
                f"{N8N_CHAT_TIMEOUT_SECONDS} seconds. "
                "n8n is taking too long to finish before the Respond to Webhook step."
            ) from exc
        raise RuntimeError(f"Failed to reach chat webhook at {N8N_CHAT_WEBHOOK_URL}: {exc.reason}") from exc

    if not response_body:
        return {}

    decoded = response_body.decode("utf-8").strip()
    if not decoded:
        return {}

    try:
        parsed = json.loads(decoded)
        if isinstance(parsed, dict):
            return make_json_safe(parsed)
        return {"data": make_json_safe(parsed)}
    except json.JSONDecodeError:
        return {"reply": decoded}


def parse_event_payload(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("JSON body is required")

    required_fields = ("date", "category", "title")
    for field in required_fields:
        if not str(payload.get(field, "")).strip():
            raise ValueError(f"{field} is required")

    raw_all_day = payload.get("all_day", False)
    if isinstance(raw_all_day, str):
        all_day = raw_all_day.strip().lower() in {"1", "true", "yes", "on"}
    else:
        all_day = bool(raw_all_day)

    time_value = str(payload.get("time", "")).strip()
    if not all_day and not time_value:
        raise ValueError("time is required")

    category = str(payload["category"]).strip().lower()
    if category not in VALID_CATEGORIES:
        raise ValueError("Unsupported category")

    raw_assistant_hours = payload.get("assistant_hours", {})
    if raw_assistant_hours is None:
        raw_assistant_hours = {}
    if not isinstance(raw_assistant_hours, dict):
        raise ValueError("assistant_hours must be an object")

    assistant_hours = {}
    for field in ASSISTANT_HOUR_FIELDS:
        try:
            value = float(raw_assistant_hours.get(field, 0.0) or 0.0)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field} must be a number") from exc
        if value < 0:
            raise ValueError(f"{field} must be greater than or equal to 0")
        assistant_hours[field] = value

    try:
        legacy_hours = float(payload.get("hours", 0.0) or 0.0)
    except (TypeError, ValueError) as exc:
        raise ValueError("hours must be a number") from exc

    if legacy_hours < 0:
        raise ValueError("hours must be greater than or equal to 0")

    recurrence = str(payload.get("recurrence", "none") or "none").strip().lower()
    if recurrence not in VALID_RECURRENCE_PATTERNS:
        raise ValueError("Unsupported recurrence")

    try:
        repeat_count = int(payload.get("repeat_count", 0) or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError("repeat_count must be an integer") from exc

    if repeat_count < 0:
        raise ValueError("repeat_count must be greater than or equal to 0")

    transport_mode = str(payload.get("transport_mode", "") or "").strip().lower()
    if transport_mode not in VALID_TRANSPORT_MODES:
        raise ValueError("Unsupported transport_mode")

    try:
        transport_kilometers = float(payload.get("transport_kilometers", 0.0) or 0.0)
    except (TypeError, ValueError) as exc:
        raise ValueError("transport_kilometers must be a number") from exc

    if transport_kilometers < 0:
        raise ValueError("transport_kilometers must be greater than or equal to 0")

    return {
        "date": str(payload["date"]).strip(),
        "time": time_value,
        "end_time": str(payload.get("end_time", "")).strip(),
        "all_day": all_day,
        "category": category,
        "title": str(payload["title"]).strip(),
        "notes": str(payload.get("notes", "")).strip(),
        "hours": legacy_hours,
        "assistant_hours": assistant_hours,
        "transport_mode": transport_mode,
        "transport_kilometers": transport_kilometers,
        "transport_address": str(payload.get("transport_address", "") or "").strip(),
        "recurrence": recurrence,
        "repeat_count": repeat_count,
    }


def parse_report_types(payload: dict) -> list[str]:
    raw_report_types = payload.get("report_types")
    if raw_report_types is None:
        return ["assistenzbeitrag"]

    if not isinstance(raw_report_types, list):
        raise ValueError("report_types must be a list")

    normalized = []
    for report_type in raw_report_types:
        value = str(report_type or "").strip().lower()
        if not value:
            continue
        if value not in VALID_REPORT_TYPES:
            raise ValueError(f"Unsupported report type: {value}")
        if value not in normalized:
            normalized.append(value)

    if not normalized:
        raise ValueError("At least one report type must be selected")

    return normalized


def generate_assistenz_report(
    month: str,
    profile_payload: dict,
    *,
    profile_id: str | None = None,
    triggered_by_reminder: str | None = None,
) -> dict:
    output_filename = f"Assistenzbeitrag_{month}.pdf"
    total_hours = get_assistant_hours(month)
    assistant_breakdown = get_assistant_hours_breakdown(month)
    dual_template_paths = resolve_dual_template_paths()

    with ExitStack() as exit_stack:
        if dual_template_paths:
            stundenblatt_template_path = exit_stack.enter_context(
                materialize_binary_reference(dual_template_paths[0], suffix=".pdf")
            )
            rechnung_template_path = exit_stack.enter_context(
                materialize_binary_reference(dual_template_paths[1], suffix=".pdf")
            )
            report_bytes = fill_assistenz_dual_form_auto_bytes(
                stundenblatt_template_pdf_path=stundenblatt_template_path,
                rechnung_template_pdf_path=rechnung_template_path,
                month=month,
                profile_data=profile_payload,
                preview=False,
            )
            gross_amount = round(total_hours * DUAL_REPORT_HOURLY_RATE, 2)
        else:
            template_path = exit_stack.enter_context(
                materialize_binary_reference(resolve_template_path(), suffix=".pdf")
            )
            report_bytes = fill_assistenz_form_auto_bytes(
                template_pdf_path=template_path,
                month=month,
                profile_data=profile_payload,
                preview=False,
            )
            gross_amount = round(total_hours * STANDARD_RATE, 2)

    metadata = {
        "assistant_hours": total_hours,
        "assistant_breakdown": assistant_breakdown,
        "gross_amount_chf": f"{gross_amount:.2f}",
    }
    if triggered_by_reminder:
        metadata["triggered_by_reminder"] = triggered_by_reminder

    stored_report = get_report_store().save_report(
        month=month,
        report_type="assistenzbeitrag",
        file_name=output_filename,
        content=report_bytes,
        profile_id=profile_id,
        metadata=metadata,
    )

    return {
        "report_id": stored_report["report_id"],
        "type": "assistenzbeitrag",
        "label": "Assistenzbeitraege report",
        "file_name": stored_report["file_name"],
        "download_url": build_report_download_path(stored_report),
        "preview_url": build_report_preview_path(stored_report),
        "year": int(month.split("-")[0]),
        "assistant_hours": total_hours,
        "assistant_breakdown": assistant_breakdown,
        "gross_amount_chf": f"{gross_amount:.2f}",
    }


def parse_chat_payload(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("JSON body is required")

    message = str(payload.get("message", "")).strip()
    if not message:
        raise ValueError("message is required")

    raw_history = payload.get("history", [])
    history = raw_history if isinstance(raw_history, list) else []

    return {
        "message": message,
        "history": history[-20:],
        "source": "iv-helper-web",
        "path": N8N_CHAT_WEBHOOK_PATH,
        "timestamp": utc_timestamp(),
    }


@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.get("/style.css")
def style():
    return send_from_directory(app.static_folder, "style.css")


@app.get("/app.js")
def script():
    return send_from_directory(app.static_folder, "app.js")


@app.get("/api/events")
def api_get_events():
    try:
        month = parse_month(request.args.get("month", "").strip())
        return jsonify({"events": get_events(month)})
    except ValueError as exc:
        return json_error(str(exc), 400)


@app.post("/api/events")
def api_add_event():
    try:
        payload = parse_event_payload(get_json_payload(required=True))
        created_events = add_events(**payload)
        response = jsonify(
            {
                "event": created_events[0],
                "events": created_events,
                "created_count": len(created_events),
            }
        )
        response.status_code = 201
        return response
    except ValueError as exc:
        return json_error(str(exc), 400)


@app.delete("/api/events/<event_id>")
def api_delete_event(event_id: str):
    if delete_event(event_id):
        return jsonify({"deleted": True, "event_id": event_id})
    return json_error("Event not found", 404)


@app.put("/api/events/<event_id>")
def api_update_event(event_id: str):
    try:
        payload = parse_event_payload(get_json_payload(required=True))
        payload.pop("recurrence", None)
        payload.pop("repeat_count", None)
        updated_event = update_event(event_id=event_id, **payload)
        if not updated_event:
            return json_error("Event not found", 404)
        return jsonify({"updated": True, "event": updated_event})
    except ValueError as exc:
        return json_error(str(exc), 400)


@app.get("/api/hours")
def api_get_hours():
    try:
        month = parse_month(request.args.get("month", "").strip())
        return jsonify(
            {
                "month": month,
                "total_hours": get_assistant_hours(month),
                "assistant_breakdown": get_assistant_hours_breakdown(month),
            }
        )
    except ValueError as exc:
        return json_error(str(exc), 400)


@app.get("/api/export")
def api_export_month():
    try:
        month = parse_month(request.args.get("month", "").strip())
        return jsonify({"month": month, "summary": export_month_plan(month)})
    except ValueError as exc:
        return json_error(str(exc), 400)


@app.get("/api/ai/status")
def api_ai_status():
    status = openai_configuration_status()
    status["models"]["automation"] = (
        os.environ.get("OPENAI_AUTOMATION_MODEL")
        or os.environ.get("OPENAI_CALENDAR_AGENT_MODEL")
        or "gpt-5.4-mini"
    ).strip() or "gpt-5.4-mini"
    status["models"]["vision"] = OPENAI_VISION_MODEL
    return jsonify({"openai": status})


@app.post("/api/chat")
def api_chat():
    try:
        chat_payload = parse_chat_payload(get_json_payload(required=True))
        webhook_response = trigger_chat_webhook(chat_payload)
        return jsonify({"webhook_response": make_json_safe(webhook_response)})
    except ValueError as exc:
        return json_error(str(exc), 400)
    except urllib.error.URLError as exc:
        logger.error("chat webhook request failed: %s", exc)
        return json_error("Failed to reach chat webhook", 502)
    except RuntimeError as exc:
        logger.error("chat webhook runtime error: %s", exc)
        return json_error(str(exc), 502)
    except Exception as exc:
        logger.exception("Unexpected chat webhook error: %s", exc)
        return json_error(f"Failed to process chat request: {exc}", 500)


@app.post("/api/calendar/voice/draft")
def api_calendar_voice_draft():
    try:
        audio_file = request.files.get("audio")
        if not audio_file:
            return json_error("audio is required", 400)

        audio_bytes = audio_file.read()
        if not audio_bytes:
            return json_error("audio is required", 400)
        if len(audio_bytes) > MAX_AUDIO_BYTES:
            return json_error("audio must be 25 MB or smaller", 413)

        draft_payload = build_voice_calendar_draft(
            audio_bytes,
            audio_file.filename or "calendar-voice.webm",
            timezone_name=request.form.get("timezone"),
            now_value=request.form.get("now"),
        )
        return jsonify(make_json_safe(draft_payload))
    except ValueError as exc:
        return json_error(str(exc), 400)
    except MissingOpenAIConfigurationError:
        return ai_configuration_error()
    except RuntimeError as exc:
        logger.error("voice calendar draft failed: %s", exc)
        return json_error(str(exc), 502)
    except Exception as exc:
        logger.exception("Unexpected voice calendar draft error: %s", exc)
        return json_error("Failed to process voice calendar request", 500)


@app.post("/api/reports/generate")
def api_generate_report():
    try:
        payload = get_json_payload()
        month = parse_month(str(payload.get("month", "")).strip())
        report_types = parse_report_types(payload)
        raw_profile_id = payload.get("profile_id")
        profile_id = str(raw_profile_id).strip() if raw_profile_id is not None else None
        profile_payload = load_profile_payload(profile_id or None)
        generated_reports = []
        unavailable_reports = []

        if "assistenzbeitrag" in report_types:
            generated_reports.append(generate_assistenz_report(month, profile_payload, profile_id=profile_id))

        if "transportkostenabrechnung" in report_types:
            unavailable_reports.append(
                {
                    "type": "transportkostenabrechnung",
                    "label": "Transportkostenabrechnung report",
                    "message": "Transport report generation will be wired once the updated PDF form and field mapping are available.",
                }
            )

        response_payload = {
            "month": month,
            "generated_reports": generated_reports,
            "unavailable_reports": unavailable_reports,
        }

        if len(generated_reports) == 1 and not unavailable_reports:
            first_report = generated_reports[0]
            response_payload.update(
                {
                    "report_id": first_report["report_id"],
                    "file_name": first_report["file_name"],
                    "download_url": first_report["download_url"],
                    "preview_url": first_report["preview_url"],
                    "year": first_report["year"],
                    "assistant_hours": first_report.get("assistant_hours", 0),
                    "gross_amount_chf": first_report.get("gross_amount_chf", "0.00"),
                }
            )

        return jsonify(response_payload)
    except ValueError as exc:
        logger.error("Report generation validation failed: %s", exc)
        return json_error(str(exc), 400)
    except FileNotFoundError as exc:
        logger.error("Report generation missing file: %s", exc)
        return json_error(str(exc), 404)
    except Exception as exc:
        logger.exception("Unexpected report generation error: %s", exc)
        return json_error("Failed to generate report", 500)


@app.get("/api/reports/download/<report_id>/<path:filename>")
def api_download_report_by_id(report_id: str, filename: str):
    try:
        if not filename.lower().endswith(".pdf"):
            return json_error("Invalid file type", 400)
        report_record = resolve_report_record(report_id=report_id)
        if not report_record:
            return json_error("Report file not found", 404)
        return serve_report_response(report_record, as_attachment=True)
    except FileNotFoundError:
        return json_error("Report file not found", 404)


@app.get("/api/reports/download/<path:filename>")
def api_download_report(filename: str):
    try:
        if not filename.lower().endswith(".pdf"):
            return json_error("Invalid file type", 400)
        report_record = resolve_report_record(file_name=filename)
        if not report_record:
            return json_error("Report file not found", 404)
        return serve_report_response(report_record, as_attachment=True)
    except FileNotFoundError:
        return json_error("Report file not found", 404)


@app.get("/api/reports/view/<report_id>/<path:filename>")
def api_view_report_by_id(report_id: str, filename: str):
    try:
        if not filename.lower().endswith(".pdf"):
            return json_error("Invalid file type", 400)
        report_record = resolve_report_record(report_id=report_id)
        if not report_record:
            return json_error("Report file not found", 404)
        return serve_report_response(report_record, as_attachment=False)
    except FileNotFoundError:
        return json_error("Report file not found", 404)


@app.get("/api/reports/view/<path:filename>")
def api_view_report(filename: str):
    try:
        if not filename.lower().endswith(".pdf"):
            return json_error("Invalid file type", 400)
        report_record = resolve_report_record(file_name=filename)
        if not report_record:
            return json_error("Report file not found", 404)
        return serve_report_response(report_record, as_attachment=False)
    except FileNotFoundError:
        return json_error("Report file not found", 404)


@app.post("/api/reports/send")
def api_send_report():
    try:
        payload = get_json_payload()
        month = parse_month(str(payload.get("month", "")).strip())
        report_id = str(payload.get("report_id", "") or "").strip() or None
        file_name = str(payload.get("file_name", "")).strip()

        if not report_id and (not file_name or not file_name.lower().endswith(".pdf")):
            return json_error("report_id or valid file_name (.pdf) is required", 400)

        report_record = resolve_report_record(report_id=report_id, file_name=file_name or None, month=month)
        if not report_record:
            return json_error("Report file not found", 404)

        if not N8N_WEBHOOK_URL:
            return json_error("Send endpoint not configured (missing IV_AGENT_N8N_WEBHOOK_URL)", 501)

        base_url = request.host_url.rstrip("/")
        download_path = build_report_download_path(report_record)
        preview_path = build_report_preview_path(report_record)
        webhook_payload = {
            "month": month,
            "report_id": report_record["report_id"],
            "report_type": report_record["type"],
            "file_name": report_record["file_name"],
            "download_url": f"{base_url}{download_path}",
            "preview_url": f"{base_url}{preview_path}",
            "storage_backend": report_record.get("storage_backend"),
        }
        trigger_n8n_webhook(webhook_payload)
        return jsonify(
            {
                "sent": True,
                "report_id": report_record["report_id"],
                "file_name": report_record["file_name"],
                "month": month,
            }
        )
    except ValueError as exc:
        return json_error(str(exc), 400)
    except urllib.error.URLError as exc:
        logger.error("n8n webhook request failed: %s", exc)
        return json_error("Failed to reach n8n webhook", 502)
    except RuntimeError as exc:
        logger.error("n8n webhook runtime error: %s", exc)
        return json_error(str(exc), 502)
    except Exception as exc:
        logger.exception("Unexpected report send error: %s", exc)
        return json_error("Failed to send report", 500)


OPENAI_VISION_MODEL = (
    os.environ.get("OPENAI_VISION_MODEL")
    or os.environ.get("OPENAI_CALENDAR_AGENT_MODEL")
    or "gpt-5.4-mini"
).strip() or "gpt-5.4-mini"
INVOICE_MIME_EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "application/pdf": ".pdf",
}

INVOICE_PROMPT = (
    "Extract structured fields from this receipt/invoice image. "
    "Respond with ONLY compact JSON, no prose, no code fences. Schema: "
    '{"merchant": string, "date": "YYYY-MM-DD"|null, "total": number|null, '
    '"currency": string|null, "invoice_number": string|null, "vat": number|null, '
    '"confidence": "high"|"medium"|"low"}. '
    "If a field is not clearly visible, use null. "
    "If the image is not a receipt/invoice at all, respond: "
    '{"error": "not_a_receipt"}'
)


def build_invoice_image_path(capture_record: dict) -> str:
    return (
        f"/api/invoices/{capture_record['sid']}/files/"
        f"{capture_record['invoice_id']}/{capture_record['file_name']}"
    )


def parse_invoice_capture_payload(payload: dict) -> tuple[bytes, str, str]:
    if not isinstance(payload, dict):
        raise ValueError("JSON body is required")

    image_b64 = str(payload.get("image_base64", "")).strip()
    mime = str(payload.get("mime", "image/jpeg")).strip() or "image/jpeg"
    if not image_b64:
        raise ValueError("image_base64 is required")
    if mime not in INVOICE_MIME_EXTENSIONS:
        raise ValueError("unsupported mime")

    try:
        image_bytes = base64.b64decode(image_b64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("Invalid base64 image payload") from exc

    if not image_bytes:
        raise ValueError("Image payload is empty")

    file_name = os.path.basename(str(payload.get("file_name", "")).strip())
    if not file_name:
        timestamp = utc_now().strftime("%Y%m%d_%H%M%S")
        file_name = f"invoice_{timestamp}{INVOICE_MIME_EXTENSIONS[mime]}"
    elif "." not in file_name:
        file_name = f"{file_name}{INVOICE_MIME_EXTENSIONS[mime]}"

    return image_bytes, mime, file_name


def normalize_invoice_fields(fields: dict) -> dict:
    normalized = {
        "merchant": (fields.get("merchant") or "").strip() or None,
        "date": (fields.get("date") or "").strip() or None,
        "total": fields.get("total"),
        "currency": (fields.get("currency") or "").strip() or None,
        "invoice_number": (fields.get("invoice_number") or "").strip() or None,
        "vat": fields.get("vat"),
        "confidence": (fields.get("confidence") or "").strip() or None,
    }
    return normalized


def serialize_invoice_capture(capture_record: dict) -> dict:
    fields = capture_record.get("fields") or None
    summary = _format_invoice(fields) if fields else None
    content_type = capture_record.get("content_type")
    return {
        "invoice_id": capture_record["invoice_id"],
        "sid": capture_record["sid"],
        "file_name": capture_record["file_name"],
        "folder_path": capture_record.get("folder_path"),
        "created_at": capture_record.get("created_at"),
        "content_type": content_type,
        "content_size": capture_record.get("content_size"),
        "fields": fields,
        "summary": summary,
        "extraction_error": capture_record.get("extraction_error"),
        "storage_backend": capture_record.get("storage_backend"),
        "image_url": build_invoice_image_path(capture_record),
        "file_url": build_invoice_image_path(capture_record),
        "previewable": str(content_type or "").startswith("image/"),
    }


def capture_invoice(sid: str, payload: dict):
    image_bytes, mime, file_name = parse_invoice_capture_payload(payload)
    image_b64 = base64.b64encode(image_bytes).decode("ascii")
    invoice_store = get_invoice_store()

    normalized_fields = None
    extraction_error = None
    if mime.startswith("image/"):
        try:
            extracted_fields = _call_openai_vision(image_b64, mime)
            if extracted_fields.get("error") == "not_a_receipt":
                extraction_error = "Image does not look like a receipt"
            else:
                normalized_fields = normalize_invoice_fields(extracted_fields)
                if (
                    not normalized_fields["merchant"]
                    and normalized_fields["total"] is None
                    and not normalized_fields["date"]
                ):
                    normalized_fields = None
                    extraction_error = "Could not extract any fields from image"
        except RuntimeError as exc:
            logger.warning("Invoice extraction failed, storing raw capture only: %s", exc)
            extraction_error = str(exc)
        except Exception as exc:
            logger.exception("Unexpected invoice extraction error")
            extraction_error = f"extraction failed: {exc}"
    capture_record = invoice_store.save_capture(
        sid=sid,
        file_name=file_name,
        content=image_bytes,
        content_type=mime,
        fields=normalized_fields,
        extraction_error=extraction_error,
    )
    serialized_capture = serialize_invoice_capture(capture_record)
    capture_count = len(invoice_store.list_captures(sid))
    response = jsonify(
        {
            "ok": True,
            "stored": True,
            "count": capture_count,
            "capture": serialized_capture,
            "fields": serialized_capture["fields"] or {},
            "extraction_error": serialized_capture.get("extraction_error"),
        }
    )
    response.status_code = 201
    return response


@app.get("/api/invoices/<sid>/scan-url")
def api_invoice_scan_url(sid: str):
    camera_url = url_for("camera_page", sid=sid, _external=True)
    return jsonify({"scan_url": camera_url, "camera_url": camera_url})


@app.get("/camera")
def camera_page():
    return send_from_directory(app.static_folder, "camera.html")


@app.get("/camera/<sid>")
def camera_page_with_sid(sid: str):
    return redirect(url_for("camera_page", sid=sid), code=302)


@app.get("/scan/<sid>")
def scan_page(sid: str):
    return redirect(url_for("camera_page", sid=sid), code=302)


@app.get("/api/invoices/<sid>")
def api_invoices_get(sid: str):
    try:
        items = get_invoice_store().list_captures(sid)
    except ValueError as exc:
        return json_error(str(exc), 400)
    serialized_captures = [serialize_invoice_capture(item) for item in items]
    return jsonify({
        "invoices": [capture["summary"] or capture["file_name"] for capture in serialized_captures],
        "fields": [capture["fields"] or {} for capture in serialized_captures],
        "captures": serialized_captures,
    })


@app.get("/api/invoices/<sid>/files/<invoice_id>/<path:filename>")
def api_invoice_file(sid: str, invoice_id: str, filename: str):
    try:
        capture_record = get_invoice_store().get_capture(sid=sid, invoice_id=invoice_id)
    except ValueError as exc:
        return json_error(str(exc), 400)
    if not capture_record:
        return json_error("Invoice file not found", 404)
    if os.path.basename(filename) != capture_record["file_name"]:
        return json_error("Invoice file not found", 404)
    try:
        image_bytes, content_type = get_invoice_store().read_capture_bytes(capture_record)
    except FileNotFoundError:
        return json_error("Invoice file not found", 404)

    return send_file(
        io.BytesIO(image_bytes),
        mimetype=content_type or capture_record.get("content_type") or "image/jpeg",
        as_attachment=False,
        download_name=capture_record["file_name"],
    )


def _format_invoice(fields: dict) -> str:
    parts = [
        f"Merchant: {fields.get('merchant') or '?'}",
        f"Date: {fields.get('date') or '?'}",
    ]
    total = fields.get("total")
    if total is not None:
        parts.append(f"Total: {total} {fields.get('currency') or ''}".strip())
    else:
        parts.append("Total: ?")
    if fields.get("invoice_number"):
        parts.append(f"Invoice #: {fields['invoice_number']}")
    if fields.get("vat") is not None:
        parts.append(f"VAT: {fields['vat']}")
    return "\n".join(parts)


def _call_openai_vision(image_b64: str, mime: str) -> dict:
    openai_client = _get_openai_client()
    response = openai_client.responses.create(
        model=OPENAI_VISION_MODEL,
        input=[
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": INVOICE_PROMPT},
                    {"type": "input_image", "image_url": f"data:{mime};base64,{image_b64}"},
                ],
            }
        ],
        max_output_tokens=400,
    )
    text = _extract_text_response(response)
    if not text:
        raise RuntimeError("Empty response from OpenAI")

    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:].lstrip()

    try:
        fields = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"OpenAI returned non-JSON: {text[:200]}") from exc

    if not isinstance(fields, dict):
        raise RuntimeError("OpenAI returned non-object")

    return fields


@app.post("/api/invoices/<sid>/extract")
def api_invoices_extract(sid: str):
    try:
        return capture_invoice(sid, get_json_payload(required=True))
    except ValueError as exc:
        return json_error(str(exc), 400)
    except Exception as exc:
        logger.exception("Unexpected invoice capture error")
        return json_error(f"Failed to store invoice capture: {exc}", 500)


@app.post("/api/invoices/<sid>/capture")
def api_invoices_capture(sid: str):
    try:
        return capture_invoice(sid, get_json_payload(required=True))
    except ValueError as exc:
        return json_error(str(exc), 400)
    except Exception as exc:
        logger.exception("Unexpected invoice capture error")
        return json_error(f"Failed to store invoice capture: {exc}", 500)


@app.post("/api/invoices/<sid>")
def api_invoices_post(sid: str):
    return json_error("Use the camera capture flow to add invoices", 400)


def _execute_reminder_action(reminder: dict) -> tuple[bool, str]:
    action = reminder.get("action") or "notify"
    if action == "notify":
        return True, reminder.get("note") or reminder.get("title") or "Reminder fired"

    if action == "generate_assistenzbeitrag":
        try:
            now_iso = reminder.get("next_run_at") or ""
            month_value = now_iso[:7] if now_iso and len(now_iso) >= 7 else utc_now().strftime("%Y-%m")
            parse_month(month_value)
            profile_payload = load_profile_payload(None)
            generate_assistenz_report(
                month_value,
                profile_payload,
                triggered_by_reminder=str(reminder.get("id") or "") or None,
            )
            return True, f"Generated Assistenzbeitrag for {month_value}"
        except Exception as exc:
            logger.exception("Reminder action failed")
            return False, f"Action failed: {exc}"

    return False, f"Unsupported action: {action}"


@app.get("/api/reminders")
def api_list_reminders():
    return jsonify({"reminders": reminders_module.list_reminders()})


@app.post("/api/reminders")
def api_create_reminder():
    try:
        payload = get_json_payload()
        reminder = reminders_module.create_reminder(payload)
        response = jsonify({"reminder": reminder})
        response.status_code = 201
        return response
    except ValueError as exc:
        return json_error(str(exc), 400)
    except Exception as exc:
        logger.exception("Failed to create reminder")
        return json_error(f"Failed to create reminder: {exc}", 500)


@app.delete("/api/reminders/<reminder_id>")
def api_delete_reminder(reminder_id: str):
    if reminders_module.delete_reminder(reminder_id):
        return jsonify({"deleted": True, "id": reminder_id})
    return json_error("Reminder not found", 404)


@app.post("/api/reminders/<reminder_id>/run")
def api_run_reminder(reminder_id: str):
    reminder = reminders_module.get_reminder(reminder_id)
    if not reminder:
        return json_error("Reminder not found", 404)
    success, message = _execute_reminder_action(reminder)
    updated = reminders_module.mark_run(reminder_id, success=success, message=message)
    return jsonify({
        "ok": success,
        "message": message,
        "reminder": updated,
    })


@app.route("/api/reminders/tick", methods=["GET", "POST"])
def api_tick_reminders():
    due_items = reminders_module.due_reminders()
    fired = []
    for reminder in due_items:
        success, message = _execute_reminder_action(reminder)
        updated = reminders_module.mark_run(reminder["id"], success=success, message=message)
        fired.append({
            "id": reminder["id"],
            "ok": success,
            "message": message,
            "reminder": updated,
        })
    return jsonify({"fired_count": len(fired), "fired": fired})


@app.post("/api/reminders/voice")
def api_reminders_voice():
    try:
        audio_file = request.files.get("audio")
        timezone_name = request.form.get("timezone")
        now_value = request.form.get("now")
        text_value = request.form.get("text")

        if audio_file:
            audio_bytes = audio_file.read()
            if not audio_bytes:
                return json_error("audio is required", 400)
            if len(audio_bytes) > MAX_AUDIO_BYTES:
                return json_error("audio must be 25 MB or smaller", 413)
            draft_payload = build_reminder_draft_from_audio(
                audio_bytes,
                audio_file.filename or "automation-voice.webm",
                timezone_name=timezone_name,
                now_value=now_value,
            )
        elif text_value:
            draft_payload = build_reminder_draft_from_text(
                text_value,
                timezone_name=timezone_name,
                now_value=now_value,
            )
        else:
            return json_error("audio or text is required", 400)

        created_record = None
        draft = draft_payload.get("draft")
        if isinstance(draft, dict):
            try:
                created_record = reminders_module.create_reminder(draft)
            except ValueError as exc:
                draft_payload["error"] = str(exc)

        return jsonify({
            "transcript": draft_payload.get("transcript", ""),
            "draft": draft_payload.get("draft"),
            "created": created_record is not None,
            "reminder": created_record,
            "error": draft_payload.get("error"),
        })
    except ValueError as exc:
        return json_error(str(exc), 400)
    except MissingOpenAIConfigurationError:
        return ai_configuration_error()
    except RuntimeError as exc:
        logger.error("voice reminder draft failed: %s", exc)
        return json_error(str(exc), 502)
    except Exception as exc:
        logger.exception("Unexpected voice reminder error")
        return json_error(f"Failed to process voice reminder: {exc}", 500)


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
