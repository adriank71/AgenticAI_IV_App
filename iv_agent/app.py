import logging
import json
import io
import os
import socket
import urllib.error
import urllib.request
from contextlib import ExitStack
from datetime import datetime

from flask import Flask, jsonify, request, send_file, send_from_directory
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
        materialize_binary_reference,
        resolve_profile_file_path,
    )
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
        materialize_binary_reference,
        resolve_profile_file_path,
    )


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
        "timestamp": datetime.utcnow().isoformat() + "Z",
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
        payload = parse_event_payload(request.get_json(silent=True))
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
        payload = parse_event_payload(request.get_json(silent=True))
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


@app.post("/api/chat")
def api_chat():
    try:
        chat_payload = parse_chat_payload(request.get_json(silent=True))
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


@app.post("/api/reports/generate")
def api_generate_report():
    try:
        payload = request.get_json(silent=True) or {}
        month = parse_month(str(payload.get("month", "")).strip())
        report_types = parse_report_types(payload)
        raw_profile_id = payload.get("profile_id")
        profile_id = str(raw_profile_id).strip() if raw_profile_id is not None else None
        profile_payload = load_profile_payload(profile_id or None)
        report_store = get_report_store()
        generated_reports = []
        unavailable_reports = []

        if "assistenzbeitrag" in report_types:
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

            stored_report = report_store.save_report(
                month=month,
                report_type="assistenzbeitrag",
                file_name=output_filename,
                content=report_bytes,
                profile_id=profile_id,
                metadata={
                    "assistant_hours": total_hours,
                    "assistant_breakdown": assistant_breakdown,
                    "gross_amount_chf": f"{gross_amount:.2f}",
                },
            )

            generated_reports.append(
                {
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
            )

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
        payload = request.get_json(silent=True) or {}
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


if __name__ == "__main__":
    app.run(debug=True)
