import logging
import json
import io
import os
import base64
import binascii
import tempfile
import urllib.error
import urllib.request
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone

from flask import Flask, jsonify, redirect, request, send_file, send_from_directory, url_for
from flask_cors import CORS

try:
    from .calendar_manager import (
        ASSISTANT_HOUR_FIELDS,
        add_events,
        delete_event,
        export_month_plan,
        get_assistant_hours_breakdown_for_events,
        get_assistant_hours_for_events,
        get_events,
        update_event,
    )
    from .services.calendar_service import (
        create_calendar_event as create_service_calendar_event,
        delete_calendar_event as delete_service_calendar_event,
        normalize_user_id,
        update_calendar_event as update_service_calendar_event,
    )
    from .services.storage_service import (
        create_folder as create_document_folder,
        delete_document as delete_service_document,
        document_bucket_name,
        move_document_to_folder,
        move_documents_to_folder,
        process_chat_attachments,
        update_document_metadata,
    )
    from .form_pilot import (
        DUAL_REPORT_HOURLY_RATE,
        STANDARD_RATE,
        fill_assistenz_dual_form_auto_bytes,
        fill_assistenz_form_auto_bytes,
    )
    from .storage import (
        _create_supabase_client,
        _supabase_storage_configured,
        make_profile_store,
        make_report_store,
        make_invoice_capture_store,
        make_template_store,
        materialize_binary_reference,
        resolve_profile_file_path,
    )
    from .voice_calendar_agent import (
        MAX_AUDIO_BYTES,
        MissingOpenAIConfigurationError,
        build_voice_calendar_draft,
        openai_configuration_status,
        transcribe_audio,
        _extract_text_response,
        _get_openai_client,
    )
    from . import reminders as reminders_module
    from .agents.orchestrator import confirm_pending_action, run_agent_chat
    from .reminders_agent import build_reminder_draft_from_audio, build_reminder_draft_from_text
except ImportError:
    from calendar_manager import (
        ASSISTANT_HOUR_FIELDS,
        add_events,
        delete_event,
        export_month_plan,
        get_assistant_hours_breakdown_for_events,
        get_assistant_hours_for_events,
        get_events,
        update_event,
    )
    from services.calendar_service import (
        create_calendar_event as create_service_calendar_event,
        delete_calendar_event as delete_service_calendar_event,
        normalize_user_id,
        update_calendar_event as update_service_calendar_event,
    )
    from services.storage_service import (
        create_folder as create_document_folder,
        delete_document as delete_service_document,
        document_bucket_name,
        move_document_to_folder,
        move_documents_to_folder,
        process_chat_attachments,
        update_document_metadata,
    )
    from form_pilot import (
        DUAL_REPORT_HOURLY_RATE,
        STANDARD_RATE,
        fill_assistenz_dual_form_auto_bytes,
        fill_assistenz_form_auto_bytes,
    )
    from storage import (
        _create_supabase_client,
        _supabase_storage_configured,
        make_profile_store,
        make_report_store,
        make_invoice_capture_store,
        make_template_store,
        materialize_binary_reference,
        resolve_profile_file_path,
    )
    from voice_calendar_agent import (
        MAX_AUDIO_BYTES,
        MissingOpenAIConfigurationError,
        build_voice_calendar_draft,
        openai_configuration_status,
        transcribe_audio,
        _extract_text_response,
        _get_openai_client,
    )
    import reminders as reminders_module
    from agents.orchestrator import confirm_pending_action, run_agent_chat
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
TEMPLATE_STORE_PREFIX = "template-store:"
POSTGRES_TEMPLATE_PREFIX = TEMPLATE_STORE_PREFIX
N8N_WEBHOOK_URL = os.environ.get(
    "IV_AGENT_N8N_WEBHOOK_URL",
    "https://adrx.app.n8n.cloud/webhook/da1ab6f3-73d4-4eaa-9063-ebf8d0e6226f",
).strip()


def json_error(message: str, status_code: int):
    response = jsonify({"error": message})
    response.status_code = status_code
    return response


def ai_configuration_error():
    return json_error(
        "AI is not configured on this server. Add OPENAI_API_KEY in Vercel Project Settings -> Environment Variables, then redeploy.",
        503,
    )


def external_service_error_message(exc: Exception, fallback: str) -> str:
    message = getattr(exc, "message", None) or str(exc)
    message = " ".join(str(message or "").split())
    if not message:
        return fallback
    if len(message) > 360:
        message = f"{message[:357]}..."
    return message


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


def get_template_store():
    return make_template_store()


def template_store_reference(template_key: str) -> str | None:
    try:
        template_store = get_template_store()
        if template_store and template_store.get_template(template_key):
            return f"{TEMPLATE_STORE_PREFIX}{template_key}"
    except Exception as exc:
        logger.warning("Could not resolve template %s from configured store: %s", template_key, exc)
    return None


@contextmanager
def materialize_template_reference(reference: str, *, suffix: str = ".pdf"):
    normalized_reference = str(reference or "").strip()
    if normalized_reference.startswith(TEMPLATE_STORE_PREFIX):
        template_key = normalized_reference.removeprefix(TEMPLATE_STORE_PREFIX)
        template_store = get_template_store()
        if not template_store:
            raise FileNotFoundError(f"Template store is not configured for {template_key}")
        data, _ = template_store.read_template_bytes(template_key)
        fd, temp_path = tempfile.mkstemp(suffix=suffix)
        try:
            with os.fdopen(fd, "wb") as temp_file:
                temp_file.write(data)
            yield temp_path
        finally:
            try:
                os.remove(temp_path)
            except FileNotFoundError:
                pass
        return

    with materialize_binary_reference(normalized_reference, suffix=suffix) as materialized_path:
        yield materialized_path


def resolve_template_path() -> str:
    db_reference = template_store_reference("assistenz_standard")
    if db_reference:
        return db_reference

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
    db_stundenblatt = template_store_reference("stundenblatt")
    db_rechnung = template_store_reference("rechnung")
    if db_stundenblatt and db_rechnung:
        return db_stundenblatt, db_rechnung

    stundenblatt_path = resolve_configured_reference(DEFAULT_STUNDENBLATT_TEMPLATE_CANDIDATES[0]) or resolve_existing_path(
        DEFAULT_STUNDENBLATT_TEMPLATE_CANDIDATES[1:]
    )
    rechnung_path = resolve_configured_reference(DEFAULT_RECHNUNG_TEMPLATE_CANDIDATES[0]) or resolve_existing_path(
        DEFAULT_RECHNUNG_TEMPLATE_CANDIDATES[1:]
    )
    if stundenblatt_path and rechnung_path:
        return stundenblatt_path, rechnung_path
    return None


def resolve_transportkosten_template_path() -> str | None:
    return template_store_reference("transportkosten")


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


def save_profile_payload(profile_id: str | None, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("JSON body is required")
    profile_store = make_profile_store(DEFAULT_PROFILE_PATH, PROFILE_DIR)
    target_profile_id = profile_id or "default"
    profile_store.upsert_profile(target_profile_id, payload)
    return payload


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
    month_events = get_events(month, user_id=normalize_user_id(profile_id or "default"))
    total_hours = get_assistant_hours_for_events(month_events)
    assistant_breakdown = get_assistant_hours_breakdown_for_events(month_events)
    dual_template_paths = resolve_dual_template_paths()

    with ExitStack() as exit_stack:
        if dual_template_paths:
            stundenblatt_template_path = exit_stack.enter_context(
                materialize_template_reference(dual_template_paths[0], suffix=".pdf")
            )
            rechnung_template_path = exit_stack.enter_context(
                materialize_template_reference(dual_template_paths[1], suffix=".pdf")
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
                materialize_template_reference(resolve_template_path(), suffix=".pdf")
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


def build_report_webhook_payload(month: str, report_record: dict) -> dict:
    base_url = request.host_url.rstrip("/")
    return {
        "month": month,
        "report_id": report_record["report_id"],
        "report_type": report_record["type"],
        "file_name": report_record["file_name"],
        "download_url": f"{base_url}{build_report_download_path(report_record)}",
        "preview_url": f"{base_url}{build_report_preview_path(report_record)}",
        "storage_backend": report_record.get("storage_backend"),
    }


def send_report_via_webhook(month: str, report_record: dict) -> dict:
    if not N8N_WEBHOOK_URL:
        raise RuntimeError("Send endpoint not configured (missing IV_AGENT_N8N_WEBHOOK_URL)")

    trigger_n8n_webhook(build_report_webhook_payload(month, report_record))
    return {
        "sent": True,
        "report_id": report_record["report_id"],
        "file_name": report_record["file_name"],
        "month": month,
    }


def generate_reports_payload(
    month: str,
    report_types: list[str],
    profile_payload: dict,
    *,
    profile_id: str | None = None,
    transport_unavailable_message: str,
) -> dict:
    generated_reports = []
    unavailable_reports = []

    if "assistenzbeitrag" in report_types:
        generated_reports.append(generate_assistenz_report(month, profile_payload, profile_id=profile_id))

    if "transportkostenabrechnung" in report_types:
        unavailable_reports.append(
            {
                "type": "transportkostenabrechnung",
                "label": "Transportkostenabrechnung report",
                "message": transport_unavailable_message,
            }
        )

    return {
        "month": month,
        "generated_reports": generated_reports,
        "unavailable_reports": unavailable_reports,
    }


def build_agent_chat_payload(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("JSON body is required")

    message = str(payload.get("message", "")).strip()
    if not message:
        raise ValueError("message is required")

    raw_context = payload.get("client_context") if isinstance(payload.get("client_context"), dict) else {}
    profile_id = str(
        raw_context.get("profile_id")
        or payload.get("profile_id")
        or payload.get("user_id")
        or "default"
    ).strip() or "default"
    context = {
        **raw_context,
        "profile_id": normalize_user_id(profile_id),
        "timezone": str(raw_context.get("timezone") or payload.get("timezone") or "").strip(),
        "now": str(raw_context.get("now") or payload.get("now") or utc_timestamp()).strip(),
    }
    raw_attachments = payload.get("attachments") if isinstance(payload.get("attachments"), list) else []
    attachments, uploaded_documents = process_chat_attachments(
        [item for item in raw_attachments if isinstance(item, dict)],
        user_id=context["profile_id"],
    )

    return {
        "message": message,
        "thread_id": str(payload.get("thread_id") or "").strip(),
        "attachments": attachments,
        "uploaded_documents": uploaded_documents,
        "client_context": context,
        "history": payload.get("history") if isinstance(payload.get("history"), list) else [],
    }


def enrich_agent_response_with_uploads(response_payload: dict, agent_payload: dict) -> dict:
    uploaded_documents = agent_payload.get("uploaded_documents")
    if not isinstance(uploaded_documents, list) or not uploaded_documents:
        return response_payload

    artifacts = response_payload.get("artifacts") if isinstance(response_payload.get("artifacts"), list) else []
    for document in uploaded_documents:
        if not isinstance(document, dict):
            continue
        artifacts.append(
            {
                "id": document.get("document_id"),
                "type": "document",
                "title": document.get("file_name") or document.get("safe_file_name") or "Document",
                "document_id": document.get("document_id"),
                "content_type": document.get("content_type"),
                "content_size": document.get("content_size"),
                "summary": document.get("summary"),
                "extraction_status": document.get("extraction_status"),
            }
        )

    response_payload["artifacts"] = artifacts
    response_payload["uploaded_documents"] = uploaded_documents
    upload_lines = []
    for document in uploaded_documents:
        if not isinstance(document, dict):
            continue
        name = str(document.get("file_name") or document.get("safe_file_name") or "Dokument").strip()
        document_type = str(document.get("document_type") or "Dokument").strip()
        institution = str(document.get("institution") or "").strip()
        document_date = str(document.get("document_date") or "").strip()
        summary = str(document.get("summary") or "").strip()
        details = [document_type]
        if institution:
            details.append(f"von {institution}")
        if document_date:
            details.append(f"vom {document_date}")
        upload_lines.append(f"- {name}: als {' '.join(details)} erkannt.")
        if summary:
            first_summary_line = summary.splitlines()[0].strip()
            if first_summary_line:
                upload_lines.append(f"  {first_summary_line}")
        if document.get("extraction_status") in {"no_text", "empty", "failed"}:
            upload_lines.append("  Text konnte nicht extrahiert werden.")
    if upload_lines:
        upload_note = "Datei gespeichert. Dokument wird analysiert. Zusammenfassung fertig.\n" + "\n".join(upload_lines[:10])
        existing_answer = str(response_payload.get("answer") or "").rstrip()
        response_payload["answer"] = f"{existing_answer}\n\n{upload_note}".strip()
    return response_payload


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
        profile_id = normalize_user_id(request.args.get("profile_id", "default"))
        return jsonify({"events": get_events(month, user_id=profile_id)})
    except ValueError as exc:
        return json_error(str(exc), 400)


@app.post("/api/events")
def api_add_event():
    try:
        payload = parse_event_payload(get_json_payload(required=True))
        profile_id = normalize_user_id(request.args.get("profile_id", request.args.get("user_id", "default")))
        created_events = add_events(**payload, user_id=profile_id)
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
    profile_id = normalize_user_id(request.args.get("profile_id", request.args.get("user_id", "default")))
    if delete_event(event_id, user_id=profile_id):
        return jsonify({"deleted": True, "event_id": event_id})
    return json_error("Event not found", 404)


@app.put("/api/events/<event_id>")
def api_update_event(event_id: str):
    try:
        payload = parse_event_payload(get_json_payload(required=True))
        payload.pop("recurrence", None)
        payload.pop("repeat_count", None)
        profile_id = normalize_user_id(request.args.get("profile_id", request.args.get("user_id", "default")))
        updated_event = update_event(event_id=event_id, user_id=profile_id, **payload)
        if not updated_event:
            return json_error("Event not found", 404)
        return jsonify({"updated": True, "event": updated_event})
    except ValueError as exc:
        return json_error(str(exc), 400)


@app.get("/api/hours")
def api_get_hours():
    try:
        month = parse_month(request.args.get("month", "").strip())
        profile_id = normalize_user_id(request.args.get("profile_id", request.args.get("user_id", "default")))
        month_events = get_events(month, user_id=profile_id)
        return jsonify(
            {
                "month": month,
                "total_hours": get_assistant_hours_for_events(month_events),
                "assistant_breakdown": get_assistant_hours_breakdown_for_events(month_events),
            }
        )
    except ValueError as exc:
        return json_error(str(exc), 400)


@app.route("/api/profile", methods=["GET", "PUT"])
def api_profile():
    profile_id = request.args.get("profile_id", "default").strip() or "default"
    try:
        if request.method == "GET":
            return jsonify({"profile_id": profile_id, "profile": load_profile_payload(profile_id)})

        payload = get_json_payload(required=True)
        saved_profile = save_profile_payload(profile_id, payload)
        return jsonify({"profile_id": profile_id, "profile": saved_profile, "saved": True})
    except FileNotFoundError:
        return json_error("Profile not found", 404)
    except ValueError as exc:
        return json_error(str(exc), 400)
    except Exception as exc:
        logger.exception("Profile API failed")
        return json_error(f"Failed to save profile: {exc}", 500)


@app.get("/api/calendar-data")
def api_calendar_data():
    profile_id = request.args.get("profile_id", "default").strip() or "default"
    try:
        month = parse_month(request.args.get("month", "").strip())
        month_events = get_events(month, user_id=normalize_user_id(profile_id))
        return jsonify(
            {
                "profile_id": profile_id,
                "profile": load_profile_payload(profile_id),
                "month": month,
                "events": month_events,
                "total_hours": get_assistant_hours_for_events(month_events),
                "assistant_breakdown": get_assistant_hours_breakdown_for_events(month_events),
                "reminders": reminders_module.list_reminders(),
            }
        )
    except FileNotFoundError:
        return json_error("Profile not found", 404)
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
        agent_payload = build_agent_chat_payload(get_json_payload(required=True))
        response_payload = run_agent_chat(
            agent_payload,
            local_tools={"calendar_snapshot": build_agent_calendar_snapshot},
        )
        response_payload = enrich_agent_response_with_uploads(response_payload, agent_payload)
        return jsonify(make_json_safe(response_payload))
    except ValueError as exc:
        return json_error(str(exc), 400)
    except RuntimeError as exc:
        logger.error("chat runtime error: %s", exc)
        return json_error(str(exc), 502)
    except Exception as exc:
        logger.exception("Unexpected chat error: %s", exc)
        return json_error(f"Failed to process chat request: {exc}", 500)


def execute_pending_agent_action(action: dict) -> dict:
    action_type = str(action.get("type") or "").strip()
    payload = action.get("payload") if isinstance(action.get("payload"), dict) else {}
    user_id = normalize_user_id(payload.get("user_id") or action.get("user_id") or "default")
    timezone_name = str(payload.get("timezone") or "").strip() or None

    if action_type == "create_event":
        recurrence = str(payload.get("recurrence", "none") or "none").strip().lower()
        if recurrence and recurrence != "none":
            event_payload = parse_event_payload(payload)
            created_events = add_events(**event_payload, user_id=user_id)
        else:
            created = create_service_calendar_event(payload, user_id=user_id, timezone_name=timezone_name)
            created_events = [created["event"]]
        return {
            "event": created_events[0],
            "events": created_events,
            "created_count": len(created_events),
        }

    if action_type == "update_event":
        event_id = str(payload.get("event_id") or payload.get("id") or "").strip()
        if not event_id:
            raise ValueError("event_id is required")
        updates = {
            key: value
            for key, value in payload.items()
            if key not in {"event_id", "id", "matched_event", "user_id", "timezone"}
        }
        updated_result = update_service_calendar_event(
            event_id,
            updates,
            user_id=user_id,
            timezone_name=timezone_name,
        )
        if not updated_result:
            raise FileNotFoundError("Event not found")
        return {"updated": True, "event": updated_result["event"]}

    if action_type == "delete_event":
        event_id = str(payload.get("event_id") or payload.get("id") or "").strip()
        if not event_id:
            raise ValueError("event_id is required")
        if not delete_service_calendar_event(event_id, user_id=user_id):
            raise FileNotFoundError("Event not found")
        return {"deleted": True, "event_id": event_id}

    if action_type == "storage.create_folder":
        folder = create_document_folder(
            user_id=user_id,
            name=str(payload.get("name") or "").strip(),
            parent_folder_id=payload.get("parent_folder_id") or None,
            color=str(payload.get("color") or "").strip(),
            metadata=payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {},
        )
        document_ids = [
            str(document_id or "").strip()
            for document_id in (payload.get("document_ids") if isinstance(payload.get("document_ids"), list) else [])
            if str(document_id or "").strip()
        ]
        moved_documents = []
        if document_ids:
            moved_documents = move_documents_to_folder(
                user_id=user_id,
                document_ids=document_ids,
                folder_id=folder.get("folder_id"),
            )
        return {
            "folder": folder,
            "documents": moved_documents,
            "assigned_count": len(moved_documents),
        }

    if action_type == "storage.move_document":
        document_ids = [
            str(document_id or "").strip()
            for document_id in (payload.get("document_ids") if isinstance(payload.get("document_ids"), list) else [])
            if str(document_id or "").strip()
        ]
        document_id = str(payload.get("document_id") or "").strip()
        if document_id and document_id not in document_ids:
            document_ids.insert(0, document_id)
        if not document_ids:
            raise ValueError("document_id is required")
        if len(document_ids) == 1:
            document = move_document_to_folder(
                user_id=user_id,
                document_id=document_ids[0],
                folder_id=payload.get("folder_id") or None,
            )
            return {"document": document, "documents": [document], "moved": True, "moved_count": 1}
        documents = move_documents_to_folder(
            user_id=user_id,
            document_ids=document_ids,
            folder_id=payload.get("folder_id") or None,
        )
        return {"documents": documents, "moved": True, "moved_count": len(documents)}

    if action_type == "storage.delete_document":
        document_id = str(payload.get("document_id") or "").strip()
        if not document_id:
            raise ValueError("document_id is required")
        return delete_service_document(user_id=user_id, document_id=document_id)

    if action_type == "storage.update_metadata":
        document_id = str(payload.get("document_id") or "").strip()
        updates = payload.get("updates") if isinstance(payload.get("updates"), dict) else {}
        if not document_id:
            raise ValueError("document_id is required")
        document = update_document_metadata(user_id=user_id, document_id=document_id, updates=updates)
        return {"document": document, "updated": True}

    if action_type == "create_reminder":
        reminder = reminders_module.create_reminder(payload)
        return {"reminder": reminder}

    if action_type == "generate_report":
        month = parse_month(str(payload.get("month", "")).strip())
        report_types = parse_report_types(payload)
        raw_profile_id = payload.get("profile_id")
        profile_id = str(raw_profile_id).strip() if raw_profile_id is not None else None
        profile_payload = load_profile_payload(profile_id or None)
        return generate_reports_payload(
            month,
            report_types,
            profile_payload,
            profile_id=profile_id,
            transport_unavailable_message="Transport report generation is not available yet.",
        )

    if action_type == "send_report":
        month = parse_month(str(payload.get("month", "")).strip())
        report_id = str(payload.get("report_id", "") or "").strip() or None
        file_name = str(payload.get("file_name", "") or "").strip()
        if not report_id and (not file_name or not file_name.lower().endswith(".pdf")):
            raise ValueError("report_id or valid file_name (.pdf) is required")
        report_record = resolve_report_record(report_id=report_id, file_name=file_name or None, month=month)
        if not report_record:
            raise FileNotFoundError("Report file not found")
        return send_report_via_webhook(month, report_record)

    raise ValueError(f"Unsupported pending action type: {action_type}")


def build_agent_calendar_snapshot(payload: dict) -> dict:
    raw_month = str(payload.get("month") or "").strip()
    if not raw_month:
        raw_month = utc_now().strftime("%Y-%m")
    month = parse_month(raw_month)
    profile_id = normalize_user_id(payload.get("profile_id") or "default")
    month_events = get_events(month, user_id=profile_id)
    try:
        profile_payload = load_profile_payload(profile_id)
    except FileNotFoundError:
        profile_payload = {}
    return {
        "profile_id": profile_id,
        "profile": profile_payload,
        "month": month,
        "events": month_events,
        "total_hours": get_assistant_hours_for_events(month_events),
        "assistant_breakdown": get_assistant_hours_breakdown_for_events(month_events),
        "reminders": reminders_module.list_reminders(),
    }


@app.post("/api/agent/chat")
def api_agent_chat():
    try:
        agent_payload = build_agent_chat_payload(get_json_payload(required=True))
        response_payload = run_agent_chat(
            agent_payload,
            local_tools={"calendar_snapshot": build_agent_calendar_snapshot},
        )
        response_payload = enrich_agent_response_with_uploads(response_payload, agent_payload)
        return jsonify(make_json_safe(response_payload))
    except ValueError as exc:
        return json_error(str(exc), 400)
    except RuntimeError as exc:
        logger.error("agent chat runtime error: %s", exc)
        return json_error(str(exc), 502)
    except Exception as exc:
        logger.exception("Unexpected agent chat error: %s", exc)
        return json_error(f"Failed to process agent chat request: {exc}", 500)


@app.post("/api/agent/actions/<action_id>/confirm")
def api_confirm_agent_action(action_id: str):
    try:
        payload = get_json_payload(required=False)
        raw_context = payload.get("client_context") if isinstance(payload.get("client_context"), dict) else {}
        confirmation = confirm_pending_action(
            action_id,
            execute_pending_agent_action,
            thread_id=str(payload.get("thread_id") or raw_context.get("thread_id") or "").strip() or None,
            user_id=(
                normalize_user_id(payload.get("profile_id") or payload.get("user_id") or raw_context.get("profile_id"))
                if (payload.get("profile_id") or payload.get("user_id") or raw_context.get("profile_id"))
                else None
            ),
        )
        action_type = confirmation.get("action", {}).get("type")
        return jsonify(
            {
                "confirmed": True,
                "calendar_updated": action_type in {"create_event", "update_event", "delete_event"},
                "storage_updated": action_type in {
                    "storage.create_folder",
                    "storage.move_document",
                    "storage.delete_document",
                    "storage.update_metadata",
                },
                **make_json_safe(confirmation),
            }
        )
    except KeyError:
        return json_error("Pending action not found", 404)
    except ValueError as exc:
        return json_error(str(exc), 400)
    except FileNotFoundError as exc:
        return json_error(str(exc), 404)
    except PermissionError as exc:
        return json_error(str(exc), 403)
    except RuntimeError as exc:
        message = str(exc)
        status_code = 409 if "already been handled" in message else 502
        return json_error(message, status_code)
    except Exception as exc:
        logger.exception("Unexpected pending action confirmation error: %s", exc)
        return json_error(f"Failed to confirm pending action: {exc}", 500)


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


@app.post("/api/chat/voice/transcribe")
def api_chat_voice_transcribe():
    try:
        audio_file = request.files.get("audio")
        if not audio_file:
            return json_error("audio is required", 400)

        audio_bytes = audio_file.read()
        if not audio_bytes:
            return json_error("audio is required", 400)
        if len(audio_bytes) > MAX_AUDIO_BYTES:
            return json_error("audio must be 25 MB or smaller", 413)

        transcript = transcribe_audio(audio_bytes, audio_file.filename or "chat-voice.webm")
        return jsonify({"transcript": transcript})
    except MissingOpenAIConfigurationError:
        return ai_configuration_error()
    except RuntimeError as exc:
        logger.error("chat voice transcription failed: %s", exc)
        return json_error(str(exc), 502)
    except Exception as exc:
        logger.error("chat voice transcription failed: %s", exc)
        return json_error(
            external_service_error_message(exc, "Failed to process chat voice request"),
            502,
        )


@app.post("/api/reports/generate")
def api_generate_report():
    try:
        payload = get_json_payload()
        month = parse_month(str(payload.get("month", "")).strip())
        report_types = parse_report_types(payload)
        raw_profile_id = payload.get("profile_id")
        profile_id = str(raw_profile_id).strip() if raw_profile_id is not None else None
        profile_payload = load_profile_payload(profile_id or None)
        response_payload = generate_reports_payload(
            month,
            report_types,
            profile_payload,
            profile_id=profile_id,
            transport_unavailable_message=(
                "Transport report generation will be wired once the updated PDF form and field mapping are available."
            ),
        )

        if len(response_payload["generated_reports"]) == 1 and not response_payload["unavailable_reports"]:
            first_report = response_payload["generated_reports"][0]
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

        return jsonify(send_report_via_webhook(month, report_record))
    except ValueError as exc:
        return json_error(str(exc), 400)
    except urllib.error.URLError as exc:
        logger.error("n8n webhook request failed: %s", exc)
        return json_error("Failed to reach n8n webhook", 502)
    except RuntimeError as exc:
        logger.error("n8n webhook runtime error: %s", exc)
        status_code = 501 if "Send endpoint not configured" in str(exc) else 502
        return json_error(str(exc), status_code)
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
        "storage_bucket": capture_record.get("storage_bucket"),
        "image_url": build_invoice_image_path(capture_record),
        "file_url": build_invoice_image_path(capture_record),
        "previewable": str(content_type or "").startswith("image/"),
    }


def _storage_model_to_dict(value) -> dict:
    if isinstance(value, dict):
        return {str(key): make_json_safe(item) for key, item in value.items()}

    for method_name in ("model_dump", "dict"):
        method = getattr(value, method_name, None)
        if callable(method):
            try:
                result = method()
                if isinstance(result, dict):
                    return {str(key): make_json_safe(item) for key, item in result.items()}
            except TypeError:
                continue

    result = {}
    for field in (
        "id",
        "name",
        "public",
        "created_at",
        "updated_at",
        "last_accessed_at",
        "metadata",
        "owner",
        "file_size_limit",
        "allowed_mime_types",
    ):
        if hasattr(value, field):
            result[field] = make_json_safe(getattr(value, field))
    return result


def _storage_item_is_folder(item: dict) -> bool:
    if str(item.get("type") or "").lower() == "folder":
        return True
    return not item.get("id") and not item.get("metadata") and not item.get("created_at")


def _format_storage_object(bucket_name: str, item: dict, parent_path: str) -> dict:
    name = str(item.get("name") or "").strip()
    object_path = "/".join(part for part in (parent_path.strip("/"), name) if part)
    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    is_folder = _storage_item_is_folder(item)
    return {
        "bucket": bucket_name,
        "name": name,
        "path": object_path,
        "type": "folder" if is_folder else "file",
        "size": metadata.get("size") or item.get("size"),
        "content_type": metadata.get("mimetype") or metadata.get("contentType") or item.get("content_type"),
        "created_at": item.get("created_at"),
        "updated_at": item.get("updated_at") or item.get("last_accessed_at"),
        "storage_url": None if is_folder else f"supabase://{bucket_name}/{object_path}",
    }


def _list_supabase_bucket_objects(client, bucket_name: str, *, path: str = "", depth: int = 0, max_depth: int = 4) -> list[dict]:
    try:
        raw_items = client.storage.from_(bucket_name).list(
            path,
            {
                "limit": 200,
                "offset": 0,
                "sortBy": {"column": "name", "order": "asc"},
            },
        )
    except TypeError:
        raw_items = client.storage.from_(bucket_name).list(path)

    objects = []
    for raw_item in raw_items or []:
        item = _storage_model_to_dict(raw_item)
        formatted = _format_storage_object(bucket_name, item, path)
        if not formatted["name"]:
            continue
        objects.append(formatted)
        if formatted["type"] == "folder" and depth < max_depth:
            objects.extend(
                _list_supabase_bucket_objects(
                    client,
                    bucket_name,
                    path=formatted["path"],
                    depth=depth + 1,
                    max_depth=max_depth,
                )
            )
    return objects


@app.get("/api/storage/browser")
def api_storage_browser():
    if not _supabase_storage_configured():
        return jsonify(
            {
                "configured": False,
                "buckets": [],
                "message": "Supabase Storage is not configured. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY.",
            }
        )

    try:
        client = _create_supabase_client()
        raw_buckets = client.storage.list_buckets()
        buckets = []
        for raw_bucket in raw_buckets or []:
            bucket = _storage_model_to_dict(raw_bucket)
            bucket_name = str(bucket.get("id") or bucket.get("name") or "").strip()
            if not bucket_name:
                continue
            objects = _list_supabase_bucket_objects(client, bucket_name)
            if bucket_name == document_bucket_name():
                objects = [
                    item
                    for item in objects
                    if item.get("path") != "Documents" and not str(item.get("path") or "").startswith("Documents/")
                ]
            buckets.append(
                {
                    "id": bucket_name,
                    "name": str(bucket.get("name") or bucket_name),
                    "public": bool(bucket.get("public")),
                    "created_at": bucket.get("created_at"),
                    "updated_at": bucket.get("updated_at"),
                    "file_count": len([item for item in objects if item["type"] == "file"]),
                    "objects": objects,
                }
            )
        return jsonify({"configured": True, "buckets": buckets})
    except RuntimeError as exc:
        logger.error("Supabase Storage browser configuration error: %s", exc)
        return json_error(str(exc), 503)
    except Exception as exc:
        logger.exception("Supabase Storage browser failed")
        return json_error(f"Failed to list Supabase Storage buckets: {exc}", 502)


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
@app.post("/api/invoices/<sid>/capture")
def api_invoices_capture(sid: str):
    try:
        return capture_invoice(sid, get_json_payload(required=True))
    except ValueError as exc:
        return json_error(str(exc), 400)
    except RuntimeError as exc:
        logger.error("Invoice capture storage error: %s", exc)
        return json_error(f"Failed to store invoice capture: {exc}", 503)
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
