import importlib.util
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Callable


logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
PENDING_ACTIONS_PATH = os.environ.get(
    "IV_AGENT_PENDING_ACTIONS_PATH",
    os.path.join(OUTPUT_DIR, "agent_pending_actions.json"),
)

AGENT_MODEL = (
    os.environ.get("OPENAI_AGENT_MODEL")
    or os.environ.get("OPENAI_ORCHESTRATOR_MODEL")
    or os.environ.get("OPENAI_CALENDAR_AGENT_MODEL")
    or "gpt-5.4-mini"
).strip() or "gpt-5.4-mini"

CALENDAR_AGENT_MODEL = (
    os.environ.get("OPENAI_CALENDAR_AGENT_MODEL")
    or os.environ.get("OPENAI_AGENT_MODEL")
    or "gpt-5.4-mini"
).strip() or "gpt-5.4-mini"

STORAGE_AGENT_MODEL = (
    os.environ.get("OPENAI_DOCUMENT_AGENT_MODEL")
    or os.environ.get("OPENAI_STORAGE_AGENT_MODEL")
    or os.environ.get("OPENAI_AGENT_MODEL")
    or "gpt-5.4-mini"
).strip() or "gpt-5.4-mini"

KNOWLEDGE_AGENT_MODEL = (
    os.environ.get("OPENAI_DOCUMENT_AGENT_MODEL")
    or os.environ.get("OPENAI_KNOWLEDGE_AGENT_MODEL")
    or os.environ.get("OPENAI_AGENT_MODEL")
    or "gpt-5.4-mini"
).strip() or "gpt-5.4-mini"

AUTOMATIONS_AGENT_MODEL = (
    os.environ.get("OPENAI_AUTOMATION_MODEL")
    or os.environ.get("OPENAI_AGENT_MODEL")
    or "gpt-5.4-mini"
).strip() or "gpt-5.4-mini"

ACTION_TYPE_ALIASES = {
    "calendar.create_event": "create_event",
    "calendar.update_event": "update_event",
    "calendar.delete_event": "delete_event",
    "reminder.create": "create_reminder",
    "automation.create": "create_reminder",
    "automation.save": "create_reminder",
    "report.generate": "generate_report",
    "report.send": "send_report",
    "storage.create_document_folder": "storage.create_folder",
    "storage.move": "storage.move_document",
    "storage.delete": "storage.delete_document",
    "storage.update_document_metadata": "storage.update_metadata",
}

SUPPORTED_ACTION_TYPES = {
    "create_event",
    "update_event",
    "delete_event",
    "create_reminder",
    "generate_report",
    "send_report",
    "storage.create_folder",
    "storage.move_document",
    "storage.delete_document",
    "storage.update_metadata",
    "storage.reassign_bucket",
}


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def make_json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {str(key): make_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [make_json_safe(item) for item in value]
    return str(value)


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _normalize_thread_id(value: Any) -> str:
    candidate = str(value or "").strip()
    if not candidate:
        return _new_id("thread")
    if len(candidate) > 120:
        return _new_id("thread")
    if all(ch.isalnum() or ch in ("-", "_", ".") for ch in candidate):
        return candidate
    return _new_id("thread")


def normalize_agent_chat_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("JSON body is required")

    message = str(payload.get("message", "")).strip()
    if not message:
        raise ValueError("message is required")

    attachments = payload.get("attachments", [])
    if not isinstance(attachments, list):
        raise ValueError("attachments must be a list")

    client_context = payload.get("client_context", {})
    if client_context is None:
        client_context = {}
    if not isinstance(client_context, dict):
        raise ValueError("client_context must be an object")

    raw_history = payload.get("history", [])
    history = raw_history if isinstance(raw_history, list) else []

    return {
        "message": message,
        "thread_id": _normalize_thread_id(payload.get("thread_id")),
        "attachments": [item for item in attachments if isinstance(item, dict)],
        "client_context": client_context,
        "history": history[-20:],
        "timestamp": utc_timestamp(),
    }


def _normalize_action_type(value: Any) -> str:
    action_type = str(value or "").strip().lower()
    return ACTION_TYPE_ALIASES.get(action_type, action_type)


_PENDING_ACTION_STORE: Any = None


def _coerce_action_row(row: dict[str, Any]) -> dict[str, Any]:
    def _iso(value: Any) -> str:
        if not value:
            return ""
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return str(value)

    return {
        "action_id": str(row.get("action_id") or ""),
        "type": str(row.get("type") or ""),
        "title": str(row.get("title") or ""),
        "payload": row.get("payload") if isinstance(row.get("payload"), dict) else {},
        "status": str(row.get("status") or "pending"),
        "thread_id": str(row.get("thread_id") or ""),
        "user_id": str(row.get("user_id") or "default"),
        "created_at": _iso(row.get("created_at")),
        "confirmed_at": _iso(row.get("confirmed_at")) if row.get("confirmed_at") else None,
        "failed_at": _iso(row.get("failed_at")) if row.get("failed_at") else None,
        "result": row.get("result") if isinstance(row.get("result"), dict) else None,
        "error": str(row.get("error")) if row.get("error") else None,
    }


class _FilePendingActionStore:
    """Local filesystem store. Suitable for dev. Not safe on serverless platforms."""

    def __init__(self, path: str):
        self._path = path

    def _read(self) -> list[dict[str, Any]]:
        if not os.path.exists(self._path):
            return []
        try:
            with open(self._path, "r", encoding="utf-8") as file:
                payload = json.load(file)
        except (OSError, json.JSONDecodeError):
            logger.warning("Could not read pending action store; starting empty")
            return []
        if not isinstance(payload, dict) or not isinstance(payload.get("actions"), list):
            return []
        return [item for item in payload["actions"] if isinstance(item, dict)]

    def _write(self, actions: list[dict[str, Any]]) -> None:
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as file:
            json.dump(make_json_safe({"actions": actions}), file, indent=2)

    def add(self, action: dict[str, Any]) -> None:
        actions = self._read()
        actions.append(action)
        self._write(actions)

    def get(self, action_id: str) -> dict[str, Any] | None:
        for action in self._read():
            if action.get("action_id") == action_id:
                return action
        return None

    def latest_pending(self, thread_id: str, user_id: str | None = None) -> dict[str, Any] | None:
        candidates = [
            action for action in self._read()
            if action.get("status") == "pending"
            and action.get("thread_id") == thread_id
            and (not user_id or not action.get("user_id") or action.get("user_id") == user_id)
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
        return candidates[0]

    def update(self, action_id: str, fields: dict[str, Any]) -> dict[str, Any] | None:
        actions = self._read()
        target = None
        for action in actions:
            if action.get("action_id") == action_id:
                action.update(fields)
                target = action
                break
        if target is None:
            return None
        self._write(actions)
        return target


class _PostgresPendingActionStore:
    """Persistent pending action store backed by Supabase Postgres."""

    def __init__(self, database_url: str):
        try:
            from ..storage import _connect_postgres
        except ImportError:
            from storage import _connect_postgres
        self._connect = lambda: _connect_postgres(database_url)
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS agent_pending_actions (
                        action_id TEXT PRIMARY KEY,
                        type TEXT NOT NULL,
                        title TEXT NOT NULL DEFAULT '',
                        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                        status TEXT NOT NULL DEFAULT 'pending',
                        thread_id TEXT NOT NULL DEFAULT '',
                        user_id TEXT NOT NULL DEFAULT 'default',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        confirmed_at TIMESTAMPTZ,
                        failed_at TIMESTAMPTZ,
                        result JSONB,
                        error TEXT
                    )
                    """
                )
                cursor.execute("CREATE INDEX IF NOT EXISTS agent_pending_actions_thread_idx ON agent_pending_actions (thread_id, status, created_at DESC)")
                cursor.execute("CREATE INDEX IF NOT EXISTS agent_pending_actions_user_status_idx ON agent_pending_actions (user_id, status, created_at DESC)")

    def add(self, action: dict[str, Any]) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO agent_pending_actions (
                        action_id, type, title, payload, status, thread_id, user_id, created_at
                    ) VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, NOW())
                    ON CONFLICT (action_id) DO NOTHING
                    """,
                    (
                        action["action_id"],
                        action["type"],
                        action.get("title") or "",
                        json.dumps(make_json_safe(action.get("payload") or {})),
                        action.get("status") or "pending",
                        action.get("thread_id") or "",
                        action.get("user_id") or "default",
                    ),
                )

    def get(self, action_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM agent_pending_actions WHERE action_id = %s LIMIT 1",
                    (action_id,),
                )
                row = cursor.fetchone()
        return _coerce_action_row(row) if row else None

    def latest_pending(self, thread_id: str, user_id: str | None = None) -> dict[str, Any] | None:
        sql = "SELECT * FROM agent_pending_actions WHERE thread_id = %s AND status = 'pending'"
        params: list[Any] = [thread_id]
        if user_id:
            sql += " AND user_id = %s"
            params.append(user_id)
        sql += " ORDER BY created_at DESC LIMIT 1"
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                row = cursor.fetchone()
        return _coerce_action_row(row) if row else None

    def update(self, action_id: str, fields: dict[str, Any]) -> dict[str, Any] | None:
        if not fields:
            return self.get(action_id)
        column_map = {
            "status": ("status", lambda v: str(v)),
            "confirmed_at": ("confirmed_at", lambda v: v),
            "failed_at": ("failed_at", lambda v: v),
            "result": ("result", lambda v: json.dumps(make_json_safe(v)) if v is not None else None),
            "error": ("error", lambda v: str(v) if v is not None else None),
        }
        set_clauses: list[str] = []
        params: list[Any] = []
        for key, value in fields.items():
            if key not in column_map:
                continue
            column, transform = column_map[key]
            if column == "result":
                set_clauses.append(f"{column} = %s::jsonb")
            else:
                set_clauses.append(f"{column} = %s")
            params.append(transform(value))
        if not set_clauses:
            return self.get(action_id)
        params.append(action_id)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"UPDATE agent_pending_actions SET {', '.join(set_clauses)} WHERE action_id = %s RETURNING *",
                    tuple(params),
                )
                row = cursor.fetchone()
        return _coerce_action_row(row) if row else None


def _get_pending_action_store():
    global _PENDING_ACTION_STORE
    if _PENDING_ACTION_STORE is not None:
        return _PENDING_ACTION_STORE
    database_url = os.environ.get("DATABASE_URL", "").strip()
    backend = str(os.environ.get("IV_AGENT_STORAGE_BACKEND", "auto") or "auto").strip().lower()
    if database_url and backend != "local":
        try:
            _PENDING_ACTION_STORE = _PostgresPendingActionStore(database_url)
            return _PENDING_ACTION_STORE
        except Exception as exc:
            logger.warning("Falling back to file pending action store: %s", exc)
    _PENDING_ACTION_STORE = _FilePendingActionStore(PENDING_ACTIONS_PATH)
    return _PENDING_ACTION_STORE


def _public_pending_action(action: dict[str, Any]) -> dict[str, Any]:
    return {
        "action_id": action["action_id"],
        "type": action["type"],
        "title": action.get("title") or action["type"].replace("_", " ").title(),
        "payload": make_json_safe(action.get("payload") or {}),
        "status": action.get("status", "pending"),
        "thread_id": action.get("thread_id", ""),
        "user_id": action.get("user_id", ""),
        "requires_confirmation": True,
        "created_at": action.get("created_at", ""),
    }


def register_pending_actions(
    raw_actions: list[Any],
    *,
    thread_id: str,
    user_id: str | None = None,
) -> list[dict[str, Any]]:
    if not raw_actions:
        return []

    store = _get_pending_action_store()
    registered: list[dict[str, Any]] = []

    for raw_action in raw_actions:
        if not isinstance(raw_action, dict):
            continue

        action_type = _normalize_action_type(
            raw_action.get("type") or raw_action.get("action_type") or raw_action.get("name")
        )
        if action_type not in SUPPORTED_ACTION_TYPES:
            continue

        action_id = str(raw_action.get("action_id") or raw_action.get("id") or "").strip()
        if not action_id:
            action_id = _new_id("act")

        payload = raw_action.get("payload")
        if payload is None:
            payload = raw_action.get("args") or raw_action.get("arguments") or {}
        if not isinstance(payload, dict):
            payload = {"value": payload}

        action = {
            "action_id": action_id,
            "type": action_type,
            "title": str(raw_action.get("title") or raw_action.get("label") or "").strip(),
            "payload": make_json_safe(payload),
            "status": "pending",
            "thread_id": thread_id,
            "user_id": str(raw_action.get("user_id") or user_id or payload.get("user_id") or "default").strip() or "default",
            "created_at": utc_timestamp(),
        }
        store.add(action)
        registered.append(_public_pending_action(action))

    return registered


def find_latest_pending_action_for_thread(
    thread_id: str,
    *,
    user_id: str | None = None,
) -> dict[str, Any] | None:
    """Return the most recently drafted, still-pending action for a given thread."""
    normalized_thread_id = str(thread_id or "").strip()
    if not normalized_thread_id:
        return None
    normalized_user_id = str(user_id or "").strip() or None
    store = _get_pending_action_store()
    action = store.latest_pending(normalized_thread_id, user_id=normalized_user_id)
    return _public_pending_action(action) if action else None


def confirm_pending_action(
    action_id: str,
    executor: Callable[[dict[str, Any]], dict[str, Any]],
    *,
    thread_id: str | None = None,
    user_id: str | None = None,
) -> dict[str, Any]:
    normalized_action_id = str(action_id or "").strip()
    if not normalized_action_id:
        raise KeyError("Pending action not found")

    store = _get_pending_action_store()
    target_action = store.get(normalized_action_id)
    if not target_action:
        raise KeyError("Pending action not found")

    expected_thread_id = str(thread_id or "").strip()
    if expected_thread_id and target_action.get("thread_id") != expected_thread_id:
        raise PermissionError("Pending action does not belong to this chat thread")

    expected_user_id = str(user_id or "").strip()
    if expected_user_id and target_action.get("user_id") and target_action.get("user_id") != expected_user_id:
        raise PermissionError("Pending action does not belong to this user")

    if target_action.get("status") != "pending":
        raise RuntimeError("Pending action has already been handled")

    try:
        result = executor(target_action)
    except Exception as exc:
        store.update(normalized_action_id, {
            "status": "failed",
            "failed_at": datetime.now(timezone.utc),
            "error": str(exc),
        })
        raise

    updated = store.update(normalized_action_id, {
        "status": "confirmed",
        "confirmed_at": datetime.now(timezone.utc),
        "result": make_json_safe(result),
    }) or target_action
    updated["status"] = "confirmed"
    return {
        "action": _public_pending_action(updated),
        "result": make_json_safe(result),
    }


def _tool_event(name: str, status: str, message: str, *, event_type: str = "tool_call") -> dict[str, Any]:
    return {
        "id": _new_id("tool"),
        "type": event_type,
        "name": name,
        "status": status,
        "message": message,
        "timestamp": utc_timestamp(),
    }

def _run_orchestrator_unavailable(
    request_payload: dict[str, Any],
    *,
    reason: str,
    local_tools: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    tool_events = [
        _tool_event("orchestrator", "unavailable", reason, event_type="agent"),
    ]
    lower_message = request_payload["message"].lower()
    calendar_hint = ""
    document_answer = ""
    document_artifacts: list[dict[str, Any]] = []
    if local_tools and "calendar_snapshot" in local_tools and any(
        token in lower_message for token in ("calendar", "kalender", "termin", "event")
    ):
        try:
            snapshot = local_tools["calendar_snapshot"](
                {
                    "month": request_payload.get("client_context", {}).get("current_month", ""),
                    "profile_id": request_payload.get("client_context", {}).get("profile_id", "default"),
                }
            )
            month = str(snapshot.get("month") or "").strip()
            event_count = len(snapshot.get("events") or [])
            calendar_hint = (
                f" Der lokale Kalenderzugriff ist im Backend registriert; "
                f"fuer {month or 'den aktuellen Monat'} wurden {event_count} Termine gefunden."
            )
            tool_events.append(_tool_event("calendar_snapshot", "completed", "Local calendar snapshot checked"))
        except Exception as exc:
            logger.warning("Could not inspect local calendar while orchestrator is unavailable: %s", exc)
            tool_events.append(_tool_event("calendar_snapshot", "failed", "Local calendar snapshot failed"))

    if any(
        token in lower_message
        for token in ("dokument", "datei", "rechnung", "storage", "bucket", "download", "ablage")
    ):
        try:
            try:
                from ..services.storage_service import (
                    build_chat_document_artifact,
                    infer_document_bucket_from_text,
                    list_documents,
                    search_documents,
                )
            except ImportError:
                from services.storage_service import (
                    build_chat_document_artifact,
                    infer_document_bucket_from_text,
                    list_documents,
                    search_documents,
                )
            context = request_payload.get("client_context", {}) if isinstance(request_payload.get("client_context"), dict) else {}
            user_id = str(context.get("profile_id") or context.get("user_id") or "default").strip() or "default"
            bucket = infer_document_bucket_from_text(request_payload["message"])
            documents = (
                search_documents(user_id=user_id, query=request_payload["message"], storage_bucket=bucket, limit=10)
                if any(token in lower_message for token in ("suche", "finde", "zeige", "rechnung", "download"))
                else list_documents(user_id=user_id, storage_bucket=bucket, limit=10)
            )
            document_artifacts = [build_chat_document_artifact(document) for document in documents]
            lines = [
                f"Ich konnte lokal {len(documents)} Dokument(e) aus der Storage-Metadata lesen.",
            ]
            for document in documents[:5]:
                parts = [
                    str(document.get("file_name") or "Dokument"),
                    str(document.get("document_type") or "").strip(),
                    str(document.get("institution") or "").strip(),
                    str(document.get("storage_bucket") or "").strip(),
                ]
                lines.append("- " + " | ".join(part for part in parts if part))
            if documents:
                lines.append("Die Download-Links sind als Dokument-Artefakte angehaengt.")
            document_answer = "\n".join(lines)
            tool_events.append(_tool_event("list_user_documents", "completed", "Local document metadata read"))
        except Exception as exc:
            logger.warning("Could not inspect documents while orchestrator is unavailable: %s", exc)
            tool_events.append(_tool_event("list_user_documents", "failed", "Local document metadata failed"))

    if document_answer:
        answer = (
            f"{document_answer}\n\n"
            f"Hinweis: Der OpenAI Agents SDK Lauf konnte aktuell nicht starten: {reason}."
        )
    else:
        answer = (
            "Der Chat ist jetzt auf den Orchestrator ausgerichtet, aber der OpenAI Agents SDK Lauf "
            f"kann aktuell nicht starten: {reason}.{calendar_hint} "
            "Lokale Kalender- und Dokumentfunktionen bleiben ueber die Backend-APIs verfuegbar."
        )

    return {
        "answer": answer,
        "citations": [],
        "tool_events": tool_events,
        "artifacts": document_artifacts,
        "pending_actions": [],
        "structured_actions": [],
        "thread_id": request_payload["thread_id"],
    }


def _agents_sdk_available() -> bool:
    return importlib.util.find_spec("agents") is not None


def _should_run_agents_sdk() -> bool:
    if str(os.environ.get("IV_AGENT_DISABLE_OPENAI_AGENTS", "")).strip().lower() in {"1", "true", "yes"}:
        return False
    return bool(os.environ.get("OPENAI_API_KEY", "").strip() and _agents_sdk_available())


def _run_agents_sdk(
    request_payload: dict[str, Any],
    *,
    local_tools: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    from agents import Agent, Runner, function_tool, set_tracing_disabled, trace  # type: ignore

    try:
        from .automations_agent import build_automations_agent
        from .calendar_agent import build_calendar_agent
        from .knowledge_agent import build_knowledge_agent
        from .storage_agent import build_storage_agent
        from ..services.calendar_service import (
            list_calendar_events as service_list_calendar_events,
            normalize_timezone,
            normalize_user_id,
        )
        from ..services.storage_service import (
            DOCUMENT_BUNDLE_MAX_FILES,
            build_chat_document_artifact as service_build_chat_document_artifact,
            build_document_bundle_artifact as service_build_document_bundle_artifact,
            get_document as service_get_document,
            infer_document_bucket_from_text,
            list_documents as service_list_documents,
            search_documents as service_search_documents,
            sum_invoice_amounts as service_sum_invoice_amounts,
        )
    except ImportError:
        from agents.automations_agent import build_automations_agent
        from agents.calendar_agent import build_calendar_agent
        from agents.knowledge_agent import build_knowledge_agent
        from agents.storage_agent import build_storage_agent
        from services.calendar_service import (
            list_calendar_events as service_list_calendar_events,
            normalize_timezone,
            normalize_user_id,
        )
        from services.storage_service import (
            DOCUMENT_BUNDLE_MAX_FILES,
            build_chat_document_artifact as service_build_chat_document_artifact,
            build_document_bundle_artifact as service_build_document_bundle_artifact,
            get_document as service_get_document,
            infer_document_bucket_from_text,
            list_documents as service_list_documents,
            search_documents as service_search_documents,
            sum_invoice_amounts as service_sum_invoice_amounts,
        )

    if str(os.environ.get("IV_AGENT_ENABLE_OPENAI_TRACING", "")).strip().lower() not in {"1", "true", "yes"}:
        set_tracing_disabled(True)

    drafted_actions: list[dict[str, Any]] = []
    collected_citations: list[dict[str, Any]] = []
    collected_artifacts: list[dict[str, Any]] = []
    structured_actions: list[dict[str, Any]] = []
    tool_events: list[dict[str, Any]] = [
        _tool_event("orchestrator", "started", f"OpenAI Agents SDK orchestrator using {AGENT_MODEL}", event_type="agent")
    ]
    orchestrator_tools = []

    client_context = request_payload.get("client_context", {}) if isinstance(request_payload.get("client_context"), dict) else {}
    context_user_id = normalize_user_id(client_context.get("profile_id") or client_context.get("user_id") or "default")
    context_timezone = normalize_timezone(client_context.get("timezone"))

    def _collect_document_artifacts(documents: Any) -> None:
        existing_ids = {
            str(item.get("document_id") or item.get("id"))
            for item in collected_artifacts
            if isinstance(item, dict)
        }
        for document in documents if isinstance(documents, list) else [documents]:
            if not isinstance(document, dict) or not document.get("document_id"):
                continue
            document_id = str(document.get("document_id"))
            if document_id in existing_ids:
                continue
            artifact = dict(document) if document.get("download_url") else service_build_chat_document_artifact(document)
            artifact.setdefault("id", document_id)
            artifact.setdefault("type", "document")
            collected_artifacts.append(artifact)
            existing_ids.add(document_id)

    @function_tool
    def draft_pending_action(action_type: str, title: str, payload_json: str) -> str:
        """Draft a side-effecting action for user confirmation without executing it."""
        try:
            payload = json.loads(payload_json or "{}")
        except json.JSONDecodeError as exc:
            raise ValueError("payload_json must be valid JSON") from exc
        actions = register_pending_actions(
            [{"type": action_type, "title": title, "payload": payload}],
            thread_id=request_payload["thread_id"],
            user_id=context_user_id,
        )
        drafted_actions.extend(actions)
        structured_actions.extend(actions)
        return json.dumps(
            {
                "pending_actions": actions,
            },
            ensure_ascii=True,
        )

    orchestrator_tools.append(draft_pending_action)

    def _optional_int(value: Any) -> int | None:
        try:
            parsed = int(value or 0)
        except (TypeError, ValueError):
            return None
        return parsed or None

    def _json_list(value: str) -> list[str]:
        raw = str(value or "").strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        except json.JSONDecodeError:
            pass
        return [item.strip() for item in raw.split(",") if item.strip()]

    @function_tool
    def list_calendar_range(start_at: str, end_at: str, query: str = "") -> str:
        """Read calendar events for mixed calendar/document questions using explicit ISO datetime bounds."""
        tool_events.append(_tool_event("list_calendar_range", "started", "Reading local calendar"))
        payload = service_list_calendar_events(
            user_id=context_user_id,
            start_at=start_at,
            end_at=end_at,
            query=query,
            timezone_name=context_timezone,
        )
        tool_events.append(_tool_event("list_calendar_range", "completed", "Calendar range read"))
        return json.dumps(make_json_safe(payload), ensure_ascii=True)

    orchestrator_tools.append(list_calendar_range)

    @function_tool
    def list_user_documents(
        year: int = 0,
        month: int = 0,
        start_date: str = "",
        end_date: str = "",
        document_type: str = "",
        institution: str = "",
        tags_json: str = "[]",
        folder_id: str = "",
        storage_bucket: str = "",
        limit: int = 25,
    ) -> str:
        """Read stored documents for mixed calendar/document questions with optional storage bucket."""
        tool_events.append(_tool_event("list_user_documents", "started", "Reading document metadata"))
        documents = service_list_documents(
            user_id=context_user_id,
            year=_optional_int(year),
            month=_optional_int(month),
            start_date=start_date or None,
            end_date=end_date or None,
            document_type=document_type,
            institution=institution,
            tags=_json_list(tags_json),
            folder_id=folder_id or None,
            storage_bucket=storage_bucket or infer_document_bucket_from_text(institution),
            limit=limit,
        )
        _collect_document_artifacts(documents)
        tool_events.append(_tool_event("list_user_documents", "completed", "Document metadata read"))
        return json.dumps(make_json_safe({"documents": documents}), ensure_ascii=True)

    orchestrator_tools.append(list_user_documents)

    @function_tool
    def search_user_documents(
        query: str,
        year: int = 0,
        month: int = 0,
        start_date: str = "",
        end_date: str = "",
        document_type: str = "",
        institution: str = "",
        tags_json: str = "[]",
        folder_id: str = "",
        storage_bucket: str = "",
        limit: int = 10,
    ) -> str:
        """Search stored documents for mixed calendar/document questions with optional storage bucket."""
        try:
            from ..tools.storage_tools import _structured_storage_query as _normalize_storage_query
        except ImportError:
            from tools.storage_tools import _structured_storage_query as _normalize_storage_query
        tool_events.append(_tool_event("search_user_documents", "started", "Searching documents"))
        bucket_filter = storage_bucket or infer_document_bucket_from_text(f"{query} {institution}")
        structural_filters_present = any(
            [
                bucket_filter,
                document_type,
                institution,
                _json_list(tags_json),
                _optional_int(year),
                _optional_int(month),
                start_date,
                end_date,
            ]
        )
        normalized_query = _normalize_storage_query(
            query,
            storage_bucket=bucket_filter,
            institution=institution,
        )
        effective_query = normalized_query if structural_filters_present else (normalized_query or query)
        documents = service_search_documents(
            user_id=context_user_id,
            query=effective_query,
            year=_optional_int(year),
            month=_optional_int(month),
            start_date=start_date or None,
            end_date=end_date or None,
            document_type=document_type,
            institution=institution,
            tags=_json_list(tags_json),
            storage_bucket=bucket_filter,
            folder_id=folder_id or None,
            limit=limit,
        )
        if not documents and effective_query and structural_filters_present:
            documents = service_search_documents(
                user_id=context_user_id,
                query="",
                year=_optional_int(year),
                month=_optional_int(month),
                start_date=start_date or None,
                end_date=end_date or None,
                document_type=document_type,
                institution=institution,
                tags=_json_list(tags_json),
                storage_bucket=bucket_filter,
                folder_id=folder_id or None,
                limit=limit,
            )
        _collect_document_artifacts(documents)
        tool_events.append(_tool_event("search_user_documents", "completed", "Document search completed"))
        return json.dumps(make_json_safe({"query": query, "effective_query": effective_query, "documents": documents}), ensure_ascii=True)

    orchestrator_tools.append(search_user_documents)

    @function_tool
    def sum_user_invoice_amounts(
        query: str = "",
        year: int = 0,
        month: int = 0,
        start_date: str = "",
        end_date: str = "",
        institution: str = "",
        tags_json: str = "[]",
        storage_bucket: str = "",
        limit: int = 100,
    ) -> str:
        """Sum stored invoices after applying document filters; exact duplicate checksums are ignored."""
        tool_events.append(_tool_event("sum_user_invoice_amounts", "started", "Summing invoice amounts"))
        payload = service_sum_invoice_amounts(
            user_id=context_user_id,
            query=query,
            year=_optional_int(year),
            month=_optional_int(month),
            start_date=start_date or None,
            end_date=end_date or None,
            institution=institution,
            tags=_json_list(tags_json),
            storage_bucket=storage_bucket or infer_document_bucket_from_text(f"{query} {institution}"),
            limit=limit,
        )
        _collect_document_artifacts(payload.get("counted_documents") or [])
        _collect_document_artifacts(payload.get("documents_without_amount") or [])
        tool_events.append(_tool_event("sum_user_invoice_amounts", "completed", "Invoice sum completed"))
        return json.dumps(make_json_safe(payload), ensure_ascii=True)

    orchestrator_tools.append(sum_user_invoice_amounts)

    @function_tool
    def bundle_user_documents(
        document_ids_json: str = "[]",
        query: str = "",
        storage_bucket: str = "",
        document_type: str = "",
        institution: str = "",
        tags_json: str = "[]",
        year: int = 0,
        month: int = 0,
        start_date: str = "",
        end_date: str = "",
        limit: int = 20,
        bundle_name: str = "",
    ) -> str:
        """Bundle stored documents into a downloadable ZIP artifact (read-only, no pending action)."""
        tool_events.append(_tool_event("bundle_user_documents", "started", "Building ZIP bundle"))
        document_ids = _json_list(document_ids_json)
        documents: list[dict[str, Any]] = []
        if document_ids:
            for document_id in document_ids[:DOCUMENT_BUNDLE_MAX_FILES]:
                document = service_get_document(user_id=context_user_id, document_id=document_id)
                if document:
                    documents.append(document)
        else:
            try:
                from ..tools.storage_tools import _structured_storage_query as _normalize_storage_query
            except ImportError:
                from tools.storage_tools import _structured_storage_query as _normalize_storage_query
            bucket_filter = storage_bucket or infer_document_bucket_from_text(f"{query} {institution}")
            bounded_limit = min(max(1, int(limit or 20)), DOCUMENT_BUNDLE_MAX_FILES)
            normalized_query = _normalize_storage_query(
                query,
                storage_bucket=bucket_filter,
                institution=institution,
            )
            structural_filters_present = any(
                [
                    bucket_filter,
                    document_type,
                    institution,
                    _json_list(tags_json),
                    _optional_int(year),
                    _optional_int(month),
                    start_date,
                    end_date,
                ]
            )
            effective_query = normalized_query if structural_filters_present else (normalized_query or query)
            documents = service_search_documents(
                user_id=context_user_id,
                query=effective_query,
                year=_optional_int(year),
                month=_optional_int(month),
                start_date=start_date or None,
                end_date=end_date or None,
                document_type=document_type,
                institution=institution,
                tags=_json_list(tags_json),
                storage_bucket=bucket_filter,
                limit=bounded_limit,
            )
            if not documents and effective_query and structural_filters_present:
                documents = service_search_documents(
                    user_id=context_user_id,
                    query="",
                    year=_optional_int(year),
                    month=_optional_int(month),
                    start_date=start_date or None,
                    end_date=end_date or None,
                    document_type=document_type,
                    institution=institution,
                    tags=_json_list(tags_json),
                    storage_bucket=bucket_filter,
                    limit=bounded_limit,
                )
            if not documents:
                fallback_ids: list[str] = []
                for item in collected_artifacts:
                    if not isinstance(item, dict) or item.get("type") != "document":
                        continue
                    candidate = str(item.get("document_id") or item.get("id") or "").strip()
                    if candidate and candidate not in fallback_ids:
                        fallback_ids.append(candidate)
                for document_id in fallback_ids[:DOCUMENT_BUNDLE_MAX_FILES]:
                    document = service_get_document(user_id=context_user_id, document_id=document_id)
                    if document:
                        documents.append(document)
        _collect_document_artifacts(documents)
        bundle_title = (bundle_name or "Dokumentenpaket.zip").strip() or "Dokumentenpaket.zip"
        bundle = service_build_document_bundle_artifact(
            documents,
            user_id=context_user_id,
            title=bundle_title,
            file_name=bundle_title if bundle_title.lower().endswith(".zip") else "documents_bundle.zip",
        )
        if not bundle:
            tool_events.append(_tool_event("bundle_user_documents", "completed", "No documents to bundle"))
            return json.dumps(
                make_json_safe(
                    {
                        "bundle": None,
                        "documents": [],
                        "count": 0,
                        "message": "Keine Dokumente zum Bündeln gefunden. Bitte zuerst Dokumente auflisten oder konkrete document_ids angeben.",
                    }
                ),
                ensure_ascii=True,
            )
        collected_artifacts.append(bundle)
        tool_events.append(_tool_event("bundle_user_documents", "completed", f"Bundle with {len(bundle['document_ids'])} files"))
        return json.dumps(
            make_json_safe(
                {
                    "bundle": bundle,
                    "documents": documents,
                    "count": len(bundle["document_ids"]),
                    "download_url": bundle["download_url"],
                    "max_files": DOCUMENT_BUNDLE_MAX_FILES,
                }
            ),
            ensure_ascii=True,
        )

    orchestrator_tools.append(bundle_user_documents)

    now_value = str(client_context.get("now") or request_payload.get("timestamp") or utc_timestamp())
    current_month = str(client_context.get("current_month") or "").strip()
    calendar_view = str(client_context.get("calendar_view") or "").strip()
    uploaded_documents = [
        item for item in request_payload.get("attachments", [])
        if isinstance(item, dict) and item.get("type") == "document" and item.get("document_id")
    ]

    calendar_agent = build_calendar_agent(
        Agent,
        function_tool,
        model=CALENDAR_AGENT_MODEL,
        context_user_id=context_user_id,
        context_timezone=context_timezone,
        now_value=now_value,
        current_month=current_month,
        calendar_view=calendar_view,
        thread_id=request_payload["thread_id"],
        tool_events=tool_events,
        drafted_actions=drafted_actions,
        structured_actions=structured_actions,
        register_pending_actions=register_pending_actions,
        make_json_safe=make_json_safe,
        tool_event_factory=_tool_event,
    )
    tool_events.append(_tool_event("CalendarAgent", "available", "CalendarAgent handoff registered", event_type="agent"))

    storage_agent = build_storage_agent(
        Agent,
        function_tool,
        model=STORAGE_AGENT_MODEL,
        context_user_id=context_user_id,
        now_value=now_value,
        uploaded_documents=uploaded_documents,
        thread_id=request_payload["thread_id"],
        tool_events=tool_events,
        drafted_actions=drafted_actions,
        structured_actions=structured_actions,
        collected_artifacts=collected_artifacts,
        register_pending_actions=register_pending_actions,
        make_json_safe=make_json_safe,
        tool_event_factory=_tool_event,
    )
    tool_events.append(_tool_event("StorageAgent", "available", "StorageAgent handoff registered", event_type="agent"))

    knowledge_agent = build_knowledge_agent(
        Agent,
        function_tool,
        model=KNOWLEDGE_AGENT_MODEL,
        context_user_id=context_user_id,
        now_value=now_value,
        thread_id=request_payload["thread_id"],
        recent_history=request_payload.get("history", [])[-8:],
        tool_events=tool_events,
        make_json_safe=make_json_safe,
        tool_event_factory=_tool_event,
    )
    tool_events.append(_tool_event("KnowledgeAgent", "available", "KnowledgeAgent handoff registered", event_type="agent"))

    automations_agent = build_automations_agent(
        Agent,
        function_tool,
        model=AUTOMATIONS_AGENT_MODEL,
        context_user_id=context_user_id,
        context_timezone=context_timezone,
        now_value=now_value,
        current_month=current_month,
        thread_id=request_payload["thread_id"],
        tool_events=tool_events,
        drafted_actions=drafted_actions,
        structured_actions=structured_actions,
        register_pending_actions=register_pending_actions,
        make_json_safe=make_json_safe,
        tool_event_factory=_tool_event,
    )
    tool_events.append(_tool_event("AutomationsAgent", "available", "AutomationsAgent handoff registered", event_type="agent"))

    instructions = (
        "You are the IV-Helper orchestrator. Every chat message reaches you first. "
        "Before answering, decide whether to answer directly, inspect local app state, draft a pending action, "
        "or hand off to a specialized agent. "
        "For every calendar, appointment, Termin, Therapie, scheduling, counting, or availability request, hand off to CalendarAgent. "
        "For upload, delete, move, metadata, folder, Datei speichern, Dokument loeschen, or document organization requests, hand off to StorageAgent. "
        "For document retrieval requests ('gib mir alle Dokumente', 'zeige Rechnungen', 'download Datei') always read storage first and ensure document artifacts are produced. "
        "For any request to bundle/zip/pack/download multiple files together ('bündeln', 'als ZIP', 'als Ordner herunterladen', 'Paket', 'zusammen herunterladen'), "
        "use the bundle_user_documents tool directly or hand off to StorageAgent — never draft a pending action and never claim that bundling is read-only or blocked. "
        "For invoice total or sum requests, use sum_user_invoice_amounts or hand off to StorageAgent so the sum is based on filtered storage documents and checksum duplicate removal. "
        "When the user mentions IV, TixiTaxi, Stiftung, Versicherung, or Versicherungen, use it as the storage_bucket filter for storage reads. "
        "For report generation, Assistenzbeitrag report, Transportkosten report, automation, reminder, Monatsende, or recurring reminder requests, hand off to AutomationsAgent. "
        "For 'Was bedeutet...', IV questions, deadlines, Fristen, document understanding, comparisons, action items, broader knowledge, "
        "or 'frag den IV Assistant' requests, hand off to KnowledgeAgent. "
        "If the request combines calendar and documents, use local read tools from both domains before answering or hand off to the best specialized agent. "
        "Raw file Base64 is never available to you; uploaded attachments are already persisted and represented as document metadata. "
        "Do not execute side effects directly. For create, update, delete, reminder creation, PDF generation, report sending, "
        "or automation saves, call draft_pending_action and explain that the user must confirm it. "
        "Supported generic action_type values are create_reminder, generate_report, and send_report. Calendar mutations belong to CalendarAgent. "
        "Report generation and automation saves belong to AutomationsAgent. Storage mutations belong to StorageAgent. Document explanation and synthesis belongs to KnowledgeAgent. "
        "Answer in German unless the user explicitly requests another language. Format final answers as clean concise Markdown. "
        "Keep answers concise, mention which capability you used when useful, and cite retrieved sources when available."
    )
    agent = Agent(
        name="IV-Helper Orchestrator",
        instructions=instructions,
        model=AGENT_MODEL,
        tools=orchestrator_tools,
        handoffs=[calendar_agent, storage_agent, knowledge_agent, automations_agent],
    )

    input_text = json.dumps(
        {
            "message": request_payload["message"],
            "thread_id": request_payload["thread_id"],
            "attachments": request_payload.get("attachments", []),
            "client_context": request_payload.get("client_context", {}),
            "recent_history": request_payload.get("history", [])[-8:],
        },
        ensure_ascii=True,
    )
    with trace(workflow_name="IV-Helper Agent Chat", group_id=request_payload["thread_id"]):
        result = Runner.run_sync(agent, input_text, max_turns=12)

    tool_events.append(_tool_event("orchestrator", "completed", "OpenAI Agents SDK run completed", event_type="agent"))
    return {
        "answer": str(getattr(result, "final_output", "") or "").strip(),
        "citations": collected_citations,
        "tool_events": tool_events,
        "artifacts": collected_artifacts,
        "pending_actions": drafted_actions,
        "structured_actions": structured_actions,
        "thread_id": request_payload["thread_id"],
    }


def run_agent_chat(
    payload: dict[str, Any],
    *,
    local_tools: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    request_payload = normalize_agent_chat_payload(payload)

    if str(os.environ.get("IV_AGENT_DISABLE_OPENAI_AGENTS", "")).strip().lower() in {"1", "true", "yes"}:
        return make_json_safe(
            _run_orchestrator_unavailable(
                request_payload,
                reason="OpenAI Agents SDK is disabled by IV_AGENT_DISABLE_OPENAI_AGENTS",
                local_tools=local_tools,
            )
        )

    if not os.environ.get("OPENAI_API_KEY", "").strip():
        return make_json_safe(
            _run_orchestrator_unavailable(
                request_payload,
                reason="OPENAI_API_KEY is not configured",
                local_tools=local_tools,
            )
        )

    if not _agents_sdk_available():
        return make_json_safe(
            _run_orchestrator_unavailable(
                request_payload,
                reason="openai-agents is not installed in the active Python environment",
                local_tools=local_tools,
            )
        )

    if _should_run_agents_sdk():
        try:
            response = _run_agents_sdk(request_payload, local_tools=local_tools)
            if response.get("answer"):
                return make_json_safe(response)
        except Exception as exc:
            logger.warning("Agents SDK run failed: %s", exc)
            return make_json_safe(
                _run_orchestrator_unavailable(
                    request_payload,
                    reason=f"OpenAI Agents SDK run failed: {exc}",
                    local_tools=local_tools,
                )
            )

    return make_json_safe(
        _run_orchestrator_unavailable(
            request_payload,
            reason="No orchestrator runtime is available",
            local_tools=local_tools,
        )
    )
