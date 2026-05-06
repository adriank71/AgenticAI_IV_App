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


def _read_pending_action_state() -> dict[str, Any]:
    if not os.path.exists(PENDING_ACTIONS_PATH):
        return {"actions": []}
    try:
        with open(PENDING_ACTIONS_PATH, "r", encoding="utf-8") as file:
            payload = json.load(file)
    except (OSError, json.JSONDecodeError):
        logger.warning("Could not read pending action store; starting empty")
        return {"actions": []}
    if not isinstance(payload, dict) or not isinstance(payload.get("actions"), list):
        return {"actions": []}
    return payload


def _write_pending_action_state(payload: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(PENDING_ACTIONS_PATH), exist_ok=True)
    with open(PENDING_ACTIONS_PATH, "w", encoding="utf-8") as file:
        json.dump(make_json_safe(payload), file, indent=2)


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

    state = _read_pending_action_state()
    existing_ids = {str(item.get("action_id")) for item in state["actions"] if isinstance(item, dict)}
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
        if not action_id or action_id in existing_ids:
            action_id = _new_id("act")
        existing_ids.add(action_id)

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
        state["actions"].append(action)
        registered.append(_public_pending_action(action))

    if registered:
        _write_pending_action_state(state)
    return registered


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

    state = _read_pending_action_state()
    target_action: dict[str, Any] | None = None
    for action in state["actions"]:
        if isinstance(action, dict) and action.get("action_id") == normalized_action_id:
            target_action = action
            break

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
        target_action["status"] = "failed"
        target_action["failed_at"] = utc_timestamp()
        target_action["error"] = str(exc)
        _write_pending_action_state(state)
        raise

    target_action["status"] = "confirmed"
    target_action["confirmed_at"] = utc_timestamp()
    target_action["result"] = make_json_safe(result)
    _write_pending_action_state(state)
    return {
        "action": _public_pending_action(target_action),
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

    return {
        "answer": (
            "Der Chat ist jetzt auf den Orchestrator ausgerichtet, aber der OpenAI Agents SDK Lauf "
            f"kann aktuell nicht starten: {reason}.{calendar_hint} "
            "Lokale Kalender- und Dokumentfunktionen bleiben ueber die Backend-APIs verfuegbar."
        ),
        "citations": [],
        "tool_events": tool_events,
        "artifacts": [],
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
        from .calendar_agent import build_calendar_agent
        from .knowledge_agent import build_knowledge_agent
        from .storage_agent import build_storage_agent
        from ..services.calendar_service import (
            list_calendar_events as service_list_calendar_events,
            normalize_timezone,
            normalize_user_id,
        )
        from ..services.storage_service import (
            list_documents as service_list_documents,
            search_documents as service_search_documents,
        )
    except ImportError:
        from agents.calendar_agent import build_calendar_agent
        from agents.knowledge_agent import build_knowledge_agent
        from agents.storage_agent import build_storage_agent
        from services.calendar_service import (
            list_calendar_events as service_list_calendar_events,
            normalize_timezone,
            normalize_user_id,
        )
        from services.storage_service import (
            list_documents as service_list_documents,
            search_documents as service_search_documents,
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
        limit: int = 25,
    ) -> str:
        """Read stored documents for mixed calendar/document questions."""
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
            limit=limit,
        )
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
        limit: int = 10,
    ) -> str:
        """Search stored documents for mixed calendar/document questions."""
        tool_events.append(_tool_event("search_user_documents", "started", "Searching documents"))
        documents = service_search_documents(
            user_id=context_user_id,
            query=query,
            year=_optional_int(year),
            month=_optional_int(month),
            start_date=start_date or None,
            end_date=end_date or None,
            document_type=document_type,
            institution=institution,
            tags=_json_list(tags_json),
            folder_id=folder_id or None,
            limit=limit,
        )
        tool_events.append(_tool_event("search_user_documents", "completed", "Document search completed"))
        return json.dumps(make_json_safe({"query": query, "documents": documents}), ensure_ascii=True)

    orchestrator_tools.append(search_user_documents)

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
        register_pending_actions=register_pending_actions,
        make_json_safe=make_json_safe,
        tool_event_factory=_tool_event,
    )

    knowledge_agent = build_knowledge_agent(
        Agent,
        function_tool,
        model=KNOWLEDGE_AGENT_MODEL,
        context_user_id=context_user_id,
        now_value=now_value,
        thread_id=request_payload["thread_id"],
        tool_events=tool_events,
        make_json_safe=make_json_safe,
        tool_event_factory=_tool_event,
    )

    instructions = (
        "You are the IV-Helper orchestrator. Every chat message reaches you first. "
        "Before answering, decide whether to answer directly, inspect local app state, draft a pending action, "
        "or hand off to a specialized agent. "
        "For every calendar, appointment, Termin, Therapie, scheduling, counting, or availability request, hand off to CalendarAgent. "
        "For upload, delete, move, metadata, folder, Datei speichern, Dokument loeschen, or document organization requests, hand off to StorageAgent. "
        "For 'Was bedeutet...', IV questions, deadlines, Fristen, document understanding, comparisons, action items, broader knowledge, "
        "or 'frag den IV Assistant' requests, hand off to KnowledgeAgent. "
        "If the request combines calendar and documents, use local read tools from both domains before answering or hand off to the best specialized agent. "
        "Raw file Base64 is never available to you; uploaded attachments are already persisted and represented as document metadata. "
        "Do not execute side effects directly. For create, update, delete, reminder creation, PDF generation, report sending, "
        "or automation saves, call draft_pending_action and explain that the user must confirm it. "
        "Supported generic action_type values are create_reminder, generate_report, and send_report. Calendar mutations belong to CalendarAgent. "
        "Storage mutations belong to StorageAgent. Document explanation and synthesis belongs to KnowledgeAgent. "
        "Answer in German unless the user explicitly requests another language. Format final answers as clean concise Markdown. "
        "Keep answers concise, mention which capability you used when useful, and cite retrieved sources when available."
    )
    agent = Agent(
        name="IV-Helper Orchestrator",
        instructions=instructions,
        model=AGENT_MODEL,
        tools=orchestrator_tools,
        handoffs=[calendar_agent, storage_agent, knowledge_agent],
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
        result = Runner.run_sync(agent, input_text, max_turns=6)

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
