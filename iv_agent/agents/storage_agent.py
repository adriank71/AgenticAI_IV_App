from typing import Any, Callable

try:
    from ..tools.storage_tools import build_storage_tools
except ImportError:
    from tools.storage_tools import build_storage_tools


def build_storage_agent(
    Agent: Callable[..., Any],
    function_tool: Callable[..., Any],
    *,
    model: str,
    context_user_id: str,
    now_value: str,
    uploaded_documents: list[dict[str, Any]],
    thread_id: str,
    tool_events: list[dict[str, Any]],
    drafted_actions: list[dict[str, Any]],
    structured_actions: list[dict[str, Any]],
    register_pending_actions: Callable[..., list[dict[str, Any]]],
    make_json_safe: Callable[[Any], Any],
    tool_event_factory: Callable[..., dict[str, Any]],
) -> Any:
    storage_tools = build_storage_tools(
        function_tool,
        context_user_id=context_user_id,
        thread_id=thread_id,
        tool_events=tool_events,
        drafted_actions=drafted_actions,
        structured_actions=structured_actions,
        register_pending_actions=register_pending_actions,
        make_json_safe=make_json_safe,
        tool_event_factory=tool_event_factory,
    )
    uploaded_summary = ", ".join(
        f"{item.get('file_name') or item.get('name')} ({item.get('document_id')})"
        for item in uploaded_documents[:5]
        if item.get("document_id")
    )
    instructions = (
        "You are StorageAgent, a specialized sub-agent for IV-Helper documents and files. "
        "Answer only in German, short and action-oriented. "
        f"Current user_id/profile_id: {context_user_id}. Current datetime: {now_value}. "
        f"Documents uploaded in this turn: {uploaded_summary or 'none'}. "
        "Use list_documents, search_documents, count_documents, get_document_details, summarize_document, classify_document, "
        "group_documents, list_document_folders, and match_documents for reads. "
        "Uploads have already been stored by the backend before you see the request. Never ask for or expose Base64 content. "
        "For folder creation, moving documents, deleting documents, or user-visible metadata updates, draft a pending action. "
        "Never claim a delete, move, folder creation, or metadata update is complete before user confirmation. "
        "If an image-only document has no extracted text, state that text could not be extracted instead of inventing details. "
        "When listing documents, include filename, document type if known, institution if known, and document_id only when needed for a follow-up action."
    )
    return Agent(
        name="StorageAgent",
        handoff_description="Handles document uploads, document search, file metadata, folders, matching, summaries, and storage pending actions.",
        instructions=instructions,
        model=model,
        tools=storage_tools,
    )
