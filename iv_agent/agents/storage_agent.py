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
    collected_artifacts: list[dict[str, Any]],
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
        collected_artifacts=collected_artifacts,
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
        "group_documents, sum_invoice_amounts, list_document_folders, match_documents, and bundle_documents for reads/download bundles. "
        "When documents were uploaded in this turn, always base your answer on the stored document metadata/attachments, name the suggested bucket, and say whether bucket_confirmed is still false. "
        "For any document retrieval request such as 'gib mir alle Dokumente', 'zeige Rechnungen', or 'finde Dateien', read storage first and return the documents from the tool result; never answer only from chat history. "
        "For invoice total or sum questions, always apply the user's document filters first and call sum_invoice_amounts; do not add amounts from memory or from a plain text summary. "
        "If the user mentions IV, TixiTaxi, Stiftung, Versicherung, or Versicherungen, pass that value as storage_bucket to document read tools. "
        "Uploads have already been stored by the backend before you see the request. Never ask for or expose Base64 content. "
        "For folder creation, moving documents, deleting documents, bucket reassignment, or user-visible metadata updates, draft a pending action. "
        "Never claim a delete, move, bucket change, folder creation, or metadata update is complete before user confirmation. "
        "If an image-only document has no extracted text, state that text could not be extracted instead of inventing details. "
        "When listing documents, include filename, document type if known, institution if known, and document_id only when needed for a follow-up action. "
        "For any request to bundle, zip, package, batch-download, or 'als eine Datei/ZIP/Ordner herunterladen', "
        "call bundle_documents immediately. bundle_documents is READ-ONLY: it only generates a download URL, it never modifies storage and must NEVER be drafted as a pending action. "
        "Prefer passing concrete document_ids_json when the user references a previous list; otherwise pass query/storage_bucket/year/month/institution filters and bundle_documents will collect them. "
        "If bundle_documents already ran in this turn and produced a bundle, do not call it again — reuse the existing download_url. "
        "After bundle_documents succeeds, answer with the ZIP file name and the download URL from the tool result and stop. "
        "Never tell the user that bundling failed because of read-only, write-protection, or report generation — those errors do not apply to bundle_documents."
    )
    return Agent(
        name="StorageAgent",
        handoff_description="Handles document uploads, document search, file metadata, folders, matching, summaries, and storage pending actions.",
        instructions=instructions,
        model=model,
        tools=storage_tools,
    )
