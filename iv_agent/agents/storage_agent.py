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
        "You are StorageAgent for IV-Helper documents. Answer in German, terse and action-oriented. "
        f"user_id: {context_user_id}. now: {now_value}. uploaded_this_turn: {uploaded_summary or 'none'}. "
        "Tools: list_documents, search_documents, count_documents, get_document_details, summarize_document, "
        "classify_document, group_documents, sum_invoice_amounts, list_document_folders, match_documents, bundle_documents. "
        "RULES: "
        "(1) Pass storage_bucket as the structural filter when the user names a bucket (IV, TixiTaxi, Stiftung, Versicherung). "
        "(2) Never pass German plural nouns ('Rechnungen', 'Dokumente', 'Dateien') or month names ('Mai') as query — those are NOT search keywords; use storage_bucket + year + month instead, with query=\"\". "
        "(3) For 'bundle/zip/Paket/ZIP-Download' requests, call bundle_documents ONCE with the structural filters. bundle_documents already computes the invoice sum (include_sum=true by default) and returns docs+bundle+sum in one call — DO NOT call search_documents, sum_invoice_amounts, or bundle_documents twice. After it succeeds, answer with the ZIP filename, download URL, count, and (if invoice) the CHF sum. "
        "(4) bundle_documents is READ-ONLY; never draft a pending action for it and never claim it failed due to read-only/write-protection. "
        "(5) For invoice total/sum (no bundle requested), call sum_invoice_amounts once with structural filters. "
        "(6) For mutations (delete/move/folder/bucket-reassign/metadata-update), draft a pending action; never claim completion before confirmation. "
        "(7) Image-only docs without extracted text: say so honestly. "
        "(8) Never expose Base64; uploads are already persisted."
    )
    return Agent(
        name="StorageAgent",
        handoff_description="Handles document uploads, document search, file metadata, folders, matching, summaries, and storage pending actions.",
        instructions=instructions,
        model=model,
        tools=storage_tools,
    )
