import json
from typing import Any

try:
    from ..services import knowledge_service
except ImportError:
    import services.knowledge_service as knowledge_service


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


def _optional_int(value: int | str | None) -> int | None:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return None
    return parsed or None


def _json_dict(value: str) -> dict[str, Any]:
    raw = str(value or "").strip()
    if not raw:
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("JSON value must be an object")
    return parsed


def build_knowledge_tools(
    function_tool: Any,
    *,
    context_user_id: str,
    thread_id: str,
    tool_events: list[dict[str, Any]],
    make_json_safe: Any,
    tool_event_factory: Any,
) -> list[Any]:
    tools: list[Any] = []

    def _knowledge_tool_result(tool_name: str, callback: Any) -> str:
        tool_events.append(tool_event_factory(tool_name, "started", f"{tool_name} started"))
        try:
            payload = callback()
        except Exception as exc:
            tool_events.append(tool_event_factory(tool_name, "failed", str(exc)))
            raise
        tool_events.append(tool_event_factory(tool_name, "completed", f"{tool_name} completed"))
        return json.dumps(make_json_safe(payload), ensure_ascii=True)

    @function_tool
    def search_internal_knowledge(
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
        """Search current user's stored IV documents by extracted text, summary, tags, metadata, type, and institution."""
        return _knowledge_tool_result(
            "search_internal_knowledge",
            lambda: knowledge_service.search_internal_knowledge(
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
            ),
        )

    tools.append(search_internal_knowledge)

    @function_tool
    def retrieve_relevant_documents(document_ids_json: str = "[]", query: str = "", limit: int = 5) -> str:
        """Retrieve selected current-user documents with bounded extracted-text excerpts and metadata."""
        return _knowledge_tool_result(
            "retrieve_relevant_documents",
            lambda: knowledge_service.retrieve_relevant_documents(
                user_id=context_user_id,
                document_ids=_json_list(document_ids_json),
                query=query,
                limit=limit,
            ),
        )

    tools.append(retrieve_relevant_documents)

    @function_tool
    def ask_watsonx_iv_assistant(question: str, context: str = "") -> str:
        """Ask the optional WatsonX Orchestrate IV Assistant. If unavailable, returns a structured unavailable result."""
        return _knowledge_tool_result(
            "ask_watsonx_iv_assistant",
            lambda: knowledge_service.ask_watsonx_iv_assistant(
                question=question,
                context=context,
                thread_id=thread_id,
                user_id=context_user_id,
            ),
        )

    tools.append(ask_watsonx_iv_assistant)

    @function_tool
    def summarize_document_context(documents_json: str, question: str = "") -> str:
        """Summarize retrieved document snippets into a structured local context package."""
        parsed = json.loads(documents_json or "[]")
        if isinstance(parsed, dict):
            documents = parsed.get("documents") if isinstance(parsed.get("documents"), list) else []
        elif isinstance(parsed, list):
            documents = parsed
        else:
            documents = []
        return _knowledge_tool_result(
            "summarize_document_context",
            lambda: knowledge_service.summarize_document_context(documents=documents, question=question),
        )

    tools.append(summarize_document_context)

    @function_tool
    def compare_documents(document_ids_json: str, query: str = "") -> str:
        """Compare two or more current-user documents by metadata, dates, amounts, institutions, text overlap, and match reasons."""
        return _knowledge_tool_result(
            "compare_documents",
            lambda: knowledge_service.compare_documents(
                user_id=context_user_id,
                document_ids=_json_list(document_ids_json),
                query=query,
            ),
        )

    tools.append(compare_documents)

    @function_tool
    def extract_action_items(document_ids_json: str = "[]", query: str = "", limit: int = 10) -> str:
        """Extract deadlines, frists, and to-dos from current-user document text and summaries."""
        return _knowledge_tool_result(
            "extract_action_items",
            lambda: knowledge_service.extract_action_items(
                user_id=context_user_id,
                document_ids=_json_list(document_ids_json),
                query=query,
                limit=limit,
            ),
        )

    tools.append(extract_action_items)

    @function_tool
    def synthesize_answer(
        question: str,
        internal_findings_json: str = "{}",
        watsonx_result_json: str = "{}",
        action_items_json: str = "{}",
        comparison_json: str = "{}",
    ) -> str:
        """Package internal findings, WatsonX result, action items, comparison data, sources, and uncertainty flags."""
        return _knowledge_tool_result(
            "synthesize_answer",
            lambda: knowledge_service.synthesize_answer(
                question=question,
                internal_findings=_json_dict(internal_findings_json),
                watsonx_result=_json_dict(watsonx_result_json),
                action_items=_json_dict(action_items_json),
                comparison=_json_dict(comparison_json),
            ),
        )

    tools.append(synthesize_answer)

    return tools
