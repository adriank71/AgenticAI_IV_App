from typing import Any, Callable

try:
    from ..tools.knowledge_tools import build_knowledge_tools
except ImportError:
    from tools.knowledge_tools import build_knowledge_tools


def build_knowledge_agent(
    Agent: Callable[..., Any],
    function_tool: Callable[..., Any],
    *,
    model: str,
    context_user_id: str,
    now_value: str,
    thread_id: str,
    tool_events: list[dict[str, Any]],
    make_json_safe: Callable[[Any], Any],
    tool_event_factory: Callable[..., dict[str, Any]],
) -> Any:
    knowledge_tools = build_knowledge_tools(
        function_tool,
        context_user_id=context_user_id,
        thread_id=thread_id,
        tool_events=tool_events,
        make_json_safe=make_json_safe,
        tool_event_factory=tool_event_factory,
    )
    instructions = (
        "You are KnowledgeAgent, the specialized IV knowledge and document-understanding sub-agent. "
        "Answer only in German, concise, and explain uncertainty clearly. "
        f"Current user_id/profile_id: {context_user_id}. Current datetime: {now_value}. "
        "Your job is reading, explaining, comparing, and synthesizing stored documents and IV knowledge. "
        "Never upload, delete, move, classify, or mutate document metadata; those tasks belong to StorageAgent. "
        "Use search_internal_knowledge for local document search. Use retrieve_relevant_documents before detailed explanations. "
        "Use summarize_document_context to organize retrieved snippets, compare_documents for comparisons, "
        "extract_action_items for Fristen, deadlines, and to-dos, and synthesize_answer before final source-based answers. "
        "Use ask_watsonx_iv_assistant when the user explicitly says 'frag den IV Assistant', 'WatsonX', or asks for general IV knowledge "
        "that local documents do not answer sufficiently. If WatsonX is unavailable, continue with internal findings and say so plainly. "
        "For medical, legal, IV, or financial topics, be supportive and practical but never give binding advice. "
        "If no relevant local documents are found, say that honestly and do not invent document content. "
        "When citing local evidence, mention filenames or document titles and dates when available."
    )
    return Agent(
        name="KnowledgeAgent",
        handoff_description=(
            "Handles IV knowledge questions, document understanding, document comparison, action-item extraction, "
            "synthesis, and optional WatsonX Orchestrate IV Assistant calls."
        ),
        instructions=instructions,
        model=model,
        tools=knowledge_tools,
    )
