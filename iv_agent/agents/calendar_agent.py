from typing import Any, Callable

try:
    from ..tools.calendar_tools import build_calendar_tools
except ImportError:
    from tools.calendar_tools import build_calendar_tools


def build_calendar_agent(
    Agent: Callable[..., Any],
    function_tool: Callable[..., Any],
    *,
    model: str,
    context_user_id: str,
    context_timezone: str,
    now_value: str,
    current_month: str,
    calendar_view: str,
    thread_id: str,
    tool_events: list[dict[str, Any]],
    drafted_actions: list[dict[str, Any]],
    structured_actions: list[dict[str, Any]],
    register_pending_actions: Callable[..., list[dict[str, Any]]],
    make_json_safe: Callable[[Any], Any],
    tool_event_factory: Callable[..., dict[str, Any]],
) -> Any:
    calendar_tools = build_calendar_tools(
        function_tool,
        context_user_id=context_user_id,
        context_timezone=context_timezone,
        thread_id=thread_id,
        tool_events=tool_events,
        drafted_actions=drafted_actions,
        structured_actions=structured_actions,
        register_pending_actions=register_pending_actions,
        make_json_safe=make_json_safe,
        tool_event_factory=tool_event_factory,
    )
    instructions = (
        "You are CalendarAgent, a specialized sub-agent for the IV-Helper calendar. "
        "Answer only in German, short and action-oriented. "
        f"Current user_id/profile_id: {context_user_id}. Timezone: {context_timezone}. Current local datetime: {now_value}. "
        f"Current visible month: {current_month or 'unknown'}. Current calendar view: {calendar_view or 'unknown'}. "
        "Use list_calendar_events for calendar reads, count_calendar_events for aggregations, and check_availability for free/busy checks. "
        "For create/update/delete, call create_calendar_event, update_calendar_event, or delete_calendar_event; these tools only draft pending actions. "
        "Never claim that a create, update, or delete is complete before confirmation. "
        "Resolve German date phrases like morgen, diese Woche, naechste Woche, im Mai, and Donnerstagvormittag into explicit ISO datetimes before tool calls. "
        "If a timed create has no end time, use a 60 minute end time. "
        "If a delete/update request matches no event or multiple events, ask which event is meant and include the matching options if available. "
        "When listing events, group by date and show HH:MM-HH:MM, title, and category."
    )
    return Agent(
        name="CalendarAgent",
        handoff_description="Handles calendar reads, calendar pending mutations, counts, and availability checks.",
        instructions=instructions,
        model=model,
        tools=calendar_tools,
    )
