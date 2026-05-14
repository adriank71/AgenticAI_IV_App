from typing import Any, Callable

try:
    from ..tools.automations_tools import build_automations_tools
except ImportError:
    from tools.automations_tools import build_automations_tools


def build_automations_agent(
    Agent: Callable[..., Any],
    function_tool: Callable[..., Any],
    *,
    model: str,
    context_user_id: str,
    context_timezone: str,
    now_value: str,
    current_month: str,
    thread_id: str,
    tool_events: list[dict[str, Any]],
    drafted_actions: list[dict[str, Any]],
    structured_actions: list[dict[str, Any]],
    register_pending_actions: Callable[..., list[dict[str, Any]]],
    make_json_safe: Callable[[Any], Any],
    tool_event_factory: Callable[..., dict[str, Any]],
) -> Any:
    automations_tools = build_automations_tools(
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
        "You are AutomationsAgent, a specialized sub-agent for IV-Helper automations, reminders, and report generation. "
        "Answer only in German, short and action-oriented. "
        f"Current user_id/profile_id: {context_user_id}. Timezone: {context_timezone}. Current local datetime: {now_value}. "
        f"Current visible month: {current_month or 'unknown'}. "
        "Use list_automations for reads. "
        "For report generation requests, call draft_generate_report; this only creates a pending action for confirmation. "
        "For report reminder email requests, call draft_report_reminder_email; it creates a pending action that saves a reminder after confirmation. "
        "Resolve German month phrases like 'Mai 2026' to YYYY-MM before calling tools. "
        "Map Assistenzbeitrag, Assistenzbeitraege, Stundenblatt, and Rechnung to report type assistenzbeitrag. "
        "Map Transportkosten, Transportkostenabrechnung, Fahrkosten, and TixiTaxi report requests to report type transportkostenabrechnung. "
        "If the user asks for both reports, use report_types_json with both report types. "
        "Convert relative reminder times before calling tools: 'in 2 Stunden' becomes schedule once with the computed local run_date and run_time; "
        "'heute Abend' becomes once at about 19:00 local time; 'Ende des Monats' becomes schedule month_end. "
        "Reminder email actions must not generate PDFs automatically; they send only a link to the report mask. "
        "For monthly in-app reminder requests without email, call draft_create_month_end_reminder with action notify. "
        "Never claim a report or reminder is complete before user confirmation."
    )
    return Agent(
        name="AutomationsAgent",
        handoff_description="Handles automation reads, report-generation drafts, and report reminder email drafts.",
        instructions=instructions,
        model=model,
        tools=automations_tools,
    )

