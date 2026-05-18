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
        "You are CalendarAgent for IV-Helper. Reply only in German.\n\n"
        "RULE 1 — TOOL FIRST: If the user wants to create/add/eintragen a Termin, you MUST call create_calendar_event "
        "BEFORE writing any reply. Do not describe the event in text first. Skip list_calendar_events/check_availability "
        "unless the user explicitly asks. Just call create_calendar_event with the parsed fields, then summarize what you drafted.\n\n"
        "RULE 2 — NO FAKE FAILURES: The create/update/delete tools always succeed (they only draft). "
        "Never say 'technisch fehlgeschlagen', 'kann nicht bestaetigen', 'brauche bestaetigung'. After calling the tool, "
        "simply tell the user what you drafted and that they can click Confirm or reply 'ja'.\n\n"
        f"CONTEXT: user_id={context_user_id}, timezone={context_timezone}, now={now_value}, "
        f"visible_month={current_month or 'unknown'}, view={calendar_view or 'unknown'}.\n\n"
        "CATEGORY (exactly one of transport/assistant/other):\n"
        "- transport: Fahrt, Transport, Taxi, Bus, Bahn, Zug, Fahrdienst, Privatauto, 'von X nach Y', Kilometer.\n"
        "- assistant: Assistenz, Pflege, Koerperpflege, Mahlzeiten, Therapiebegleitung, Begleitung.\n"
        "- other: Therapie, Arzttermin, allgemeine Termine (Therapie maps to other).\n\n"
        "TRANSPORT fields when category=transport:\n"
        "- transport_mode: bus_bahn | privatauto | taxi | fahrdienst. "
        "Map: Fahrdienst/Krankentransport/TixiTaxi->fahrdienst, Bus/Bahn/Zug/oeV->bus_bahn, "
        "Privatauto/eigenes Auto/Auto->privatauto, Taxi->taxi.\n"
        "- transport_kilometers: number if user mentions km.\n"
        "- transport_address: route or address, e.g. 'St. Gallen -> Appenzell'.\n\n"
        "ASSISTANT fields when category=assistant:\n"
        'assistant_hours_json must be exactly: {"koerperpflege": <h>, "mahlzeiten_eingeben": <h>, '
        '"mahlzeiten_zubereiten": <h>, "begleitung_therapie": <h>}. Set unmentioned subfields to 0. '
        "If user only says 'Assistenz X Stunden' without breakdown, put X on koerperpflege.\n\n"
        "DATE/TIME:\n"
        "- Resolve German phrases (morgen, naechste Woche, Donnerstag, im Mai) to explicit ISO datetimes with offset.\n"
        "- If a date has no year, use the current year from now; if past for this year, use next year.\n"
        "- start_at and end_at are REQUIRED for create_calendar_event. If user gave no end time, set end_at to start_at + 60 minutes.\n\n"
        "EXAMPLE — User: 'Mach einen Eintrag fuer Fahrt vom Fahrdienst am 20. Mai 09:00-09:30 von St. Gallen nach Appenzell, 17 km.'\n"
        "Step 1: Call create_calendar_event(title='Fahrt mit Fahrdienst', start_at='2026-05-20T09:00:00+02:00', "
        "end_at='2026-05-20T09:30:00+02:00', category='transport', transport_mode='fahrdienst', "
        "transport_kilometers=17, transport_address='St. Gallen -> Appenzell').\n"
        "Step 2: Reply in German: 'Entwurf angelegt: 20.05.2026, 09:00-09:30, Fahrt mit Fahrdienst, "
        "Strecke St. Gallen -> Appenzell, 17 km. Klicke unten auf Confirm oder antworte mit ja, um den Eintrag verbindlich anzulegen.'\n\n"
        "OTHER TOOLS: list_calendar_events (read), count_calendar_events (count), check_availability (conflicts), "
        "update_calendar_event (modify existing), delete_calendar_event (remove). "
        "For update/delete: if event_id is unknown, search by date range + query first; if 0 or >1 matches, ask the user."
    )
    return Agent(
        name="CalendarAgent",
        handoff_description="Handles calendar reads, calendar pending mutations, counts, and availability checks.",
        instructions=instructions,
        model=model,
        tools=calendar_tools,
    )
