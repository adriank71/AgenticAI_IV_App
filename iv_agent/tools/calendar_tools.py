import json
import time
from typing import Any, Callable

try:
    from ..services.calendar_service import (
        check_availability as service_check_availability,
        count_calendar_events as service_count_calendar_events,
        find_matching_calendar_events,
        list_calendar_events as service_list_calendar_events,
    )
except ImportError:
    from services.calendar_service import (
        check_availability as service_check_availability,
        count_calendar_events as service_count_calendar_events,
        find_matching_calendar_events,
        list_calendar_events as service_list_calendar_events,
    )


def build_calendar_tools(
    function_tool: Callable[..., Any],
    *,
    context_user_id: str,
    context_timezone: str,
    thread_id: str,
    tool_events: list[dict[str, Any]],
    drafted_actions: list[dict[str, Any]],
    structured_actions: list[dict[str, Any]],
    register_pending_actions: Callable[..., list[dict[str, Any]]],
    make_json_safe: Callable[[Any], Any],
    tool_event_factory: Callable[..., dict[str, Any]],
) -> list[Any]:
    tools: list[Any] = []

    def _calendar_tool_result(tool_name: str, callback: Callable[[], dict[str, Any]]) -> str:
        tool_events.append(tool_event_factory(tool_name, "started", f"{tool_name} started"))
        started_at = time.perf_counter()
        try:
            payload = callback()
        except Exception as exc:
            tool_events.append(tool_event_factory(tool_name, "failed", str(exc), duration_ms=max(0, int(round((time.perf_counter() - started_at) * 1000)))))
            raise
        tool_events.append(tool_event_factory(tool_name, "completed", f"{tool_name} completed", duration_ms=max(0, int(round((time.perf_counter() - started_at) * 1000)))))
        return json.dumps(make_json_safe(payload), ensure_ascii=True)

    def _register_calendar_pending_action(action_type: str, title: str, payload: dict[str, Any]) -> dict[str, Any]:
        action_payload = {
            **payload,
            "user_id": context_user_id,
            "timezone": context_timezone,
        }
        actions = register_pending_actions(
            [{"type": action_type, "title": title, "payload": action_payload, "user_id": context_user_id}],
            thread_id=thread_id,
            user_id=context_user_id,
        )
        drafted_actions.extend(actions)
        structured_actions.extend(actions)
        return {"pending_actions": actions}

    @function_tool
    def list_calendar_events(start_at: str, end_at: str, query: str = "") -> str:
        """List calendar events for the current user in an ISO datetime range. Use query for optional title text search."""
        return _calendar_tool_result(
            "list_calendar_events",
            lambda: service_list_calendar_events(
                user_id=context_user_id,
                start_at=start_at,
                end_at=end_at,
                query=query,
                timezone_name=context_timezone,
            ),
        )

    tools.append(list_calendar_events)

    @function_tool
    def count_calendar_events(start_at: str, end_at: str, query: str = "") -> str:
        """Count calendar events for the current user in an ISO datetime range, optionally filtered by query."""
        return _calendar_tool_result(
            "count_calendar_events",
            lambda: service_count_calendar_events(
                user_id=context_user_id,
                start_at=start_at,
                end_at=end_at,
                query=query,
                timezone_name=context_timezone,
            ),
        )

    tools.append(count_calendar_events)

    @function_tool
    def check_availability(start_at: str, end_at: str) -> str:
        """Check whether the current user has any calendar event conflict in an ISO datetime range."""
        return _calendar_tool_result(
            "check_availability",
            lambda: service_check_availability(
                user_id=context_user_id,
                start_at=start_at,
                end_at=end_at,
                timezone_name=context_timezone,
            ),
        )

    tools.append(check_availability)

    @function_tool
    def create_calendar_event(
        title: str,
        start_at: str,
        end_at: str,
        category: str,
        transport_mode: str = "",
        transport_kilometers: float = 0.0,
        transport_address: str = "",
        notes: str = "",
        assistant_hours_json: str = "",
    ) -> str:
        """Draft a NEW calendar event for user confirmation. Call this whenever the user wants to add/create a Termin.

        Required: title, start_at (ISO with timezone offset), end_at (ISO with timezone offset), category.
        category must be exactly one of: 'transport', 'assistant', 'other' (use 'other' for therapy/general appointments).
        For category='transport': set transport_mode to one of 'bus_bahn', 'privatauto', 'taxi', 'fahrdienst'; set transport_kilometers; set transport_address (e.g. 'St. Gallen -> Appenzell').
        For category='assistant': pass assistant_hours_json like {"koerperpflege": 1.5, "mahlzeiten_eingeben": 0, "mahlzeiten_zubereiten": 0, "begleitung_therapie": 0}.
        """

        def draft() -> dict[str, Any]:
            try:
                assistant_hours = json.loads(assistant_hours_json) if assistant_hours_json else None
            except json.JSONDecodeError:
                assistant_hours = None
            if assistant_hours is not None and not isinstance(assistant_hours, dict):
                assistant_hours = None

            normalized_category = str(category or "other").strip().lower()
            if normalized_category in {"therapy", "therapie"}:
                normalized_category = "other"

            payload: dict[str, Any] = {
                "title": title,
                "start_at": start_at,
                "end_at": end_at,
                "category": normalized_category,
                "description": notes,
                "all_day": False,
            }
            if normalized_category == "transport":
                if transport_mode:
                    payload["transport_mode"] = transport_mode.strip().lower()
                if transport_kilometers:
                    payload["transport_kilometers"] = float(transport_kilometers)
                if transport_address:
                    payload["transport_address"] = transport_address
                    payload["location"] = transport_address
            if normalized_category == "assistant" and assistant_hours is not None:
                payload["assistant_hours"] = assistant_hours
            return _register_calendar_pending_action("create_event", f"Termin erstellen: {title}", payload)

        return _calendar_tool_result("create_calendar_event", draft)

    tools.append(create_calendar_event)

    @function_tool
    def update_calendar_event(
        event_id: str = "",
        search_start_at: str = "",
        search_end_at: str = "",
        query: str = "",
        title: str = "",
        start_at: str = "",
        end_at: str = "",
        category: str = "",
        notes: str = "",
        transport_mode: str = "",
        transport_kilometers: float = -1.0,
        transport_address: str = "",
        assistant_hours_json: str = "",
    ) -> str:
        """Draft a calendar event UPDATE for user confirmation. If event_id is missing, search by range and query first.

        Pass only the fields you want to change. transport_kilometers=-1 (default) means leave unchanged.
        Field semantics identical to create_calendar_event.
        """

        def draft() -> dict[str, Any]:
            updates: dict[str, Any] = {}
            if title:
                updates["title"] = title
            if start_at:
                updates["start_at"] = start_at
            if end_at:
                updates["end_at"] = end_at
            if category:
                normalized_category = category.strip().lower()
                if normalized_category in {"therapy", "therapie"}:
                    normalized_category = "other"
                updates["category"] = normalized_category
            if notes:
                updates["notes"] = notes
                updates["description"] = notes
            if transport_mode:
                updates["transport_mode"] = transport_mode.strip().lower()
            if transport_kilometers is not None and transport_kilometers >= 0:
                updates["transport_kilometers"] = float(transport_kilometers)
            if transport_address:
                updates["transport_address"] = transport_address
                updates["location"] = transport_address
            if assistant_hours_json:
                try:
                    parsed_hours = json.loads(assistant_hours_json)
                    if isinstance(parsed_hours, dict):
                        updates["assistant_hours"] = parsed_hours
                except json.JSONDecodeError:
                    pass
            resolved_event_id = str(event_id or "").strip()
            matches: list[dict[str, Any]] = []
            if not resolved_event_id:
                if not search_start_at or not search_end_at:
                    return {
                        "needs_clarification": True,
                        "message": "Bitte nenne Datum oder Zeitraum des Termins, der geaendert werden soll.",
                    }
                matches.extend(
                    find_matching_calendar_events(
                        user_id=context_user_id,
                        start_at=search_start_at,
                        end_at=search_end_at,
                        query=query,
                        timezone_name=context_timezone,
                    )
                )
                if len(matches) != 1:
                    return {
                        "needs_clarification": True,
                        "message": (
                            "Ich habe keinen eindeutigen Termin gefunden."
                            if not matches
                            else "Ich habe mehrere passende Termine gefunden. Bitte waehle einen aus."
                        ),
                        "matches": matches,
                    }
                resolved_event_id = matches[0]["id"]
            payload = {"event_id": resolved_event_id, **updates}
            if matches:
                payload.setdefault("matched_event", matches[0])
            return _register_calendar_pending_action("update_event", "Termin aendern", payload)

        return _calendar_tool_result("update_calendar_event", draft)

    tools.append(update_calendar_event)

    @function_tool
    def delete_calendar_event(
        event_id: str = "",
        search_start_at: str = "",
        search_end_at: str = "",
        query: str = "",
    ) -> str:
        """Draft a calendar event deletion for user confirmation. If event_id is missing, search by range and query first."""

        def draft() -> dict[str, Any]:
            resolved_event_id = str(event_id or "").strip()
            matches: list[dict[str, Any]] = []
            if not resolved_event_id:
                if not search_start_at or not search_end_at:
                    return {
                        "needs_clarification": True,
                        "message": "Bitte nenne Datum oder Zeitraum des Termins, der geloescht werden soll.",
                    }
                matches.extend(
                    find_matching_calendar_events(
                        user_id=context_user_id,
                        start_at=search_start_at,
                        end_at=search_end_at,
                        query=query,
                        timezone_name=context_timezone,
                    )
                )
                if len(matches) != 1:
                    return {
                        "needs_clarification": True,
                        "message": (
                            "Ich habe keinen eindeutigen Termin gefunden."
                            if not matches
                            else "Ich habe mehrere passende Termine gefunden. Bitte waehle einen aus."
                        ),
                        "matches": matches,
                    }
                resolved_event_id = matches[0]["id"]
            payload = {"event_id": resolved_event_id}
            if matches:
                payload["matched_event"] = matches[0]
            return _register_calendar_pending_action("delete_event", "Termin loeschen", payload)

        return _calendar_tool_result("delete_calendar_event", draft)

    tools.append(delete_calendar_event)
    return tools
