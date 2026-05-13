import json
from typing import Any, Callable

try:
    from .. import reminders as reminders_module
except ImportError:
    import reminders as reminders_module


VALID_REPORT_TYPES = {"assistenzbeitrag", "transportkostenabrechnung"}


def _json_list(value: str) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, list):
        return [str(item).strip().lower() for item in parsed if str(item).strip()]
    return [item.strip().lower() for item in raw.split(",") if item.strip()]


def build_automations_tools(
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

    def _automation_tool_result(tool_name: str, callback: Callable[[], dict[str, Any]]) -> str:
        tool_events.append(tool_event_factory(tool_name, "started", f"{tool_name} started"))
        try:
            payload = callback()
        except Exception as exc:
            tool_events.append(tool_event_factory(tool_name, "failed", str(exc)))
            raise
        tool_events.append(tool_event_factory(tool_name, "completed", f"{tool_name} completed"))
        return json.dumps(make_json_safe(payload), ensure_ascii=True)

    def _register_automation_pending_action(action_type: str, title: str, payload: dict[str, Any]) -> dict[str, Any]:
        action_payload = {
            **payload,
            "user_id": context_user_id,
            "profile_id": context_user_id,
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
    def list_automations() -> str:
        """List saved reminders and automations for the current user."""
        return _automation_tool_result(
            "list_automations",
            lambda: {"reminders": reminders_module.list_reminders()},
        )

    tools.append(list_automations)

    @function_tool
    def draft_generate_report(
        month: str,
        report_types_json: str = '["assistenzbeitrag"]',
        title: str = "",
    ) -> str:
        """Draft report PDF generation for user confirmation. month must be YYYY-MM."""

        def draft() -> dict[str, Any]:
            report_types = _json_list(report_types_json) or ["assistenzbeitrag"]
            unsupported = [item for item in report_types if item not in VALID_REPORT_TYPES]
            if unsupported:
                raise ValueError(f"Unsupported report type: {unsupported[0]}")
            action_title = title.strip() or (
                "Reports erstellen" if len(report_types) > 1 else "Assistenzbeitrag Report erstellen"
            )
            return _register_automation_pending_action(
                "generate_report",
                action_title,
                {
                    "month": month,
                    "report_types": report_types,
                },
            )

        return _automation_tool_result("draft_generate_report", draft)

    tools.append(draft_generate_report)

    @function_tool
    def draft_create_month_end_reminder(
        title: str,
        note: str = "",
        run_time: str = "09:00",
    ) -> str:
        """Draft an in-app reminder that runs on the last day of each month."""

        def draft() -> dict[str, Any]:
            reminder_title = title.strip() or "Monatsende: Bericht ausfuellen"
            return _register_automation_pending_action(
                "create_reminder",
                reminder_title,
                {
                    "title": reminder_title,
                    "action": "notify",
                    "schedule": "month_end",
                    "run_time": run_time or "09:00",
                    "note": note,
                },
            )

        return _automation_tool_result("draft_create_month_end_reminder", draft)

    tools.append(draft_create_month_end_reminder)
    return tools
