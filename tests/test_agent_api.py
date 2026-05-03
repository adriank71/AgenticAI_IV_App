import os
import shutil
import unittest
import uuid
from contextlib import contextmanager
from unittest.mock import patch

from iv_agent import agent_orchestrator
from iv_agent import app as app_module


@contextmanager
def isolated_pending_action_storage():
    base_tmp = os.path.join(os.getcwd(), "output", "test_tmp")
    os.makedirs(base_tmp, exist_ok=True)
    temp_dir = os.path.join(base_tmp, f"agent_{uuid.uuid4().hex}")
    os.makedirs(temp_dir, exist_ok=True)
    pending_path = os.path.join(temp_dir, "pending_actions.json")
    try:
        with patch.object(agent_orchestrator, "PENDING_ACTIONS_PATH", pending_path):
            yield pending_path
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


class AgentApiTests(unittest.TestCase):
    def test_agent_chat_normalizes_n8n_response_to_agent_contract(self):
        client = app_module.app.test_client()
        with isolated_pending_action_storage(), patch.object(
            app_module,
            "trigger_chat_webhook",
            return_value={
                "answer": "TixiTaxi can be claimed when it is medically required and documented.",
                "citations": [
                    {
                        "title": "IV transport guidance",
                        "url": "https://example.test/iv-transport",
                        "snippet": "Transport costs require supporting documents.",
                    }
                ],
                "pending_actions": [
                    {
                        "type": "reminder.create",
                        "title": "Collect TixiTaxi receipts",
                        "payload": {
                            "title": "Collect TixiTaxi receipts",
                            "action": "notify",
                            "schedule": "once",
                            "run_date": "2026-05-10",
                            "run_time": "09:00",
                            "timezone": "Europe/Berlin",
                        },
                    }
                ],
            },
        ) as webhook_mock:
            response = client.post(
                "/api/agent/chat",
                json={
                    "message": "Can I claim TixiTaxi?",
                    "thread_id": "thread-test",
                    "attachments": [],
                    "client_context": {"active_panel": "files"},
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["thread_id"], "thread-test")
        self.assertIn("TixiTaxi", payload["answer"])
        self.assertEqual(payload["citations"][0]["title"], "IV transport guidance")
        self.assertEqual(payload["pending_actions"][0]["type"], "create_reminder")
        self.assertTrue(payload["pending_actions"][0]["requires_confirmation"])
        self.assertGreaterEqual(len(payload["tool_events"]), 2)
        webhook_payload = webhook_mock.call_args.args[0]
        self.assertEqual(webhook_payload["source"], "iv-helper-agent")
        self.assertEqual(webhook_payload["client_context"]["active_panel"], "files")

    def test_confirm_pending_action_executes_reminder_after_user_confirmation(self):
        client = app_module.app.test_client()
        with isolated_pending_action_storage(), patch.object(
            app_module,
            "trigger_chat_webhook",
            return_value={
                "answer": "I drafted the reminder.",
                "pending_actions": [
                    {
                        "type": "create_reminder",
                        "title": "Call IV office",
                        "payload": {
                            "title": "Call IV office",
                            "action": "notify",
                            "schedule": "once",
                            "run_date": "2026-05-11",
                            "run_time": "10:00",
                            "timezone": "Europe/Berlin",
                        },
                    }
                ],
            },
        ):
            chat_response = client.post("/api/agent/chat", json={"message": "Remind me to call IV"})
            action_id = chat_response.get_json()["pending_actions"][0]["action_id"]

            with patch.object(
                app_module.reminders_module,
                "create_reminder",
                return_value={"id": "rem-1", "title": "Call IV office", "status": "active"},
            ) as create_reminder_mock:
                confirm_response = client.post(f"/api/agent/actions/{action_id}/confirm")

        self.assertEqual(confirm_response.status_code, 200)
        payload = confirm_response.get_json()
        self.assertTrue(payload["confirmed"])
        self.assertEqual(payload["result"]["reminder"]["id"], "rem-1")
        create_reminder_mock.assert_called_once()
        self.assertEqual(create_reminder_mock.call_args.args[0]["title"], "Call IV office")

    def test_existing_chat_webhook_route_is_unchanged(self):
        client = app_module.app.test_client()
        with patch.object(app_module, "trigger_chat_webhook", return_value={"reply": "legacy route"}):
            response = client.post("/api/chat", json={"message": "hello", "history": []})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["webhook_response"]["reply"], "legacy route")


if __name__ == "__main__":
    unittest.main()
