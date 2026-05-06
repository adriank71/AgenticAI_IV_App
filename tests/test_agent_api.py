import io
import os
import shutil
import unittest
import uuid
from contextlib import contextmanager
from unittest.mock import patch

from iv_agent import app as app_module
from iv_agent import calendar_manager
from iv_agent import voice_calendar_agent
from iv_agent.agents import orchestrator as agent_orchestrator


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
    def test_chat_voice_transcribe_returns_whisper_transcript(self):
        client = app_module.app.test_client()

        with patch.object(app_module, "transcribe_audio", return_value="Hallo IV Desk") as transcribe_mock:
            response = client.post(
                "/api/chat/voice/transcribe",
                data={
                    "audio": (io.BytesIO(b"webm audio"), "chat-voice.webm"),
                    "timezone": "Europe/Berlin",
                    "now": "2026-05-03T12:00:00+02:00",
                },
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"transcript": "Hallo IV Desk"})
        transcribe_mock.assert_called_once_with(b"webm audio", "chat-voice.webm")

    def test_chat_voice_transcribe_requires_audio(self):
        client = app_module.app.test_client()

        with patch.object(app_module, "transcribe_audio") as transcribe_mock:
            response = client.post(
                "/api/chat/voice/transcribe",
                data={"timezone": "Europe/Berlin"},
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "audio is required")
        transcribe_mock.assert_not_called()

    def test_chat_voice_transcribe_returns_service_unavailable_when_openai_key_is_missing(self):
        client = app_module.app.test_client()

        with patch.object(
            app_module,
            "transcribe_audio",
            side_effect=voice_calendar_agent.MissingOpenAIConfigurationError("missing key"),
        ):
            response = client.post(
                "/api/chat/voice/transcribe",
                data={"audio": (io.BytesIO(b"webm audio"), "chat-voice.webm")},
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 503)
        self.assertIn("OPENAI_API_KEY", response.get_json()["error"])

    def test_chat_voice_transcribe_returns_openai_error_message(self):
        client = app_module.app.test_client()

        with patch.object(
            app_module,
            "transcribe_audio",
            side_effect=Exception("OpenAI rejected audio: invalid file format"),
        ):
            response = client.post(
                "/api/chat/voice/transcribe",
                data={"audio": (io.BytesIO(b"not audio"), "chat-voice.webm")},
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 502)
        self.assertIn("invalid file format", response.get_json()["error"])

    def test_agent_chat_does_not_call_legacy_webhook_when_sdk_unavailable(self):
        client = app_module.app.test_client()
        with isolated_pending_action_storage(), patch.object(
            app_module,
            "trigger_chat_webhook",
            side_effect=AssertionError("legacy webhook should not be called"),
        ), patch.object(agent_orchestrator, "_agents_sdk_available", return_value=False):
            response = client.post(
                "/api/agent/chat",
                json={
                    "message": "Kannst du auf meinen Kalender zugreifen?",
                    "thread_id": "thread-test",
                    "attachments": [],
                    "client_context": {"active_panel": "calendar", "current_month": "2026-05"},
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["thread_id"], "thread-test")
        self.assertIn("Orchestrator", payload["answer"])
        self.assertIn("nicht mehr automatisch aufgerufen", payload["answer"])
        self.assertEqual(payload["pending_actions"], [])
        self.assertTrue(any(event["name"] == "calendar_snapshot" for event in payload["tool_events"]))

    def test_agent_chat_uploads_attachments_before_model_input(self):
        client = app_module.app.test_client()
        uploaded_document = {
            "document_id": "doc-1",
            "file_name": "brief.txt",
            "safe_file_name": "brief.txt",
            "content_type": "text/plain",
            "content_size": 5,
            "summary": "Hallo",
            "extraction_status": "completed",
        }

        def fake_process_attachments(attachments, *, user_id):
            self.assertEqual(user_id, "default")
            self.assertEqual(attachments[0]["content_base64"], "SGFsbG8=")
            return (
                [
                    {
                        "type": "document",
                        "document_id": "doc-1",
                        "file_name": "brief.txt",
                        "mime": "text/plain",
                        "summary": "Hallo",
                    }
                ],
                [uploaded_document],
            )

        with isolated_pending_action_storage(), patch.object(
            app_module,
            "process_chat_attachments",
            side_effect=fake_process_attachments,
        ), patch.object(
            app_module,
            "run_agent_chat",
            return_value={
                "answer": "ok",
                "citations": [],
                "tool_events": [],
                "artifacts": [],
                "pending_actions": [],
                "structured_actions": [],
                "thread_id": "thread-test",
            },
        ) as run_agent_mock:
            response = client.post(
                "/api/agent/chat",
                json={
                    "message": "Bitte speichere das Dokument",
                    "thread_id": "thread-test",
                    "attachments": [
                        {
                            "file_name": "brief.txt",
                            "mime": "text/plain",
                            "content_base64": "SGFsbG8=",
                        }
                    ],
                    "client_context": {"profile_id": "default"},
                },
            )

        self.assertEqual(response.status_code, 200)
        model_payload = run_agent_mock.call_args.args[0]
        self.assertNotIn("content_base64", model_payload["attachments"][0])
        self.assertEqual(model_payload["attachments"][0]["document_id"], "doc-1")
        payload = response.get_json()
        self.assertEqual(payload["uploaded_documents"][0]["document_id"], "doc-1")
        self.assertIn("Datei gespeichert", payload["answer"])
        self.assertIn("Zusammenfassung fertig", payload["answer"])

    def test_confirm_pending_action_executes_reminder_after_user_confirmation(self):
        client = app_module.app.test_client()
        with isolated_pending_action_storage():
            pending_actions = agent_orchestrator.register_pending_actions(
                [
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
                thread_id="thread-test",
            )
            action_id = pending_actions[0]["action_id"]

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

    def test_confirm_pending_storage_folder_create_executes_after_confirmation(self):
        client = app_module.app.test_client()
        with isolated_pending_action_storage():
            pending_actions = agent_orchestrator.register_pending_actions(
                [
                    {
                        "type": "storage.create_folder",
                        "title": "Ordner erstellen: Rechnungen",
                        "payload": {"name": "Rechnungen", "user_id": "default", "document_ids": ["doc-1", "doc-2"]},
                    }
                ],
                thread_id="thread-test",
                user_id="default",
            )
            with patch.object(
                app_module,
                "create_document_folder",
                return_value={"folder_id": "folder-1", "name": "Rechnungen"},
            ) as create_folder_mock, patch.object(
                app_module,
                "move_documents_to_folder",
                return_value=[{"document_id": "doc-1"}, {"document_id": "doc-2"}],
            ) as move_documents_mock:
                response = client.post(
                    f"/api/agent/actions/{pending_actions[0]['action_id']}/confirm",
                    json={"thread_id": "thread-test", "profile_id": "default"},
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["confirmed"])
        self.assertTrue(payload["storage_updated"])
        self.assertFalse(payload["calendar_updated"])
        create_folder_mock.assert_called_once()
        self.assertEqual(create_folder_mock.call_args.kwargs["name"], "Rechnungen")
        move_documents_mock.assert_called_once_with(
            user_id="default",
            document_ids=["doc-1", "doc-2"],
            folder_id="folder-1",
        )
        self.assertEqual(payload["result"]["assigned_count"], 2)

    def test_existing_chat_webhook_route_is_unchanged(self):
        client = app_module.app.test_client()
        with isolated_pending_action_storage(), patch.object(
            app_module,
            "trigger_chat_webhook",
            side_effect=AssertionError("legacy webhook should not be called"),
        ), patch.object(agent_orchestrator, "_agents_sdk_available", return_value=False):
            response = client.post("/api/chat", json={"message": "hello", "history": []})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertIn("answer", payload)
        self.assertNotIn("webhook_response", payload)

    def test_confirm_pending_calendar_create_executes_after_confirmation(self):
        client = app_module.app.test_client()
        base_tmp = os.path.join(os.getcwd(), "output", "test_tmp")
        os.makedirs(base_tmp, exist_ok=True)
        temp_dir = os.path.join(base_tmp, f"calendar_agent_{uuid.uuid4().hex}")
        os.makedirs(temp_dir, exist_ok=True)
        calendar_path = os.path.join(temp_dir, "calendar.json")
        try:
            with isolated_pending_action_storage(), patch.object(calendar_manager, "DATA_DIR", temp_dir), patch.object(
                calendar_manager, "CALENDAR_PATH", calendar_path
            ), patch.dict(os.environ, {"IV_AGENT_STORAGE_BACKEND": "local"}, clear=False):
                calendar_manager._EVENT_STORE_CACHE.clear()
                pending_actions = agent_orchestrator.register_pending_actions(
                    [
                        {
                            "type": "create_event",
                            "title": "Termin erstellen: Therapie",
                            "payload": {
                                "title": "Therapie",
                                "date": "2026-05-04",
                                "time": "09:00",
                                "category": "other",
                                "user_id": "default",
                                "timezone": "Europe/Berlin",
                            },
                        }
                    ],
                    thread_id="thread-test",
                    user_id="default",
                )

                response = client.post(
                    f"/api/agent/actions/{pending_actions[0]['action_id']}/confirm",
                    json={"thread_id": "thread-test", "profile_id": "default"},
                )

                events = calendar_manager.get_events("2026-05")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["confirmed"])
        self.assertTrue(payload["calendar_updated"])
        self.assertEqual(events[0]["title"], "Therapie")


if __name__ == "__main__":
    unittest.main()
