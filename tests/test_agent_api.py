import io
import json
import os
import shutil
import sys
import types
import unittest
import uuid
import zipfile
from contextlib import contextmanager
from unittest.mock import patch

from iv_agent import app as app_module
import iv_agent as iv_agent_package
from iv_agent import calendar_manager
from iv_agent import voice_calendar_agent
from iv_agent.agents import orchestrator as agent_orchestrator
from iv_agent.tools.calendar_tools import build_calendar_tools
from iv_agent.tools.storage_tools import build_storage_tools
from iv_agent.tools.knowledge_tools import build_knowledge_tools
from iv_agent.tools.automations_tools import build_automations_tools


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

    def test_agent_chat_ignores_legacy_chat_env_when_sdk_unavailable(self):
        client = app_module.app.test_client()
        with isolated_pending_action_storage(), patch.dict(
            os.environ,
            {
                "IV_AGENT_CHAT_WEBHOOK_URL": "https://example.invalid/legacy",
                "IV_AGENT_ENABLE_EXTERNAL_KNOWLEDGE": "true",
                "IV_AGENT_ENABLE_LEGACY_N8N_RAG": "true",
            },
            clear=False,
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
        self.assertNotIn("n8n", payload["answer"].lower())
        self.assertEqual(payload["pending_actions"], [])
        self.assertTrue(any(event["name"] == "calendar_snapshot" for event in payload["tool_events"]))

    def test_agent_chat_agents_sdk_path_builds_specialized_agents(self):
        created_agents = []

        class FakeAgent:
            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.name = kwargs.get("name")
                self.handoffs = kwargs.get("handoffs", [])
                created_agents.append(self)

        class FakeRunner:
            @staticmethod
            def run_sync(agent, input_text, max_turns=0):
                self = types.SimpleNamespace()
                self.final_output = "SDK ok"
                return self

        class FakeTrace:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, traceback):
                return False

        fake_agents_module = types.ModuleType("agents")
        fake_agents_module.Agent = FakeAgent
        fake_agents_module.Runner = FakeRunner
        fake_agents_module.function_tool = lambda func: func
        fake_agents_module.set_tracing_disabled = lambda disabled: None
        fake_agents_module.trace = lambda **kwargs: FakeTrace()

        with isolated_pending_action_storage(), patch.dict(
            os.environ,
            {"OPENAI_API_KEY": "test-key"},
            clear=False,
        ), patch.dict(sys.modules, {"agents": fake_agents_module}), patch.object(
            agent_orchestrator,
            "_agents_sdk_available",
            return_value=True,
        ):
            payload = agent_orchestrator.run_agent_chat(
                {
                    "message": "Zeige mir meine Dokumente",
                    "thread_id": "thread-test",
                    "attachments": [],
                    "client_context": {"profile_id": "default", "timezone": "Europe/Berlin"},
                }
            )

        self.assertEqual(payload["answer"], "SDK ok")
        self.assertEqual(created_agents[-1].name, "IV-Helper Orchestrator")
        self.assertEqual(
            [agent.name for agent in created_agents[-1].handoffs],
            ["CalendarAgent", "StorageAgent", "KnowledgeAgent", "AutomationsAgent"],
        )
        self.assertTrue(any(event["name"] == "orchestrator" and event["status"] == "completed" for event in payload["tool_events"]))

    def test_calendar_agent_tool_drafts_pending_create_action(self):
        tool_events = []
        drafted_actions = []
        structured_actions = []

        with isolated_pending_action_storage():
            tools = {
                tool.__name__: tool
                for tool in build_calendar_tools(
                    lambda func: func,
                    context_user_id="default",
                    context_timezone="Europe/Berlin",
                    thread_id="thread-test",
                    tool_events=tool_events,
                    drafted_actions=drafted_actions,
                    structured_actions=structured_actions,
                    register_pending_actions=agent_orchestrator.register_pending_actions,
                    make_json_safe=agent_orchestrator.make_json_safe,
                    tool_event_factory=agent_orchestrator._tool_event,
                )
            }
            payload = json.loads(
                tools["create_calendar_event"](
                    title="Therapie",
                    start_at="2026-05-04T09:00:00+02:00",
                    end_at="2026-05-04T10:00:00+02:00",
                    category="other",
                )
            )

        self.assertEqual(len(payload["pending_actions"]), 1)
        self.assertEqual(payload["pending_actions"][0]["type"], "create_event")
        self.assertEqual(payload["pending_actions"][0]["payload"]["title"], "Therapie")
        self.assertEqual(payload["pending_actions"][0]["payload"]["user_id"], "default")
        self.assertTrue(any(event["name"] == "create_calendar_event" and event["status"] == "completed" for event in tool_events))

    def test_storage_agent_tool_reads_documents_and_collects_artifacts(self):
        tool_events = []
        artifacts = []
        document = {
            "document_id": "doc-1",
            "file_name": "iv-brief.pdf",
            "content_type": "application/pdf",
            "storage_bucket": "IV",
            "summary": "IV Brief",
        }

        with patch("iv_agent.tools.storage_tools.service_list_documents", return_value=[document]):
            tools = {
                tool.__name__: tool
                for tool in build_storage_tools(
                    lambda func: func,
                    context_user_id="default",
                    thread_id="thread-test",
                    tool_events=tool_events,
                    drafted_actions=[],
                    structured_actions=[],
                    collected_artifacts=artifacts,
                    register_pending_actions=agent_orchestrator.register_pending_actions,
                    make_json_safe=agent_orchestrator.make_json_safe,
                    tool_event_factory=agent_orchestrator._tool_event,
                )
            }
            payload = json.loads(tools["list_documents"](storage_bucket="IV"))

        self.assertEqual(payload["documents"][0]["document_id"], "doc-1")
        self.assertEqual(artifacts[0]["download_url"], "/api/documents/doc-1/file?profile_id=default&download=1")
        self.assertTrue(any(event["name"] == "list_documents" and event["status"] == "completed" for event in tool_events))

    def test_knowledge_agent_tool_returns_watsonx_unavailable_result(self):
        tool_events = []
        unavailable = {
            "available": False,
            "reason": "WatsonX Orchestrate ist nicht konfiguriert.",
            "answer": "",
            "citations": [],
        }

        with patch("iv_agent.tools.knowledge_tools.knowledge_service.ask_watsonx_iv_assistant", return_value=unavailable):
            tools = {
                tool.__name__: tool
                for tool in build_knowledge_tools(
                    lambda func: func,
                    context_user_id="default",
                    thread_id="thread-test",
                    tool_events=tool_events,
                    make_json_safe=agent_orchestrator.make_json_safe,
                    tool_event_factory=agent_orchestrator._tool_event,
                )
            }
            payload = json.loads(tools["ask_watsonx_iv_assistant"]("Was bedeutet IV-Grad?"))

        self.assertFalse(payload["available"])
        self.assertIn("WatsonX", payload["reason"])
        self.assertTrue(any(event["name"] == "ask_watsonx_iv_assistant" and event["status"] == "completed" for event in tool_events))

    def test_automations_agent_tool_drafts_pending_generate_report_action(self):
        tool_events = []
        drafted_actions = []
        structured_actions = []

        with isolated_pending_action_storage():
            tools = {
                tool.__name__: tool
                for tool in build_automations_tools(
                    lambda func: func,
                    context_user_id="default",
                    context_timezone="Europe/Berlin",
                    thread_id="thread-test",
                    tool_events=tool_events,
                    drafted_actions=drafted_actions,
                    structured_actions=structured_actions,
                    register_pending_actions=agent_orchestrator.register_pending_actions,
                    make_json_safe=agent_orchestrator.make_json_safe,
                    tool_event_factory=agent_orchestrator._tool_event,
                )
            }
            payload = json.loads(
                tools["draft_generate_report"](
                    month="2026-05",
                    report_types_json='["assistenzbeitrag", "transportkostenabrechnung"]',
                    title="Reports Mai 2026 erstellen",
                )
            )

        self.assertEqual(len(payload["pending_actions"]), 1)
        action = payload["pending_actions"][0]
        self.assertEqual(action["type"], "generate_report")
        self.assertEqual(action["payload"]["month"], "2026-05")
        self.assertEqual(action["payload"]["report_types"], ["assistenzbeitrag", "transportkostenabrechnung"])
        self.assertEqual(action["payload"]["profile_id"], "default")
        self.assertTrue(any(event["name"] == "draft_generate_report" and event["status"] == "completed" for event in tool_events))

    def test_automations_agent_tool_drafts_pending_month_end_reminder_action(self):
        tool_events = []
        drafted_actions = []
        structured_actions = []

        with isolated_pending_action_storage():
            tools = {
                tool.__name__: tool
                for tool in build_automations_tools(
                    lambda func: func,
                    context_user_id="default",
                    context_timezone="Europe/Berlin",
                    thread_id="thread-test",
                    tool_events=tool_events,
                    drafted_actions=drafted_actions,
                    structured_actions=structured_actions,
                    register_pending_actions=agent_orchestrator.register_pending_actions,
                    make_json_safe=agent_orchestrator.make_json_safe,
                    tool_event_factory=agent_orchestrator._tool_event,
                )
            }
            payload = json.loads(
                tools["draft_create_month_end_reminder"](
                    title="Bericht ausfuellen",
                    note="Bitte Assistenzbeitrag ausfuellen.",
                    run_time="18:00",
                )
            )

        self.assertEqual(len(payload["pending_actions"]), 1)
        action = payload["pending_actions"][0]
        self.assertEqual(action["type"], "create_reminder")
        self.assertEqual(action["payload"]["action"], "notify")
        self.assertEqual(action["payload"]["schedule"], "month_end")
        self.assertEqual(action["payload"]["run_time"], "18:00")
        self.assertEqual(action["payload"]["timezone"], "Europe/Berlin")
        self.assertTrue(any(event["name"] == "draft_create_month_end_reminder" and event["status"] == "completed" for event in tool_events))

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
            "storage_bucket": "IV",
            "bucket_confirmed": False,
            "bucket_reason": "Keine starke Zuordnung gefunden; Standard-Bucket IV verwendet.",
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
        self.assertNotIn("rag_callback", run_agent_mock.call_args.kwargs)
        self.assertNotIn("content_base64", model_payload["attachments"][0])
        self.assertEqual(model_payload["attachments"][0]["document_id"], "doc-1")
        payload = response.get_json()
        self.assertEqual(payload["uploaded_documents"][0]["document_id"], "doc-1")
        self.assertEqual(payload["artifacts"][0]["download_url"], "/api/documents/doc-1/file?profile_id=default&download=1")
        self.assertIn("Datei gespeichert", payload["answer"])
        self.assertIn("Bucket IV", payload["answer"])
        self.assertIn("automatisch gesetzt", payload["answer"])

    def test_env_local_loader_allows_supabase_and_database_keys(self):
        temp_dir = os.path.join(os.getcwd(), "output", "test_tmp", f"env_{uuid.uuid4().hex}")
        try:
            package_dir = os.path.join(temp_dir, "iv_agent")
            os.makedirs(package_dir)
            env_path = os.path.join(temp_dir, ".env.local")
            with open(env_path, "w", encoding="utf-8") as file:
                file.write("DATABASE_URL=postgres://example\n")
                file.write("SUPABASE_URL=https://project.supabase.co\n")
                file.write("SUPABASE_SERVICE_ROLE_KEY=service-key\n")
                file.write("IV_AGENT_DOCUMENT_BUCKETS=IV,Versicherung\n")
            with patch.object(iv_agent_package, "__file__", os.path.join(package_dir, "__init__.py")), patch.dict(
                os.environ,
                {
                    "DATABASE_URL": "",
                    "SUPABASE_URL": "",
                    "SUPABASE_SERVICE_ROLE_KEY": "",
                    "IV_AGENT_DOCUMENT_BUCKETS": "",
                },
                clear=False,
            ):
                for key in ("DATABASE_URL", "SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY", "IV_AGENT_DOCUMENT_BUCKETS"):
                    os.environ.pop(key, None)
                iv_agent_package._load_env_local()
                loaded_values = {
                    key: os.environ.get(key)
                    for key in ("DATABASE_URL", "SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY", "IV_AGENT_DOCUMENT_BUCKETS")
                }
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

        self.assertEqual(loaded_values["DATABASE_URL"], "postgres://example")
        self.assertEqual(loaded_values["SUPABASE_URL"], "https://project.supabase.co")
        self.assertEqual(loaded_values["SUPABASE_SERVICE_ROLE_KEY"], "service-key")
        self.assertEqual(loaded_values["IV_AGENT_DOCUMENT_BUCKETS"], "IV,Versicherung")

    def test_documents_browser_uses_structured_storage_service(self):
        client = app_module.app.test_client()

        class FakeStorageService:
            def build_document_browser(self, *, user_id):
                self.user_id = user_id
                return {
                    "configured": True,
                    "document_buckets": ["IV"],
                    "total_count": 1,
                    "buckets": [
                        {
                            "id": "IV",
                            "name": "IV",
                            "count": 1,
                            "confirmed_count": 0,
                            "unconfirmed_count": 1,
                            "documents": [
                                {
                                    "document_id": "doc-1",
                                    "file_name": "brief.txt",
                                    "storage_bucket": "IV",
                                    "bucket_confirmed": False,
                                }
                            ],
                        }
                    ],
                }

        fake_service = FakeStorageService()
        with patch.object(app_module, "get_storage_service", return_value=fake_service):
            response = client.get("/api/documents/browser?profile_id=profile_a")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(fake_service.user_id, "profile_a")
        self.assertEqual(payload["buckets"][0]["documents"][0]["document_id"], "doc-1")
        self.assertFalse(payload["buckets"][0]["documents"][0]["bucket_confirmed"])

    def test_document_bundle_endpoint_zips_requested_documents(self):
        client = app_module.app.test_client()

        class FakeStorageService:
            def __init__(self):
                self.calls = []

            def read_document_bytes(self, *, user_id, document_id):
                self.calls.append((user_id, document_id))
                if document_id == "missing":
                    raise FileNotFoundError("Document not found")
                return f"content-{document_id}".encode("utf-8"), {
                    "document_id": document_id,
                    "file_name": f"{document_id}.txt",
                    "content_type": "text/plain",
                }

        fake_service = FakeStorageService()
        with patch.object(app_module, "get_storage_service", return_value=fake_service):
            response = client.get("/api/documents/bundle?profile_id=profile_a&document_ids=doc-1,doc-2")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "application/zip")
        with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
            self.assertEqual(sorted(archive.namelist()), ["doc-1.txt", "doc-2.txt"])
            self.assertEqual(archive.read("doc-1.txt"), b"content-doc-1")
        self.assertEqual(fake_service.calls, [("profile_a", "doc-1"), ("profile_a", "doc-2")])

    def test_chat_frontend_renders_document_bundle_artifacts(self):
        app_js = os.path.join(os.getcwd(), "iv_agent", "static", "app.js")
        with open(app_js, "r", encoding="utf-8") as handle:
            source = handle.read()

        self.assertIn('artifact.type === "document_bundle"', source)
        self.assertIn('artifact.type === "document_bundle" ? "folder_zip"', source)
        self.assertIn("Download-Paket", source)

    def test_camera_capture_stores_document_when_vision_fails(self):
        uploaded_document = {
            "document_id": "doc-1",
            "user_id": "default",
            "file_name": "photo.jpg",
            "safe_file_name": "photo.jpg",
            "storage_bucket": "Versicherung",
            "storage_key": "Documents/default/2026/05/doc-1-photo.jpg",
            "storage_url": "supabase://Versicherung/Documents/default/2026/05/doc-1-photo.jpg",
            "content_type": "image/jpeg",
            "content_size": 3,
            "document_type": "image",
            "institution": "",
            "document_date": None,
            "tags": [],
            "summary": "Text konnte nicht extrahiert werden.",
            "extracted_text": "",
            "extraction_status": "no_text",
            "extraction_error": "Text konnte nicht extrahiert werden.",
            "metadata": {
                "camera_session_id": "session123",
                "folder_path": "Invoices/session123",
                "invoice_extraction_error": "vision timeout",
            },
            "bucket_confirmed": False,
            "bucket_reason": "Upload-Regel hat den Bucket Versicherung vorgegeben.",
            "bucket_confidence": "high",
            "created_at": "2026-05-01T10:00:00+00:00",
            "updated_at": "2026-05-01T10:00:00+00:00",
        }

        class FakeStorageService:
            def upload_document(self, **kwargs):
                self.kwargs = kwargs
                return uploaded_document

        fake_service = FakeStorageService()
        with app_module.app.app_context(), patch.object(
            app_module, "get_storage_service", return_value=fake_service
        ), patch.object(
            app_module, "_call_openai_vision", side_effect=RuntimeError("vision timeout")
        ), patch.object(app_module, "list_documents_for_session", return_value=[uploaded_document]):
            response = app_module.capture_invoice(
                "session123",
                {
                    "image_base64": "/9j/",
                    "mime": "image/jpeg",
                    "file_name": "photo.jpg",
                },
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 201)
        self.assertTrue(payload["stored"])
        self.assertEqual(payload["capture"]["storage_bucket"], "Versicherung")
        self.assertFalse(payload["capture"]["bucket_confirmed"])
        self.assertEqual(fake_service.kwargs["metadata"]["storage_bucket"], "Versicherung")

    def test_agent_chat_preserves_document_artifacts_from_agent_response(self):
        client = app_module.app.test_client()
        artifact = {
            "id": "doc-1",
            "type": "document",
            "document_id": "doc-1",
            "title": "rechnung.txt",
            "file_name": "rechnung.txt",
            "content_type": "text/plain",
            "storage_bucket": "IV",
            "download_url": "/api/documents/doc-1/file?profile_id=default&download=1",
        }

        with isolated_pending_action_storage(), patch.object(
            app_module,
            "run_agent_chat",
            return_value={
                "answer": "Ich habe 1 Dokument gefunden.",
                "citations": [],
                "tool_events": [],
                "artifacts": [artifact],
                "pending_actions": [],
                "structured_actions": [],
                "thread_id": "thread-test",
            },
        ):
            response = client.post(
                "/api/agent/chat",
                json={
                    "message": "Zeige mir meine IV Dokumente",
                    "thread_id": "thread-test",
                    "attachments": [],
                    "client_context": {"profile_id": "default"},
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["artifacts"], [artifact])

    def test_document_download_endpoint_uses_requested_user_scope(self):
        client = app_module.app.test_client()

        class FakeStorageService:
            def read_document_bytes(self, *, user_id, document_id):
                self.user_id = user_id
                self.document_id = document_id
                if user_id != "profile_a":
                    raise FileNotFoundError("Document not found")
                return b"Hallo", {
                    "file_name": "brief.txt",
                    "content_type": "text/plain",
                }

        fake_service = FakeStorageService()
        with patch.object(app_module, "get_storage_service", return_value=fake_service):
            response = client.get("/api/documents/doc-1/file?profile_id=profile_a")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"Hallo")
        self.assertEqual(fake_service.user_id, "profile_a")
        self.assertEqual(fake_service.document_id, "doc-1")

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

    def test_confirm_pending_generate_report_returns_report_artifact(self):
        client = app_module.app.test_client()
        generated_payload = {
            "month": "2026-05",
            "generated_reports": [
                {
                    "report_id": "rpt-1",
                    "type": "assistenzbeitrag",
                    "label": "Assistenzbeitraege report",
                    "file_name": "Assistenzbeitrag_2026-05.pdf",
                    "download_url": "/api/reports/download/rpt-1/Assistenzbeitrag_2026-05.pdf",
                    "preview_url": "/api/reports/view/rpt-1/Assistenzbeitrag_2026-05.pdf",
                }
            ],
            "unavailable_reports": [],
        }

        with isolated_pending_action_storage():
            pending_actions = agent_orchestrator.register_pending_actions(
                [
                    {
                        "type": "generate_report",
                        "title": "Assistenzbeitrag Report Mai 2026 erstellen",
                        "payload": {
                            "month": "2026-05",
                            "report_types": ["assistenzbeitrag"],
                            "profile_id": "default",
                        },
                    }
                ],
                thread_id="thread-test",
                user_id="default",
            )

            with patch.object(app_module, "load_profile_payload", return_value={}), patch.object(
                app_module,
                "generate_reports_payload",
                return_value=generated_payload,
            ) as generate_mock:
                response = client.post(
                    f"/api/agent/actions/{pending_actions[0]['action_id']}/confirm",
                    json={"thread_id": "thread-test", "profile_id": "default"},
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["confirmed"])
        self.assertTrue(payload["reports_generated"])
        self.assertEqual(payload["artifacts"][0]["type"], "report")
        self.assertEqual(payload["artifacts"][0]["report_id"], "rpt-1")
        self.assertEqual(payload["artifacts"][0]["download_url"], "/api/reports/download/rpt-1/Assistenzbeitrag_2026-05.pdf")
        generate_mock.assert_called_once_with(
            "2026-05",
            ["assistenzbeitrag"],
            {},
            profile_id="default",
        )

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

    def test_confirm_pending_storage_bucket_reassignment_executes_after_confirmation(self):
        client = app_module.app.test_client()
        with isolated_pending_action_storage():
            pending_actions = agent_orchestrator.register_pending_actions(
                [
                    {
                        "type": "storage.reassign_bucket",
                        "title": "Dokument in Bucket verschieben: TixiTaxi",
                        "payload": {
                            "document_id": "doc-1",
                            "bucket": "TixiTaxi",
                            "user_id": "default",
                        },
                    }
                ],
                thread_id="thread-test",
                user_id="default",
            )
            with patch.object(
                app_module,
                "reassign_document_bucket",
                return_value={"document_id": "doc-1", "storage_bucket": "TixiTaxi", "bucket_confirmed": True},
            ) as reassign_mock:
                response = client.post(
                    f"/api/agent/actions/{pending_actions[0]['action_id']}/confirm",
                    json={"thread_id": "thread-test", "profile_id": "default"},
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["confirmed"])
        self.assertTrue(payload["storage_updated"])
        self.assertTrue(payload["result"]["bucket_reassigned"])
        reassign_mock.assert_called_once_with(
            user_id="default",
            document_id="doc-1",
            bucket_name="TixiTaxi",
            confirmed=True,
            bucket_reason="",
            review_source="agent_confirmation",
        )

    def test_basic_chat_route_uses_orchestrator_without_legacy_webhook(self):
        client = app_module.app.test_client()
        with isolated_pending_action_storage(), patch.dict(
            os.environ,
            {"IV_AGENT_CHAT_WEBHOOK_URL": "https://example.invalid/legacy"},
            clear=False,
        ), patch.object(agent_orchestrator, "_agents_sdk_available", return_value=False):
            response = client.post("/api/chat", json={"message": "hello", "history": []})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertIn("answer", payload)
        self.assertNotIn("webhook_response", payload)
        self.assertNotIn("n8n", payload["answer"].lower())

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
