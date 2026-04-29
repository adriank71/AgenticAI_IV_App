import json
import io
import os
import shutil
import unittest
import uuid
from types import SimpleNamespace
from contextlib import contextmanager
from unittest.mock import patch

from iv_agent import app as app_module
from iv_agent import calendar_manager
from iv_agent import reminders_agent
from iv_agent import voice_calendar_agent


@contextmanager
def isolated_calendar_storage():
    base_tmp = os.path.join(os.getcwd(), "output", "test_tmp")
    os.makedirs(base_tmp, exist_ok=True)
    temp_dir = os.path.join(base_tmp, f"calendar_{uuid.uuid4().hex}")
    os.makedirs(temp_dir, exist_ok=True)
    try:
        calendar_path = os.path.join(temp_dir, "calendar.json")
        with patch.object(calendar_manager, "DATA_DIR", temp_dir), patch.object(
            calendar_manager, "CALENDAR_PATH", calendar_path
        ):
            yield calendar_path
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


@contextmanager
def passthrough_materialized_path(path, suffix=".pdf"):
    yield path


class FakeReportStore:
    def __init__(self):
        self.saved_reports = []
        self.report_lookup = {}

    def save_report(self, **kwargs):
        report_id = f"rpt-{len(self.saved_reports) + 1}"
        record = {
            "report_id": report_id,
            "month": kwargs["month"],
            "type": kwargs["report_type"],
            "file_name": kwargs["file_name"],
            "storage_backend": "blob",
            "storage_key": f"reports/{report_id}",
            "storage_url": "https://blob.example/report.pdf",
            "content_type": kwargs.get("content_type", "application/pdf"),
            "metadata": kwargs.get("metadata", {}),
        }
        self.saved_reports.append(record)
        self.report_lookup[report_id] = record
        self.report_lookup[record["file_name"]] = record
        return record

    def get_report(self, *, report_id=None, file_name=None, month=None):
        if report_id:
            return self.report_lookup.get(report_id)
        if file_name:
            record = self.report_lookup.get(file_name)
            if record and month and record["month"] != month:
                return None
            return record
        return None

    def read_report_bytes(self, report):
        return b"%PDF-1.4\n", "application/pdf"


class FakeInvoiceStore:
    def __init__(self):
        self.captures = []

    def save_capture(self, **kwargs):
        record = {
            "invoice_id": f"inv-{len(self.captures) + 1}",
            "sid": kwargs["sid"],
            "file_name": kwargs["file_name"],
            "storage_backend": "blob",
            "storage_key": f"Invoices/{kwargs['sid']}/inv-{len(self.captures) + 1}_{kwargs['file_name']}",
            "storage_url": "https://blob.example/private/invoice.jpg",
            "content_type": kwargs["content_type"],
            "content_size": len(kwargs["content"]),
            "fields": kwargs.get("fields"),
            "extraction_error": kwargs.get("extraction_error"),
            "folder_path": f"Invoices/{kwargs['sid']}",
            "created_at": "2026-04-22T12:00:00+00:00",
            "updated_at": "2026-04-22T12:00:00+00:00",
        }
        self.captures.append(record)
        return record

    def list_captures(self, sid):
        return [capture for capture in self.captures if capture["sid"] == sid]

    def get_capture(self, *, sid, invoice_id):
        for capture in self.captures:
            if capture["sid"] == sid and capture["invoice_id"] == invoice_id:
                return capture
        return None

    def read_capture_bytes(self, capture):
        return b"\xff\xd8\xff", capture.get("content_type") or "image/jpeg"


class CalendarManagerTests(unittest.TestCase):
    def test_add_events_supports_weekly_repetition_and_breakdown_totals(self):
        with isolated_calendar_storage():
            created = calendar_manager.add_events(
                date="2026-04-01",
                time="09:00",
                end_time="10:00",
                category="assistant",
                title="Morning support",
                assistant_hours={
                    "koerperpflege": 1.0,
                    "mahlzeiten_eingeben": 0.5,
                    "mahlzeiten_zubereiten": 0.25,
                    "begleitung_therapie": 0.75,
                },
                recurrence="weekly",
                repeat_count=2,
            )

            self.assertEqual(len(created), 3)
            self.assertEqual([event["date"] for event in created], ["2026-04-01", "2026-04-08", "2026-04-15"])
            self.assertEqual(created[0]["end_time"], "10:00")
            self.assertEqual(calendar_manager.get_assistant_hours("2026-04"), 7.5)
            self.assertEqual(
                calendar_manager.get_assistant_hours_breakdown("2026-04"),
                {
                    "koerperpflege": 3.0,
                    "mahlzeiten_eingeben": 1.5,
                    "mahlzeiten_zubereiten": 0.75,
                    "begleitung_therapie": 2.25,
                },
            )

    def test_get_events_normalizes_legacy_category_aliases(self):
        with isolated_calendar_storage() as calendar_path:
            with open(calendar_path, "w", encoding="utf-8") as file:
                json.dump(
                    [
                        {
                            "id": "evt-1",
                            "date": "2026-04-03",
                            "time": "10:30",
                            "end_time": "11:00",
                            "category": "tixi",
                            "title": "Ride",
                            "notes": "",
                            "hours": 0,
                        }
                    ],
                    file,
                )

            events = calendar_manager.get_events("2026-04")
            self.assertEqual(events[0]["category"], "transport")


class CalendarApiTests(unittest.TestCase):
    def test_post_transport_event_persists_transport_fields(self):
        with isolated_calendar_storage():
            client = app_module.app.test_client()
            response = client.post(
                "/api/events",
                json={
                    "date": "2026-04-09",
                    "time": "14:00",
                    "end_time": "15:00",
                    "category": "transport",
                    "title": "Trip to clinic",
                    "transport_mode": "taxi",
                    "transport_kilometers": 18.4,
                    "transport_address": "Clinic name, street, city",
                    "recurrence": "none",
                    "repeat_count": 0,
                },
            )

            self.assertEqual(response.status_code, 201)
            payload = response.get_json()
            self.assertEqual(payload["event"]["category"], "transport")
            self.assertEqual(payload["event"]["transport_mode"], "taxi")
            self.assertEqual(payload["event"]["transport_kilometers"], 18.4)
            self.assertEqual(payload["event"]["transport_address"], "Clinic name, street, city")
            self.assertEqual(payload["event"]["hours"], 0.0)

    def test_post_events_creates_multiple_occurrences(self):
        with isolated_calendar_storage():
            client = app_module.app.test_client()
            response = client.post(
                "/api/events",
                json={
                    "date": "2026-04-07",
                    "time": "08:00",
                    "end_time": "09:00",
                    "category": "assistant",
                    "title": "Support block",
                    "assistant_hours": {
                        "koerperpflege": 1.0,
                        "mahlzeiten_eingeben": 0.0,
                        "mahlzeiten_zubereiten": 0.5,
                        "begleitung_therapie": 0.0,
                    },
                    "recurrence": "weekly",
                    "repeat_count": 1,
                },
            )

            self.assertEqual(response.status_code, 201)
            payload = response.get_json()
            self.assertEqual(payload["created_count"], 2)
            self.assertEqual(payload["events"][0]["end_time"], "09:00")

            hours_response = client.get("/api/hours?month=2026-04")
            self.assertEqual(hours_response.status_code, 200)
            hours_payload = hours_response.get_json()
            self.assertEqual(hours_payload["total_hours"], 3.0)
            self.assertEqual(
                hours_payload["assistant_breakdown"],
                {
                    "koerperpflege": 2.0,
                    "mahlzeiten_eingeben": 0.0,
                    "mahlzeiten_zubereiten": 1.0,
                    "begleitung_therapie": 0.0,
                },
            )

    def test_put_event_updates_existing_event(self):
        with isolated_calendar_storage():
            created = calendar_manager.add_event(
                date="2026-04-10",
                time="09:00",
                end_time="09:30",
                category="assistant",
                title="Original",
                assistant_hours={
                    "koerperpflege": 0.5,
                    "mahlzeiten_eingeben": 0.0,
                    "mahlzeiten_zubereiten": 0.0,
                    "begleitung_therapie": 0.0,
                },
            )

            client = app_module.app.test_client()
            response = client.put(
                f"/api/events/{created['id']}",
                json={
                    "date": "2026-04-10",
                    "time": "10:00",
                    "end_time": "11:30",
                    "category": "assistant",
                    "title": "Updated support",
                    "notes": "Adjusted duration",
                    "assistant_hours": {
                        "koerperpflege": 1.0,
                        "mahlzeiten_eingeben": 0.5,
                        "mahlzeiten_zubereiten": 0.0,
                        "begleitung_therapie": 0.0,
                    },
                    "recurrence": "none",
                    "repeat_count": 0,
                },
            )

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertTrue(payload["updated"])
            self.assertEqual(payload["event"]["time"], "10:00")
            self.assertEqual(payload["event"]["end_time"], "11:30")
            self.assertEqual(payload["event"]["title"], "Updated support")

    def test_post_all_day_event_creates_reminder_without_time(self):
        with isolated_calendar_storage():
            client = app_module.app.test_client()
            response = client.post(
                "/api/events",
                json={
                    "date": "2026-04-12",
                    "all_day": True,
                    "category": "other",
                    "title": "Reminder",
                    "notes": "Bring documents",
                    "recurrence": "none",
                    "repeat_count": 0,
                },
            )

            self.assertEqual(response.status_code, 201)
            payload = response.get_json()
            self.assertTrue(payload["event"]["all_day"])
            self.assertEqual(payload["event"]["time"], "")
            self.assertEqual(payload["event"]["end_time"], "")

    def test_generate_report_accepts_multiple_report_types(self):
        client = app_module.app.test_client()
        fake_report_store = FakeReportStore()
        with patch.object(
            app_module,
            "load_profile_payload",
            return_value={"insured_name": "Max Muster", "ahv_number": "1", "street": "Street", "plz_ort": "City", "iban": "IBAN", "mitteilungsnummer": "REF"},
        ), patch.object(app_module, "get_report_store", return_value=fake_report_store), patch.object(
            app_module, "resolve_dual_template_paths", return_value=None
        ), patch.object(
            app_module, "resolve_template_path", return_value="template.pdf"
        ), patch.object(
            app_module, "materialize_binary_reference", side_effect=passthrough_materialized_path
        ), patch.object(
            app_module, "fill_assistenz_form_auto_bytes", return_value=b"%PDF-1.4\n"
        ), patch.object(app_module, "get_assistant_hours", return_value=4.5), patch.object(
            app_module,
            "get_assistant_hours_breakdown",
            return_value={
                "koerperpflege": 1.0,
                "mahlzeiten_eingeben": 1.0,
                "mahlzeiten_zubereiten": 1.5,
                "begleitung_therapie": 1.0,
            },
        ):
            response = client.post(
                "/api/reports/generate",
                json={
                    "month": "2026-04",
                    "report_types": ["assistenzbeitrag", "transportkostenabrechnung"],
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(len(payload["generated_reports"]), 1)
        self.assertEqual(payload["generated_reports"][0]["type"], "assistenzbeitrag")
        self.assertEqual(payload["generated_reports"][0]["report_id"], "rpt-1")
        self.assertEqual(len(payload["unavailable_reports"]), 1)
        self.assertEqual(payload["unavailable_reports"][0]["type"], "transportkostenabrechnung")

    def test_generate_report_prefers_dual_template_workflow(self):
        client = app_module.app.test_client()
        fake_report_store = FakeReportStore()
        with patch.object(
            app_module,
            "load_profile_payload",
            return_value={"insured_name": "Max Muster", "ahv_number": "1", "street": "Street", "plz_ort": "City", "iban": "IBAN", "mitteilungsnummer": "REF"},
        ), patch.object(app_module, "get_report_store", return_value=fake_report_store), patch.object(
            app_module, "resolve_dual_template_paths", return_value=("stundenblatt.pdf", "rechnung.pdf")
        ), patch.object(
            app_module, "materialize_binary_reference", side_effect=passthrough_materialized_path
        ), patch.object(
            app_module, "fill_assistenz_dual_form_auto_bytes", return_value=b"%PDF-1.4\n"
        ) as dual_fill_mock, patch.object(
            app_module, "fill_assistenz_form_auto_bytes"
        ) as single_fill_mock, patch.object(
            app_module, "get_assistant_hours", return_value=4.5
        ), patch.object(
            app_module,
            "get_assistant_hours_breakdown",
            return_value={
                "koerperpflege": 1.0,
                "mahlzeiten_eingeben": 1.0,
                "mahlzeiten_zubereiten": 1.5,
                "begleitung_therapie": 1.0,
            },
        ):
            response = client.post(
                "/api/reports/generate",
                json={
                    "month": "2026-04",
                    "report_types": ["assistenzbeitrag"],
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        dual_fill_mock.assert_called_once()
        single_fill_mock.assert_not_called()
        self.assertEqual(payload["generated_reports"][0]["gross_amount_chf"], "157.50")

    def test_send_report_accepts_report_id_and_omits_file_path(self):
        client = app_module.app.test_client()
        fake_report_store = FakeReportStore()
        saved_report = fake_report_store.save_report(
            month="2026-04",
            report_type="assistenzbeitrag",
            file_name="Assistenzbeitrag_2026-04.pdf",
            content=b"%PDF-1.4\n",
        )

        captured_payload = {}

        def fake_trigger(payload):
            captured_payload.update(payload)

        with patch.object(app_module, "get_report_store", return_value=fake_report_store), patch.object(
            app_module, "trigger_n8n_webhook", side_effect=fake_trigger
        ):
            response = client.post(
                "/api/reports/send",
                json={
                    "month": "2026-04",
                    "report_id": saved_report["report_id"],
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["sent"])
        self.assertEqual(payload["report_id"], saved_report["report_id"])
        self.assertNotIn("file_path", captured_payload)
        self.assertEqual(captured_payload["report_id"], saved_report["report_id"])

    def test_download_report_streams_from_store_lookup(self):
        client = app_module.app.test_client()
        fake_report_store = FakeReportStore()
        saved_report = fake_report_store.save_report(
            month="2026-04",
            report_type="assistenzbeitrag",
            file_name="Assistenzbeitrag_2026-04.pdf",
            content=b"%PDF-1.4\n",
        )

        with patch.object(app_module, "get_report_store", return_value=fake_report_store):
            response = client.get(
                f"/api/reports/download/{saved_report['report_id']}/{saved_report['file_name']}"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "application/pdf")

    def test_invoice_capture_stores_image_even_when_extraction_fails(self):
        client = app_module.app.test_client()
        fake_invoice_store = FakeInvoiceStore()

        with patch.object(app_module, "get_invoice_store", return_value=fake_invoice_store), patch.object(
            app_module, "_call_openai_vision", side_effect=RuntimeError("openai unavailable")
        ):
            response = client.post(
                "/api/invoices/session123/capture",
                json={
                    "image_base64": "/9j/",
                    "mime": "image/jpeg",
                    "file_name": "phone.jpg",
                },
            )

        self.assertEqual(response.status_code, 201)
        payload = response.get_json()
        self.assertTrue(payload["stored"])
        self.assertEqual(payload["capture"]["folder_path"], "Invoices/session123")
        self.assertEqual(payload["capture"]["file_name"], "phone.jpg")
        self.assertEqual(payload["extraction_error"], "openai unavailable")
        self.assertEqual(len(fake_invoice_store.captures), 1)

    def test_invoice_capture_accepts_pdf_without_vision_extraction(self):
        client = app_module.app.test_client()
        fake_invoice_store = FakeInvoiceStore()

        with patch.object(app_module, "get_invoice_store", return_value=fake_invoice_store), patch.object(
            app_module, "_call_openai_vision"
        ) as vision_mock:
            response = client.post(
                "/api/invoices/session123/capture",
                json={
                    "image_base64": "JVBERi0xLjQK",
                    "mime": "application/pdf",
                    "file_name": "invoice.pdf",
                },
            )

        self.assertEqual(response.status_code, 201)
        payload = response.get_json()
        self.assertTrue(payload["stored"])
        self.assertEqual(payload["capture"]["content_type"], "application/pdf")
        self.assertEqual(payload["capture"]["file_name"], "invoice.pdf")
        self.assertFalse(payload["capture"]["previewable"])
        self.assertEqual(payload["capture"]["storage_backend"], "blob")
        self.assertIsNone(payload["extraction_error"])
        vision_mock.assert_not_called()

    def test_scan_url_uses_camera_route_and_scan_redirects(self):
        client = app_module.app.test_client()

        scan_response = client.get("/api/invoices/session123/scan-url")
        self.assertEqual(scan_response.status_code, 200)
        scan_payload = scan_response.get_json()
        self.assertIn("/camera?sid=session123", scan_payload["camera_url"])

        redirect_response = client.get("/scan/session123")
        self.assertEqual(redirect_response.status_code, 302)
        self.assertIn("/camera?sid=session123", redirect_response.headers["Location"])

    def test_voice_calendar_draft_returns_ai_event_draft(self):
        client = app_module.app.test_client()
        fake_payload = {
            "transcript": "Tomorrow at 10 therapy appointment",
            "draft": {
                "date": "2026-04-29",
                "time": "10:00",
                "end_time": "10:30",
                "all_day": False,
                "category": "other",
                "title": "Therapy appointment",
                "notes": "Transcript: Tomorrow at 10 therapy appointment",
                "hours": 0.0,
                "assistant_hours": {
                    "koerperpflege": 0.0,
                    "mahlzeiten_eingeben": 0.0,
                    "mahlzeiten_zubereiten": 0.0,
                    "begleitung_therapie": 0.0,
                },
                "transport_mode": "",
                "transport_kilometers": 0.0,
                "transport_address": "",
                "recurrence": "none",
                "repeat_count": 0,
            },
            "missing_fields": [],
            "confidence": 0.9,
            "warnings": [],
        }

        with patch.object(app_module, "build_voice_calendar_draft", return_value=fake_payload) as draft_mock:
            response = client.post(
                "/api/calendar/voice/draft",
                data={
                    "audio": (io.BytesIO(b"webm audio"), "calendar-voice.webm"),
                    "timezone": "Europe/Berlin",
                    "now": "2026-04-28T12:00:00+02:00",
                },
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["transcript"], fake_payload["transcript"])
        self.assertEqual(payload["draft"]["title"], "Therapy appointment")
        self.assertEqual(app_module.parse_event_payload(payload["draft"])["date"], "2026-04-29")
        draft_mock.assert_called_once()

    def test_voice_calendar_draft_requires_audio(self):
        client = app_module.app.test_client()

        with patch.object(app_module, "build_voice_calendar_draft") as draft_mock:
            response = client.post(
                "/api/calendar/voice/draft",
                data={"timezone": "Europe/Berlin"},
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "audio is required")
        draft_mock.assert_not_called()

    def test_voice_calendar_draft_returns_gateway_error_for_agent_failure(self):
        client = app_module.app.test_client()

        with patch.object(app_module, "build_voice_calendar_draft", side_effect=RuntimeError("OpenAI unavailable")):
            response = client.post(
                "/api/calendar/voice/draft",
                data={"audio": (io.BytesIO(b"webm audio"), "calendar-voice.webm")},
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 502)
        self.assertIn("OpenAI unavailable", response.get_json()["error"])

    def test_voice_calendar_draft_can_return_missing_fields_without_saving(self):
        client = app_module.app.test_client()
        fake_payload = {
            "transcript": "Add therapy sometime next week",
            "draft": {
                "date": "",
                "time": "",
                "end_time": "",
                "all_day": False,
                "category": "other",
                "title": "Therapy",
                "notes": "Transcript: Add therapy sometime next week",
                "hours": 0.0,
                "assistant_hours": {
                    "koerperpflege": 0.0,
                    "mahlzeiten_eingeben": 0.0,
                    "mahlzeiten_zubereiten": 0.0,
                    "begleitung_therapie": 0.0,
                },
                "transport_mode": "",
                "transport_kilometers": 0.0,
                "transport_address": "",
                "recurrence": "none",
                "repeat_count": 0,
            },
            "missing_fields": ["date", "time", "end_time"],
            "confidence": 0.35,
            "warnings": ["Date and time were not clear."],
        }

        with patch.object(app_module, "build_voice_calendar_draft", return_value=fake_payload), patch.object(
            app_module, "add_events"
        ) as add_events_mock:
            response = client.post(
                "/api/calendar/voice/draft",
                data={"audio": (io.BytesIO(b"webm audio"), "calendar-voice.webm")},
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["missing_fields"], ["date", "time", "end_time"])
        add_events_mock.assert_not_called()


class VoiceCalendarAgentTests(unittest.TestCase):
    def test_build_voice_calendar_draft_uses_mocked_openai_client(self):
        response_json = json.dumps(
            {
                "draft": {
                    "date": "2026-04-29",
                    "time": "11:00",
                    "end_time": "12:00",
                    "all_day": False,
                    "category": "assistant",
                    "title": "Morning support",
                    "notes": "",
                    "hours": 1.0,
                    "assistant_hours": {
                        "koerperpflege": 1.0,
                        "mahlzeiten_eingeben": 0.0,
                        "mahlzeiten_zubereiten": 0.0,
                        "begleitung_therapie": 0.0,
                    },
                    "transport_mode": "",
                    "transport_kilometers": 0.0,
                    "transport_address": "",
                    "recurrence": "none",
                    "repeat_count": 0,
                },
                "missing_fields": [],
                "confidence": 0.88,
                "warnings": [],
            }
        )

        class FakeTranscriptions:
            def create(self, **kwargs):
                self.kwargs = kwargs
                return SimpleNamespace(text="Tomorrow at 11 morning support for one hour")

        class FakeResponses:
            def create(self, **kwargs):
                self.kwargs = kwargs
                return SimpleNamespace(output_text=response_json)

        fake_transcriptions = FakeTranscriptions()
        fake_responses = FakeResponses()
        fake_client = SimpleNamespace(
            audio=SimpleNamespace(transcriptions=fake_transcriptions),
            responses=fake_responses,
        )

        payload = voice_calendar_agent.build_voice_calendar_draft(
            b"webm audio",
            "calendar-voice.webm",
            timezone_name="Europe/Berlin",
            now_value="2026-04-28T12:00:00+02:00",
            client=fake_client,
        )

        self.assertEqual(fake_transcriptions.kwargs["model"], "whisper-1")
        self.assertEqual(payload["transcript"], "Tomorrow at 11 morning support for one hour")
        self.assertEqual(payload["draft"]["category"], "assistant")
        self.assertEqual(payload["draft"]["hours"], 1.0)


class ReminderAgentTests(unittest.TestCase):
    def test_build_reminder_draft_from_text_uses_openai_tool_call(self):
        tool_args = {
            "title": "Generate Assistenzbeitrag at month-end",
            "action": "generate_assistenzbeitrag",
            "schedule": "month_end",
            "run_time": "09:00",
            "note": "Prepare monthly Assistenzbeitrag",
        }

        class FakeResponses:
            def create(self, **kwargs):
                self.kwargs = kwargs
                return SimpleNamespace(
                    output=[
                        SimpleNamespace(
                            type="function_call",
                            name="create_reminder",
                            arguments=json.dumps(tool_args),
                        )
                    ]
                )

        fake_responses = FakeResponses()
        fake_client = SimpleNamespace(responses=fake_responses)

        payload = reminders_agent.build_reminder_draft_from_text(
            "Remind me at the end of every month and prepare the Assistenzbeitrag",
            timezone_name="Europe/Berlin",
            now_value="2026-04-29T12:00:00+02:00",
            client=fake_client,
        )

        self.assertEqual(payload["transcript"], "Remind me at the end of every month and prepare the Assistenzbeitrag")
        self.assertEqual(payload["draft"]["action"], "generate_assistenzbeitrag")
        self.assertEqual(payload["draft"]["schedule"], "month_end")
        self.assertEqual(payload["draft"]["timezone"], "Europe/Berlin")
        self.assertEqual(fake_responses.kwargs["tools"][0]["name"], "create_reminder")
        self.assertEqual(fake_responses.kwargs["tool_choice"]["name"], "create_reminder")


if __name__ == "__main__":
    unittest.main()
