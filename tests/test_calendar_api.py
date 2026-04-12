import json
import os
import shutil
import unittest
import uuid
from contextlib import contextmanager
from unittest.mock import patch

from iv_agent import app as app_module
from iv_agent import calendar_manager


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
        base_tmp = os.path.join(os.getcwd(), "output", "test_tmp")
        os.makedirs(base_tmp, exist_ok=True)
        temp_dir = os.path.join(base_tmp, f"report_{uuid.uuid4().hex}")
        os.makedirs(temp_dir, exist_ok=True)
        try:
            profile_path = os.path.join(temp_dir, "profile.json")
            output_dir = os.path.join(temp_dir, "output")
            os.makedirs(output_dir, exist_ok=True)
            with open(profile_path, "w", encoding="utf-8") as file:
                json.dump({"insured_name": "Max Muster"}, file)

            generated_pdf_path = os.path.join(output_dir, "Assistenzbeitrag_2026-04.pdf")

            with patch.object(app_module, "OUTPUT_DIR", output_dir), patch.object(
                app_module, "resolve_profile_path", return_value=profile_path
            ), patch.object(app_module, "resolve_dual_template_paths", return_value=None), patch.object(
                app_module, "resolve_template_path", return_value="template.pdf"
            ), patch.object(
                app_module, "fill_assistenz_form_auto", return_value=generated_pdf_path
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
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(len(payload["generated_reports"]), 1)
        self.assertEqual(payload["generated_reports"][0]["type"], "assistenzbeitrag")
        self.assertEqual(len(payload["unavailable_reports"]), 1)
        self.assertEqual(payload["unavailable_reports"][0]["type"], "transportkostenabrechnung")

    def test_generate_report_prefers_dual_template_workflow(self):
        client = app_module.app.test_client()
        base_tmp = os.path.join(os.getcwd(), "output", "test_tmp")
        os.makedirs(base_tmp, exist_ok=True)
        temp_dir = os.path.join(base_tmp, f"report_dual_{uuid.uuid4().hex}")
        os.makedirs(temp_dir, exist_ok=True)
        try:
            profile_path = os.path.join(temp_dir, "profile.json")
            output_dir = os.path.join(temp_dir, "output")
            os.makedirs(output_dir, exist_ok=True)
            with open(profile_path, "w", encoding="utf-8") as file:
                json.dump({"insured_name": "Max Muster"}, file)

            generated_pdf_path = os.path.join(output_dir, "Assistenzbeitrag_2026-04.pdf")

            with patch.object(app_module, "OUTPUT_DIR", output_dir), patch.object(
                app_module, "resolve_profile_path", return_value=profile_path
            ), patch.object(
                app_module, "resolve_dual_template_paths", return_value=("stundenblatt.pdf", "rechnung.pdf")
            ), patch.object(
                app_module, "fill_assistenz_dual_form_auto", return_value=generated_pdf_path
            ) as dual_fill_mock, patch.object(
                app_module, "fill_assistenz_form_auto"
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
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        dual_fill_mock.assert_called_once()
        single_fill_mock.assert_not_called()
        self.assertEqual(payload["generated_reports"][0]["gross_amount_chf"], "157.50")


if __name__ == "__main__":
    unittest.main()
