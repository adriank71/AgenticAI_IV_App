import json
import os
import shutil
import unittest
import uuid
from contextlib import contextmanager

from iv_agent.calendar_manager import JsonEventStore, PostgresEventStore
from iv_agent.migrate_local_data import migrate_local_data
from iv_agent.storage import LocalInvoiceCaptureStore, PostgresProfileStore, PostgresReportStore


@contextmanager
def workspace_tempdir():
    base_dir = os.path.join(os.getcwd(), "tests", ".tmp")
    os.makedirs(base_dir, exist_ok=True)
    temp_dir = os.path.join(base_dir, f"storage_{uuid.uuid4().hex}")
    os.makedirs(temp_dir, exist_ok=True)
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


class RecordingCursor:
    def __init__(self, fetchone_results=None, fetchall_results=None):
        self.statements = []
        self.rowcount = 0
        self._fetchone_results = list(fetchone_results or [])
        self._fetchall_results = list(fetchall_results or [])

    def execute(self, query, params=None):
        self.statements.append((" ".join(str(query).split()), params))
        if "DELETE FROM events WHERE" in str(query):
            self.rowcount = 1

    def fetchone(self):
        if self._fetchone_results:
            return self._fetchone_results.pop(0)
        return None

    def fetchall(self):
        if self._fetchall_results:
            return self._fetchall_results.pop(0)
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class RecordingConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeAssetStore:
    backend_name = "blob"

    def __init__(self):
        self.saved = None

    def store_report(self, **kwargs):
        self.saved = kwargs
        return {
            "storage_key": "reports/rpt-1_Assistenzbeitrag_2026-04.pdf",
            "storage_url": "https://blob.example/private/report.pdf",
            "storage_download_url": "https://blob.example/private/report.pdf?download=1",
            "content_type": kwargs["content_type"],
            "content_size": len(kwargs["content"]),
        }

    def read_bytes(self, *, storage_key, storage_url=None):
        return b"%PDF-1.4\n", "application/pdf"


class CaptureEventStore:
    def __init__(self):
        self.events = None

    def replace_all_events(self, events):
        self.events = list(events)
        return len(events)


class CaptureProfileStore:
    def __init__(self):
        self.profiles = {}

    def upsert_profile(self, profile_id, payload):
        self.profiles[profile_id] = payload


class StorageTests(unittest.TestCase):
    def test_local_invoice_capture_store_persists_image_and_metadata(self):
        with workspace_tempdir() as temp_dir:
            store = LocalInvoiceCaptureStore(temp_dir)

            saved = store.save_capture(
                sid="session123",
                file_name="receipt.jpg",
                content=b"\xff\xd8\xff",
                content_type="image/jpeg",
                fields={"merchant": "Cafe Example", "total": 12.4, "currency": "CHF"},
                extraction_error=None,
            )

            self.assertEqual(saved["sid"], "session123")
            self.assertEqual(saved["folder_path"], "Invoices/session123")
            self.assertTrue(saved["storage_key"].endswith("_receipt.jpg"))

            listed = store.list_captures("session123")
            self.assertEqual(len(listed), 1)
            self.assertEqual(listed[0]["invoice_id"], saved["invoice_id"])
            self.assertEqual(listed[0]["fields"]["merchant"], "Cafe Example")

            fetched = store.get_capture(sid="session123", invoice_id=saved["invoice_id"])
            self.assertEqual(fetched["file_name"], "receipt.jpg")

            image_bytes, content_type = store.read_capture_bytes(fetched)
            self.assertEqual(image_bytes, b"\xff\xd8\xff")
            self.assertEqual(content_type, "image/jpeg")

    def test_postgres_profile_store_reads_json_payload(self):
        cursor = RecordingCursor(fetchone_results=[{"payload": {"insured_name": "Max Muster"}}])
        store = PostgresProfileStore(
            "postgres://example",
            connection_factory=lambda: RecordingConnection(cursor),
        )

        profile = store.get_profile(None)

        self.assertEqual(profile["insured_name"], "Max Muster")
        self.assertTrue(any("CREATE TABLE IF NOT EXISTS profiles" in query for query, _ in cursor.statements))
        self.assertTrue(any("SELECT payload FROM profiles" in query for query, _ in cursor.statements))

    def test_postgres_event_store_returns_normalized_rows(self):
        cursor = RecordingCursor(
            fetchall_results=[
                [
                    {
                        "event_id": "evt-1",
                        "event_date": "2026-04-12",
                        "start_time": "09:00",
                        "end_time": "10:00",
                        "all_day": False,
                        "category": "assistant",
                        "title": "Morning support",
                        "notes": "Arrive early",
                        "hours": 1.5,
                        "assistant_hours": {"koerperpflege": 1.0, "mahlzeiten_eingeben": 0.5},
                        "transport_mode": "",
                        "transport_kilometers": 0,
                        "transport_address": "",
                    }
                ]
            ]
        )
        store = PostgresEventStore(
            "postgres://example",
            connection_factory=lambda: RecordingConnection(cursor),
        )

        events = store.get_events("2026-04")

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["id"], "evt-1")
        self.assertEqual(events[0]["assistant_hours"]["koerperpflege"], 1.0)
        self.assertTrue(any("WHERE TO_CHAR(event_date, 'YYYY-MM') = %s" in query for query, _ in cursor.statements))

    def test_postgres_report_store_saves_metadata_and_uses_asset_store(self):
        cursor = RecordingCursor()
        asset_store = FakeAssetStore()
        store = PostgresReportStore(
            "postgres://example",
            asset_store=asset_store,
            connection_factory=lambda: RecordingConnection(cursor),
        )

        report = store.save_report(
            month="2026-04",
            report_type="assistenzbeitrag",
            file_name="Assistenzbeitrag_2026-04.pdf",
            content=b"%PDF-1.4\n",
            metadata={"assistant_hours": 4.5},
        )

        self.assertEqual(report["type"], "assistenzbeitrag")
        self.assertEqual(asset_store.saved["month"], "2026-04")
        self.assertTrue(any("INSERT INTO reports" in query for query, _ in cursor.statements))

    def test_migrate_local_data_imports_calendar_and_profiles(self):
        with workspace_tempdir() as temp_dir:
            calendar_path = os.path.join(temp_dir, "calendar.json")
            profile_path = os.path.join(temp_dir, "profile.json")
            profile_dir = os.path.join(temp_dir, "profiles")
            os.makedirs(profile_dir, exist_ok=True)

            source_event_store = JsonEventStore(temp_dir, calendar_path)
            source_event_store.replace_all_events(
                [
                    {
                        "id": "evt-1",
                        "date": "2026-04-05",
                        "time": "09:00",
                        "end_time": "10:00",
                        "all_day": False,
                        "category": "assistant",
                        "title": "Morning support",
                        "notes": "",
                        "hours": 1.0,
                        "assistant_hours": {"koerperpflege": 1.0},
                        "transport_mode": "",
                        "transport_kilometers": 0.0,
                        "transport_address": "",
                    }
                ]
            )

            with open(profile_path, "w", encoding="utf-8") as file:
                json.dump({"insured_name": "Default"}, file)

            with open(os.path.join(profile_dir, "child.json"), "w", encoding="utf-8") as file:
                json.dump({"insured_name": "Child"}, file)

            target_event_store = CaptureEventStore()
            target_profile_store = CaptureProfileStore()

            summary = migrate_local_data(
                database_url="postgres://example",
                calendar_path=calendar_path,
                default_profile_path=profile_path,
                profile_dir=profile_dir,
                event_store_factory=lambda _url: target_event_store,
                profile_store_factory=lambda _url: target_profile_store,
            )

        self.assertEqual(summary, {"events": 1, "profiles": 2})
        self.assertEqual(target_event_store.events[0]["id"], "evt-1")
        self.assertIn("default", target_profile_store.profiles)
        self.assertIn("child", target_profile_store.profiles)


if __name__ == "__main__":
    unittest.main()
