import os
import shutil
import unittest
import uuid
from contextlib import contextmanager
from unittest.mock import patch

from iv_agent.calendar_manager import PostgresEventStore
from iv_agent import migrate_local_data
from iv_agent.reminders import PostgresReminderStore
from iv_agent.storage import (
    LocalInvoiceCaptureStore,
    PostgresAssetStore,
    PostgresInvoiceCaptureStore,
    PostgresProfileStore,
    PostgresReportStore,
    PostgresTemplateStore,
    SupabaseStorageAssetStore,
    SupabaseStorageInvoiceCaptureStore,
    SupabaseStorageTemplateStore,
    make_asset_store,
    make_invoice_capture_store,
    make_template_store,
)


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
        self.rowcount = 1
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
    backend_name = "supabase"

    def __init__(self):
        self.saved = None

    def store_report(self, **kwargs):
        self.saved = kwargs
        return {
            "storage_key": "reports/rpt-1_Assistenzbeitrag_2026-04.pdf",
            "storage_url": "supabase://iv-agent-reports/reports/report.pdf",
            "storage_download_url": None,
            "content_type": kwargs["content_type"],
            "content_size": len(kwargs["content"]),
        }

    def read_bytes(self, *, storage_key, storage_url=None):
        return b"%PDF-1.4\n", "application/pdf"


class FakeSupabaseBucket:
    def __init__(self, objects, bucket):
        self.objects = objects
        self.bucket = bucket

    def upload(self, *, path, file, file_options):
        self.objects[(self.bucket, path)] = {
            "content": bytes(file),
            "options": dict(file_options),
        }

    def download(self, path):
        return self.objects[(self.bucket, path)]["content"]


class FakeSupabaseStorage:
    def __init__(self, objects):
        self.objects = objects

    def from_(self, bucket):
        return FakeSupabaseBucket(self.objects, bucket)


class FakeSupabaseClient:
    def __init__(self):
        self.objects = {}
        self.storage = FakeSupabaseStorage(self.objects)


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

    def test_postgres_reminder_store_reads_structured_rows(self):
        cursor = RecordingCursor(
            fetchall_results=[
                [
                    {
                        "reminder_id": "rem-1",
                        "title": "Month-end report",
                        "action": "generate_assistenzbeitrag",
                        "schedule": "month_end",
                        "note": "",
                        "run_time": "09:00",
                        "run_date": "",
                        "timezone": "Europe/Berlin",
                        "status": "active",
                        "last_run_at": None,
                        "next_run_at": "2026-05-31T09:00:00+02:00",
                        "last_run_status": None,
                        "last_run_message": None,
                        "created_at": "2026-04-30T10:00:00+00:00",
                        "updated_at": "2026-04-30T10:00:00+00:00",
                    }
                ]
            ]
        )
        store = PostgresReminderStore(
            "postgres://example",
            connection_factory=lambda: RecordingConnection(cursor),
        )

        reminders = store.load_all()

        self.assertEqual(reminders[0]["id"], "rem-1")
        self.assertEqual(reminders[0]["action"], "generate_assistenzbeitrag")
        self.assertTrue(any("CREATE TABLE IF NOT EXISTS reminders" in query for query, _ in cursor.statements))
        self.assertTrue(any("FROM reminders" in query for query, _ in cursor.statements))

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

    def test_postgres_report_store_can_read_migrated_postgres_asset(self):
        cursor = RecordingCursor(fetchone_results=[{"content": b"%PDF-legacy\n", "content_type": "application/pdf"}])
        store = PostgresReportStore(
            "postgres://example",
            asset_store=FakeAssetStore(),
            connection_factory=lambda: RecordingConnection(cursor),
        )

        content, content_type = store.read_report_bytes(
            {
                "storage_backend": "postgres",
                "storage_key": "reports/legacy.pdf",
                "storage_url": None,
            }
        )

        self.assertEqual(content, b"%PDF-legacy\n")
        self.assertEqual(content_type, "application/pdf")

    def test_postgres_template_store_upserts_and_reads_template_bytes(self):
        cursor = RecordingCursor(
            fetchone_results=[
                {
                    "template_key": "stundenblatt",
                    "file_name": "Stundenblatt.pdf",
                    "content_type": "application/pdf",
                    "content_size": 11,
                    "checksum_sha256": "abc",
                    "metadata": {"source": "test"},
                    "created_at": "2026-04-30T10:00:00+00:00",
                    "updated_at": "2026-04-30T10:00:00+00:00",
                },
                {"content": b"%PDF-test\n", "content_type": "application/pdf"},
            ]
        )
        store = PostgresTemplateStore(
            "postgres://example",
            connection_factory=lambda: RecordingConnection(cursor),
        )

        saved = store.upsert_template(
            template_key="stundenblatt",
            file_name="Stundenblatt.pdf",
            content=b"%PDF-test\n",
        )
        template = store.get_template("stundenblatt")
        content, content_type = store.read_template_bytes("stundenblatt")

        self.assertEqual(saved["template_key"], "stundenblatt")
        self.assertEqual(template["file_name"], "Stundenblatt.pdf")
        self.assertEqual(content, b"%PDF-test\n")
        self.assertEqual(content_type, "application/pdf")
        self.assertTrue(any("CREATE TABLE IF NOT EXISTS document_templates" in query for query, _ in cursor.statements))
        self.assertTrue(any("INSERT INTO document_templates" in query for query, _ in cursor.statements))

    def test_postgres_invoice_capture_store_saves_lists_and_reads_bytes(self):
        cursor = RecordingCursor(
            fetchall_results=[
                [
                    {
                        "invoice_id": "inv-1",
                        "sid": "session123",
                        "file_name": "receipt.jpg",
                        "storage_key": "Invoices/session123/inv-1_receipt.jpg",
                        "content_type": "image/jpeg",
                        "content_size": 3,
                        "fields": {"merchant": "Cafe Example"},
                        "extraction_error": None,
                        "folder_path": "Invoices/session123",
                        "created_at": "2026-04-30T10:00:00+00:00",
                        "updated_at": "2026-04-30T10:00:00+00:00",
                    }
                ]
            ],
            fetchone_results=[
                {
                    "invoice_id": "inv-1",
                    "sid": "session123",
                    "file_name": "receipt.jpg",
                    "storage_key": "Invoices/session123/inv-1_receipt.jpg",
                    "content_type": "image/jpeg",
                    "content_size": 3,
                    "fields": {"merchant": "Cafe Example"},
                    "extraction_error": None,
                    "folder_path": "Invoices/session123",
                    "created_at": "2026-04-30T10:00:00+00:00",
                    "updated_at": "2026-04-30T10:00:00+00:00",
                },
                {"content": b"\xff\xd8\xff", "content_type": "image/jpeg"},
            ],
        )
        store = PostgresInvoiceCaptureStore(
            "postgres://example",
            connection_factory=lambda: RecordingConnection(cursor),
        )

        saved = store.save_capture(
            sid="session123",
            file_name="receipt.jpg",
            content=b"\xff\xd8\xff",
            content_type="image/jpeg",
            fields={"merchant": "Cafe Example"},
        )
        listed = store.list_captures("session123")
        fetched = store.get_capture(sid="session123", invoice_id="inv-1")
        image_bytes, content_type = store.read_capture_bytes(fetched)

        self.assertEqual(saved["storage_backend"], "postgres")
        self.assertEqual(listed[0]["fields"]["merchant"], "Cafe Example")
        self.assertEqual(fetched["storage_backend"], "postgres")
        self.assertEqual(image_bytes, b"\xff\xd8\xff")
        self.assertEqual(content_type, "image/jpeg")
        self.assertTrue(any("CREATE TABLE IF NOT EXISTS invoice_captures" in query for query, _ in cursor.statements))

    def test_postgres_asset_store_saves_and_reads_report_bytes(self):
        cursor = RecordingCursor(fetchone_results=[{"content": b"%PDF-asset\n", "content_type": "application/pdf"}])
        store = PostgresAssetStore(
            "postgres://example",
            connection_factory=lambda: RecordingConnection(cursor),
        )

        saved = store.store_report(
            month="2026-04",
            report_id="rpt-1",
            file_name="Assistenzbeitrag_2026-04.pdf",
            content=b"%PDF-asset\n",
            content_type="application/pdf",
        )
        content, content_type = store.read_bytes(storage_key=saved["storage_key"])

        self.assertEqual(saved["storage_key"], "reports/2026-04/rpt-1_Assistenzbeitrag_2026-04.pdf")
        self.assertEqual(content, b"%PDF-asset\n")
        self.assertEqual(content_type, "application/pdf")
        self.assertTrue(any("CREATE TABLE IF NOT EXISTS asset_blobs" in query for query, _ in cursor.statements))

    def test_supabase_template_store_uses_template_bucket_and_keys(self):
        client = FakeSupabaseClient()
        store = SupabaseStorageTemplateStore(client=client, bucket="iv-agent-templates")

        saved = store.upsert_template(
            template_key="transportkosten",
            file_name="AK_Formular_EL_Transportkosten.pdf",
            content=b"%PDF-transport\n",
        )
        template = store.get_template("transportkosten")
        content, content_type = store.read_template_bytes("transportkosten")

        self.assertEqual(saved["storage_backend"], "supabase")
        self.assertEqual(saved["storage_key"], "transportkosten/AK_Formular_EL_Transportkosten.pdf")
        self.assertEqual(template["storage_key"], "transportkosten/AK_Formular_EL_Transportkosten.pdf")
        self.assertEqual(content, b"%PDF-transport\n")
        self.assertEqual(content_type, "application/pdf")
        self.assertEqual(
            client.objects[("iv-agent-templates", "transportkosten/AK_Formular_EL_Transportkosten.pdf")]["options"]["upsert"],
            "true",
        )

    def test_supabase_report_asset_store_uploads_to_reports_bucket(self):
        client = FakeSupabaseClient()
        store = SupabaseStorageAssetStore(client=client, bucket="iv-agent-reports")

        saved = store.store_report(
            month="2026-04",
            report_id="rpt-1",
            file_name="Assistenzbeitrag_2026-04.pdf",
            content=b"%PDF-report\n",
            content_type="application/pdf",
        )
        content, content_type = store.read_bytes(storage_key=saved["storage_key"])

        self.assertEqual(store.backend_name, "supabase")
        self.assertEqual(saved["storage_key"], "reports/2026-04/rpt-1_Assistenzbeitrag_2026-04.pdf")
        self.assertEqual(saved["storage_url"], "supabase://iv-agent-reports/reports/2026-04/rpt-1_Assistenzbeitrag_2026-04.pdf")
        self.assertEqual(content, b"%PDF-report\n")
        self.assertEqual(content_type, "application/pdf")

    def test_supabase_invoice_capture_store_uploads_file_and_metadata(self):
        cursor = RecordingCursor(
            fetchall_results=[
                [
                    {
                        "invoice_id": "inv-1",
                        "sid": "session123",
                        "file_name": "receipt.jpg",
                        "storage_key": "Invoices/session123/inv-1_receipt.jpg",
                        "storage_backend": "supabase",
                        "storage_bucket": "iv-agent-invoices",
                        "storage_url": "supabase://iv-agent-invoices/Invoices/session123/inv-1_receipt.jpg",
                        "content_type": "image/jpeg",
                        "content_size": 3,
                        "fields": {"merchant": "Cafe Example"},
                        "extraction_error": None,
                        "folder_path": "Invoices/session123",
                        "created_at": "2026-04-30T10:00:00+00:00",
                        "updated_at": "2026-04-30T10:00:00+00:00",
                    }
                ]
            ]
        )
        client = FakeSupabaseClient()
        store = SupabaseStorageInvoiceCaptureStore(
            "postgres://example",
            client=client,
            bucket="iv-agent-invoices",
            connection_factory=lambda: RecordingConnection(cursor),
        )

        saved = store.save_capture(
            sid="session123",
            file_name="receipt.jpg",
            content=b"\xff\xd8\xff",
            content_type="image/jpeg",
            fields={"merchant": "Cafe Example"},
        )
        listed = store.list_captures("session123")
        content, content_type = store.read_capture_bytes(saved)

        self.assertEqual(saved["storage_backend"], "supabase")
        self.assertTrue(saved["storage_key"].startswith("Invoices/session123/"))
        self.assertEqual(listed[0]["storage_backend"], "supabase")
        self.assertEqual(content, b"\xff\xd8\xff")
        self.assertEqual(content_type, "image/jpeg")
        self.assertTrue(any("INSERT INTO invoice_captures" in query for query, _ in cursor.statements))

    def test_supabase_invoice_capture_store_can_read_migrated_postgres_capture(self):
        cursor = RecordingCursor(
            fetchone_results=[{"content": b"\xff\xd8\xff", "content_type": "image/jpeg"}]
        )
        store = SupabaseStorageInvoiceCaptureStore(
            "postgres://example",
            client=FakeSupabaseClient(),
            bucket="iv-agent-invoices",
            connection_factory=lambda: RecordingConnection(cursor),
        )

        content, content_type = store.read_capture_bytes(
            {
                "invoice_id": "inv-1",
                "storage_backend": "postgres",
                "storage_key": "Invoices/session123/inv-1_receipt.jpg",
                "content_type": "image/jpeg",
            }
        )

        self.assertEqual(content, b"\xff\xd8\xff")
        self.assertEqual(content_type, "image/jpeg")

    def test_supabase_upload_error_mentions_required_bucket_and_service_key(self):
        class FailingBucket:
            def upload(self, **kwargs):
                raise RuntimeError("bucket not found")

        class FailingStorage:
            def from_(self, bucket):
                return FailingBucket()

        class FailingClient:
            storage = FailingStorage()

        store = SupabaseStorageAssetStore(client=FailingClient(), bucket="iv-agent-reports")

        with self.assertRaisesRegex(RuntimeError, "SUPABASE_SERVICE_ROLE_KEY.*private bucket"):
            store.store_report(
                month="2026-04",
                report_id="rpt-1",
                file_name="report.pdf",
                content=b"%PDF\n",
                content_type="application/pdf",
            )

    def test_backend_factories_select_supabase_from_env(self):
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgres://example",
                "SUPABASE_URL": "https://project.supabase.co",
                "SUPABASE_SERVICE_ROLE_KEY": "service-role-key",
                "IV_AGENT_STORAGE_BACKEND": "postgres",
                "IV_AGENT_REPORT_ASSET_BACKEND": "supabase",
                "IV_AGENT_INVOICE_ASSET_BACKEND": "supabase",
                "IV_AGENT_TEMPLATE_BACKEND": "supabase",
            },
        ), patch("iv_agent.storage.SupabaseStorageAssetStore", return_value="asset-store"), patch(
            "iv_agent.storage.SupabaseStorageInvoiceCaptureStore", return_value="invoice-store"
        ), patch("iv_agent.storage.SupabaseStorageTemplateStore", return_value="template-store"):
            self.assertEqual(make_asset_store("output"), "asset-store")
            self.assertEqual(make_invoice_capture_store("output"), "invoice-store")
            self.assertEqual(make_template_store(), "template-store")

    def test_env_loader_handles_bom_and_dry_run_helpers_do_not_connect(self):
        with workspace_tempdir() as temp_dir:
            env_path = os.path.join(temp_dir, ".env.local")
            with open(env_path, "w", encoding="utf-8-sig") as file:
                file.write("DATABASE_URL=postgres://example\nSUPABASE_SERVICE_ROLE_KEY=\n")

            with patch.dict(os.environ, {}, clear=True):
                loaded = migrate_local_data.load_env_file(env_path)
                self.assertEqual(loaded["DATABASE_URL"], "postgres://example")
                self.assertEqual(os.environ["DATABASE_URL"], "postgres://example")

            with patch.object(migrate_local_data, "_connect_postgres", side_effect=AssertionError("should not connect")):
                migrate_local_data.ensure_tables("postgres://example", dry_run=True)
                migrate_local_data.seed_database(
                    "postgres://example",
                    {"insuredPerson": {"fullName": "Example"}},
                    [],
                    [],
                    dry_run=True,
                )


if __name__ == "__main__":
    unittest.main()
