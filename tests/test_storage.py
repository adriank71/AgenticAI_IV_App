import os
import shutil
import unittest
import uuid
import io
import zipfile
from contextlib import contextmanager
from unittest.mock import patch

from iv_agent.calendar_manager import PostgresEventStore
from iv_agent.reminders import PostgresReminderStore
from iv_agent.services.storage_service import (
    StorageService,
    build_chat_document_artifact,
    extract_invoice_amount_fields,
    extract_document_text,
    process_chat_attachments,
    sanitize_document_filename,
)
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
    backend_health_status,
    clear_store_cache,
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

    def remove(self, paths):
        for path in paths or []:
            self.objects.pop((self.bucket, path), None)

    def create_signed_url(self, path, expires_in):
        return {"signedURL": f"https://signed.invalid/{self.bucket}/{path}?expires={expires_in}"}


class FakeSupabaseStorage:
    def __init__(self, objects):
        self.objects = objects

    def from_(self, bucket):
        return FakeSupabaseBucket(self.objects, bucket)

    def list_buckets(self):
        return [{"id": bucket} for bucket in sorted({bucket for bucket, _ in self.objects})]


class FakeSupabaseClient:
    def __init__(self):
        self.objects = {}
        self.storage = FakeSupabaseStorage(self.objects)


class StorageTests(unittest.TestCase):
    def tearDown(self):
        clear_store_cache()

    def test_document_filename_sanitization_and_text_extraction(self):
        self.assertEqual(
            sanitize_document_filename("../Rechnung Mai 2026.pdf", content_type="application/pdf"),
            "Rechnung_Mai_2026.pdf",
        )

        text, status, error = extract_document_text("Gruezi\nIV Rechnung".encode("utf-8"), "text/plain")
        self.assertEqual(status, "completed")
        self.assertIsNone(error)
        self.assertIn("IV Rechnung", text)

        docx_buffer = io.BytesIO()
        with zipfile.ZipFile(docx_buffer, "w") as docx_zip:
            docx_zip.writestr(
                "word/document.xml",
                """
                <w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
                  <w:body><w:p><w:r><w:t>Therapie Bericht</w:t></w:r></w:p></w:body>
                </w:document>
                """,
            )
        docx_text, docx_status, docx_error = extract_document_text(
            docx_buffer.getvalue(),
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
        self.assertEqual(docx_status, "completed")
        self.assertIsNone(docx_error)
        self.assertIn("Therapie Bericht", docx_text)

    def test_invoice_amount_extraction_prefers_totals_and_ignores_dates(self):
        self.assertEqual(extract_invoice_amount_fields("Betrag CHF 02.05.2026"), {})
        self.assertEqual(extract_invoice_amount_fields("Total CHF: 74.33")["total"], 74.33)
        self.assertEqual(
            extract_invoice_amount_fields("Gesamtbetrag Transportkosten: CHF 289.90")["total"],
            289.90,
        )
        self.assertEqual(extract_invoice_amount_fields("IV-VERGUETUNG CHF 260.91")["total"], 260.91)

    def test_storage_service_uploads_document_to_iv_bucket_with_review_metadata(self):
        cursor = RecordingCursor()
        client = FakeSupabaseClient()
        service = StorageService(
            "postgres://example",
            client=client,
            bucket="IV",
            connection_factory=lambda: RecordingConnection(cursor),
        )

        document = service.upload_document(
            user_id="default",
            file_name="Rechnung Mai 2026.txt",
            content=b"Rechnung der IV-Stelle vom 03.05.2026 Betrag CHF 50",
            content_type="text/plain",
        )

        self.assertEqual(document["storage_bucket"], "IV")
        self.assertTrue(document["storage_key"].startswith("Documents/default/2026/05/"))
        self.assertTrue(document["storage_key"].endswith("-Rechnung_Mai_2026.txt"))
        self.assertIn(("IV", document["storage_key"]), client.objects)
        self.assertEqual(document["document_type"], "invoice")
        self.assertEqual(document["institution"], "IV-Stelle")
        self.assertEqual(document["metadata"]["invoice_fields"]["total"], 50.0)
        self.assertEqual(document["metadata"]["classification"]["facts"]["amount"], "CHF 50.00")
        self.assertEqual(document["suggested_bucket"], "IV")
        self.assertFalse(document["bucket_confirmed"])
        self.assertTrue(any("INSERT INTO documents" in query for query, _ in cursor.statements))

    def test_storage_service_routes_transport_receipt_to_tixitaxi_bucket(self):
        cursor = RecordingCursor()
        client = FakeSupabaseClient()
        service = StorageService(
            "postgres://example",
            client=client,
            bucket="IV",
            connection_factory=lambda: RecordingConnection(cursor),
        )

        document = service.upload_document(
            user_id="default",
            file_name="taxi_quittung.txt",
            content="Taxi Tixi Fahrt zur Therapie CHF 45".encode("utf-8"),
            content_type="text/plain",
            metadata={"tags": ["transport"]},
        )

        self.assertEqual(document["storage_bucket"], "TixiTaxi")
        self.assertIn(("TixiTaxi", document["storage_key"]), client.objects)
        self.assertIn("Transport", document["bucket_reason"])

    def test_storage_service_honors_explicit_camera_insurance_bucket_alias(self):
        cursor = RecordingCursor()
        client = FakeSupabaseClient()
        service = StorageService(
            "postgres://example",
            client=client,
            bucket="IV",
            connection_factory=lambda: RecordingConnection(cursor),
        )

        document = service.upload_document(
            user_id="default",
            file_name="phone.jpg",
            content=b"\xff\xd8\xff",
            content_type="image/jpeg",
            metadata={"source": "camera_capture", "storage_bucket": "Versicherungen"},
        )

        self.assertEqual(document["storage_bucket"], "Versicherung")
        self.assertEqual(document["suggested_bucket"], "Versicherung")
        self.assertEqual(document["bucket_confidence"], "high")
        self.assertIn(("Versicherung", document["storage_key"]), client.objects)

    def test_storage_service_honors_plural_configured_insurance_bucket(self):
        cursor = RecordingCursor()
        client = FakeSupabaseClient()
        with patch.dict(os.environ, {"IV_AGENT_DOCUMENT_BUCKETS": "Stiftung,TixiTaxi,IV,Versicherungen"}, clear=False):
            service = StorageService(
                "postgres://example",
                client=client,
                bucket="IV",
                connection_factory=lambda: RecordingConnection(cursor),
            )

            document = service.upload_document(
                user_id="default",
                file_name="versicherung.jpg",
                content=b"\xff\xd8\xff",
                content_type="image/jpeg",
                metadata={"source": "camera_capture", "storage_bucket": "Versicherung"},
            )

        self.assertEqual(document["storage_bucket"], "Versicherungen")
        self.assertIn(("Versicherungen", document["storage_key"]), client.objects)

    def test_storage_service_list_documents_scopes_and_filters_queries(self):
        cursor = RecordingCursor(fetchall_results=[[]])
        service = StorageService(
            "postgres://example",
            client=FakeSupabaseClient(),
            bucket="IV",
            connection_factory=lambda: RecordingConnection(cursor),
        )

        documents = service.list_documents(
            user_id="profile_a",
            year=2026,
            month=5,
            document_type="invoice",
            institution="IV",
            tags=["rechnung"],
            start_date="2026-05-01",
            end_date="2026-05-31",
            folder_id=None,
            storage_bucket="IV",
        )

        self.assertEqual(documents, [])
        query, params = cursor.statements[-1]
        self.assertIn("FROM documents", query)
        self.assertIn("user_id = %s", query)
        self.assertIn("COALESCE(EXTRACT(YEAR FROM document_date)::int, year) = %s", query)
        self.assertIn("COALESCE(EXTRACT(MONTH FROM document_date)::int, month) = %s", query)
        self.assertIn("COALESCE(document_date, created_at::date) >= %s::date", query)
        self.assertIn("COALESCE(document_date, created_at::date) <= %s::date", query)
        self.assertIn("tags && %s::text[]", query)
        self.assertIn("storage_bucket = %s", query)
        self.assertEqual(params[0], "profile_a")
        self.assertEqual(params[1], "IV")
        self.assertEqual(params[2], 2026)
        self.assertEqual(params[3], 5)

    def test_chat_document_artifact_contains_stable_download_url(self):
        artifact = build_chat_document_artifact(
            {
                "document_id": "doc-1",
                "user_id": "profile_a",
                "file_name": "rechnung.txt",
                "content_type": "text/plain",
                "content_size": 12,
                "document_type": "invoice",
                "institution": "IV",
                "document_date": "2026-05-01",
                "storage_bucket": "IV",
                "summary": "Betrag CHF 42.50",
            }
        )

        self.assertEqual(artifact["type"], "document")
        self.assertEqual(artifact["document_id"], "doc-1")
        self.assertEqual(artifact["download_url"], "/api/documents/doc-1/file?profile_id=profile_a&download=1")
        self.assertNotIn("signed_url", artifact)

    def test_sum_invoice_amounts_filters_bucket_and_ignores_exact_duplicates(self):
        first = {
            "document_id": "doc-1",
            "user_id": "default",
            "folder_id": None,
            "file_name": "tixi_1.txt",
            "safe_file_name": "tixi_1.txt",
            "storage_bucket": "TixiTaxi",
            "storage_key": "Documents/default/2026/05/doc-1-tixi_1.txt",
            "storage_url": "supabase://TixiTaxi/Documents/default/2026/05/doc-1-tixi_1.txt",
            "content_type": "text/plain",
            "content_size": 10,
            "checksum_sha256": "same",
            "document_type": "invoice",
            "institution": "Tixi",
            "document_date": "2026-05-01",
            "year": 2026,
            "month": 5,
            "tags": ["transport"],
            "summary": "Tixi Rechnung",
            "extracted_text": "Tixi Rechnung CHF 10",
            "extraction_status": "completed",
            "extraction_error": None,
            "metadata": {"invoice_fields": {"total": 10, "currency": "CHF"}},
            "created_at": "2026-05-01T10:00:00+00:00",
            "updated_at": "2026-05-01T10:00:00+00:00",
        }
        second = {**first, "document_id": "doc-2"}
        missing = {
            **first,
            "document_id": "doc-3",
            "checksum_sha256": "missing",
            "metadata": {},
            "summary": "Tixi Rechnung ohne Betrag",
            "extracted_text": "Tixi Rechnung Betrag CHF 02.05.2026",
        }
        labeled = {
            **first,
            "document_id": "doc-4",
            "checksum_sha256": "labeled",
            "metadata": {},
            "summary": "IV Rechnung",
            "extracted_text": "Gesamtbetrag Transportkosten: CHF 289.90",
        }
        cursor = RecordingCursor(fetchall_results=[[first, second, missing, labeled]])
        service = StorageService(
            "postgres://example",
            client=FakeSupabaseClient(),
            bucket="IV",
            connection_factory=lambda: RecordingConnection(cursor),
        )

        result = service.sum_invoice_amounts(user_id="default", query="TixiTaxi Rechnungen")

        self.assertEqual(result["storage_bucket"], "TixiTaxi")
        self.assertEqual(result["query_filter"], "")
        self.assertEqual(result["matched_document_count"], 4)
        self.assertEqual(result["counted_document_count"], 2)
        self.assertEqual(result["ignored_duplicate_count"], 1)
        self.assertEqual(result["missing_amount_count"], 1)
        self.assertEqual(result["total_amount_chf"], 299.9)
        query, params = cursor.statements[-1]
        self.assertIn("storage_bucket = %s", query)
        self.assertIn("lower(document_type) = lower(%s)", query)
        self.assertEqual(params[1], "TixiTaxi")

    def test_backfill_invoice_amount_metadata_dry_run_reports_metadata_changes(self):
        document = {
            "document_id": "doc-1",
            "user_id": "default",
            "folder_id": None,
            "file_name": "iv_rechnung.txt",
            "safe_file_name": "iv_rechnung.txt",
            "storage_bucket": "IV",
            "storage_key": "Documents/default/2026/05/doc-1-iv_rechnung.txt",
            "storage_url": "supabase://IV/Documents/default/2026/05/doc-1-iv_rechnung.txt",
            "content_type": "text/plain",
            "content_size": 10,
            "checksum_sha256": "checksum",
            "document_type": "invoice",
            "institution": "IV-Stelle",
            "document_date": "2026-05-06",
            "year": 2026,
            "month": 5,
            "tags": ["rechnung"],
            "summary": "IV Rechnung",
            "extracted_text": "Gesamtbetrag Transportkosten: CHF 289.90",
            "extraction_status": "completed",
            "extraction_error": None,
            "metadata": {"classification": {"facts": {"amount": "CHF 06.05.2026"}}},
            "created_at": "2026-05-06T10:00:00+00:00",
            "updated_at": "2026-05-06T10:00:00+00:00",
        }
        cursor = RecordingCursor(fetchall_results=[[document]])
        service = StorageService(
            "postgres://example",
            client=FakeSupabaseClient(),
            bucket="IV",
            connection_factory=lambda: RecordingConnection(cursor),
        )

        result = service.backfill_invoice_amount_metadata(user_id="default", year=2026, month=5, dry_run=True)

        self.assertTrue(result["dry_run"])
        self.assertEqual(result["changed_count"], 1)
        self.assertEqual(result["changed_documents"][0]["amount"], 289.9)
        self.assertFalse(any("UPDATE documents" in query for query, _ in cursor.statements))

    def test_process_chat_attachments_uploads_and_removes_base64(self):
        uploaded = {
            "document_id": "doc-1",
            "file_name": "brief.txt",
            "safe_file_name": "brief.txt",
            "storage_bucket": "IV",
            "storage_key": "Documents/default/2026/05/doc-1-brief.txt",
            "content_type": "text/plain",
            "content_size": 5,
            "document_type": "letter",
            "institution": "",
            "document_date": None,
            "tags": [],
            "summary": "Hallo",
            "extracted_text": "Hallo",
            "extraction_status": "completed",
            "extraction_error": "",
            "suggested_bucket": "IV",
            "bucket_reason": "Keine starke Zuordnung gefunden; Standard-Bucket IV verwendet.",
            "bucket_confidence": "low",
            "bucket_confirmed": False,
        }

        class FakeDocumentService:
            def upload_document(self, **kwargs):
                self.kwargs = kwargs
                return uploaded

        fake_service = FakeDocumentService()
        with patch("iv_agent.services.storage_service.get_storage_service", return_value=fake_service):
            sanitized, documents = process_chat_attachments(
                [
                    {
                        "file_name": "brief.txt",
                        "mime": "text/plain",
                        "content_base64": "SGFsbG8=",
                    }
                ],
                user_id="default",
            )

        self.assertEqual(documents, [uploaded])
        self.assertEqual(fake_service.kwargs["content"], b"Hallo")
        self.assertNotIn("content_base64", sanitized[0])
        self.assertEqual(sanitized[0]["document_id"], "doc-1")
        self.assertEqual(sanitized[0]["storage_bucket"], "IV")

    def test_storage_service_reassigns_document_across_buckets(self):
        existing_row = {
            "document_id": "doc-1",
            "user_id": "default",
            "folder_id": None,
            "file_name": "receipt.txt",
            "safe_file_name": "receipt.txt",
            "storage_bucket": "IV",
            "storage_key": "Documents/default/2026/05/doc-1-receipt.txt",
            "storage_url": "supabase://IV/Documents/default/2026/05/doc-1-receipt.txt",
            "content_type": "text/plain",
            "content_size": 16,
            "checksum_sha256": "abc",
            "document_type": "receipt",
            "institution": "",
            "document_date": "2026-05-01",
            "year": 2026,
            "month": 5,
            "tags": ["transport"],
            "summary": "Taxi receipt",
            "extracted_text": "Taxi Tixi Fahrt",
            "extraction_status": "completed",
            "extraction_error": None,
            "metadata": {
                "source": "chat_attachment",
                "suggested_bucket": "IV",
                "bucket_reason": "Initial suggestion",
                "bucket_confidence": "low",
                "bucket_confirmed": False,
            },
            "created_at": "2026-05-01T10:00:00+00:00",
            "updated_at": "2026-05-01T10:00:00+00:00",
        }
        updated_row = {
            **existing_row,
            "storage_bucket": "TixiTaxi",
            "storage_url": "supabase://TixiTaxi/Documents/default/2026/05/doc-1-receipt.txt",
            "metadata": {
                "source": "chat_attachment",
                "suggested_bucket": "TixiTaxi",
                "bucket_reason": "Dokument von IV nach TixiTaxi verschoben.",
                "bucket_confidence": "high",
                "bucket_confirmed": True,
                "bucket_confirmed_at": "2026-05-01T10:05:00+00:00",
                "bucket_review_source": "user",
            },
        }
        cursor = RecordingCursor(fetchone_results=[existing_row, existing_row, updated_row])
        client = FakeSupabaseClient()
        client.objects[("IV", existing_row["storage_key"])] = {"content": b"Taxi Tixi Fahrt", "options": {}}
        service = StorageService(
            "postgres://example",
            client=client,
            bucket="IV",
            connection_factory=lambda: RecordingConnection(cursor),
        )

        document = service.reassign_document_bucket(
            user_id="default",
            document_id="doc-1",
            bucket_name="TixiTaxi",
        )

        self.assertEqual(document["storage_bucket"], "TixiTaxi")
        self.assertTrue(document["bucket_confirmed"])
        self.assertIn(("TixiTaxi", existing_row["storage_key"]), client.objects)
        self.assertNotIn(("IV", existing_row["storage_key"]), client.objects)

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
        self.assertTrue(any("WHERE event_date >= %s::date AND event_date < %s::date" in query for query, _ in cursor.statements))

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
        with patch.dict(os.environ, {}, clear=True):
            store = SupabaseStorageTemplateStore(client=client)

        saved = store.upsert_template(
            template_key="transportkosten",
            file_name="AK_Formular_EL_Transportkosten.pdf",
            content=b"%PDF-transport\n",
        )
        template = store.get_template("transportkosten")
        content, content_type = store.read_template_bytes("transportkosten")

        self.assertEqual(saved["storage_backend"], "supabase")
        self.assertEqual(saved["storage_key"], "AK_Formular_EL_Transportkosten.pdf")
        self.assertEqual(template["storage_key"], "AK_Formular_EL_Transportkosten.pdf")
        self.assertEqual(content, b"%PDF-transport\n")
        self.assertEqual(content_type, "application/pdf")
        self.assertEqual(
            client.objects[("Report_template", "AK_Formular_EL_Transportkosten.pdf")]["options"]["upsert"],
            "true",
        )

    def test_supabase_template_store_reads_exact_filename_with_parentheses(self):
        client = FakeSupabaseClient()
        client.objects[("Report_template", "Rechnungsvorlage_aL_elektronisch (1).pdf")] = {
            "content": b"%PDF-exact\n",
            "options": {},
        }
        store = SupabaseStorageTemplateStore(client=client)

        template = store.get_template("rechnung")
        content, content_type = store.read_template_bytes("rechnung")

        self.assertEqual(template["storage_key"], "Rechnungsvorlage_aL_elektronisch (1).pdf")
        self.assertEqual(content, b"%PDF-exact\n")
        self.assertEqual(content_type, "application/pdf")

    def test_supabase_template_store_reads_legacy_sanitized_alias(self):
        client = FakeSupabaseClient()
        client.objects[("Report_template", "Rechnungsvorlage_aL_elektronisch_1_.pdf")] = {
            "content": b"%PDF-alias\n",
            "options": {},
        }
        store = SupabaseStorageTemplateStore(client=client)

        content, content_type = store.read_template_bytes("rechnung")

        self.assertEqual(content, b"%PDF-alias\n")
        self.assertEqual(content_type, "application/pdf")

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
                        "storage_bucket": "IV",
                        "storage_url": "supabase://IV/Invoices/session123/inv-1_receipt.jpg",
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
            bucket="IV",
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
        self.assertEqual(saved["storage_bucket"], "IV")
        self.assertTrue(saved["storage_key"].startswith("Invoices/session123/"))
        self.assertEqual(listed[0]["storage_backend"], "supabase")
        self.assertEqual(listed[0]["storage_bucket"], "IV")
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
            bucket="IV",
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

    def test_backend_health_status_checks_tables_and_buckets_without_secrets(self):
        cursor = RecordingCursor(
            fetchall_results=[
                [
                    {"table_name": "calendar_events"},
                    {"table_name": "documents"},
                    {"table_name": "document_folders"},
                    {"table_name": "document_matches"},
                ]
            ]
        )
        client = FakeSupabaseClient()
        for bucket in ("Report_template", "reports_generated", "Stiftung", "TixiTaxi", "IV", "Versicherung"):
            client.objects[(bucket, ".keep")] = {"content": b"", "options": {}}

        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgres://example",
                "SUPABASE_URL": "https://project.supabase.co",
                "SUPABASE_SERVICE_ROLE_KEY": "service-role-key",
                "SUPABASE_STORAGE_TEMPLATES_BUCKET": "Report_template",
                "SUPABASE_STORAGE_REPORTS_BUCKET": "reports_generated",
            },
            clear=False,
        ), patch("iv_agent.storage._connect_postgres", return_value=RecordingConnection(cursor)), patch(
            "iv_agent.storage._create_supabase_client", return_value=client
        ):
            status = backend_health_status(document_buckets=["Stiftung", "TixiTaxi", "IV", "Versicherung"])

        self.assertTrue(status["ok"])
        self.assertTrue(all(check["ok"] for check in status["checks"]))
        self.assertIn("Report_template", status["storage"]["required_buckets"])
        self.assertNotIn("service-role-key", str(status))

if __name__ == "__main__":
    unittest.main()
