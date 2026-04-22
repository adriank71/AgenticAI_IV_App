import json
import mimetypes
import os
import re
import tempfile
import urllib.parse
import urllib.request
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Iterator, Protocol


DEFAULT_PROFILE_ID = "default"


class ProfileStore(Protocol):
    def get_profile(self, profile_id: str | None = None) -> dict[str, Any] | None:
        ...

    def upsert_profile(self, profile_id: str, payload: dict[str, Any]) -> None:
        ...


class ReportStore(Protocol):
    def save_report(
        self,
        *,
        month: str,
        report_type: str,
        file_name: str,
        content: bytes,
        profile_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        content_type: str = "application/pdf",
    ) -> dict[str, Any]:
        ...

    def get_report(
        self,
        *,
        report_id: str | None = None,
        file_name: str | None = None,
        month: str | None = None,
    ) -> dict[str, Any] | None:
        ...

    def read_report_bytes(self, report: dict[str, Any]) -> tuple[bytes, str]:
        ...


class InvoiceCaptureStore(Protocol):
    def save_capture(
        self,
        *,
        sid: str,
        file_name: str,
        content: bytes,
        content_type: str,
        fields: dict[str, Any] | None = None,
        extraction_error: str | None = None,
    ) -> dict[str, Any]:
        ...

    def list_captures(self, sid: str) -> list[dict[str, Any]]:
        ...

    def get_capture(self, *, sid: str, invoice_id: str) -> dict[str, Any] | None:
        ...

    def read_capture_bytes(self, capture: dict[str, Any]) -> tuple[bytes, str]:
        ...


class AssetStore(Protocol):
    @property
    def backend_name(self) -> str:
        ...

    def store_report(
        self,
        *,
        month: str,
        report_id: str,
        file_name: str,
        content: bytes,
        content_type: str,
    ) -> dict[str, Any]:
        ...

    def read_bytes(self, *, storage_key: str, storage_url: str | None = None) -> tuple[bytes, str]:
        ...


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_profile_id(profile_id: str | None) -> str:
    candidate = str(profile_id or "").strip()
    if not candidate:
        return DEFAULT_PROFILE_ID

    safe_profile_id = "".join(ch for ch in candidate if ch.isalnum() or ch in ("-", "_"))
    if safe_profile_id != candidate:
        raise ValueError("Invalid profile_id")
    return safe_profile_id


def sanitize_invoice_sid(sid: str | None) -> str:
    candidate = str(sid or "").strip()
    if not candidate:
        raise ValueError("Invalid invoice session id")

    safe_sid = "".join(ch for ch in candidate if ch.isalnum() or ch in ("-", "_"))
    if safe_sid != candidate:
        raise ValueError("Invalid invoice session id")
    return safe_sid


def resolve_profile_file_path(default_profile_path: str, profile_dir: str, profile_id: str | None = None) -> str:
    normalized_profile_id = sanitize_profile_id(profile_id)
    if normalized_profile_id == DEFAULT_PROFILE_ID:
        return default_profile_path
    return os.path.join(profile_dir, f"{normalized_profile_id}.json")


def _normalize_storage_backend(value: str | None) -> str:
    normalized = str(value or "auto").strip().lower()
    if normalized not in {"auto", "local", "postgres"}:
        return "auto"
    return normalized


def _database_backend_enabled() -> bool:
    backend = _normalize_storage_backend(os.environ.get("IV_AGENT_STORAGE_BACKEND"))
    if backend == "local":
        return False
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        return False
    return True


def _report_asset_backend() -> str:
    normalized = str(os.environ.get("IV_AGENT_REPORT_ASSET_BACKEND", "auto") or "auto").strip().lower()
    if normalized == "local":
        return "local"
    if normalized == "blob":
        return "blob"
    return "blob" if os.environ.get("BLOB_READ_WRITE_TOKEN", "").strip() else "local"


def _invoice_asset_backend() -> str:
    normalized = str(os.environ.get("IV_AGENT_INVOICE_ASSET_BACKEND", "auto") or "auto").strip().lower()
    if normalized == "local":
        return "local"
    if normalized == "blob":
        return "blob"
    return _report_asset_backend()


def _load_psycopg():
    try:
        import psycopg  # type: ignore
        from psycopg.rows import dict_row  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "Postgres storage requires psycopg. Install dependencies from requirements.txt."
        ) from exc
    return psycopg, dict_row


def _connect_postgres(database_url: str):
    psycopg, dict_row = _load_psycopg()
    return psycopg.connect(database_url, row_factory=dict_row)


def _load_vercel_blob():
    try:
        from vercel.blob import BlobClient, head as blob_head  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "Blob storage requires the vercel Python package. Install dependencies from requirements.txt."
        ) from exc
    return BlobClient, blob_head


def _is_url(reference: str) -> bool:
    parsed = urllib.parse.urlparse(str(reference or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _guess_content_type(reference: str, fallback: str = "application/octet-stream") -> str:
    content_type, _ = mimetypes.guess_type(reference)
    return content_type or fallback


def _blob_token() -> str:
    token = os.environ.get("BLOB_READ_WRITE_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BLOB_READ_WRITE_TOKEN is required for Blob storage.")
    return token


def _download_url_bytes(url: str, *, auth_token: str | None = None) -> tuple[bytes, str]:
    headers = {}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=30) as response:
        content_type = response.headers.get_content_type()
        return response.read(), content_type or "application/octet-stream"


def read_binary_reference(reference: str) -> tuple[bytes, str]:
    normalized_reference = str(reference or "").strip()
    if not normalized_reference:
        raise FileNotFoundError("Binary reference is not configured.")

    if os.path.exists(normalized_reference):
        with open(normalized_reference, "rb") as file:
            return file.read(), _guess_content_type(normalized_reference)

    if _is_url(normalized_reference):
        auth_token = None
        if ".blob.vercel-storage.com" in urllib.parse.urlparse(normalized_reference).netloc:
            auth_token = _blob_token()
        return _download_url_bytes(normalized_reference, auth_token=auth_token)

    _, blob_head = _load_vercel_blob()
    blob_details = blob_head(normalized_reference, token=_blob_token())
    return _download_url_bytes(blob_details.url, auth_token=_blob_token())


@contextmanager
def materialize_binary_reference(reference: str, *, suffix: str = ".pdf") -> Iterator[str]:
    normalized_reference = str(reference or "").strip()
    if os.path.exists(normalized_reference):
        yield normalized_reference
        return

    data, _ = read_binary_reference(normalized_reference)
    fd, temp_path = tempfile.mkstemp(suffix=suffix)
    try:
        with os.fdopen(fd, "wb") as temp_file:
            temp_file.write(data)
        yield temp_path
    finally:
        try:
            os.remove(temp_path)
        except FileNotFoundError:
            pass


def _sanitize_storage_name(file_name: str) -> str:
    base_name = os.path.basename(str(file_name or "").strip())
    safe_name = re.sub(r"[^0-9A-Za-z._-]+", "_", base_name)
    return safe_name or "report.pdf"


class LocalFileAssetStore:
    def __init__(self, output_dir: str):
        self._output_dir = output_dir

    @property
    def backend_name(self) -> str:
        return "local"

    def store_report(
        self,
        *,
        month: str,
        report_id: str,
        file_name: str,
        content: bytes,
        content_type: str,
    ) -> dict[str, Any]:
        os.makedirs(self._output_dir, exist_ok=True)
        safe_file_name = _sanitize_storage_name(file_name)
        storage_path = os.path.join(self._output_dir, f"{report_id}_{safe_file_name}")
        with open(storage_path, "wb") as file:
            file.write(content)
        return {
            "storage_key": storage_path,
            "storage_url": None,
            "storage_download_url": None,
            "content_type": content_type,
            "content_size": len(content),
        }

    def read_bytes(self, *, storage_key: str, storage_url: str | None = None) -> tuple[bytes, str]:
        with open(storage_key, "rb") as file:
            return file.read(), _guess_content_type(storage_key, fallback="application/pdf")


class VercelBlobAssetStore:
    def __init__(self, token: str | None = None):
        self._token = str(token or os.environ.get("BLOB_READ_WRITE_TOKEN", "")).strip()
        if not self._token:
            raise RuntimeError("BLOB_READ_WRITE_TOKEN is required for Blob storage.")

    @property
    def backend_name(self) -> str:
        return "blob"

    def store_report(
        self,
        *,
        month: str,
        report_id: str,
        file_name: str,
        content: bytes,
        content_type: str,
    ) -> dict[str, Any]:
        BlobClient, _ = _load_vercel_blob()
        client = BlobClient(token=self._token)
        safe_file_name = _sanitize_storage_name(file_name)
        report_prefix = str(os.environ.get("IV_AGENT_REPORTS_BLOB_PREFIX", "reports") or "reports").strip("/")
        pathname = "/".join(part for part in (report_prefix, month, f"{report_id}_{safe_file_name}") if part)
        uploaded_blob = client.put(
            pathname,
            content,
            access="private",
            content_type=content_type,
            overwrite=True,
        )
        return {
            "storage_key": uploaded_blob.pathname,
            "storage_url": uploaded_blob.url,
            "storage_download_url": uploaded_blob.download_url,
            "content_type": uploaded_blob.content_type or content_type,
            "content_size": len(content),
        }

    def read_bytes(self, *, storage_key: str, storage_url: str | None = None) -> tuple[bytes, str]:
        resolved_url = storage_url
        if not resolved_url:
            _, blob_head = _load_vercel_blob()
            blob_details = blob_head(storage_key, token=self._token)
            resolved_url = blob_details.url
        return _download_url_bytes(resolved_url, auth_token=self._token)


class LocalProfileStore:
    def __init__(self, default_profile_path: str, profile_dir: str):
        self._default_profile_path = default_profile_path
        self._profile_dir = profile_dir

    def get_profile(self, profile_id: str | None = None) -> dict[str, Any] | None:
        profile_path = resolve_profile_file_path(self._default_profile_path, self._profile_dir, profile_id)
        if not os.path.exists(profile_path):
            return None
        with open(profile_path, "r", encoding="utf-8") as file:
            return json.load(file)

    def upsert_profile(self, profile_id: str, payload: dict[str, Any]) -> None:
        target_profile_id = sanitize_profile_id(profile_id)
        profile_path = resolve_profile_file_path(self._default_profile_path, self._profile_dir, target_profile_id)
        os.makedirs(os.path.dirname(profile_path), exist_ok=True)
        with open(profile_path, "w", encoding="utf-8") as file:
            json.dump(payload, file, indent=2, ensure_ascii=False)

    def iter_profiles(self) -> list[tuple[str, dict[str, Any]]]:
        profiles: list[tuple[str, dict[str, Any]]] = []

        default_profile = self.get_profile(DEFAULT_PROFILE_ID)
        if default_profile is not None:
            profiles.append((DEFAULT_PROFILE_ID, default_profile))

        if not os.path.isdir(self._profile_dir):
            return profiles

        for file_name in sorted(os.listdir(self._profile_dir)):
            if not file_name.endswith(".json"):
                continue
            profile_id = file_name[:-5]
            if profile_id == DEFAULT_PROFILE_ID:
                continue
            profile_payload = self.get_profile(profile_id)
            if profile_payload is not None:
                profiles.append((profile_id, profile_payload))

        return profiles


class PostgresProfileStore:
    def __init__(
        self,
        database_url: str,
        connection_factory: Callable[[], Any] | None = None,
    ):
        self._database_url = database_url
        self._connection_factory = connection_factory or (lambda: _connect_postgres(database_url))
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS profiles (
                        profile_id TEXT PRIMARY KEY,
                        payload JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )

    def get_profile(self, profile_id: str | None = None) -> dict[str, Any] | None:
        target_profile_id = sanitize_profile_id(profile_id)
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT payload FROM profiles WHERE profile_id = %s",
                    (target_profile_id,),
                )
                row = cursor.fetchone()
        if not row:
            return None
        payload = row["payload"]
        if isinstance(payload, str):
            return json.loads(payload)
        return payload

    def upsert_profile(self, profile_id: str, payload: dict[str, Any]) -> None:
        target_profile_id = sanitize_profile_id(profile_id)
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO profiles (profile_id, payload, created_at, updated_at)
                    VALUES (%s, %s::jsonb, NOW(), NOW())
                    ON CONFLICT (profile_id)
                    DO UPDATE SET
                        payload = EXCLUDED.payload,
                        updated_at = NOW()
                    """,
                    (target_profile_id, json.dumps(payload)),
                )


class JsonReportStore:
    def __init__(self, output_dir: str, asset_store: AssetStore | None = None):
        self._output_dir = output_dir
        self._metadata_path = os.path.join(output_dir, "reports.json")
        self._asset_store = asset_store or LocalFileAssetStore(output_dir)

    def _load_records(self) -> list[dict[str, Any]]:
        if not os.path.exists(self._metadata_path):
            return []
        with open(self._metadata_path, "r", encoding="utf-8") as file:
            return json.load(file)

    def _save_records(self, records: list[dict[str, Any]]) -> None:
        os.makedirs(self._output_dir, exist_ok=True)
        with open(self._metadata_path, "w", encoding="utf-8") as file:
            json.dump(records, file, indent=2, ensure_ascii=False)

    def save_report(
        self,
        *,
        month: str,
        report_type: str,
        file_name: str,
        content: bytes,
        profile_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        content_type: str = "application/pdf",
    ) -> dict[str, Any]:
        report_id = str(uuid.uuid4())
        asset_record = self._asset_store.store_report(
            month=month,
            report_id=report_id,
            file_name=file_name,
            content=content,
            content_type=content_type,
        )
        report_record = {
            "report_id": report_id,
            "month": month,
            "type": report_type,
            "profile_id": sanitize_profile_id(profile_id) if profile_id else DEFAULT_PROFILE_ID,
            "file_name": file_name,
            "storage_backend": self._asset_store.backend_name,
            "storage_key": asset_record["storage_key"],
            "storage_url": asset_record.get("storage_url"),
            "storage_download_url": asset_record.get("storage_download_url"),
            "content_type": asset_record.get("content_type", content_type),
            "content_size": asset_record.get("content_size", len(content)),
            "metadata": metadata or {},
            "created_at": utcnow_iso(),
            "updated_at": utcnow_iso(),
        }
        records = self._load_records()
        records.append(report_record)
        self._save_records(records)
        return report_record

    def get_report(
        self,
        *,
        report_id: str | None = None,
        file_name: str | None = None,
        month: str | None = None,
    ) -> dict[str, Any] | None:
        records = self._load_records()

        if report_id:
            return next((record for record in records if record["report_id"] == report_id), None)

        if file_name:
            candidates = [record for record in records if record["file_name"] == file_name]
            if month:
                candidates = [record for record in candidates if record["month"] == month]
            candidates.sort(key=lambda record: record.get("created_at", ""), reverse=True)
            return candidates[0] if candidates else None

        return None

    def read_report_bytes(self, report: dict[str, Any]) -> tuple[bytes, str]:
        return self._asset_store.read_bytes(
            storage_key=report["storage_key"],
            storage_url=report.get("storage_url"),
        )


class PostgresReportStore:
    def __init__(
        self,
        database_url: str,
        asset_store: AssetStore,
        connection_factory: Callable[[], Any] | None = None,
    ):
        self._database_url = database_url
        self._asset_store = asset_store
        self._connection_factory = connection_factory or (lambda: _connect_postgres(database_url))
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS reports (
                        report_id TEXT PRIMARY KEY,
                        month TEXT NOT NULL,
                        report_type TEXT NOT NULL,
                        profile_id TEXT,
                        file_name TEXT NOT NULL,
                        storage_backend TEXT NOT NULL,
                        storage_key TEXT NOT NULL,
                        storage_url TEXT,
                        storage_download_url TEXT,
                        content_type TEXT NOT NULL,
                        content_size BIGINT NOT NULL DEFAULT 0,
                        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS reports_month_idx ON reports (month, created_at DESC)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS reports_filename_idx ON reports (file_name, created_at DESC)"
                )

    def save_report(
        self,
        *,
        month: str,
        report_type: str,
        file_name: str,
        content: bytes,
        profile_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        content_type: str = "application/pdf",
    ) -> dict[str, Any]:
        report_id = str(uuid.uuid4())
        asset_record = self._asset_store.store_report(
            month=month,
            report_id=report_id,
            file_name=file_name,
            content=content,
            content_type=content_type,
        )

        report_record = {
            "report_id": report_id,
            "month": month,
            "type": report_type,
            "profile_id": sanitize_profile_id(profile_id) if profile_id else DEFAULT_PROFILE_ID,
            "file_name": file_name,
            "storage_backend": self._asset_store.backend_name,
            "storage_key": asset_record["storage_key"],
            "storage_url": asset_record.get("storage_url"),
            "storage_download_url": asset_record.get("storage_download_url"),
            "content_type": asset_record.get("content_type", content_type),
            "content_size": asset_record.get("content_size", len(content)),
            "metadata": metadata or {},
        }

        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO reports (
                        report_id,
                        month,
                        report_type,
                        profile_id,
                        file_name,
                        storage_backend,
                        storage_key,
                        storage_url,
                        storage_download_url,
                        content_type,
                        content_size,
                        metadata,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW())
                    """,
                    (
                        report_record["report_id"],
                        report_record["month"],
                        report_record["type"],
                        report_record["profile_id"],
                        report_record["file_name"],
                        report_record["storage_backend"],
                        report_record["storage_key"],
                        report_record["storage_url"],
                        report_record["storage_download_url"],
                        report_record["content_type"],
                        report_record["content_size"],
                        json.dumps(report_record["metadata"]),
                    ),
                )

        report_record["created_at"] = utcnow_iso()
        report_record["updated_at"] = report_record["created_at"]
        return report_record

    def get_report(
        self,
        *,
        report_id: str | None = None,
        file_name: str | None = None,
        month: str | None = None,
    ) -> dict[str, Any] | None:
        query = None
        params: tuple[Any, ...] = ()

        if report_id:
            query = "SELECT * FROM reports WHERE report_id = %s LIMIT 1"
            params = (report_id,)
        elif file_name and month:
            query = (
                "SELECT * FROM reports WHERE file_name = %s AND month = %s "
                "ORDER BY created_at DESC LIMIT 1"
            )
            params = (file_name, month)
        elif file_name:
            query = "SELECT * FROM reports WHERE file_name = %s ORDER BY created_at DESC LIMIT 1"
            params = (file_name,)
        else:
            return None

        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                row = cursor.fetchone()

        if not row:
            return None

        metadata = row.get("metadata")
        if isinstance(metadata, str):
            metadata = json.loads(metadata)

        return {
            "report_id": row["report_id"],
            "month": row["month"],
            "type": row["report_type"],
            "profile_id": row.get("profile_id"),
            "file_name": row["file_name"],
            "storage_backend": row["storage_backend"],
            "storage_key": row["storage_key"],
            "storage_url": row.get("storage_url"),
            "storage_download_url": row.get("storage_download_url"),
            "content_type": row.get("content_type") or "application/pdf",
            "content_size": int(row.get("content_size") or 0),
            "metadata": metadata or {},
            "created_at": (
                row["created_at"].isoformat()
                if hasattr(row.get("created_at"), "isoformat")
                else str(row.get("created_at") or "")
            ),
            "updated_at": (
                row["updated_at"].isoformat()
                if hasattr(row.get("updated_at"), "isoformat")
                else str(row.get("updated_at") or "")
            ),
        }

    def read_report_bytes(self, report: dict[str, Any]) -> tuple[bytes, str]:
        return self._asset_store.read_bytes(
            storage_key=report["storage_key"],
            storage_url=report.get("storage_url"),
        )


class LocalInvoiceCaptureStore:
    def __init__(self, output_dir: str):
        self._root_dir = os.path.join(output_dir, "Invoices")

    def _session_dir(self, sid: str) -> str:
        return os.path.join(self._root_dir, sanitize_invoice_sid(sid))

    def _metadata_path(self, sid: str, invoice_id: str) -> str:
        return os.path.join(self._session_dir(sid), f"{invoice_id}.json")

    def save_capture(
        self,
        *,
        sid: str,
        file_name: str,
        content: bytes,
        content_type: str,
        fields: dict[str, Any] | None = None,
        extraction_error: str | None = None,
    ) -> dict[str, Any]:
        safe_sid = sanitize_invoice_sid(sid)
        safe_file_name = _sanitize_storage_name(file_name)
        invoice_id = str(uuid.uuid4())
        session_dir = self._session_dir(safe_sid)
        os.makedirs(session_dir, exist_ok=True)

        storage_path = os.path.join(session_dir, f"{invoice_id}_{safe_file_name}")
        with open(storage_path, "wb") as file:
            file.write(content)

        record = {
            "invoice_id": invoice_id,
            "sid": safe_sid,
            "file_name": safe_file_name,
            "storage_backend": "local",
            "storage_key": storage_path,
            "storage_url": None,
            "content_type": content_type,
            "content_size": len(content),
            "fields": fields or None,
            "extraction_error": extraction_error or None,
            "folder_path": f"Invoices/{safe_sid}",
            "created_at": utcnow_iso(),
            "updated_at": utcnow_iso(),
        }

        with open(self._metadata_path(safe_sid, invoice_id), "w", encoding="utf-8") as file:
            json.dump(record, file, indent=2, ensure_ascii=False)

        return record

    def list_captures(self, sid: str) -> list[dict[str, Any]]:
        session_dir = self._session_dir(sid)
        if not os.path.isdir(session_dir):
            return []

        captures: list[dict[str, Any]] = []
        for file_name in os.listdir(session_dir):
            if not file_name.endswith(".json"):
                continue
            metadata_path = os.path.join(session_dir, file_name)
            with open(metadata_path, "r", encoding="utf-8") as file:
                captures.append(json.load(file))

        captures.sort(key=lambda capture: str(capture.get("created_at", "")), reverse=True)
        return captures

    def get_capture(self, *, sid: str, invoice_id: str) -> dict[str, Any] | None:
        metadata_path = self._metadata_path(sid, invoice_id)
        if not os.path.exists(metadata_path):
            return None
        with open(metadata_path, "r", encoding="utf-8") as file:
            return json.load(file)

    def read_capture_bytes(self, capture: dict[str, Any]) -> tuple[bytes, str]:
        with open(capture["storage_key"], "rb") as file:
            return file.read(), capture.get("content_type") or _guess_content_type(capture["storage_key"], "image/jpeg")


class BlobInvoiceCaptureStore:
    def __init__(self, token: str | None = None):
        self._token = str(token or os.environ.get("BLOB_READ_WRITE_TOKEN", "")).strip()
        if not self._token:
            raise RuntimeError("BLOB_READ_WRITE_TOKEN is required for Blob storage.")
        self._prefix = str(os.environ.get("IV_AGENT_INVOICES_BLOB_PREFIX", "Invoices") or "Invoices").strip("/")

    def _client(self):
        BlobClient, _ = _load_vercel_blob()
        return BlobClient(token=self._token)

    def _session_prefix(self, sid: str) -> str:
        safe_sid = sanitize_invoice_sid(sid)
        return "/".join(part for part in (self._prefix, safe_sid) if part)

    def _metadata_key(self, sid: str, invoice_id: str) -> str:
        return f"{self._session_prefix(sid)}/{invoice_id}.json"

    def save_capture(
        self,
        *,
        sid: str,
        file_name: str,
        content: bytes,
        content_type: str,
        fields: dict[str, Any] | None = None,
        extraction_error: str | None = None,
    ) -> dict[str, Any]:
        safe_sid = sanitize_invoice_sid(sid)
        safe_file_name = _sanitize_storage_name(file_name)
        invoice_id = str(uuid.uuid4())
        storage_key = f"{self._session_prefix(safe_sid)}/{invoice_id}_{safe_file_name}"
        client = self._client()

        uploaded_blob = client.put(
            storage_key,
            content,
            access="private",
            content_type=content_type,
            overwrite=True,
        )
        record = {
            "invoice_id": invoice_id,
            "sid": safe_sid,
            "file_name": safe_file_name,
            "storage_backend": "blob",
            "storage_key": uploaded_blob.pathname,
            "storage_url": uploaded_blob.url,
            "content_type": uploaded_blob.content_type or content_type,
            "content_size": len(content),
            "fields": fields or None,
            "extraction_error": extraction_error or None,
            "folder_path": self._session_prefix(safe_sid),
            "created_at": utcnow_iso(),
            "updated_at": utcnow_iso(),
        }
        client.put(
            self._metadata_key(safe_sid, invoice_id),
            json.dumps(record, ensure_ascii=False).encode("utf-8"),
            access="private",
            content_type="application/json",
            overwrite=True,
        )
        return record

    def list_captures(self, sid: str) -> list[dict[str, Any]]:
        client = self._client()
        prefix = f"{self._session_prefix(sid)}/"
        captures: list[dict[str, Any]] = []

        for blob in client.iter_objects(prefix=prefix):
            if not str(blob.pathname).endswith(".json"):
                continue
            metadata = client.get(blob.pathname, access="private")
            if metadata.status_code != 200:
                continue
            captures.append(json.loads(metadata.content.decode("utf-8")))

        captures.sort(key=lambda capture: str(capture.get("created_at", "")), reverse=True)
        return captures

    def get_capture(self, *, sid: str, invoice_id: str) -> dict[str, Any] | None:
        metadata_key = self._metadata_key(sid, invoice_id)
        client = self._client()
        try:
            metadata = client.get(metadata_key, access="private")
        except Exception:
            return None
        if metadata.status_code != 200:
            return None
        return json.loads(metadata.content.decode("utf-8"))

    def read_capture_bytes(self, capture: dict[str, Any]) -> tuple[bytes, str]:
        client = self._client()
        blob = client.get(capture["storage_key"], access="private")
        if blob.status_code != 200:
            raise FileNotFoundError(capture["storage_key"])
        return blob.content, blob.content_type or capture.get("content_type") or "image/jpeg"


def make_profile_store(default_profile_path: str, profile_dir: str) -> ProfileStore:
    if _database_backend_enabled():
        return PostgresProfileStore(os.environ["DATABASE_URL"].strip())
    return LocalProfileStore(default_profile_path, profile_dir)


def make_asset_store(output_dir: str) -> AssetStore:
    backend = _report_asset_backend()
    if backend == "blob":
        return VercelBlobAssetStore()
    return LocalFileAssetStore(output_dir)


def make_report_store(output_dir: str) -> ReportStore:
    asset_store = make_asset_store(output_dir)
    if _database_backend_enabled():
        return PostgresReportStore(os.environ["DATABASE_URL"].strip(), asset_store=asset_store)
    return JsonReportStore(output_dir, asset_store=asset_store)


def make_invoice_capture_store(output_dir: str) -> InvoiceCaptureStore:
    backend = _invoice_asset_backend()
    if backend == "blob":
        return BlobInvoiceCaptureStore()
    return LocalInvoiceCaptureStore(output_dir)
