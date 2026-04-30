import json
import hashlib
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


class TemplateStore(Protocol):
    def upsert_template(
        self,
        *,
        template_key: str,
        file_name: str,
        content: bytes,
        content_type: str = "application/pdf",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        ...

    def get_template(self, template_key: str) -> dict[str, Any] | None:
        ...

    def read_template_bytes(self, template_key: str) -> tuple[bytes, str]:
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


def _normalize_file_backend(value: str | None) -> str:
    normalized = str(value or "auto").strip().lower()
    if normalized not in {"auto", "local", "postgres", "supabase"}:
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
    normalized = _normalize_file_backend(os.environ.get("IV_AGENT_REPORT_ASSET_BACKEND"))
    if normalized == "local":
        return "local"
    if normalized == "supabase":
        return "supabase"
    if normalized == "postgres":
        return "postgres"
    return "supabase" if _supabase_storage_configured() else "local"


def _invoice_asset_backend() -> str:
    normalized = _normalize_file_backend(os.environ.get("IV_AGENT_INVOICE_ASSET_BACKEND"))
    if normalized == "local":
        return "local"
    if normalized == "supabase":
        return "supabase"
    if normalized == "postgres":
        return "postgres"
    return _report_asset_backend()


def _template_backend() -> str:
    normalized = _normalize_file_backend(os.environ.get("IV_AGENT_TEMPLATE_BACKEND"))
    if normalized == "supabase":
        return "supabase"
    if normalized == "postgres":
        return "postgres"
    if normalized == "local":
        return "local"
    if _supabase_storage_configured():
        return "supabase"
    return "postgres" if _database_backend_enabled() else "local"


def _database_url() -> str:
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("DATABASE_URL is required for Postgres storage.")
    return database_url


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


def _load_supabase_client():
    try:
        from supabase import create_client  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "Supabase storage requires the supabase Python package. Install dependencies from requirements.txt."
        ) from exc
    return create_client


def _supabase_url() -> str:
    value = (
        os.environ.get("SUPABASE_URL", "")
        or os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "")
    ).strip()
    if not value:
        raise RuntimeError("SUPABASE_URL is required for Supabase storage.")
    return value


def _supabase_service_role_key() -> str:
    value = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not value:
        raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY is required for Supabase storage.")
    return value


def _supabase_storage_configured() -> bool:
    return bool(_supabase_url_configured() and os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip())


def _supabase_url_configured() -> bool:
    return bool(
        (
            os.environ.get("SUPABASE_URL", "")
            or os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "")
        ).strip()
    )


def _create_supabase_client():
    create_client = _load_supabase_client()
    return create_client(_supabase_url(), _supabase_service_role_key())


def _supabase_templates_bucket() -> str:
    return os.environ.get("SUPABASE_STORAGE_TEMPLATES_BUCKET", "iv-agent-templates").strip() or "iv-agent-templates"


def _supabase_reports_bucket() -> str:
    return os.environ.get("SUPABASE_STORAGE_REPORTS_BUCKET", "iv-agent-reports").strip() or "iv-agent-reports"


def _supabase_invoices_bucket() -> str:
    return os.environ.get("SUPABASE_STORAGE_INVOICES_BUCKET", "iv-agent-invoices").strip() or "iv-agent-invoices"


SUPABASE_TEMPLATE_FILES = {
    "stundenblatt": "Stundenblatt.pdf",
    "rechnung": "Rechnungsvorlage_aL_elektronisch (1).pdf",
    "transportkosten": "AK_Formular_EL_Transportkosten.pdf",
}


def _supabase_storage_url(bucket: str, path: str) -> str:
    return f"supabase://{bucket}/{path.lstrip('/')}"


def _supabase_upload(
    client: Any,
    *,
    bucket: str,
    path: str,
    content: bytes,
    content_type: str,
    upsert: bool = True,
) -> None:
    client.storage.from_(bucket).upload(
        path=path,
        file=content,
        file_options={
            "content-type": content_type or "application/octet-stream",
            "cache-control": "3600",
            "upsert": "true" if upsert else "false",
        },
    )


def _supabase_download(client: Any, *, bucket: str, path: str) -> bytes:
    result = client.storage.from_(bucket).download(path)
    return _coerce_bytes(result)


def _is_url(reference: str) -> bool:
    parsed = urllib.parse.urlparse(str(reference or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _guess_content_type(reference: str, fallback: str = "application/octet-stream") -> str:
    content_type, _ = mimetypes.guess_type(reference)
    return content_type or fallback


def _coerce_json(value: Any, fallback: Any = None) -> Any:
    if value is None:
        return fallback
    if isinstance(value, str):
        return json.loads(value)
    return value


def _coerce_bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    return bytes(value)


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
        return _download_url_bytes(normalized_reference)

    raise FileNotFoundError(normalized_reference)


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


class PostgresAssetStore:
    def __init__(
        self,
        database_url: str,
        connection_factory: Callable[[], Any] | None = None,
    ):
        self._database_url = database_url
        self._connection_factory = connection_factory or (lambda: _connect_postgres(database_url))
        self._ensure_schema()

    @property
    def backend_name(self) -> str:
        return "postgres"

    def _ensure_schema(self) -> None:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS asset_blobs (
                        storage_key TEXT PRIMARY KEY,
                        file_name TEXT NOT NULL,
                        content_type TEXT NOT NULL,
                        content_size BIGINT NOT NULL DEFAULT 0,
                        content BYTEA NOT NULL,
                        checksum_sha256 TEXT NOT NULL,
                        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )

    def store_report(
        self,
        *,
        month: str,
        report_id: str,
        file_name: str,
        content: bytes,
        content_type: str,
    ) -> dict[str, Any]:
        safe_file_name = _sanitize_storage_name(file_name)
        storage_key = "/".join(part for part in ("reports", month, f"{report_id}_{safe_file_name}") if part)
        checksum = hashlib.sha256(content).hexdigest()
        metadata = {"month": month, "report_id": report_id}

        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO asset_blobs (
                        storage_key,
                        file_name,
                        content_type,
                        content_size,
                        content,
                        checksum_sha256,
                        metadata,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW())
                    ON CONFLICT (storage_key)
                    DO UPDATE SET
                        file_name = EXCLUDED.file_name,
                        content_type = EXCLUDED.content_type,
                        content_size = EXCLUDED.content_size,
                        content = EXCLUDED.content,
                        checksum_sha256 = EXCLUDED.checksum_sha256,
                        metadata = EXCLUDED.metadata,
                        updated_at = NOW()
                    """,
                    (
                        storage_key,
                        safe_file_name,
                        content_type,
                        len(content),
                        content,
                        checksum,
                        json.dumps(metadata),
                    ),
                )

        return {
            "storage_key": storage_key,
            "storage_url": None,
            "storage_download_url": None,
            "content_type": content_type,
            "content_size": len(content),
        }

    def read_bytes(self, *, storage_key: str, storage_url: str | None = None) -> tuple[bytes, str]:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT content, content_type FROM asset_blobs WHERE storage_key = %s LIMIT 1",
                    (storage_key,),
                )
                row = cursor.fetchone()
        if not row:
            raise FileNotFoundError(storage_key)
        return _coerce_bytes(row["content"]), row.get("content_type") or "application/pdf"


class SupabaseStorageAssetStore:
    def __init__(
        self,
        client: Any | None = None,
        bucket: str | None = None,
    ):
        self._client = client or _create_supabase_client()
        self._bucket = bucket or _supabase_reports_bucket()

    @property
    def backend_name(self) -> str:
        return "supabase"

    def store_report(
        self,
        *,
        month: str,
        report_id: str,
        file_name: str,
        content: bytes,
        content_type: str,
    ) -> dict[str, Any]:
        safe_file_name = _sanitize_storage_name(file_name)
        storage_key = "/".join(part for part in ("reports", month, f"{report_id}_{safe_file_name}") if part)
        _supabase_upload(
            self._client,
            bucket=self._bucket,
            path=storage_key,
            content=content,
            content_type=content_type,
            upsert=True,
        )
        return {
            "storage_key": storage_key,
            "storage_url": _supabase_storage_url(self._bucket, storage_key),
            "storage_download_url": None,
            "content_type": content_type,
            "content_size": len(content),
        }

    def read_bytes(self, *, storage_key: str, storage_url: str | None = None) -> tuple[bytes, str]:
        content = _supabase_download(self._client, bucket=self._bucket, path=storage_key)
        return content, _guess_content_type(storage_key, fallback="application/pdf")


class PostgresTemplateStore:
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
                    CREATE TABLE IF NOT EXISTS document_templates (
                        template_key TEXT PRIMARY KEY,
                        file_name TEXT NOT NULL,
                        content_type TEXT NOT NULL,
                        content_size BIGINT NOT NULL DEFAULT 0,
                        content BYTEA NOT NULL,
                        checksum_sha256 TEXT NOT NULL,
                        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )

    def upsert_template(
        self,
        *,
        template_key: str,
        file_name: str,
        content: bytes,
        content_type: str = "application/pdf",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_key = str(template_key or "").strip()
        if not normalized_key:
            raise ValueError("template_key is required")
        safe_file_name = _sanitize_storage_name(file_name)
        checksum = hashlib.sha256(content).hexdigest()

        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO document_templates (
                        template_key,
                        file_name,
                        content_type,
                        content_size,
                        content,
                        checksum_sha256,
                        metadata,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW())
                    ON CONFLICT (template_key)
                    DO UPDATE SET
                        file_name = EXCLUDED.file_name,
                        content_type = EXCLUDED.content_type,
                        content_size = EXCLUDED.content_size,
                        content = EXCLUDED.content,
                        checksum_sha256 = EXCLUDED.checksum_sha256,
                        metadata = EXCLUDED.metadata,
                        updated_at = NOW()
                    """,
                    (
                        normalized_key,
                        safe_file_name,
                        content_type,
                        len(content),
                        content,
                        checksum,
                        json.dumps(metadata or {}),
                    ),
                )

        return {
            "template_key": normalized_key,
            "file_name": safe_file_name,
            "content_type": content_type,
            "content_size": len(content),
            "checksum_sha256": checksum,
            "metadata": metadata or {},
            "created_at": utcnow_iso(),
            "updated_at": utcnow_iso(),
        }

    def get_template(self, template_key: str) -> dict[str, Any] | None:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT template_key, file_name, content_type, content_size, checksum_sha256,
                           metadata, created_at, updated_at
                    FROM document_templates
                    WHERE template_key = %s
                    LIMIT 1
                    """,
                    (template_key,),
                )
                row = cursor.fetchone()
        if not row:
            return None
        metadata = _coerce_json(row.get("metadata"), {}) or {}
        return {
            "template_key": row["template_key"],
            "file_name": row["file_name"],
            "content_type": row.get("content_type") or "application/pdf",
            "content_size": int(row.get("content_size") or 0),
            "checksum_sha256": row.get("checksum_sha256"),
            "metadata": metadata,
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

    def read_template_bytes(self, template_key: str) -> tuple[bytes, str]:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT content, content_type FROM document_templates WHERE template_key = %s LIMIT 1",
                    (template_key,),
                )
                row = cursor.fetchone()
        if not row:
            raise FileNotFoundError(template_key)
        return _coerce_bytes(row["content"]), row.get("content_type") or "application/pdf"


class SupabaseStorageTemplateStore:
    def __init__(
        self,
        client: Any | None = None,
        bucket: str | None = None,
        template_files: dict[str, str] | None = None,
    ):
        self._client = client or _create_supabase_client()
        self._bucket = bucket or _supabase_templates_bucket()
        self._template_files = dict(template_files or SUPABASE_TEMPLATE_FILES)

    def _file_name_for_key(self, template_key: str, file_name: str | None = None) -> str:
        normalized_key = str(template_key or "").strip()
        if not normalized_key:
            raise ValueError("template_key is required")
        return _sanitize_storage_name(file_name or self._template_files.get(normalized_key) or f"{normalized_key}.pdf")

    def _template_path(self, template_key: str, file_name: str | None = None) -> str:
        normalized_key = str(template_key or "").strip()
        if not normalized_key:
            raise ValueError("template_key is required")
        return f"{normalized_key}/{self._file_name_for_key(normalized_key, file_name)}"

    def upsert_template(
        self,
        *,
        template_key: str,
        file_name: str,
        content: bytes,
        content_type: str = "application/pdf",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_key = str(template_key or "").strip()
        storage_key = self._template_path(normalized_key, file_name)
        _supabase_upload(
            self._client,
            bucket=self._bucket,
            path=storage_key,
            content=content,
            content_type=content_type,
            upsert=True,
        )
        checksum = hashlib.sha256(content).hexdigest()
        return {
            "template_key": normalized_key,
            "file_name": self._file_name_for_key(normalized_key, file_name),
            "content_type": content_type,
            "content_size": len(content),
            "checksum_sha256": checksum,
            "metadata": metadata or {},
            "storage_backend": "supabase",
            "storage_key": storage_key,
            "storage_url": _supabase_storage_url(self._bucket, storage_key),
            "created_at": utcnow_iso(),
            "updated_at": utcnow_iso(),
        }

    def get_template(self, template_key: str) -> dict[str, Any] | None:
        normalized_key = str(template_key or "").strip()
        if normalized_key not in self._template_files:
            return None
        file_name = self._file_name_for_key(normalized_key)
        storage_key = self._template_path(normalized_key, file_name)
        return {
            "template_key": normalized_key,
            "file_name": file_name,
            "content_type": _guess_content_type(file_name, fallback="application/pdf"),
            "content_size": 0,
            "checksum_sha256": None,
            "metadata": {},
            "storage_backend": "supabase",
            "storage_key": storage_key,
            "storage_url": _supabase_storage_url(self._bucket, storage_key),
            "created_at": "",
            "updated_at": "",
        }

    def read_template_bytes(self, template_key: str) -> tuple[bytes, str]:
        template = self.get_template(template_key)
        if not template:
            raise FileNotFoundError(template_key)
        content = _supabase_download(self._client, bucket=self._bucket, path=template["storage_key"])
        return content, template.get("content_type") or "application/pdf"


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
        if report.get("storage_backend") == "postgres" and self._asset_store.backend_name != "postgres":
            return PostgresAssetStore(
                self._database_url,
                connection_factory=self._connection_factory,
            ).read_bytes(
                storage_key=report["storage_key"],
                storage_url=report.get("storage_url"),
            )
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


class PostgresInvoiceCaptureStore:
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
                    CREATE TABLE IF NOT EXISTS invoice_captures (
                        invoice_id TEXT PRIMARY KEY,
                        sid TEXT NOT NULL,
                        file_name TEXT NOT NULL,
                        storage_key TEXT NOT NULL UNIQUE,
                        content_type TEXT NOT NULL,
                        content_size BIGINT NOT NULL DEFAULT 0,
                        content BYTEA NOT NULL,
                        fields JSONB,
                        extraction_error TEXT,
                        folder_path TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS invoice_captures_sid_idx ON invoice_captures (sid, created_at DESC)"
                )

    def _row_to_record(self, row: dict[str, Any]) -> dict[str, Any]:
        fields = _coerce_json(row.get("fields"), None)
        return {
            "invoice_id": row["invoice_id"],
            "sid": row["sid"],
            "file_name": row["file_name"],
            "storage_backend": "postgres",
            "storage_key": row["storage_key"],
            "storage_url": None,
            "content_type": row.get("content_type") or "application/octet-stream",
            "content_size": int(row.get("content_size") or 0),
            "fields": fields,
            "extraction_error": row.get("extraction_error"),
            "folder_path": row.get("folder_path") or f"Invoices/{row['sid']}",
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
        storage_key = f"Invoices/{safe_sid}/{invoice_id}_{safe_file_name}"
        now = utcnow_iso()
        self.upsert_capture_record(
            {
                "invoice_id": invoice_id,
                "sid": safe_sid,
                "file_name": safe_file_name,
                "storage_key": storage_key,
                "content_type": content_type,
                "fields": fields or None,
                "extraction_error": extraction_error or None,
                "folder_path": f"Invoices/{safe_sid}",
                "created_at": now,
                "updated_at": now,
            },
            content=content,
            overwrite=True,
        )
        return {
            "invoice_id": invoice_id,
            "sid": safe_sid,
            "file_name": safe_file_name,
            "storage_backend": "postgres",
            "storage_key": storage_key,
            "storage_url": None,
            "content_type": content_type,
            "content_size": len(content),
            "fields": fields or None,
            "extraction_error": extraction_error or None,
            "folder_path": f"Invoices/{safe_sid}",
            "created_at": now,
            "updated_at": now,
        }

    def upsert_capture_record(
        self,
        record: dict[str, Any],
        *,
        content: bytes,
        overwrite: bool = False,
    ) -> bool:
        invoice_id = str(record.get("invoice_id") or "").strip() or str(uuid.uuid4())
        safe_sid = sanitize_invoice_sid(str(record.get("sid") or ""))
        safe_file_name = _sanitize_storage_name(str(record.get("file_name") or "invoice.jpg"))
        storage_key = str(record.get("storage_key") or "").strip() or f"Invoices/{safe_sid}/{invoice_id}_{safe_file_name}"
        content_type = str(record.get("content_type") or _guess_content_type(safe_file_name)).strip()
        folder_path = str(record.get("folder_path") or f"Invoices/{safe_sid}").strip()
        created_at = str(record.get("created_at") or utcnow_iso()).strip()
        updated_at = str(record.get("updated_at") or created_at).strip()
        fields = record.get("fields")
        extraction_error = record.get("extraction_error")

        conflict_clause = (
            """
            DO UPDATE SET
                sid = EXCLUDED.sid,
                file_name = EXCLUDED.file_name,
                storage_key = EXCLUDED.storage_key,
                content_type = EXCLUDED.content_type,
                content_size = EXCLUDED.content_size,
                content = EXCLUDED.content,
                fields = EXCLUDED.fields,
                extraction_error = EXCLUDED.extraction_error,
                folder_path = EXCLUDED.folder_path,
                updated_at = EXCLUDED.updated_at
            """
            if overwrite
            else "DO NOTHING"
        )

        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO invoice_captures (
                        invoice_id,
                        sid,
                        file_name,
                        storage_key,
                        content_type,
                        content_size,
                        content,
                        fields,
                        extraction_error,
                        folder_path,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s::timestamptz, %s::timestamptz)
                    ON CONFLICT (invoice_id)
                    {conflict_clause}
                    """,
                    (
                        invoice_id,
                        safe_sid,
                        safe_file_name,
                        storage_key,
                        content_type,
                        len(content),
                        content,
                        json.dumps(fields) if fields is not None else None,
                        extraction_error,
                        folder_path,
                        created_at,
                        updated_at,
                    ),
                )
                return getattr(cursor, "rowcount", 0) != 0

    def list_captures(self, sid: str) -> list[dict[str, Any]]:
        safe_sid = sanitize_invoice_sid(sid)
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT invoice_id, sid, file_name, storage_key, content_type, content_size,
                           fields, extraction_error, folder_path, created_at, updated_at
                    FROM invoice_captures
                    WHERE sid = %s
                    ORDER BY created_at DESC
                    """,
                    (safe_sid,),
                )
                rows = cursor.fetchall()
        return [self._row_to_record(row) for row in rows]

    def get_capture(self, *, sid: str, invoice_id: str) -> dict[str, Any] | None:
        safe_sid = sanitize_invoice_sid(sid)
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT invoice_id, sid, file_name, storage_key, content_type, content_size,
                           fields, extraction_error, folder_path, created_at, updated_at
                    FROM invoice_captures
                    WHERE sid = %s AND invoice_id = %s
                    LIMIT 1
                    """,
                    (safe_sid, invoice_id),
                )
                row = cursor.fetchone()
        if not row:
            return None
        return self._row_to_record(row)

    def read_capture_bytes(self, capture: dict[str, Any]) -> tuple[bytes, str]:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT content, content_type FROM invoice_captures WHERE invoice_id = %s LIMIT 1",
                    (capture["invoice_id"],),
                )
                row = cursor.fetchone()
        if not row:
            raise FileNotFoundError(capture["invoice_id"])
        return _coerce_bytes(row["content"]), row.get("content_type") or capture.get("content_type") or "image/jpeg"


class SupabaseStorageInvoiceCaptureStore:
    def __init__(
        self,
        database_url: str,
        client: Any | None = None,
        bucket: str | None = None,
        connection_factory: Callable[[], Any] | None = None,
    ):
        self._database_url = database_url
        self._client = client or _create_supabase_client()
        self._bucket = bucket or _supabase_invoices_bucket()
        self._connection_factory = connection_factory or (lambda: _connect_postgres(database_url))
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS invoice_captures (
                        invoice_id TEXT PRIMARY KEY,
                        sid TEXT NOT NULL,
                        file_name TEXT NOT NULL,
                        storage_key TEXT NOT NULL UNIQUE,
                        storage_backend TEXT NOT NULL DEFAULT 'supabase',
                        storage_bucket TEXT,
                        storage_url TEXT,
                        content_type TEXT NOT NULL,
                        content_size BIGINT NOT NULL DEFAULT 0,
                        content BYTEA NOT NULL DEFAULT ''::bytea,
                        fields JSONB,
                        extraction_error TEXT,
                        folder_path TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute("ALTER TABLE invoice_captures ADD COLUMN IF NOT EXISTS storage_backend TEXT NOT NULL DEFAULT 'supabase'")
                cursor.execute("ALTER TABLE invoice_captures ADD COLUMN IF NOT EXISTS storage_bucket TEXT")
                cursor.execute("ALTER TABLE invoice_captures ADD COLUMN IF NOT EXISTS storage_url TEXT")
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS invoice_captures_sid_idx ON invoice_captures (sid, created_at DESC)"
                )

    def _row_to_record(self, row: dict[str, Any]) -> dict[str, Any]:
        fields = _coerce_json(row.get("fields"), None)
        return {
            "invoice_id": row["invoice_id"],
            "sid": row["sid"],
            "file_name": row["file_name"],
            "storage_backend": row.get("storage_backend") or "supabase",
            "storage_key": row["storage_key"],
            "storage_url": row.get("storage_url") or _supabase_storage_url(row.get("storage_bucket") or self._bucket, row["storage_key"]),
            "content_type": row.get("content_type") or "application/octet-stream",
            "content_size": int(row.get("content_size") or 0),
            "fields": fields,
            "extraction_error": row.get("extraction_error"),
            "folder_path": row.get("folder_path") or f"Invoices/{row['sid']}",
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
        storage_key = f"Invoices/{safe_sid}/{invoice_id}_{safe_file_name}"
        _supabase_upload(
            self._client,
            bucket=self._bucket,
            path=storage_key,
            content=content,
            content_type=content_type,
            upsert=True,
        )
        now = utcnow_iso()
        storage_url = _supabase_storage_url(self._bucket, storage_key)

        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO invoice_captures (
                        invoice_id,
                        sid,
                        file_name,
                        storage_key,
                        storage_backend,
                        storage_bucket,
                        storage_url,
                        content_type,
                        content_size,
                        content,
                        fields,
                        extraction_error,
                        folder_path,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, 'supabase', %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s::timestamptz, %s::timestamptz)
                    ON CONFLICT (invoice_id)
                    DO UPDATE SET
                        sid = EXCLUDED.sid,
                        file_name = EXCLUDED.file_name,
                        storage_key = EXCLUDED.storage_key,
                        storage_backend = EXCLUDED.storage_backend,
                        storage_bucket = EXCLUDED.storage_bucket,
                        storage_url = EXCLUDED.storage_url,
                        content_type = EXCLUDED.content_type,
                        content_size = EXCLUDED.content_size,
                        content = EXCLUDED.content,
                        fields = EXCLUDED.fields,
                        extraction_error = EXCLUDED.extraction_error,
                        folder_path = EXCLUDED.folder_path,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        invoice_id,
                        safe_sid,
                        safe_file_name,
                        storage_key,
                        self._bucket,
                        storage_url,
                        content_type,
                        len(content),
                        b"",
                        json.dumps(fields) if fields is not None else None,
                        extraction_error,
                        f"Invoices/{safe_sid}",
                        now,
                        now,
                    ),
                )

        return {
            "invoice_id": invoice_id,
            "sid": safe_sid,
            "file_name": safe_file_name,
            "storage_backend": "supabase",
            "storage_key": storage_key,
            "storage_url": storage_url,
            "content_type": content_type,
            "content_size": len(content),
            "fields": fields or None,
            "extraction_error": extraction_error or None,
            "folder_path": f"Invoices/{safe_sid}",
            "created_at": now,
            "updated_at": now,
        }

    def list_captures(self, sid: str) -> list[dict[str, Any]]:
        safe_sid = sanitize_invoice_sid(sid)
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT invoice_id, sid, file_name, storage_key, storage_backend,
                           storage_bucket, storage_url, content_type, content_size,
                           fields, extraction_error, folder_path, created_at, updated_at
                    FROM invoice_captures
                    WHERE sid = %s
                    ORDER BY created_at DESC
                    """,
                    (safe_sid,),
                )
                rows = cursor.fetchall()
        return [self._row_to_record(row) for row in rows]

    def get_capture(self, *, sid: str, invoice_id: str) -> dict[str, Any] | None:
        safe_sid = sanitize_invoice_sid(sid)
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT invoice_id, sid, file_name, storage_key, storage_backend,
                           storage_bucket, storage_url, content_type, content_size,
                           fields, extraction_error, folder_path, created_at, updated_at
                    FROM invoice_captures
                    WHERE sid = %s AND invoice_id = %s
                    LIMIT 1
                    """,
                    (safe_sid, invoice_id),
                )
                row = cursor.fetchone()
        if not row:
            return None
        return self._row_to_record(row)

    def read_capture_bytes(self, capture: dict[str, Any]) -> tuple[bytes, str]:
        storage_key = capture["storage_key"]
        if capture.get("storage_backend") == "postgres":
            with self._connection_factory() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT content, content_type FROM invoice_captures WHERE invoice_id = %s LIMIT 1",
                        (capture["invoice_id"],),
                    )
                    row = cursor.fetchone()
            if not row:
                raise FileNotFoundError(capture["invoice_id"])
            return _coerce_bytes(row["content"]), row.get("content_type") or capture.get("content_type") or "image/jpeg"
        content = _supabase_download(self._client, bucket=self._bucket, path=storage_key)
        return content, capture.get("content_type") or _guess_content_type(storage_key, "image/jpeg")


def make_profile_store(default_profile_path: str, profile_dir: str) -> ProfileStore:
    if _database_backend_enabled():
        return PostgresProfileStore(_database_url())
    return LocalProfileStore(default_profile_path, profile_dir)


def make_asset_store(output_dir: str) -> AssetStore:
    backend = _report_asset_backend()
    if backend == "supabase":
        return SupabaseStorageAssetStore()
    if backend == "postgres":
        return PostgresAssetStore(_database_url())
    return LocalFileAssetStore(output_dir)


def make_report_store(output_dir: str) -> ReportStore:
    asset_store = make_asset_store(output_dir)
    if _database_backend_enabled():
        return PostgresReportStore(_database_url(), asset_store=asset_store)
    return JsonReportStore(output_dir, asset_store=asset_store)


def make_invoice_capture_store(output_dir: str) -> InvoiceCaptureStore:
    backend = _invoice_asset_backend()
    if backend == "supabase":
        return SupabaseStorageInvoiceCaptureStore(_database_url())
    if backend == "postgres":
        return PostgresInvoiceCaptureStore(_database_url())
    return LocalInvoiceCaptureStore(output_dir)


def make_template_store() -> TemplateStore | None:
    if _template_backend() == "supabase":
        return SupabaseStorageTemplateStore()
    if _template_backend() == "postgres":
        return PostgresTemplateStore(_database_url())
    return None
