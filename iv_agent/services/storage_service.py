import base64
import binascii
import hashlib
import io
import json
import mimetypes
import os
import re
import uuid
import zipfile
from datetime import date, datetime, timezone
from typing import Any, Callable
from xml.etree import ElementTree

try:
    from ..storage import (
        _connect_postgres,
        _create_supabase_client,
        _supabase_download,
        _supabase_storage_url,
        _supabase_upload,
        sanitize_profile_id,
    )
except ImportError:
    from storage import (
        _connect_postgres,
        _create_supabase_client,
        _supabase_download,
        _supabase_storage_url,
        _supabase_upload,
        sanitize_profile_id,
    )


DEFAULT_DOCUMENT_BUCKET = "Invoice_upload"
DOCUMENT_PREFIX = "Documents"
SUPPORTED_DOCUMENT_MIME_TYPES = {
    "application/pdf": ".pdf",
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "text/plain": ".txt",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
}
DOCUMENT_AGENT_MODEL = (
    os.environ.get("OPENAI_DOCUMENT_AGENT_MODEL")
    or os.environ.get("OPENAI_AGENT_MODEL")
    or "gpt-5.4-mini"
).strip() or "gpt-5.4-mini"
_SERVICE_CACHE: dict[tuple[Any, ...], "StorageService"] = {}


def document_bucket_name() -> str:
    return os.environ.get("SUPABASE_STORAGE_DOCUMENTS_BUCKET", DEFAULT_DOCUMENT_BUCKET).strip() or DEFAULT_DOCUMENT_BUCKET


def document_max_bytes() -> int:
    raw_value = os.environ.get("IV_AGENT_DOCUMENT_MAX_BYTES", "10485760").strip() or "10485760"
    try:
        return max(1, int(raw_value))
    except ValueError:
        return 10 * 1024 * 1024


def signed_url_ttl_seconds() -> int:
    raw_value = os.environ.get("IV_AGENT_DOCUMENT_SIGNED_URL_TTL_SECONDS", "600").strip() or "600"
    try:
        return max(30, int(raw_value))
    except ValueError:
        return 600


def normalize_user_id(user_id: str | None = None) -> str:
    return sanitize_profile_id(user_id or "default")


def _database_url() -> str:
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("DATABASE_URL is required for document storage.")
    return database_url


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _date_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raw = str(value or "").strip()
    return raw[:10] if raw else None


def _parse_filter_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value or "").strip()
    if not raw:
        return None
    return datetime.strptime(raw[:10], "%Y-%m-%d").date()


def sanitize_document_filename(file_name: str, *, content_type: str = "application/octet-stream") -> str:
    base_name = os.path.basename(str(file_name or "").strip())
    safe_name = re.sub(r"[^0-9A-Za-z._-]+", "_", base_name).strip("._-")
    extension = mimetypes.guess_extension(content_type) or SUPPORTED_DOCUMENT_MIME_TYPES.get(content_type) or ""
    if not safe_name:
        safe_name = f"document{extension or '.bin'}"
    elif "." not in safe_name and extension:
        safe_name = f"{safe_name}{extension}"
    return safe_name[:180]


def normalize_document_mime_type(file_name: str, content_type: str | None = None) -> str:
    candidate = str(content_type or "").split(";")[0].strip().lower()
    if candidate in SUPPORTED_DOCUMENT_MIME_TYPES:
        return candidate
    guessed_type, _ = mimetypes.guess_type(file_name)
    guessed = str(guessed_type or "").strip().lower()
    if guessed in SUPPORTED_DOCUMENT_MIME_TYPES:
        return guessed
    return candidate or "application/octet-stream"


def _decode_attachment_content(raw_attachment: dict[str, Any]) -> bytes:
    raw_value = str(raw_attachment.get("content_base64") or "").strip()
    if not raw_value:
        raise ValueError("attachment content_base64 is required")
    if "," in raw_value and raw_value.lower().startswith("data:"):
        raw_value = raw_value.split(",", 1)[1]
    try:
        return base64.b64decode(raw_value, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("Invalid Base64 attachment payload") from exc


def _safe_text_preview(text: str, limit: int = 1200) -> str:
    compact = " ".join(str(text or "").split())
    return compact[:limit]


def _extract_txt(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def _extract_pdf(content: bytes) -> str:
    try:
        from pypdf import PdfReader  # type: ignore
    except ImportError as exc:
        raise RuntimeError("PDF extraction requires pypdf.") from exc
    reader = PdfReader(io.BytesIO(content))
    page_text = []
    for page in reader.pages:
        page_text.append(page.extract_text() or "")
    return "\n".join(page_text).strip()


def _extract_docx(content: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(content)) as docx_zip:
        xml_bytes = docx_zip.read("word/document.xml")
    root = ElementTree.fromstring(xml_bytes)
    parts: list[str] = []
    for node in root.iter():
        if node.tag.endswith("}t") and node.text:
            parts.append(node.text)
        elif node.tag.endswith("}tab"):
            parts.append("\t")
        elif node.tag.endswith("}br") or node.tag.endswith("}p"):
            parts.append("\n")
    return "".join(parts).strip()


def extract_document_text(content: bytes, content_type: str) -> tuple[str, str, str | None]:
    try:
        if content_type == "application/pdf":
            text = _extract_pdf(content)
        elif content_type == "text/plain":
            text = _extract_txt(content)
        elif content_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
            text = _extract_docx(content)
        elif content_type.startswith("image/"):
            return "", "no_text", "Text konnte nicht extrahiert werden."
        else:
            return "", "unsupported", "Unsupported document type."
    except Exception as exc:
        return "", "failed", str(exc)

    normalized_text = text.strip()
    if not normalized_text:
        return "", "empty", "Text konnte nicht extrahiert werden."
    return normalized_text, "completed", None


def _extract_document_date(text: str) -> str | None:
    for pattern in (r"\b(\d{4})-(\d{2})-(\d{2})\b", r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b"):
        match = re.search(pattern, text)
        if not match:
            continue
        try:
            if pattern.startswith("\\b(\\d{4})"):
                return date(int(match.group(1)), int(match.group(2)), int(match.group(3))).isoformat()
            return date(int(match.group(3)), int(match.group(2)), int(match.group(1))).isoformat()
        except ValueError:
            continue
    return None


def _extract_service_period(text: str) -> str:
    patterns = (
        r"(?:leistungszeitraum|zeitraum|periode|fuer den monat|für den monat)\s*[:\-]?\s*([A-Za-zÄÖÜäöü]+\.?\s+\d{4})",
        r"(?:leistungszeitraum|zeitraum|periode)\s*[:\-]?\s*(\d{1,2}\.\d{1,2}\.\d{4}\s*(?:-|bis)\s*\d{1,2}\.\d{1,2}\.\d{4})",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return " ".join(match.group(1).split())
    return ""


def _extract_amount(text: str) -> str:
    match = re.search(r"\b(?:CHF|Fr\.?)\s*([0-9][0-9'.,]*)\b|\b([0-9][0-9'.,]*)\s*(?:CHF|Fr\.?)\b", text, flags=re.IGNORECASE)
    if not match:
        return ""
    amount = match.group(1) or match.group(2)
    return f"CHF {amount}"


def _extract_deadline(text: str) -> str:
    patterns = (
        r"(?:frist|bis spaetestens|bis spätestens|zahlbar bis|einzureichen bis)\s*[:\-]?\s*(\d{1,2}\.\d{1,2}\.\d{4})",
        r"(?:frist|bis spaetestens|bis spätestens|zahlbar bis|einzureichen bis)\s*[:\-]?\s*(\d{4}-\d{2}-\d{2})",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return ""


def _extract_reference(text: str) -> str:
    match = re.search(
        r"\b(?:referenz|ref\.?|aktenzeichen|kundennummer|rechnungsnummer|rechnung nr\.?|invoice no\.?)\s*[:#\-]?\s*([A-Za-z0-9][A-Za-z0-9._/\-]{2,})",
        text,
        flags=re.IGNORECASE,
    )
    return match.group(1) if match else ""


def _extract_todos(text: str) -> list[str]:
    todos = []
    for pattern in (
        r"((?:bitte|wir bitten sie|reichen sie|senden sie|bezahlen sie).{12,180}?[.!?])",
        r"((?:einzureichen|nachzureichen|zu bezahlen).{12,160}?[.!?])",
    ):
        for match in re.finditer(pattern, text, flags=re.IGNORECASE | re.DOTALL):
            todo = " ".join(match.group(1).split())
            if todo and todo not in todos:
                todos.append(todo)
            if len(todos) >= 3:
                return todos
    return todos


def _extract_institution(text: str) -> str:
    haystack = text.lower()
    for keyword, label in (
        ("iv-stelle", "IV-Stelle"),
        ("invalidenversicherung", "IV-Stelle"),
        ("sva", "SVA"),
        ("ahv", "AHV"),
        ("suva", "SUVA"),
        ("css", "CSS"),
        ("helsana", "Helsana"),
        ("pro infirmis", "Pro Infirmis"),
        ("spitex", "Spitex"),
        ("apotheke", "Apotheke"),
        ("fahrdienst", "Fahrdienst"),
        ("therapie", "Therapie"),
    ):
        if keyword in haystack:
            return label
    return ""


def extract_structured_facts(text: str) -> dict[str, Any]:
    return {
        "institution": _extract_institution(text),
        "document_date": _extract_document_date(text),
        "service_period": _extract_service_period(text),
        "amount": _extract_amount(text),
        "deadline": _extract_deadline(text),
        "reference": _extract_reference(text),
        "todos": _extract_todos(text),
    }


def summarize_text_locally(text: str, *, max_chars: int = 900) -> str:
    compact = " ".join(str(text or "").split())
    if not compact:
        return "Text konnte nicht extrahiert werden."
    facts = extract_structured_facts(text)
    lines = []
    if facts.get("institution"):
        lines.append(f"Institution: {facts['institution']}")
    if facts.get("document_date"):
        lines.append(f"Datum: {facts['document_date']}")
    if facts.get("service_period"):
        lines.append(f"Zeitraum: {facts['service_period']}")
    if facts.get("amount"):
        lines.append(f"Betrag: {facts['amount']}")
    if facts.get("deadline"):
        lines.append(f"Frist: {facts['deadline']}")
    if facts.get("reference"):
        lines.append(f"Referenz: {facts['reference']}")
    if facts.get("todos"):
        lines.append(f"To-do: {facts['todos'][0]}")
    sentence_match = re.match(r"^(.{80,420}?[.!?])\s", compact)
    preview = sentence_match.group(1) if sentence_match else compact[:420].rstrip()
    if len(compact) > len(preview):
        preview = f"{preview}..."
    lines.append(f"Kurzinhalt: {preview}")
    return "\n".join(lines)[:max_chars]


def normalize_document_type(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    normalized = raw.replace("ü", "ue").replace("ä", "ae").replace("ö", "oe")
    aliases = {
        "rechnung": "invoice",
        "invoice": "invoice",
        "bill": "invoice",
        "brief": "letter",
        "letter": "letter",
        "schreiben": "letter",
        "verfuegung": "decision",
        "verfügung": "decision",
        "entscheid": "decision",
        "bescheid": "decision",
        "arztbericht": "medical_report",
        "medical": "medical_report",
        "medical_report": "medical_report",
        "bericht": "medical_report",
        "therapiebestaetigung": "therapy_confirmation",
        "therapiebestätigung": "therapy_confirmation",
        "therapy_confirmation": "therapy_confirmation",
        "iv-dokument": "iv_document",
        "iv dokument": "iv_document",
        "iv_document": "iv_document",
        "quittung": "receipt",
        "receipt": "receipt",
        "vertrag": "contract",
        "contract": "contract",
        "sonstiges dokument": "document",
        "document": "document",
        "image": "image",
    }
    return aliases.get(normalized, normalized)


def classify_text_locally(text: str, file_name: str = "") -> dict[str, Any]:
    haystack = f"{file_name}\n{text}".lower()
    document_type = "document"
    type_keywords = (
        ("invoice", ("rechnung", "invoice", "betrag", "mwst", "vat", "zahlbar", "total")),
        ("receipt", ("quittung", "kassenbon", "receipt", "bezahlt", "zahlung erhalten")),
        ("letter", ("brief", "anschreiben", "korrespondenz", "schreiben")),
        ("decision", ("verfuegung", "verfügung", "entscheid", "bescheid")),
        ("therapy_confirmation", ("therapiebestaetigung", "therapiebestätigung", "therapie bestaetigung", "therapie bestätigung")),
        ("medical_report", ("arztbericht", "arzt", "spital", "klinik", "diagnose", "befund")),
        ("iv_document", ("iv-stelle", "invalidenversicherung", "assistenzbeitrag", "hilflosenentschaedigung", "hilflosenentschädigung")),
        ("contract", ("vertrag", "vereinbarung", "contract", "vertragsnummer")),
    )
    for candidate, keywords in type_keywords:
        if any(keyword in haystack for keyword in keywords):
            document_type = candidate
            break

    facts = extract_structured_facts(text)
    institution = facts["institution"]

    tags = []
    for tag, keywords in {
        "rechnung": ("rechnung", "invoice"),
        "iv": ("iv-stelle", "invalidenversicherung"),
        "medizin": ("arzt", "spital", "klinik", "diagnose"),
        "therapie": ("therapie", "therapeut", "therapiebestaetigung", "therapiebestätigung"),
        "transport": ("transport", "fahrt", "taxi"),
        "frist": ("frist", "bis spaetestens", "bis spätestens", "einzureichen"),
    }.items():
        if any(keyword in haystack for keyword in keywords):
            tags.append(tag)

    return {
        "document_type": normalize_document_type(document_type),
        "institution": institution,
        "document_date": facts["document_date"],
        "tags": tags,
        "facts": facts,
        "confidence": "low" if document_type == "document" else "medium",
    }


def _coerce_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _coerce_tags(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        if value.startswith("{") and value.endswith("}"):
            raw_items = value.strip("{}").split(",")
        else:
            raw_items = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raw_items = []
    tags = []
    for raw_tag in raw_items:
        tag = re.sub(r"[^0-9A-Za-z_-]+", "_", str(raw_tag or "").strip().lower()).strip("_")
        if tag and tag not in tags:
            tags.append(tag[:40])
    return tags


def _storage_response_to_url(response: Any) -> str:
    if isinstance(response, dict):
        data = response.get("data") if isinstance(response.get("data"), dict) else response
        for key in ("signedURL", "signedUrl", "signed_url"):
            if data.get(key):
                return str(data[key])
    for attr in ("signed_url", "signedURL", "signedUrl"):
        if hasattr(response, attr):
            return str(getattr(response, attr))
    return ""


class NoopDocumentEmbeddingService:
    def index_document(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"enabled": False, "reason": "pgvector is not enabled for document_embeddings in v1"}


class StorageService:
    def __init__(
        self,
        database_url: str | None = None,
        *,
        client: Any | None = None,
        bucket: str | None = None,
        connection_factory: Callable[[], Any] | None = None,
        embedding_service: Any | None = None,
    ):
        self._database_url = database_url or _database_url()
        self._client = client or _create_supabase_client()
        self._bucket = bucket or document_bucket_name()
        self._connection_factory = connection_factory or (lambda: _connect_postgres(self._database_url))
        self._embedding_service = embedding_service or NoopDocumentEmbeddingService()
        self._ensure_schema()

    @property
    def bucket(self) -> str:
        return self._bucket

    def _ensure_schema(self) -> None:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS document_folders (
                        folder_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        user_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        parent_folder_id UUID REFERENCES document_folders(folder_id) ON DELETE SET NULL,
                        color TEXT,
                        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS document_folders_user_parent_name_idx
                    ON document_folders (user_id, COALESCE(parent_folder_id, '00000000-0000-0000-0000-000000000000'::uuid), lower(name))
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS documents (
                        document_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        user_id TEXT NOT NULL,
                        folder_id UUID REFERENCES document_folders(folder_id) ON DELETE SET NULL,
                        file_name TEXT NOT NULL,
                        safe_file_name TEXT NOT NULL,
                        storage_bucket TEXT NOT NULL,
                        storage_key TEXT NOT NULL UNIQUE,
                        storage_url TEXT NOT NULL,
                        content_type TEXT NOT NULL,
                        content_size BIGINT NOT NULL DEFAULT 0,
                        checksum_sha256 TEXT NOT NULL,
                        document_type TEXT,
                        institution TEXT,
                        document_date DATE,
                        year INTEGER NOT NULL,
                        month INTEGER NOT NULL,
                        tags TEXT[] NOT NULL DEFAULT '{}'::text[],
                        summary TEXT,
                        extracted_text TEXT,
                        extraction_status TEXT NOT NULL DEFAULT 'pending',
                        extraction_error TEXT,
                        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute("CREATE INDEX IF NOT EXISTS documents_user_created_idx ON documents (user_id, created_at DESC)")
                cursor.execute("CREATE INDEX IF NOT EXISTS documents_user_month_idx ON documents (user_id, year, month)")
                cursor.execute("CREATE INDEX IF NOT EXISTS documents_user_type_idx ON documents (user_id, lower(document_type))")
                cursor.execute("CREATE INDEX IF NOT EXISTS documents_user_institution_idx ON documents (user_id, lower(institution))")
                cursor.execute("CREATE INDEX IF NOT EXISTS documents_user_tags_idx ON documents USING GIN (tags)")
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS document_matches (
                        match_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        user_id TEXT NOT NULL,
                        source_document_id UUID NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
                        target_document_id UUID NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
                        match_type TEXT NOT NULL DEFAULT 'related',
                        score DOUBLE PRECISION NOT NULL DEFAULT 0,
                        reason TEXT,
                        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        UNIQUE (source_document_id, target_document_id, match_type)
                    )
                    """
                )
                cursor.execute("CREATE INDEX IF NOT EXISTS document_matches_user_source_idx ON document_matches (user_id, source_document_id)")
                cursor.execute("ALTER TABLE document_folders ENABLE ROW LEVEL SECURITY")
                cursor.execute("ALTER TABLE documents ENABLE ROW LEVEL SECURITY")
                cursor.execute("ALTER TABLE document_matches ENABLE ROW LEVEL SECURITY")

    def _row_to_document(self, row: dict[str, Any]) -> dict[str, Any]:
        metadata = _coerce_json_dict(row.get("metadata"))
        return {
            "document_id": str(row.get("document_id") or ""),
            "user_id": normalize_user_id(row.get("user_id")),
            "folder_id": str(row.get("folder_id") or "") or None,
            "file_name": row.get("file_name") or "",
            "title": str(metadata.get("title") or row.get("file_name") or "").strip(),
            "notes": str(metadata.get("notes") or "").strip(),
            "safe_file_name": row.get("safe_file_name") or "",
            "storage_bucket": row.get("storage_bucket") or self._bucket,
            "storage_key": row.get("storage_key") or "",
            "storage_url": row.get("storage_url") or _supabase_storage_url(row.get("storage_bucket") or self._bucket, row.get("storage_key") or ""),
            "content_type": row.get("content_type") or "application/octet-stream",
            "content_size": int(row.get("content_size") or 0),
            "checksum_sha256": row.get("checksum_sha256") or "",
            "document_type": row.get("document_type") or "",
            "institution": row.get("institution") or "",
            "document_date": _date_iso(row.get("document_date")),
            "year": int(row.get("year") or 0),
            "month": int(row.get("month") or 0),
            "tags": _coerce_tags(row.get("tags")),
            "summary": row.get("summary") or "",
            "extracted_text": row.get("extracted_text") or "",
            "extraction_status": row.get("extraction_status") or "",
            "extraction_error": row.get("extraction_error") or "",
            "metadata": metadata,
            "created_at": _iso(row.get("created_at")),
            "updated_at": _iso(row.get("updated_at")),
        }

    def _row_to_folder(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "folder_id": str(row.get("folder_id") or ""),
            "user_id": normalize_user_id(row.get("user_id")),
            "name": row.get("name") or "",
            "parent_folder_id": str(row.get("parent_folder_id") or "") or None,
            "color": row.get("color") or "",
            "metadata": _coerce_json_dict(row.get("metadata")),
            "created_at": _iso(row.get("created_at")),
            "updated_at": _iso(row.get("updated_at")),
        }

    def _create_signed_url(self, document: dict[str, Any], expires_in: int | None = None) -> str:
        response = self._client.storage.from_(document["storage_bucket"]).create_signed_url(
            document["storage_key"],
            expires_in or signed_url_ttl_seconds(),
        )
        return _storage_response_to_url(response)

    def upload_document(
        self,
        *,
        user_id: str,
        file_name: str,
        content: bytes,
        content_type: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_user_id = normalize_user_id(user_id)
        normalized_content_type = normalize_document_mime_type(file_name, content_type)
        if normalized_content_type not in SUPPORTED_DOCUMENT_MIME_TYPES:
            raise ValueError("Unsupported attachment type")
        if not content:
            raise ValueError("Attachment payload is empty")
        if len(content) > document_max_bytes():
            raise ValueError("Attachment exceeds configured max size")

        now = _utcnow()
        document_id = str(uuid.uuid4())
        safe_file_name = sanitize_document_filename(file_name, content_type=normalized_content_type)
        checksum = hashlib.sha256(content).hexdigest()
        extracted_text, extraction_status, extraction_error = extract_document_text(content, normalized_content_type)
        classification = classify_text_locally(extracted_text, safe_file_name) if extracted_text else {
            "document_type": "image" if normalized_content_type.startswith("image/") else "document",
            "institution": "",
            "document_date": None,
            "tags": [],
            "facts": {},
            "confidence": "low",
        }
        summary = summarize_text_locally(extracted_text) if extracted_text else "Text konnte nicht extrahiert werden."
        document_date = _parse_filter_date(classification.get("document_date"))
        record_date = document_date or now.date()
        storage_key = (
            f"{DOCUMENT_PREFIX}/{normalized_user_id}/{record_date.year}/{record_date.month:02d}/"
            f"{document_id}-{safe_file_name}"
        )

        _supabase_upload(
            self._client,
            bucket=self._bucket,
            path=storage_key,
            content=content,
            content_type=normalized_content_type,
            upsert=False,
        )

        row_fallback = {
            "document_id": document_id,
            "user_id": normalized_user_id,
            "folder_id": None,
            "file_name": file_name,
            "safe_file_name": safe_file_name,
            "storage_bucket": self._bucket,
            "storage_key": storage_key,
            "storage_url": _supabase_storage_url(self._bucket, storage_key),
            "content_type": normalized_content_type,
            "content_size": len(content),
            "checksum_sha256": checksum,
            "document_type": classification.get("document_type") or "",
            "institution": classification.get("institution") or "",
            "document_date": classification.get("document_date"),
            "year": record_date.year,
            "month": record_date.month,
            "tags": classification.get("tags") or [],
            "summary": summary,
            "extracted_text": extracted_text,
            "extraction_status": extraction_status,
            "extraction_error": extraction_error,
            "metadata": {**(metadata or {}), "classification": classification},
            "created_at": now,
            "updated_at": now,
        }
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO documents (
                        document_id, user_id, folder_id, file_name, safe_file_name,
                        storage_bucket, storage_key, storage_url, content_type, content_size,
                        checksum_sha256, document_type, institution, document_date, year, month,
                        tags, summary, extracted_text, extraction_status, extraction_error,
                        metadata, created_at, updated_at
                    )
                    VALUES (
                        %s::uuid, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s::date, %s, %s, %s::text[], %s, %s, %s, %s,
                        %s::jsonb, %s::timestamptz, %s::timestamptz
                    )
                    RETURNING *
                    """,
                    (
                        document_id,
                        normalized_user_id,
                        file_name,
                        safe_file_name,
                        self._bucket,
                        storage_key,
                        row_fallback["storage_url"],
                        normalized_content_type,
                        len(content),
                        checksum,
                        row_fallback["document_type"],
                        row_fallback["institution"],
                        row_fallback["document_date"],
                        record_date.year,
                        record_date.month,
                        row_fallback["tags"],
                        summary,
                        extracted_text,
                        extraction_status,
                        extraction_error,
                        json.dumps(row_fallback["metadata"]),
                        now.isoformat(),
                        now.isoformat(),
                    ),
                )
                row = cursor.fetchone()
        document = self._row_to_document(row or row_fallback)
        self._embedding_service.index_document(document)
        return document

    def _document_where_clause(
        self,
        *,
        user_id: str,
        query: str = "",
        year: int | None = None,
        month: int | None = None,
        document_type: str = "",
        institution: str = "",
        tags: list[str] | None = None,
        folder_id: str | None = None,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
    ) -> tuple[str, list[Any]]:
        where = ["user_id = %s"]
        params: list[Any] = [normalize_user_id(user_id)]
        if year:
            where.append("COALESCE(EXTRACT(YEAR FROM document_date)::int, year) = %s")
            params.append(int(year))
        if month:
            where.append("COALESCE(EXTRACT(MONTH FROM document_date)::int, month) = %s")
            params.append(int(month))
        if document_type:
            where.append("lower(document_type) = lower(%s)")
            params.append(normalize_document_type(document_type))
        if institution:
            where.append("institution ILIKE %s")
            params.append(f"%{institution}%")
        normalized_tags = _coerce_tags(tags or [])
        if normalized_tags:
            where.append("tags && %s::text[]")
            params.append(normalized_tags)
        if folder_id:
            where.append("folder_id = %s::uuid")
            params.append(folder_id)
        parsed_start_date = _parse_filter_date(start_date)
        parsed_end_date = _parse_filter_date(end_date)
        if parsed_start_date:
            where.append("COALESCE(document_date, created_at::date) >= %s::date")
            params.append(parsed_start_date.isoformat())
        if parsed_end_date:
            where.append("COALESCE(document_date, created_at::date) <= %s::date")
            params.append(parsed_end_date.isoformat())
        if query:
            where.append(
                """
                (
                    file_name ILIKE %s
                    OR metadata->>'title' ILIKE %s
                    OR metadata->>'notes' ILIKE %s
                    OR summary ILIKE %s
                    OR extracted_text ILIKE %s
                    OR document_type ILIKE %s
                    OR institution ILIKE %s
                    OR array_to_string(tags, ' ') ILIKE %s
                )
                """
            )
            pattern = f"%{query}%"
            params.extend([pattern, pattern, pattern, pattern, pattern, pattern, pattern, pattern])
        return " AND ".join(where), params

    def list_documents(
        self,
        *,
        user_id: str,
        query: str = "",
        year: int | None = None,
        month: int | None = None,
        document_type: str = "",
        institution: str = "",
        tags: list[str] | None = None,
        folder_id: str | None = None,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        where_sql, params = self._document_where_clause(
            user_id=user_id,
            query=query,
            year=year,
            month=month,
            document_type=document_type,
            institution=institution,
            tags=tags,
            folder_id=folder_id,
            start_date=start_date,
            end_date=end_date,
        )
        params.append(max(1, min(int(limit or 25), 100)))
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT *
                    FROM documents
                    WHERE {where_sql}
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = cursor.fetchall()
        return [self._row_to_document(row) for row in rows]

    def search_documents(self, *, user_id: str, query: str, limit: int = 10, **filters: Any) -> list[dict[str, Any]]:
        return self.list_documents(user_id=user_id, query=query, limit=limit, **filters)

    def count_documents(self, *, user_id: str, query: str = "", **filters: Any) -> dict[str, Any]:
        where_sql, params = self._document_where_clause(user_id=user_id, query=query, **filters)
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) AS count FROM documents WHERE {where_sql}", tuple(params))
                row = cursor.fetchone()
        return {"user_id": normalize_user_id(user_id), "count": int((row or {}).get("count") or 0)}

    def get_document(self, *, user_id: str, document_id: str, include_signed_url: bool = False) -> dict[str, Any] | None:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM documents WHERE user_id = %s AND document_id = %s::uuid LIMIT 1",
                    (normalize_user_id(user_id), str(document_id or "").strip()),
                )
                row = cursor.fetchone()
        if not row:
            return None
        document = self._row_to_document(row)
        if include_signed_url:
            document["signed_url"] = self._create_signed_url(document)
        return document

    def read_document_bytes(self, *, user_id: str, document_id: str) -> tuple[bytes, dict[str, Any]]:
        document = self.get_document(user_id=user_id, document_id=document_id)
        if not document:
            raise FileNotFoundError("Document not found")
        content = _supabase_download(self._client, bucket=document["storage_bucket"], path=document["storage_key"])
        return content, document

    def summarize_document(self, *, user_id: str, document_id: str) -> dict[str, Any]:
        document = self.get_document(user_id=user_id, document_id=document_id)
        if not document:
            raise FileNotFoundError("Document not found")
        if not document["extracted_text"]:
            return {
                "document": document,
                "summary": "Text konnte nicht extrahiert werden.",
                "used_openai": False,
            }
        summary = self._summarize_with_openai(document["extracted_text"]) or summarize_text_locally(document["extracted_text"])
        return {"document": document, "summary": summary, "used_openai": summary != summarize_text_locally(document["extracted_text"])}

    def classify_document(self, *, user_id: str, document_id: str) -> dict[str, Any]:
        document = self.get_document(user_id=user_id, document_id=document_id)
        if not document:
            raise FileNotFoundError("Document not found")
        if not document["extracted_text"]:
            return {
                "document": document,
                "classification": {
                    "document_type": document.get("document_type") or "image",
                    "institution": document.get("institution") or "",
                    "tags": document.get("tags") or [],
                    "message": "Text konnte nicht extrahiert werden.",
                },
                "used_openai": False,
            }
        classification = self._classify_with_openai(document["extracted_text"]) or classify_text_locally(
            document["extracted_text"],
            document["file_name"],
        )
        classification["document_type"] = normalize_document_type(classification.get("document_type") or "")
        if "facts" not in classification:
            classification["facts"] = extract_structured_facts(document["extracted_text"])
        if not classification.get("document_date"):
            classification["document_date"] = classification["facts"].get("document_date")
        updated = self.update_document_metadata(
            user_id=user_id,
            document_id=document_id,
            updates=classification,
        )
        return {"document": updated, "classification": classification, "used_openai": bool(classification.get("used_openai"))}

    def group_documents(self, *, user_id: str, group_by: str = "month", **filters: Any) -> dict[str, Any]:
        documents = self.list_documents(user_id=user_id, limit=100, **filters)
        normalized_group_by = str(group_by or "month").strip().lower()
        groups: dict[str, dict[str, Any]] = {}
        for document in documents:
            if normalized_group_by == "type":
                key = document.get("document_type") or "unknown"
            elif normalized_group_by == "institution":
                key = document.get("institution") or "unknown"
            elif normalized_group_by == "folder":
                key = document.get("folder_id") or "without_folder"
            else:
                key = f"{int(document.get('year') or 0):04d}-{int(document.get('month') or 0):02d}"
            group = groups.setdefault(key, {"key": key, "count": 0, "documents": []})
            group["count"] += 1
            group["documents"].append(document)
        return {"user_id": normalize_user_id(user_id), "group_by": normalized_group_by, "groups": list(groups.values())}

    def create_folder(
        self,
        *,
        user_id: str,
        name: str,
        parent_folder_id: str | None = None,
        color: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_user_id = normalize_user_id(user_id)
        folder_name = str(name or "").strip()
        if not folder_name:
            raise ValueError("folder name is required")
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM document_folders
                    WHERE user_id = %s
                      AND lower(name) = lower(%s)
                      AND COALESCE(parent_folder_id, '00000000-0000-0000-0000-000000000000'::uuid)
                          = COALESCE(%s::uuid, '00000000-0000-0000-0000-000000000000'::uuid)
                    LIMIT 1
                    """,
                    (normalized_user_id, folder_name, parent_folder_id),
                )
                existing = cursor.fetchone()
                if existing:
                    cursor.execute(
                        """
                        UPDATE document_folders
                        SET color = %s, metadata = %s::jsonb, updated_at = NOW()
                        WHERE folder_id = %s::uuid
                        RETURNING *
                        """,
                        (color, json.dumps(metadata or {}), existing["folder_id"]),
                    )
                    row = cursor.fetchone()
                    return self._row_to_folder(row or existing)

                cursor.execute(
                    """
                    INSERT INTO document_folders (
                        user_id, name, parent_folder_id, color, metadata, created_at, updated_at
                    )
                    VALUES (%s, %s, %s::uuid, %s, %s::jsonb, NOW(), NOW())
                    RETURNING *
                    """,
                    (
                        normalized_user_id,
                        folder_name,
                        parent_folder_id,
                        color,
                        json.dumps(metadata or {}),
                    ),
                )
                row = cursor.fetchone()
        return self._row_to_folder(row or {
            "folder_id": str(uuid.uuid4()),
            "user_id": normalized_user_id,
            "name": folder_name,
            "parent_folder_id": parent_folder_id,
            "color": color,
            "metadata": metadata or {},
            "created_at": _utcnow(),
            "updated_at": _utcnow(),
        })

    def list_folders(self, *, user_id: str) -> list[dict[str, Any]]:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM document_folders WHERE user_id = %s ORDER BY lower(name)",
                    (normalize_user_id(user_id),),
                )
                rows = cursor.fetchall()
        return [self._row_to_folder(row) for row in rows]

    def move_document_to_folder(self, *, user_id: str, document_id: str, folder_id: str | None = None) -> dict[str, Any]:
        normalized_user_id = normalize_user_id(user_id)
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                if folder_id:
                    cursor.execute(
                        "SELECT folder_id FROM document_folders WHERE user_id = %s AND folder_id = %s::uuid LIMIT 1",
                        (normalized_user_id, folder_id),
                    )
                    if not cursor.fetchone():
                        raise FileNotFoundError("Folder not found")
                cursor.execute(
                    """
                    UPDATE documents
                    SET folder_id = %s::uuid, updated_at = NOW()
                    WHERE user_id = %s AND document_id = %s::uuid
                    RETURNING *
                    """,
                    (folder_id, normalized_user_id, str(document_id or "").strip()),
                )
                row = cursor.fetchone()
        if not row:
            raise FileNotFoundError("Document not found")
        return self._row_to_document(row)

    def move_documents_to_folder(self, *, user_id: str, document_ids: list[str], folder_id: str | None = None) -> list[dict[str, Any]]:
        moved_documents = []
        normalized_ids = [str(document_id or "").strip() for document_id in document_ids]
        for document_id in normalized_ids:
            if document_id:
                moved_documents.append(
                    self.move_document_to_folder(user_id=user_id, document_id=document_id, folder_id=folder_id)
                )
        return moved_documents

    def update_document_metadata(self, *, user_id: str, document_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        existing = self.get_document(user_id=user_id, document_id=document_id)
        if not existing:
            raise FileNotFoundError("Document not found")
        allowed = {
            "title",
            "notes",
            "document_type",
            "institution",
            "document_date",
            "tags",
            "summary",
            "metadata",
        }
        clean_updates = {key: updates[key] for key in allowed if key in updates}
        metadata = dict(existing.get("metadata") or {})
        metadata.update(_coerce_json_dict(clean_updates.get("metadata")))
        if "title" in clean_updates:
            metadata["title"] = str(clean_updates.get("title") or "").strip()
        if "notes" in clean_updates:
            metadata["notes"] = str(clean_updates.get("notes") or "").strip()
        tags = _coerce_tags(clean_updates.get("tags", existing.get("tags") or []))
        next_document_type = normalize_document_type(clean_updates.get("document_type", existing.get("document_type") or ""))
        next_document_date = _parse_filter_date(clean_updates.get("document_date", existing.get("document_date")))
        next_year = next_document_date.year if next_document_date else int(existing.get("year") or 0)
        next_month = next_document_date.month if next_document_date else int(existing.get("month") or 0)
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE documents
                    SET
                        document_type = %s,
                        institution = %s,
                        document_date = %s::date,
                        year = %s,
                        month = %s,
                        tags = %s::text[],
                        summary = %s,
                        metadata = %s::jsonb,
                        updated_at = NOW()
                    WHERE user_id = %s AND document_id = %s::uuid
                    RETURNING *
                    """,
                    (
                        next_document_type,
                        clean_updates.get("institution", existing.get("institution") or ""),
                        next_document_date.isoformat() if next_document_date else None,
                        next_year,
                        next_month,
                        tags,
                        clean_updates.get("summary", existing.get("summary") or ""),
                        json.dumps(metadata),
                        normalize_user_id(user_id),
                        str(document_id or "").strip(),
                    ),
                )
                row = cursor.fetchone()
        if not row:
            raise FileNotFoundError("Document not found")
        return self._row_to_document(row)

    def delete_document(self, *, user_id: str, document_id: str) -> dict[str, Any]:
        document = self.get_document(user_id=user_id, document_id=document_id)
        if not document:
            raise FileNotFoundError("Document not found")
        try:
            self._client.storage.from_(document["storage_bucket"]).remove([document["storage_key"]])
        except Exception as exc:
            raise RuntimeError("Could not delete document object from Supabase Storage") from exc
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM documents WHERE user_id = %s AND document_id = %s::uuid",
                    (normalize_user_id(user_id), document_id),
                )
        return {"deleted": True, "document": document}

    def match_documents(self, *, user_id: str, document_id: str, limit: int = 5) -> dict[str, Any]:
        source = self.get_document(user_id=user_id, document_id=document_id)
        if not source:
            raise FileNotFoundError("Document not found")
        candidates = self.list_documents(user_id=user_id, limit=100)
        source_tokens = set(re.findall(r"[a-z0-9]{4,}", source.get("extracted_text", "").lower()))
        matches = []
        for candidate in candidates:
            if candidate["document_id"] == source["document_id"]:
                continue
            score = 0.0
            reasons = []
            if source.get("document_type") and source.get("document_type") == candidate.get("document_type"):
                score += 0.25
                reasons.append("same document type")
            if source.get("institution") and source.get("institution") == candidate.get("institution"):
                score += 0.3
                reasons.append("same institution")
            candidate_tokens = set(re.findall(r"[a-z0-9]{4,}", candidate.get("extracted_text", "").lower()))
            if source_tokens and candidate_tokens:
                overlap = len(source_tokens & candidate_tokens) / max(1, len(source_tokens | candidate_tokens))
                score += min(0.45, overlap)
                if overlap:
                    reasons.append("text overlap")
            if score > 0:
                matches.append({"document": candidate, "score": round(score, 3), "reason": ", ".join(reasons)})
        matches = sorted(matches, key=lambda item: item["score"], reverse=True)[: max(1, min(int(limit or 5), 20))]
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                for match in matches:
                    cursor.execute(
                        """
                        INSERT INTO document_matches (
                            user_id, source_document_id, target_document_id, match_type, score, reason, metadata, created_at
                        )
                        VALUES (%s, %s::uuid, %s::uuid, 'related', %s, %s, '{}'::jsonb, NOW())
                        ON CONFLICT (source_document_id, target_document_id, match_type)
                        DO UPDATE SET score = EXCLUDED.score, reason = EXCLUDED.reason
                        """,
                        (
                            normalize_user_id(user_id),
                            source["document_id"],
                            match["document"]["document_id"],
                            match["score"],
                            match["reason"],
                        ),
                    )
        return {"document": source, "matches": matches, "count": len(matches)}

    def _summarize_with_openai(self, text: str) -> str:
        if not text.strip() or not os.environ.get("OPENAI_API_KEY", "").strip():
            return ""
        if str(os.environ.get("IV_AGENT_DISABLE_DOCUMENT_OPENAI", "")).strip().lower() in {"1", "true", "yes"}:
            return ""
        try:
            from openai import OpenAI  # type: ignore

            client = OpenAI()
            response = client.responses.create(
                model=DOCUMENT_AGENT_MODEL,
                input=(
                    "Fasse dieses Dokument fuer eine IV-Assistenz-Verwaltungsapp knapp auf Deutsch zusammen. "
                    "Nenne nur belegbare Punkte.\n\n"
                    f"{text[:12000]}"
                ),
                max_output_tokens=350,
            )
            return _extract_openai_text(response)
        except Exception:
            return ""

    def _classify_with_openai(self, text: str) -> dict[str, Any]:
        if not text.strip() or not os.environ.get("OPENAI_API_KEY", "").strip():
            return {}
        if str(os.environ.get("IV_AGENT_DISABLE_DOCUMENT_OPENAI", "")).strip().lower() in {"1", "true", "yes"}:
            return {}
        try:
            from openai import OpenAI  # type: ignore

            client = OpenAI()
            response = client.responses.create(
                model=DOCUMENT_AGENT_MODEL,
                input=(
                    "Klassifiziere dieses Dokument als JSON mit den Keys document_type, institution, "
                    "document_date, tags. Wenn unsicher, verwende leere Strings oder [].\n\n"
                    f"{text[:12000]}"
                ),
                max_output_tokens=300,
            )
            parsed = json.loads(_extract_openai_text(response))
            if isinstance(parsed, dict):
                parsed["used_openai"] = True
                return parsed
        except Exception:
            return {}
        return {}


def _extract_openai_text(response: Any) -> str:
    output_text = getattr(response, "output_text", None)
    if output_text:
        return str(output_text).strip()
    parts = []
    for item in getattr(response, "output", []) or []:
        for content in getattr(item, "content", []) or []:
            text = getattr(content, "text", None)
            if text:
                parts.append(str(text))
    return "\n".join(parts).strip()


def get_storage_service() -> StorageService:
    cache_key = (
        os.environ.get("DATABASE_URL", "").strip(),
        os.environ.get("SUPABASE_URL", "").strip(),
        os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "").strip(),
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip(),
        document_bucket_name(),
        id(StorageService),
    )
    if cache_key not in _SERVICE_CACHE:
        _SERVICE_CACHE[cache_key] = StorageService()
    return _SERVICE_CACHE[cache_key]


def upload_document(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return get_storage_service().upload_document(*args, **kwargs)


def list_documents(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    return get_storage_service().list_documents(*args, **kwargs)


def search_documents(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    return get_storage_service().search_documents(*args, **kwargs)


def count_documents(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return get_storage_service().count_documents(*args, **kwargs)


def get_document(*args: Any, **kwargs: Any) -> dict[str, Any] | None:
    return get_storage_service().get_document(*args, **kwargs)


def summarize_document(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return get_storage_service().summarize_document(*args, **kwargs)


def classify_document(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return get_storage_service().classify_document(*args, **kwargs)


def group_documents(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return get_storage_service().group_documents(*args, **kwargs)


def create_folder(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return get_storage_service().create_folder(*args, **kwargs)


def list_folders(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    return get_storage_service().list_folders(*args, **kwargs)


def move_document_to_folder(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return get_storage_service().move_document_to_folder(*args, **kwargs)


def move_documents_to_folder(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    return get_storage_service().move_documents_to_folder(*args, **kwargs)


def update_document_metadata(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return get_storage_service().update_document_metadata(*args, **kwargs)


def delete_document(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return get_storage_service().delete_document(*args, **kwargs)


def match_documents(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return get_storage_service().match_documents(*args, **kwargs)


def prepare_document_for_agent(document: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "document",
        "document_id": document["document_id"],
        "name": document.get("file_name") or document.get("safe_file_name"),
        "file_name": document.get("file_name") or document.get("safe_file_name"),
        "title": document.get("title") or document.get("file_name") or document.get("safe_file_name"),
        "mime": document.get("content_type"),
        "size": document.get("content_size"),
        "document_type": document.get("document_type"),
        "institution": document.get("institution"),
        "document_date": document.get("document_date"),
        "tags": document.get("tags") or [],
        "summary": document.get("summary") or "",
        "facts": (document.get("metadata") or {}).get("classification", {}).get("facts", {}),
        "text_preview": _safe_text_preview(document.get("extracted_text") or ""),
        "extraction_status": document.get("extraction_status"),
        "extraction_error": document.get("extraction_error"),
        "storage_backend": "supabase",
        "storage_bucket": document.get("storage_bucket"),
        "storage_key": document.get("storage_key"),
    }


def process_chat_attachments(
    attachments: list[dict[str, Any]],
    *,
    user_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    sanitized_attachments: list[dict[str, Any]] = []
    uploaded_documents: list[dict[str, Any]] = []
    service = get_storage_service() if any(item.get("content_base64") for item in attachments if isinstance(item, dict)) else None

    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue
        if attachment.get("content_base64"):
            content = _decode_attachment_content(attachment)
            file_name = str(attachment.get("file_name") or attachment.get("name") or "document").strip()
            content_type = normalize_document_mime_type(file_name, attachment.get("mime") or attachment.get("content_type"))
            document = service.upload_document(
                user_id=user_id,
                file_name=file_name,
                content=content,
                content_type=content_type,
                metadata={
                    "source": "chat_attachment",
                    "client_attachment_name": attachment.get("name") or file_name,
                },
            )
            uploaded_documents.append(document)
            sanitized_attachments.append(prepare_document_for_agent(document))
            continue

        safe_attachment = {
            key: value
            for key, value in attachment.items()
            if key not in {"content_base64", "base64", "data", "bytes"}
        }
        sanitized_attachments.append(safe_attachment)

    return sanitized_attachments, uploaded_documents
