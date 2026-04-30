import argparse
import json
import mimetypes
import os
from pathlib import Path
from typing import Any

try:
    from .storage import PostgresInvoiceCaptureStore, PostgresTemplateStore, _blob_token
except ImportError:
    from storage import PostgresInvoiceCaptureStore, PostgresTemplateStore, _blob_token


TEMPLATE_MAPPINGS = {
    "stundenblatt": "Stundenblatt.pdf",
    "rechnung": "Rechnungsvorlage_aL_elektronisch (1).pdf",
}


def _load_vercel_blob_client(token: str):
    try:
        from vercel.blob import BlobClient  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "Blob migration requires the vercel Python package. Install dependencies from requirements.txt."
        ) from exc
    return BlobClient(token=token)


def _load_env_file(path: str = ".env.local") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _blob_content(result: Any, blob_key: str) -> tuple[bytes, str]:
    if result is None or getattr(result, "status_code", 200) != 200:
        raise FileNotFoundError(blob_key)
    content = getattr(result, "content", None)
    if content is None:
        raise FileNotFoundError(blob_key)
    content_type = (
        getattr(result, "content_type", None)
        or getattr(result, "contentType", None)
        or mimetypes.guess_type(blob_key)[0]
        or "application/octet-stream"
    )
    return bytes(content), content_type


def _read_blob(client: Any, blob_key: str) -> tuple[bytes, str]:
    return _blob_content(client.get(blob_key, access="private"), blob_key)


def migrate_templates(client: Any, template_store: PostgresTemplateStore) -> int:
    migrated = 0
    for template_key, blob_key in TEMPLATE_MAPPINGS.items():
        content, content_type = _read_blob(client, blob_key)
        template_store.upsert_template(
            template_key=template_key,
            file_name=os.path.basename(blob_key),
            content=content,
            content_type=content_type or "application/pdf",
            metadata={"source_backend": "vercel_blob", "source_key": blob_key},
        )
        migrated += 1
    return migrated


def _iter_invoice_metadata_keys(client: Any, prefix: str) -> list[str]:
    keys: list[str] = []
    for blob in client.iter_objects(prefix=prefix):
        pathname = getattr(blob, "pathname", None) or (blob.get("pathname") if isinstance(blob, dict) else None)
        if pathname and str(pathname).endswith(".json"):
            keys.append(str(pathname))
    return sorted(keys)


def migrate_invoice_captures(client: Any, invoice_store: PostgresInvoiceCaptureStore, prefix: str = "Invoices/") -> int:
    migrated = 0
    for metadata_key in _iter_invoice_metadata_keys(client, prefix):
        metadata_bytes, _ = _read_blob(client, metadata_key)
        record = json.loads(metadata_bytes.decode("utf-8"))
        storage_key = str(record.get("storage_key") or "").strip()
        if not storage_key:
            continue
        content, content_type = _read_blob(client, storage_key)
        if not record.get("content_type"):
            record["content_type"] = content_type
        if invoice_store.upsert_capture_record(record, content=content, overwrite=False):
            migrated += 1
    return migrated


def migrate_blob_to_supabase(*, database_url: str, blob_token: str | None = None) -> dict[str, int]:
    token = blob_token or _blob_token()
    client = _load_vercel_blob_client(token)
    template_store = PostgresTemplateStore(database_url)
    invoice_store = PostgresInvoiceCaptureStore(database_url)
    return {
        "templates": migrate_templates(client, template_store),
        "invoice_captures": migrate_invoice_captures(client, invoice_store),
        "reports": 0,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Migrate Vercel Blob templates and invoice captures into Postgres.")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", ""), help="Target Supabase Postgres URL")
    parser.add_argument("--env-file", default=".env.local", help="Optional local env file to load before migrating")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    _load_env_file(args.env_file)
    database_url = args.database_url or os.environ.get("DATABASE_URL", "")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required for migration.")
    summary = migrate_blob_to_supabase(database_url=database_url)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
