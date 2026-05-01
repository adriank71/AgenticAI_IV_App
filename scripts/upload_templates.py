"""One-off uploader for PDF templates into Supabase Storage.

Reads .env.local, then uploads the three Formular templates to the
SUPABASE_STORAGE_TEMPLATES_BUCKET bucket and registers them in the
document_templates table so the Flask app can resolve them via
template_store_reference().
"""

from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from iv_agent.migrate_local_data import load_env_file
from iv_agent.storage import (
    SUPABASE_TEMPLATE_FILES,
    _connect_postgres,
    _create_supabase_client,
    _guess_content_type,
    _supabase_templates_bucket,
)


TEMPLATE_ENV_KEYS = {
    "stundenblatt": "IV_AGENT_STUNDENBLATT_PDF",
    "rechnung": "IV_AGENT_RECHNUNG_PDF",
    "transportkosten": "IV_AGENT_TRANSPORTKOSTEN_PDF",
}


def resolve_template_paths() -> dict[str, Path]:
    resolved: dict[str, Path] = {}
    for template_key, env_key in TEMPLATE_ENV_KEYS.items():
        env_value = os.environ.get(env_key, "").strip()
        candidates = [env_value, str(PROJECT_ROOT / SUPABASE_TEMPLATE_FILES[template_key])]
        for candidate in candidates:
            if not candidate:
                continue
            path = Path(candidate)
            if path.exists() and path.is_file():
                resolved[template_key] = path
                break
        if template_key not in resolved:
            print(f"  MISSING: {template_key} (set {env_key} or place file in project root)")
    return resolved


def upload_one(client, *, bucket: str, template_key: str, path: Path) -> dict:
    file_name = SUPABASE_TEMPLATE_FILES.get(template_key, path.name)
    storage_key = f"{template_key}/{file_name}"
    content = path.read_bytes()
    content_type = _guess_content_type(file_name, fallback="application/pdf")

    client.storage.from_(bucket).upload(
        path=storage_key,
        file=content,
        file_options={
            "content-type": content_type,
            "cache-control": "3600",
            "upsert": "true",
        },
    )
    return {
        "template_key": template_key,
        "file_name": file_name,
        "storage_key": storage_key,
        "content_type": content_type,
        "content_size": len(content),
        "checksum": hashlib.sha256(content).hexdigest(),
    }


def register_in_db(database_url: str, bucket: str, records: list[dict]) -> None:
    if not records:
        return
    with _connect_postgres(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS document_templates (
                    template_key TEXT PRIMARY KEY,
                    file_name TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    content_size BIGINT NOT NULL DEFAULT 0,
                    content BYTEA NOT NULL DEFAULT ''::bytea,
                    checksum_sha256 TEXT NOT NULL DEFAULT '',
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    storage_backend TEXT NOT NULL DEFAULT 'supabase',
                    storage_key TEXT,
                    storage_url TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            for record in records:
                cursor.execute(
                    """
                    INSERT INTO document_templates (
                        template_key, file_name, content_type, content_size,
                        checksum_sha256, metadata, storage_backend, storage_key,
                        storage_url, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, '{}'::jsonb, 'supabase', %s, %s, NOW(), NOW())
                    ON CONFLICT (template_key)
                    DO UPDATE SET
                        file_name = EXCLUDED.file_name,
                        content_type = EXCLUDED.content_type,
                        content_size = EXCLUDED.content_size,
                        checksum_sha256 = EXCLUDED.checksum_sha256,
                        storage_backend = EXCLUDED.storage_backend,
                        storage_key = EXCLUDED.storage_key,
                        storage_url = EXCLUDED.storage_url,
                        updated_at = NOW()
                    """,
                    (
                        record["template_key"],
                        record["file_name"],
                        record["content_type"],
                        record["content_size"],
                        record["checksum"],
                        record["storage_key"],
                        f"supabase://{bucket}/{record['storage_key']}",
                    ),
                )


def main() -> int:
    load_env_file(override=True)

    if not os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip():
        print("SUPABASE_SERVICE_ROLE_KEY is not set.")
        return 2

    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        print("DATABASE_URL is required.")
        return 2

    bucket = _supabase_templates_bucket()
    print(f"Templates bucket: {bucket}")

    paths = resolve_template_paths()
    if not paths:
        print("No template files found. Aborting.")
        return 1

    client = _create_supabase_client()
    records = []
    for template_key, path in paths.items():
        print(f"Uploading {template_key}: {path.name} ({path.stat().st_size} bytes)")
        records.append(upload_one(client, bucket=bucket, template_key=template_key, path=path))

    register_in_db(database_url, bucket, records)
    print(f"Done. Uploaded {len(records)} template(s) to bucket '{bucket}'.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
