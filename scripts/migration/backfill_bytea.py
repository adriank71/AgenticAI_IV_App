"""Phase 0a — Backfill: BYTEA-Inhalte aus Neon in Supabase Storage Buckets verschieben.

Nur ausfuehren, wenn audit_bytea.sql `rows_with_bytes > 0` zeigt.

Liest aus Neon (NEON_DATABASE_URL), laedt nach Supabase Storage hoch,
setzt storage_backend/storage_key/storage_url in der Neon-DB und leert content.
Idempotent: ueberspringt Zeilen, die bereits storage_url + leere content haben.

Usage:
    NEON_DATABASE_URL=postgres://... \\
    SUPABASE_URL=https://... \\
    SUPABASE_SERVICE_ROLE_KEY=... \\
    SUPABASE_STORAGE_INVOICES_BUCKET=iv-agent-invoices \\
    SUPABASE_STORAGE_TEMPLATES_BUCKET=iv-agent-templates \\
    python scripts/migration/backfill_bytea.py [--dry-run]
"""
from __future__ import annotations

import hashlib
import os
import sys
from typing import Iterator

import psycopg
from psycopg.rows import dict_row
from supabase import create_client


def _env(name: str, *fallbacks: str) -> str:
    for candidate in (name, *fallbacks):
        value = os.environ.get(candidate, "").strip()
        if value:
            return value
    if fallbacks:
        sys.exit(f"missing required env var: {name} (or {', '.join(fallbacks)})")
    sys.exit(f"missing required env var: {name}")


def _supabase_public_url(supabase_url: str, bucket: str, path: str) -> str:
    return f"{supabase_url.rstrip('/')}/storage/v1/object/public/{bucket}/{path}"


def _stream_rows(conn: psycopg.Connection, table: str) -> Iterator[dict]:
    with conn.cursor(name=f"{table}_cursor", row_factory=dict_row) as cur:
        cur.itersize = 50
        cur.execute(
            f"""
            SELECT *
            FROM {table}
            WHERE octet_length(content) > 0
              AND (storage_url IS NULL OR storage_url = ''
                   OR storage_key IS NULL OR storage_key = '')
            """
        )
        for row in cur:
            yield row


def _upload(client, bucket: str, path: str, content: bytes, content_type: str) -> None:
    try:
        client.storage.from_(bucket).upload(
            path=path,
            file=content,
            file_options={"content-type": content_type, "upsert": "true"},
        )
    except Exception as exc:
        msg = str(exc).lower()
        if "duplicate" in msg or "already exists" in msg:
            client.storage.from_(bucket).update(
                path=path,
                file=content,
                file_options={"content-type": content_type, "upsert": "true"},
            )
        else:
            raise


def backfill_invoice_captures(conn: psycopg.Connection, sb, bucket: str, dry_run: bool) -> int:
    moved = 0
    for row in _stream_rows(conn, "invoice_captures"):
        invoice_id = row["invoice_id"]
        content = bytes(row["content"]) if row["content"] else b""
        if not content:
            continue
        sid = row.get("sid") or "unknown"
        file_name = row.get("file_name") or f"{invoice_id}.bin"
        content_type = row.get("content_type") or "application/octet-stream"
        storage_key = "/".join(p for p in ("invoices", sid, f"{invoice_id}_{file_name}") if p)
        storage_url = _supabase_public_url(sb.supabase_url, bucket, storage_key)
        print(f"  invoice_captures {invoice_id} -> {bucket}/{storage_key} ({len(content)} bytes)")
        if dry_run:
            moved += 1
            continue
        _upload(sb, bucket, storage_key, content, content_type)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE invoice_captures
                SET storage_backend = 'supabase',
                    storage_bucket = %s,
                    storage_key = %s,
                    storage_url = %s,
                    content = ''::bytea
                WHERE invoice_id = %s
                """,
                (bucket, storage_key, storage_url, invoice_id),
            )
        conn.commit()
        moved += 1
    return moved


def backfill_document_templates(conn: psycopg.Connection, sb, bucket: str, dry_run: bool) -> int:
    moved = 0
    for row in _stream_rows(conn, "document_templates"):
        template_key = row["template_key"]
        content = bytes(row["content"]) if row["content"] else b""
        if not content:
            continue
        file_name = row.get("file_name") or f"{template_key}.bin"
        content_type = row.get("content_type") or "application/octet-stream"
        storage_key = "/".join(p for p in ("templates", template_key, file_name) if p)
        storage_url = _supabase_public_url(sb.supabase_url, bucket, storage_key)
        checksum = hashlib.sha256(content).hexdigest()
        print(f"  document_templates {template_key} -> {bucket}/{storage_key} ({len(content)} bytes)")
        if dry_run:
            moved += 1
            continue
        _upload(sb, bucket, storage_key, content, content_type)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE document_templates
                SET storage_backend = 'supabase',
                    storage_key = %s,
                    storage_url = %s,
                    checksum_sha256 = COALESCE(NULLIF(checksum_sha256, ''), %s),
                    content = ''::bytea
                WHERE template_key = %s
                """,
                (storage_key, storage_url, checksum, template_key),
            )
        conn.commit()
        moved += 1
    return moved


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    neon_url = _env("NEON_DATABASE_URL", "DATABASE_URL_UNPOOLED", "POSTGRES_URL_NON_POOLING")
    supabase_url = _env("SUPABASE_URL")
    service_key = _env("SUPABASE_SERVICE_ROLE_KEY")
    invoices_bucket = _env("SUPABASE_STORAGE_INVOICES_BUCKET")
    templates_bucket = _env("SUPABASE_STORAGE_TEMPLATES_BUCKET")

    sb = create_client(supabase_url, service_key)
    sb.supabase_url = supabase_url

    print(f"{'DRY RUN — ' if dry_run else ''}reading from Neon, writing to Supabase Storage")
    with psycopg.connect(neon_url) as conn:
        invoices = backfill_invoice_captures(conn, sb, invoices_bucket, dry_run)
        templates = backfill_document_templates(conn, sb, templates_bucket, dry_run)
    print(f"\nresult: {invoices} invoice_captures, {templates} document_templates moved")
    if dry_run:
        print("(dry run — no changes written)")


if __name__ == "__main__":
    main()
