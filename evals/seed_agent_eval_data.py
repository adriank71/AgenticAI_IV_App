"""Seed synthetic data for the IV agent evaluation cases.

The seed is idempotent and only deletes/replaces rows marked with
metadata.source = "agent_eval".
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import iv_agent  # noqa: F401 - loads .env.local
from iv_agent.services.calendar_service import create_calendar_event, get_calendar_event_store
from iv_agent.storage import _connect_postgres


EVAL_NAMESPACE = uuid.UUID("35a8b365-c051-4a2e-9153-869f14ff6398")


def stable_uuid(name: str) -> str:
    return str(uuid.uuid5(EVAL_NAMESPACE, name))


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def checksum(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def database_url() -> str:
    return os.environ.get("DATABASE_URL", "").strip()


def ensure_documents_schema(connection: Any) -> None:
    with connection.cursor() as cursor:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS documents (
                document_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id TEXT NOT NULL,
                folder_id UUID,
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
                extraction_status TEXT NOT NULL DEFAULT 'completed',
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


def reset_eval_calendar(profile_id: str) -> None:
    if not database_url():
        return
    with _connect_postgres(database_url()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM calendar_events
                WHERE user_id = %s
                  AND metadata->>'source' = 'agent_eval'
                """,
                (profile_id,),
            )


def seed_calendar(profile_id: str, timezone_name: str) -> None:
    get_calendar_event_store()
    reset_eval_calendar(profile_id)
    events = [
        {
            "id": stable_uuid(f"{profile_id}:therapy-delete"),
            "title": "Therapie",
            "description": "Synthetischer Eval-Termin fuer CAL_DELETE_001.",
            "start_at": "2026-05-20T09:00:00+02:00",
            "end_at": "2026-05-20T10:00:00+02:00",
            "category": "other",
            "metadata": {"source": "agent_eval", "case": "CAL_DELETE_001"},
        },
        {
            "id": stable_uuid(f"{profile_id}:assistant-1"),
            "title": "Assistenz zuhause",
            "description": "Synthetischer Eval-Termin fuer CAL_COUNT_001.",
            "start_at": "2026-05-06T14:00:00+02:00",
            "end_at": "2026-05-06T16:00:00+02:00",
            "category": "assistant",
            "metadata": {
                "source": "agent_eval",
                "case": "CAL_COUNT_001",
                "assistant_hours": {"koerperpflege": 2.0},
            },
        },
        {
            "id": stable_uuid(f"{profile_id}:transport-1"),
            "title": "TixiTaxi Fahrt",
            "description": "Synthetischer Eval-Termin fuer CAL_COUNT_001.",
            "start_at": "2026-05-13T08:30:00+02:00",
            "end_at": "2026-05-13T09:00:00+02:00",
            "category": "transport",
            "location": "St. Gallen -> Appenzell",
            "metadata": {
                "source": "agent_eval",
                "case": "CAL_COUNT_001",
                "transport_mode": "fahrdienst",
                "transport_kilometers": 17,
                "transport_address": "St. Gallen -> Appenzell",
            },
        },
    ]
    for event in events:
        create_calendar_event(event, user_id=profile_id, timezone_name=timezone_name)


def document_rows(profile_id: str) -> list[dict[str, Any]]:
    now = utcnow()
    rows = [
        {
            "document_id": stable_uuid(f"{profile_id}:doc-1"),
            "file_name": "doc-1-iv-brief-naechster-schritt.txt",
            "safe_file_name": "doc-1-iv-brief-naechster-schritt.txt",
            "storage_bucket": "IV",
            "storage_key": f"Documents/{profile_id}/2026/05/{stable_uuid(f'{profile_id}:doc-1')}-iv-brief.txt",
            "content_type": "text/plain",
            "document_type": "letter",
            "institution": "IV-Stelle St. Gallen",
            "document_date": "2026-05-15",
            "year": 2026,
            "month": 5,
            "tags": ["iv", "brief", "naechster_schritt"],
            "summary": "IV-Brief: Naechster Schritt ist das Einreichen der Unterlagen bis 31. Mai 2026.",
            "extracted_text": (
                "IV-Stelle St. Gallen\n"
                "Betreff: Abklaerung Assistenzbeitrag\n\n"
                "Naechster Schritt: Bitte reichen Sie die fehlenden medizinischen Unterlagen "
                "und die unterschriebene Vollmacht bis 31. Mai 2026 ein. Danach pruefen wir "
                "den Anspruch auf IV-Leistungen weiter."
            ),
            "metadata": {
                "source": "agent_eval",
                "title": "doc-1 IV Brief naechster Schritt",
                "bucket_confirmed": True,
                "classification": {"document_type": "letter", "institution": "IV-Stelle St. Gallen", "facts": {}},
            },
            "created_at": now,
            "updated_at": now,
        },
        {
            "document_id": stable_uuid(f"{profile_id}:tixitaxi-invoice-may"),
            "file_name": "tixitaxi-rechnung-mai-2026.txt",
            "safe_file_name": "tixitaxi-rechnung-mai-2026.txt",
            "storage_bucket": "TixiTaxi",
            "storage_key": f"Documents/{profile_id}/2026/05/{stable_uuid(f'{profile_id}:tixitaxi-invoice-may')}-tixitaxi.txt",
            "content_type": "text/plain",
            "document_type": "invoice",
            "institution": "TixiTaxi",
            "document_date": "2026-05-20",
            "year": 2026,
            "month": 5,
            "tags": ["rechnung", "transport", "tixitaxi"],
            "summary": "TixiTaxi Rechnung Mai 2026, Total CHF 68.40.",
            "extracted_text": "TixiTaxi Rechnung Mai 2026\nFahrdienst St. Gallen - Appenzell\nTotal CHF 68.40\n",
            "metadata": {
                "source": "agent_eval",
                "title": "TixiTaxi Rechnung Mai 2026",
                "bucket_confirmed": True,
                "invoice_fields": {"total": "68.40", "currency": "CHF", "confidence": "high"},
                "classification": {"document_type": "invoice", "institution": "TixiTaxi", "facts": {"amount": "CHF 68.40"}},
            },
            "created_at": now,
            "updated_at": now,
        },
        {
            "document_id": stable_uuid(f"{profile_id}:iv-invoice-may"),
            "file_name": "iv-rechnung-mai-2026.txt",
            "safe_file_name": "iv-rechnung-mai-2026.txt",
            "storage_bucket": "IV",
            "storage_key": f"Documents/{profile_id}/2026/05/{stable_uuid(f'{profile_id}:iv-invoice-may')}-iv-rechnung.txt",
            "content_type": "text/plain",
            "document_type": "invoice",
            "institution": "IV-Stelle",
            "document_date": "2026-05-21",
            "year": 2026,
            "month": 5,
            "tags": ["rechnung", "iv"],
            "summary": "IV Rechnung Mai 2026, Total CHF 120.00.",
            "extracted_text": "IV Rechnung Mai 2026\nAssistenzbezogene Unterlage\nTotal CHF 120.00\n",
            "metadata": {
                "source": "agent_eval",
                "title": "IV Rechnung Mai 2026",
                "bucket_confirmed": True,
                "invoice_fields": {"total": "120.00", "currency": "CHF", "confidence": "high"},
                "classification": {"document_type": "invoice", "institution": "IV-Stelle", "facts": {"amount": "CHF 120.00"}},
            },
            "created_at": now,
            "updated_at": now,
        },
    ]
    for row in rows:
        row["user_id"] = profile_id
        row["storage_url"] = f"eval://{row['storage_key']}"
        row["content_size"] = len(row["extracted_text"].encode("utf-8"))
        row["checksum_sha256"] = checksum(row["extracted_text"])
        row["extraction_status"] = "completed"
        row["extraction_error"] = None
    return rows


def seed_documents(profile_id: str) -> int:
    if not database_url():
        raise RuntimeError("DATABASE_URL is required to seed synthetic document metadata.")
    rows = document_rows(profile_id)
    with _connect_postgres(database_url()) as connection:
        ensure_documents_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM documents
                WHERE user_id = %s
                  AND metadata->>'source' = 'agent_eval'
                """,
                (profile_id,),
            )
            for row in rows:
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
                    """,
                    (
                        row["document_id"],
                        row["user_id"],
                        row["file_name"],
                        row["safe_file_name"],
                        row["storage_bucket"],
                        row["storage_key"],
                        row["storage_url"],
                        row["content_type"],
                        row["content_size"],
                        row["checksum_sha256"],
                        row["document_type"],
                        row["institution"],
                        row["document_date"],
                        row["year"],
                        row["month"],
                        row["tags"],
                        row["summary"],
                        row["extracted_text"],
                        row["extraction_status"],
                        row["extraction_error"],
                        json.dumps(row["metadata"]),
                        row["created_at"].isoformat(),
                        row["updated_at"].isoformat(),
                    ),
                )
    return len(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed synthetic IV agent evaluation data.")
    parser.add_argument("--profile-id", default="default", help="Profile to seed. Defaults to the evaluation plan's default profile_id.")
    parser.add_argument("--timezone", default="Europe/Berlin", help="Timezone for calendar seed data.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    seed_calendar(args.profile_id, args.timezone)
    documents = seed_documents(args.profile_id)
    print(f"Seeded synthetic eval data for profile_id={args.profile_id}: calendar_events=3 documents={documents}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
