import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from .calendar_manager import ASSISTANT_HOUR_FIELDS, _normalize_event
from .reminders import DEFAULT_TIMEZONE
from .storage import (
    SUPABASE_TEMPLATE_FILES,
    _connect_postgres,
    _create_supabase_client,
    _guess_content_type,
    _supabase_invoices_bucket,
    _supabase_reports_bucket,
    _supabase_templates_bucket,
)


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DATA_DIR = BASE_DIR / "data"
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env.local"
PROFILE_PATH = DATA_DIR / "profile.json"
CALENDAR_PATH = DATA_DIR / "calendar.json"
REMINDERS_PATH = DATA_DIR / "reminders.json"

TABLES = (
    "profiles",
    "events",
    "reminders",
    "reports",
    "invoice_captures",
    "document_templates",
)

BUCKET_PURPOSES = {
    "templates": _supabase_templates_bucket,
    "reports": _supabase_reports_bucket,
    "invoices": _supabase_invoices_bucket,
}


def load_env_file(path: Path | str = DEFAULT_ENV_PATH, *, override: bool = False) -> dict[str, str]:
    path = Path(path)
    loaded: dict[str, str] = {}
    if not path.exists():
        return loaded

    with path.open("r", encoding="utf-8-sig") as file:
        for raw_line in file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key.startswith("export "):
                key = key[7:].strip()
            if not key or any(char.isspace() for char in key):
                continue
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            loaded[key] = value
            if override or key not in os.environ:
                os.environ[key] = value
    return loaded


def read_json(path: Path, fallback: Any) -> Any:
    if not path.exists():
        return fallback
    with path.open("r", encoding="utf-8-sig") as file:
        return json.load(file)


def digest(value: Any) -> str:
    body = json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def short_digest(value: Any) -> str:
    return digest(value)[:12]


def print_local_summary(profile: dict[str, Any], events: list[dict[str, Any]], reminders: list[dict[str, Any]]) -> None:
    print("Local seed summary:")
    print(f"  profiles: 1 hash={short_digest(profile)}")
    print(f"  events: {len(events)} hash={short_digest(events)}")
    print(f"  reminders: {len(reminders)} hash={short_digest(reminders)}")


def ensure_tables(database_url: str, *, dry_run: bool = False) -> None:
    if dry_run:
        print("Dry run: would ensure Supabase tables exist.")
        return

    with _connect_postgres(database_url) as connection:
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
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY,
                    event_date DATE NOT NULL,
                    start_time TEXT NOT NULL DEFAULT '',
                    end_time TEXT NOT NULL DEFAULT '',
                    all_day BOOLEAN NOT NULL DEFAULT FALSE,
                    category TEXT NOT NULL,
                    title TEXT NOT NULL,
                    notes TEXT NOT NULL DEFAULT '',
                    hours DOUBLE PRECISION NOT NULL DEFAULT 0,
                    assistant_hours JSONB NOT NULL DEFAULT '{}'::jsonb,
                    transport_mode TEXT NOT NULL DEFAULT '',
                    transport_kilometers DOUBLE PRECISION NOT NULL DEFAULT 0,
                    transport_address TEXT NOT NULL DEFAULT '',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cursor.execute("CREATE INDEX IF NOT EXISTS events_month_idx ON events (event_date, start_time)")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS reminders (
                    reminder_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    action TEXT NOT NULL,
                    schedule TEXT NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    run_time TEXT NOT NULL DEFAULT '09:00',
                    run_date TEXT NOT NULL DEFAULT '',
                    timezone TEXT NOT NULL DEFAULT 'Europe/Berlin',
                    status TEXT NOT NULL DEFAULT 'active',
                    last_run_at TIMESTAMPTZ,
                    next_run_at TIMESTAMPTZ,
                    last_run_status TEXT,
                    last_run_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cursor.execute("CREATE INDEX IF NOT EXISTS reminders_next_run_idx ON reminders (status, next_run_at)")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS reports (
                    report_id TEXT PRIMARY KEY,
                    month TEXT NOT NULL,
                    report_type TEXT NOT NULL,
                    profile_id TEXT,
                    file_name TEXT NOT NULL,
                    storage_backend TEXT NOT NULL DEFAULT 'local',
                    storage_key TEXT NOT NULL,
                    storage_url TEXT,
                    storage_download_url TEXT,
                    content_type TEXT NOT NULL DEFAULT 'application/pdf',
                    content_size BIGINT NOT NULL DEFAULT 0,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cursor.execute("CREATE INDEX IF NOT EXISTS reports_month_idx ON reports (month, created_at DESC)")
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
            cursor.execute("ALTER TABLE invoice_captures ALTER COLUMN content SET DEFAULT ''::bytea")
            cursor.execute("CREATE INDEX IF NOT EXISTS invoice_captures_sid_idx ON invoice_captures (sid, created_at DESC)")
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
            cursor.execute("ALTER TABLE document_templates ADD COLUMN IF NOT EXISTS storage_backend TEXT NOT NULL DEFAULT 'supabase'")
            cursor.execute("ALTER TABLE document_templates ADD COLUMN IF NOT EXISTS storage_key TEXT")
            cursor.execute("ALTER TABLE document_templates ADD COLUMN IF NOT EXISTS storage_url TEXT")
            cursor.execute("ALTER TABLE document_templates ALTER COLUMN content SET DEFAULT ''::bytea")
            cursor.execute("ALTER TABLE document_templates ALTER COLUMN checksum_sha256 SET DEFAULT ''")
    print("Supabase tables verified.")


def upsert_profile(cursor, profile: dict[str, Any]) -> None:
    cursor.execute(
        """
        INSERT INTO profiles (profile_id, payload, created_at, updated_at)
        VALUES ('default', %s::jsonb, NOW(), NOW())
        ON CONFLICT (profile_id)
        DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()
        """,
        (json.dumps(profile),),
    )


def upsert_events(cursor, events: list[dict[str, Any]]) -> None:
    for raw_event in events:
        event = _normalize_event(raw_event)
        assistant_hours = {field: float((event.get("assistant_hours") or {}).get(field, 0.0) or 0.0) for field in ASSISTANT_HOUR_FIELDS}
        cursor.execute(
            """
            INSERT INTO events (
                event_id, event_date, start_time, end_time, all_day, category,
                title, notes, hours, assistant_hours, transport_mode,
                transport_kilometers, transport_address, created_at, updated_at
            )
            VALUES (%s, %s::date, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (event_id)
            DO UPDATE SET
                event_date = EXCLUDED.event_date,
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time,
                all_day = EXCLUDED.all_day,
                category = EXCLUDED.category,
                title = EXCLUDED.title,
                notes = EXCLUDED.notes,
                hours = EXCLUDED.hours,
                assistant_hours = EXCLUDED.assistant_hours,
                transport_mode = EXCLUDED.transport_mode,
                transport_kilometers = EXCLUDED.transport_kilometers,
                transport_address = EXCLUDED.transport_address,
                updated_at = NOW()
            """,
            (
                event["id"],
                event["date"],
                event.get("time") or "",
                event.get("end_time") or "",
                bool(event.get("all_day")),
                event.get("category") or "other",
                event.get("title") or "",
                event.get("notes") or "",
                float(event.get("hours") or 0.0),
                json.dumps(assistant_hours),
                event.get("transport_mode") or "",
                float(event.get("transport_kilometers") or 0.0),
                event.get("transport_address") or "",
            ),
        )


def upsert_reminders(cursor, reminders: list[dict[str, Any]]) -> None:
    for reminder in reminders:
        reminder_id = str(reminder.get("id") or "").strip()
        if not reminder_id:
            continue
        cursor.execute(
            """
            INSERT INTO reminders (
                reminder_id, title, action, schedule, note, run_time, run_date,
                timezone, status, last_run_at, next_run_at, last_run_status,
                last_run_message, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::timestamptz, %s::timestamptz, %s, %s, %s::timestamptz, NOW())
            ON CONFLICT (reminder_id)
            DO UPDATE SET
                title = EXCLUDED.title,
                action = EXCLUDED.action,
                schedule = EXCLUDED.schedule,
                note = EXCLUDED.note,
                run_time = EXCLUDED.run_time,
                run_date = EXCLUDED.run_date,
                timezone = EXCLUDED.timezone,
                status = EXCLUDED.status,
                last_run_at = EXCLUDED.last_run_at,
                next_run_at = EXCLUDED.next_run_at,
                last_run_status = EXCLUDED.last_run_status,
                last_run_message = EXCLUDED.last_run_message,
                updated_at = NOW()
            """,
            (
                reminder_id,
                reminder.get("title") or "",
                reminder.get("action") or "notify",
                reminder.get("schedule") or "month_end",
                reminder.get("note") or "",
                reminder.get("run_time") or "09:00",
                reminder.get("run_date") or "",
                reminder.get("timezone") or DEFAULT_TIMEZONE,
                reminder.get("status") or "active",
                reminder.get("last_run_at"),
                reminder.get("next_run_at"),
                reminder.get("last_run_status"),
                reminder.get("last_run_message"),
                reminder.get("created_at"),
            ),
        )


def seed_database(database_url: str, profile: dict[str, Any], events: list[dict[str, Any]], reminders: list[dict[str, Any]], *, dry_run: bool = False) -> None:
    if dry_run:
        print("Dry run: would upsert local profile, events, and reminders.")
        return

    with _connect_postgres(database_url) as connection:
        with connection.cursor() as cursor:
            upsert_profile(cursor, profile)
            upsert_events(cursor, events)
            upsert_reminders(cursor, reminders)
    print(f"Seed upsert complete: profiles=1 events={len(events)} reminders={len(reminders)}")


def _bucket_id(bucket: Any) -> str:
    if isinstance(bucket, dict):
        return str(bucket.get("id") or bucket.get("name") or "")
    return str(getattr(bucket, "id", "") or getattr(bucket, "name", ""))


def ensure_storage_buckets(*, dry_run: bool = False) -> None:
    if not os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip():
        print("Supabase Storage skipped: SUPABASE_SERVICE_ROLE_KEY is not set.")
        return
    if dry_run:
        print("Dry run: would create or verify Supabase Storage buckets.")
        return

    client = _create_supabase_client()
    existing = {_bucket_id(bucket) for bucket in client.storage.list_buckets()}
    for purpose, bucket_name_factory in BUCKET_PURPOSES.items():
        bucket_name = bucket_name_factory()
        if bucket_name in existing:
            print(f"Storage bucket verified: {purpose}")
            continue
        client.storage.create_bucket(bucket_name, options={"public": False})
        print(f"Storage bucket created: {purpose}")


def configured_template_paths() -> dict[str, Path]:
    candidates = {
        "assistenz_standard": [
            os.environ.get("IV_AGENT_TEMPLATE_PDF", ""),
            str(PROJECT_ROOT / "318.536_D_Rechnung_AB_01_2025_V1.pdf"),
        ],
        "stundenblatt": [
            os.environ.get("IV_AGENT_STUNDENBLATT_PDF", ""),
            str(PROJECT_ROOT / SUPABASE_TEMPLATE_FILES["stundenblatt"]),
        ],
        "rechnung": [
            os.environ.get("IV_AGENT_RECHNUNG_PDF", ""),
            str(PROJECT_ROOT / SUPABASE_TEMPLATE_FILES["rechnung"]),
        ],
        "transportkosten": [
            os.environ.get("IV_AGENT_TRANSPORTKOSTEN_PDF", ""),
            str(PROJECT_ROOT / SUPABASE_TEMPLATE_FILES["transportkosten"]),
        ],
    }
    resolved: dict[str, Path] = {}
    for template_key, paths in candidates.items():
        for path_value in paths:
            if not path_value:
                continue
            path = Path(path_value)
            if path.exists() and path.is_file():
                resolved[template_key] = path
                break
    return resolved


def upload_templates(*, dry_run: bool = False) -> None:
    if not os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip():
        print("Template upload skipped: SUPABASE_SERVICE_ROLE_KEY is not set.")
        return
    template_paths = configured_template_paths()
    if not template_paths:
        print("Template upload skipped: no configured template files found.")
        return
    if dry_run:
        print(f"Dry run: would upload {len(template_paths)} configured template file(s).")
        return

    client = _create_supabase_client()
    bucket = _supabase_templates_bucket()
    with _connect_postgres(os.environ["DATABASE_URL"].strip()) as connection:
        with connection.cursor() as cursor:
            for template_key, path in template_paths.items():
                file_name = path.name
                storage_key = f"{template_key}/{file_name}"
                content = path.read_bytes()
                content_type = _guess_content_type(file_name, fallback="application/pdf")
                client.storage.from_(bucket).upload(
                    path=storage_key,
                    file=content,
                    file_options={"content-type": content_type, "cache-control": "3600", "upsert": "true"},
                )
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
                        template_key,
                        file_name,
                        content_type,
                        len(content),
                        hashlib.sha256(content).hexdigest(),
                        storage_key,
                        f"supabase://{bucket}/{storage_key}",
                    ),
                )
    print(f"Template upload complete: {len(template_paths)} file(s)")


def table_exists(cursor, table_name: str) -> bool:
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
        )
        """,
        (table_name,),
    )
    row = cursor.fetchone()
    return bool(row and row.get("exists"))


def table_count_hashes(database_url: str) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    with _connect_postgres(database_url) as connection:
        with connection.cursor() as cursor:
            for table_name in TABLES:
                if not table_exists(cursor, table_name):
                    continue
                cursor.execute(f'SELECT COUNT(*) AS count FROM "{table_name}"')
                count_row = cursor.fetchone()
                cursor.execute(
                    f"""
                    SELECT COALESCE(
                        md5(string_agg(md5(row_to_json(t)::text), '' ORDER BY md5(row_to_json(t)::text))),
                        ''
                    ) AS checksum
                    FROM (SELECT * FROM "{table_name}") t
                    """
                )
                checksum_row = cursor.fetchone()
                results[table_name] = {
                    "count": int((count_row or {}).get("count") or 0),
                    "checksum": (checksum_row or {}).get("checksum") or "",
                }
    return results


def print_database_summary(label: str, summary: dict[str, dict[str, Any]]) -> None:
    print(f"{label} summary:")
    if not summary:
        print("  no readable app tables")
        return
    for table_name in TABLES:
        if table_name not in summary:
            continue
        row = summary[table_name]
        checksum = str(row.get("checksum") or "")[:12] or "empty"
        print(f"  {table_name}: count={row.get('count', 0)} hash={checksum}")


def compare_neon(neon_url: str | None, supabase_summary: dict[str, dict[str, Any]]) -> None:
    if not neon_url:
        print("Neon comparison skipped: no Neon database URL configured.")
        return
    try:
        neon_summary = table_count_hashes(neon_url)
    except Exception as exc:
        print(f"Neon comparison skipped: {type(exc).__name__}: {exc}")
        return
    print_database_summary("Neon", neon_summary)
    differences = []
    for table_name in TABLES:
        supabase_row = supabase_summary.get(table_name)
        neon_row = neon_summary.get(table_name)
        if not supabase_row or not neon_row:
            continue
        if supabase_row.get("count") != neon_row.get("count") or supabase_row.get("checksum") != neon_row.get("checksum"):
            differences.append(table_name)
    if differences:
        print("Neon comparison differences: " + ", ".join(differences))
    else:
        print("Neon comparison: no differences for shared readable tables.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed local IV Agent JSON data into Supabase Postgres and Storage.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned work without writing to Supabase.")
    parser.add_argument("--skip-neon", action="store_true", help="Skip best-effort Neon comparison.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env_file()

    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        print("DATABASE_URL is required to seed Supabase Postgres.")
        return 2

    profile = read_json(PROFILE_PATH, {})
    events = read_json(CALENDAR_PATH, [])
    reminders = read_json(REMINDERS_PATH, [])

    if not isinstance(profile, dict):
        print("profile.json must contain a JSON object.")
        return 2
    if not isinstance(events, list):
        print("calendar.json must contain a JSON array.")
        return 2
    if not isinstance(reminders, list):
        print("reminders.json must contain a JSON array.")
        return 2

    print_local_summary(profile, events, reminders)
    ensure_tables(database_url, dry_run=args.dry_run)
    seed_database(database_url, profile, events, reminders, dry_run=args.dry_run)
    ensure_storage_buckets(dry_run=args.dry_run)
    upload_templates(dry_run=args.dry_run)

    if args.dry_run:
        return 0

    supabase_summary = table_count_hashes(database_url)
    print_database_summary("Supabase", supabase_summary)
    if not args.skip_neon:
        neon_url = os.environ.get("NEON_DATABASE_URL", "").strip() or os.environ.get("DATABASE_URL_UNPOOLED", "").strip()
        compare_neon(neon_url, supabase_summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
