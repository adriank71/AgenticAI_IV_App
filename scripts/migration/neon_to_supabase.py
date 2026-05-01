"""Direkter Daten-Transfer Neon -> Supabase via psycopg.

Klein genug fuer eine in-memory Migration (35 Zeilen): fetch all from Neon,
upsert into Supabase. Kein pg_dump noetig.

Schema: wird via DDL aus iv_agent.migrate_local_data angelegt (idempotent).

Tabellen die migriert werden:
    profiles, events, reports

Tabellen die nur das Schema bekommen (kein Daten-Transfer noetig):
    reminders, invoice_captures, document_templates

Usage:
    python scripts/migration/neon_to_supabase.py [--dry-run]

Env (aus .env.local):
    DATABASE_URL_UNPOOLED   -> Neon (source)
    SUPABASE_DB_URL         -> Supabase (target, Session-Pooler oder Direct)
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    raw = path.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    for line in raw.decode("utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key.strip(), value)


def _ensure_schema(database_url: str) -> None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from iv_agent.migrate_local_data import ensure_tables  # type: ignore

    ensure_tables(database_url)


def _fetch_all(source: psycopg.Connection, table: str) -> list[dict]:
    with source.cursor(row_factory=dict_row) as cur:
        cur.execute(f"SELECT * FROM {table}")
        return cur.fetchall()


def _columns_of(target: psycopg.Connection, table: str) -> list[str]:
    with target.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position",
            (table,),
        )
        return [r[0] for r in cur.fetchall()]


def _coerce(value):
    if isinstance(value, dict) or isinstance(value, list):
        return Json(value)
    return value


def _upsert(target: psycopg.Connection, table: str, pk: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    target_cols = _columns_of(target, table)
    cols = [c for c in rows[0].keys() if c in target_cols]
    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)
    update_set = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c != pk)
    sql = (
        f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT ({pk}) DO UPDATE SET {update_set}"
    )
    with target.cursor() as cur:
        for row in rows:
            cur.execute(sql, [_coerce(row[c]) for c in cols])
    return len(rows)


def main() -> None:
    dry_run = "--dry-run" in sys.argv

    repo_root = Path(__file__).resolve().parents[2]
    _load_dotenv(repo_root / ".env.local")

    neon_url = os.environ.get("DATABASE_URL_UNPOOLED") or os.environ.get("NEON_DATABASE_URL")
    target_url = os.environ.get("SUPABASE_DB_URL")
    if not neon_url:
        sys.exit("missing DATABASE_URL_UNPOOLED / NEON_DATABASE_URL")
    if not target_url:
        sys.exit("missing SUPABASE_DB_URL")

    print(f"source: {neon_url.split('@')[1].split('/')[0]}")
    print(f"target: {target_url.split('@')[1].split('/')[0]}")
    print()

    print("[1/4] connecting to Neon (source) ...")
    source = psycopg.connect(neon_url)

    print("[2/4] connecting to Supabase (target) ...")
    target = psycopg.connect(target_url, autocommit=False)

    print("[3/4] ensuring target schema (DDL idempotent) ...")
    if not dry_run:
        _ensure_schema(target_url)
    else:
        print("  (dry run -- skipping DDL)")

    print("[4/4] copying data ...")
    transfers = [
        ("profiles", "profile_id"),
        ("events", "event_id"),
        ("reports", "report_id"),
    ]
    for table, pk in transfers:
        rows = _fetch_all(source, table)
        print(f"  {table}: {len(rows)} rows", end="")
        if dry_run:
            print(" (dry run)")
            continue
        copied = _upsert(target, table, pk, rows)
        target.commit()
        print(f" -> copied {copied}")

    source.close()
    target.close()
    print("\ndone.")


if __name__ == "__main__":
    main()
