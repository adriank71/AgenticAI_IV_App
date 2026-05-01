#!/usr/bin/env bash
# Phase A + B — Schema- und Daten-Cutover Neon -> Supabase
# Voraussetzung: Phase 0 abgeschlossen (BYTEA-Spalten leer in Neon).
#
# Erforderliche env-vars:
#   NEON_DATABASE_URL   — Source (mit ?sslmode=require)
#   SUPABASE_DB_URL     — Target, DIRECT connection (Port 5432, NICHT Pooler 6543)
#
# Usage:
#   bash scripts/migration/cutover.sh

set -euo pipefail

# Windows: pg_dump/pg_restore werden meist nicht auf den PATH gesetzt
if [[ -d "/c/Program Files/PostgreSQL/16/bin" ]] && ! command -v pg_dump >/dev/null 2>&1; then
  export PATH="/c/Program Files/PostgreSQL/16/bin:$PATH"
fi
command -v pg_dump >/dev/null 2>&1 || { echo "pg_dump not found in PATH" >&2; exit 1; }

# Fallbacks zu existierenden .env.local-Variablennamen
: "${NEON_DATABASE_URL:=${DATABASE_URL_UNPOOLED:-${POSTGRES_URL_NON_POOLING:-}}}"
: "${NEON_DATABASE_URL:?set NEON_DATABASE_URL or DATABASE_URL_UNPOOLED}"
: "${SUPABASE_DB_URL:?set SUPABASE_DB_URL (direct connection, port 5432)}"

OUT_DIR="${OUT_DIR:-./migration_artifacts}"
mkdir -p "$OUT_DIR"

echo "[1/5] dumping schema from Neon..."
pg_dump --schema-only --no-owner --no-privileges \
  --exclude-schema='auth' \
  --exclude-schema='storage' \
  --exclude-schema='pgbouncer' \
  --exclude-schema='realtime' \
  --exclude-schema='vault' \
  --exclude-schema='supabase_functions' \
  --exclude-schema='supabase_migrations' \
  --exclude-schema='extensions' \
  --schema=public \
  "$NEON_DATABASE_URL" > "$OUT_DIR/schema.sql"

echo "[2/5] applying schema to Supabase..."
psql "$SUPABASE_DB_URL" \
  --variable=ON_ERROR_STOP=1 \
  --single-transaction \
  -f "$OUT_DIR/schema.sql"

echo "[3/5] dumping data from Neon (custom format)..."
pg_dump --data-only --format=custom --no-owner --no-privileges \
  --schema=public \
  "$NEON_DATABASE_URL" > "$OUT_DIR/data.dump"

echo "[4/5] restoring data into Supabase..."
pg_restore --data-only --no-owner --no-privileges \
  --disable-triggers \
  --jobs=4 \
  --dbname="$SUPABASE_DB_URL" \
  "$OUT_DIR/data.dump"

echo "[5/5] resetting sequences in Supabase..."
psql "$SUPABASE_DB_URL" -At <<'SQL' | psql "$SUPABASE_DB_URL"
SELECT format(
  'SELECT setval(pg_get_serial_sequence(%L, %L), COALESCE((SELECT MAX(%I) FROM %I), 1));',
  c.oid::regclass::text, a.attname, a.attname, c.relname
)
FROM pg_class c
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND pg_get_serial_sequence(c.oid::regclass::text, a.attname) IS NOT NULL;
SQL

echo "done. artifacts in $OUT_DIR/"
