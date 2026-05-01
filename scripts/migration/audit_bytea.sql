-- Phase 0 — Pre-Flight: BYTEA-Audit auf Neon
-- Ausfuehrung gegen NEON_DATABASE_URL (Source).
-- Zeigt, ob noch Binary-Daten in den content-Spalten der DB-Tabellen liegen.
-- Falls rows_with_bytes > 0: backfill_bytea.py ausfuehren, bevor Phase A startet.

\echo '=== invoice_captures ==='
SELECT
  COUNT(*)                                                                       AS total_rows,
  COUNT(*) FILTER (WHERE octet_length(content) > 0)                              AS rows_with_bytes,
  COALESCE(SUM(octet_length(content)) FILTER (WHERE octet_length(content) > 0), 0) AS total_bytes,
  COUNT(*) FILTER (WHERE storage_url IS NULL OR storage_url = '')                AS rows_missing_storage_url,
  COUNT(*) FILTER (WHERE storage_key IS NULL OR storage_key = '')                AS rows_missing_storage_key
FROM invoice_captures;

\echo '=== document_templates ==='
SELECT
  COUNT(*)                                                                       AS total_rows,
  COUNT(*) FILTER (WHERE octet_length(content) > 0)                              AS rows_with_bytes,
  COALESCE(SUM(octet_length(content)) FILTER (WHERE octet_length(content) > 0), 0) AS total_bytes,
  COUNT(*) FILTER (WHERE storage_url IS NULL OR storage_url = '')                AS rows_missing_storage_url,
  COUNT(*) FILTER (WHERE storage_key IS NULL OR storage_key = '')                AS rows_missing_storage_key
FROM document_templates;

\echo '=== row counts pro Tabelle (zur Verifikation nach pg_restore) ==='
SELECT 'profiles'           AS table_name, COUNT(*) AS rows FROM profiles
UNION ALL SELECT 'events',            COUNT(*) FROM events
UNION ALL SELECT 'reminders',         COUNT(*) FROM reminders
UNION ALL SELECT 'reports',           COUNT(*) FROM reports
UNION ALL SELECT 'invoice_captures',  COUNT(*) FROM invoice_captures
UNION ALL SELECT 'document_templates', COUNT(*) FROM document_templates;
