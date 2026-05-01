-- Phase B — Verifikation nach pg_restore
-- Gegen SUPABASE_DB_URL ausfuehren und mit der Audit-Ausgabe aus audit_bytea.sql vergleichen.

\echo '=== row counts ==='
SELECT 'profiles'            AS table_name, COUNT(*) AS rows FROM profiles
UNION ALL SELECT 'events',            COUNT(*) FROM events
UNION ALL SELECT 'reminders',         COUNT(*) FROM reminders
UNION ALL SELECT 'reports',           COUNT(*) FROM reports
UNION ALL SELECT 'invoice_captures',  COUNT(*) FROM invoice_captures
UNION ALL SELECT 'document_templates', COUNT(*) FROM document_templates;

\echo '=== schema (columns pro Tabelle, fuer Diff gegen Neon) ==='
SELECT table_name, column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'public'
ORDER BY table_name, ordinal_position;

\echo '=== indices ==='
SELECT tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;

\echo '=== sequence current values ==='
SELECT sequence_name, last_value, is_called
FROM pg_sequences
WHERE schemaname = 'public';

\echo '=== sanity: invoice_captures should have empty content BYTEA ==='
SELECT
  COUNT(*)                                                AS total_rows,
  COUNT(*) FILTER (WHERE octet_length(content) > 0)       AS rows_with_bytes,
  COUNT(*) FILTER (WHERE storage_url IS NOT NULL
                     AND storage_url <> '')              AS rows_with_storage_url
FROM invoice_captures;

\echo '=== sanity: document_templates should have empty content BYTEA ==='
SELECT
  COUNT(*)                                                AS total_rows,
  COUNT(*) FILTER (WHERE octet_length(content) > 0)       AS rows_with_bytes,
  COUNT(*) FILTER (WHERE storage_url IS NOT NULL
                     AND storage_url <> '')              AS rows_with_storage_url
FROM document_templates;
