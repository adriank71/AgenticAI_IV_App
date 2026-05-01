# Neon -> Supabase Migration — Runbook

Dieses Verzeichnis enthaelt alle Scripts zur Migration der App-Datenbank von Neon nach Supabase. Plan-Referenz: `~/.claude/plans/ich-will-mein-backend-cosmic-hanrahan.md`.

## Ziel-Architektur

| Daten-Typ | Speicherort |
|---|---|
| `profile.json`, `calendar.json`, `reminders.json`, Reports-Metadaten | Supabase Postgres (SQL) |
| Generierte PDFs aus `form_pilot.py` (Stundenblatt, Rechnung) | Supabase Storage Bucket `iv-agent-reports` |
| Hochgeladene Rechnungs-Bilder/PDFs | Supabase Storage Bucket `iv-agent-invoices` |
| PDF-Templates | Supabase Storage Bucket `iv-agent-templates` |

Die DB-Spalten `invoice_captures.content` und `document_templates.content` (BYTEA) sind Legacy. Sie muessen vor dem Cutover leer sein, sonst werden Binaer-Daten doppelt nach Supabase gezogen.

---

## Schritt 1 — User: MCP-Server installieren

In `~/.claude/mcp_servers.json` (oder via `claude mcp add ...`):

### Supabase MCP
```json
{
  "supabase": {
    "command": "npx",
    "args": ["-y", "@supabase/mcp-server-supabase@latest", "--read-only", "--project-ref=<DEIN_PROJECT_REF>"],
    "env": { "SUPABASE_ACCESS_TOKEN": "<TOKEN>" }
  }
}
```
Token: Supabase Dashboard -> Account -> Access Tokens. Project-Ref: aus Project Settings -> General. `--read-only` Flag bleibt waehrend der Verifikations-Phase, wird fuer Phase A entfernt.

### Neon MCP
```json
{
  "neon": {
    "command": "npx",
    "args": ["-y", "@neondatabase/mcp-server-neon", "start"],
    "env": { "NEON_API_KEY": "<KEY>" }
  }
}
```
API-Key: Neon Console -> Account Settings -> API Keys.

### Vercel MCP
```json
{
  "vercel": {
    "command": "npx",
    "args": ["-y", "@vercel/mcp-server"],
    "env": { "VERCEL_TOKEN": "<TOKEN>", "VERCEL_TEAM_ID": "<TEAM>" }
  }
}
```

Nach der Aenderung Claude Code neu starten.

---

## Schritt 2 — User: Lokale Tools + Credentials

1. PostgreSQL 16 client installieren (Windows: `winget install PostgreSQL.PostgreSQL.16`). `pg_dump --version` muss `(PostgreSQL) 16.x` zeigen.
2. In `.env.local` ergaenzen (nur lokal, NICHT comitten):
   ```
   NEON_DATABASE_URL=postgres://...neon.../neondb?sslmode=require
   SUPABASE_DB_URL=postgres://postgres:<password>@db.<ref>.supabase.co:5432/postgres
   SUPABASE_URL=https://<ref>.supabase.co
   SUPABASE_SERVICE_ROLE_KEY=<service-role-key>
   SUPABASE_STORAGE_INVOICES_BUCKET=iv-agent-invoices
   SUPABASE_STORAGE_TEMPLATES_BUCKET=iv-agent-templates
   VERCEL_TOKEN=<vercel-personal-access-token>
   ```
   `SUPABASE_DB_URL` MUSS die **Direct Connection** sein (Port 5432), nicht der Pooler (6543) — `pg_restore` braucht Session-Mode.

---

## Schritt 3 — Phase 0: BYTEA-Audit

```bash
psql "$NEON_DATABASE_URL" -f scripts/migration/audit_bytea.sql
```

Wenn `rows_with_bytes = 0` in beiden Tabellen: weiter zu Schritt 5.

Wenn `rows_with_bytes > 0`: weiter zu Schritt 4.

---

## Schritt 4 — Phase 0a: Backfill (nur falls noetig)

```bash
# erst dry-run zur Kontrolle
python scripts/migration/backfill_bytea.py --dry-run

# dann echt
python scripts/migration/backfill_bytea.py

# verifizieren
psql "$NEON_DATABASE_URL" -f scripts/migration/audit_bytea.sql
# rows_with_bytes muss jetzt 0 sein
```

---

## Schritt 5 — Phase A + B: Cutover

```bash
bash scripts/migration/cutover.sh
```
Erzeugt `migration_artifacts/schema.sql` und `migration_artifacts/data.dump`. Diese Dateien NICHT comitten (kommen ins .gitignore).

---

## Schritt 6 — Verifikation

```bash
psql "$SUPABASE_DB_URL" -f scripts/migration/verify.sql
```
Row-Counts muessen exakt mit der Audit-Ausgabe aus Schritt 3 uebereinstimmen.

---

## Schritt 7 — Phase C: App-Cutover

1. Lokal `DATABASE_URL` in `.env.local` auf den Supabase **Pooler** (Port 6543) umstellen.
2. `python -m iv_agent.app` starten, alle Routes durchklicken: `/api/calendar-data`, `/api/events` (POST), `/api/reports/generate`, `/api/profile`, `/api/chat`, `/api/calendar/voice/draft`.
3. Auf Vercel via Vercel MCP folgende env-vars **entfernen**:
   - `DATABASE_URL_UNPOOLED`, `POSTGRES_URL`, `POSTGRES_URL_NON_POOLING`, `POSTGRES_URL_NO_SSL`, `POSTGRES_PRISMA_URL`
   - `NEON_PROJECT_ID`, alle `NEON_*`
   - `PGHOST`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`
4. Behalten: `DATABASE_URL` (Pooler), `SUPABASE_*`, alle `IV_AGENT_*_BACKEND`.
5. Production-Deploy ausloesen.
6. Vercel-Logs 24h beobachten.

---

## Schritt 8 — Phase D: Neon abschalten

Nach 1 Woche stabilen Betriebs: Neon-Projekt loeschen. Davor: Neon-Branch nur pausieren (Rollback-Option).
