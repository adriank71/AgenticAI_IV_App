# AGENTS.md

## Project Overview
- **Project:** AgenticAI IV App — Flask-based IV helper for calendar planning, reports, invoices, reminders, document storage, and agent chat workflows.
- **Target user:** A German-speaking IV assistance user/operator who manages appointments, assistant hours, invoices, PDF reports, and stored documents.
- **Stack:** Python 3.12+, Flask, Flask-CORS, OpenAI Responses/Agents SDK (`openai-agents`), pypdf, psycopg, Supabase, Postgres-compatible storage, static HTML/CSS/JS frontend (FullCalendar 6), Vercel Python Function deployment.
- **Primary app entry point:** `iv_agent.app:app`
- **Vercel entry point:** `api/index.py`, which imports the Flask app.

## Commands
- **Install:** `python -m pip install -r requirements.txt`
- **Dev server:** `python -m flask --app iv_agent.app run --host 127.0.0.1 --port 5050`
- **Alternate local run:** `python app.py`
- **Tests:** `python -m unittest discover -s tests`
- **Single test file:** `python -m unittest tests.test_agent_api`
- **Build:** No separate build step; Vercel routes all requests to `api/index.py`.

## Important Paths

### Backend
- `iv_agent/app.py` — Flask routes, request validation, API orchestration, report generation, invoice capture, reminder routes, static file serving. This is the largest file; read before editing.
- `iv_agent/calendar_manager.py` — Core calendar CRUD (`get_events`, `add_events`, `update_event`, `delete_event`, `export_month_plan`, `get_assistant_hours_for_events`).
- `iv_agent/form_pilot.py` — PDF form filling for Assistenzbeitrag and Transportkosten reports (`fill_assistenz_form_auto_bytes`, `fill_assistenz_dual_form_auto_bytes`).
- `iv_agent/voice_calendar_agent.py` — Voice-to-calendar-draft logic (`build_voice_calendar_draft`, `transcribe_audio`).
- `iv_agent/reminders_agent.py` — Voice/text-to-reminder-draft (`build_reminder_draft_from_audio`, `build_reminder_draft_from_text`).
- `iv_agent/reminders.py` — Reminder persistence and scheduling helpers.
- `iv_agent/storage.py` — Store factories for profiles, reports, templates, assets, and invoice captures. Abstracts local / Postgres / Supabase backends.

### Agents
- `iv_agent/agents/orchestrator.py` — OpenAI Agents SDK orchestrator for `/api/agent/chat`. Registers pending actions; handles SDK availability fallback. Entry point: `run_agent_chat`.
- `iv_agent/agents/calendar_agent.py` — Specialized handoff agent for calendar read/write operations.
- `iv_agent/agents/storage_agent.py` — Specialized handoff agent for document storage operations.
- `iv_agent/agents/knowledge_agent.py` — Specialized handoff agent for RAG/knowledge queries over stored documents.
- `iv_agent/agents/automations_agent.py` — Placeholder; automations are currently handled via the reminders/automations service layer directly.

### Tools (function tools exposed to agents)
- `iv_agent/tools/calendar_tools.py`
- `iv_agent/tools/storage_tools.py`
- `iv_agent/tools/knowledge_tools.py`
- `iv_agent/tools/automations_tools.py`

### Services (business logic layer)
- `iv_agent/services/calendar_service.py` — Calendar event creation/update/delete with user scoping and timezone normalization.
- `iv_agent/services/storage_service.py` — Document CRUD, bucket management, chat attachment processing, structured fact extraction.
- `iv_agent/services/knowledge_service.py` — Document search and RAG context building over stored files.
- `iv_agent/services/automations_service.py` — Placeholder for future automation/reminder workflows.

### Frontend
- `iv_agent/static/index.html` — Single-page app shell. All views (calendar, adviser, reports, storage, settings, community) are rendered inside this file.
- `iv_agent/static/app.js` — All frontend logic: FullCalendar initialization, API calls, modals, voice composer, PDF export, drag-to-create/move, storage browser, report generation UI.
- `iv_agent/static/style.css` — All styles, including multiple theme layers and responsive overrides.

### Tests & Migrations
- `tests/` — `test_agent_api.py`, `test_calendar_api.py`, `test_form_pilot.py`, `test_knowledge_service.py`, `test_storage.py`, `test_storage_tools.py`.
- `supabase/migrations/` — Schema migrations for Supabase/Postgres.
- `output/` — Runtime/generated data; stays untracked.

## API Routes (app.py)
| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/events` | List events for a month/profile |
| POST | `/api/events` | Create event (supports recurrence) |
| PUT | `/api/events/<id>` | Update event |
| DELETE | `/api/events/<id>` | Delete event |
| GET | `/api/calendar-data` | Full month payload (events, hours, reminders) |
| GET | `/api/hours` | Assistant hours breakdown |
| GET/PUT | `/api/profile` | Profile read/write |
| GET | `/api/export` | Plain-text month plan export |
| POST | `/api/agent/chat` | Orchestrated agent chat (OpenAI Agents SDK) |
| POST | `/api/agent/actions/<id>/confirm` | Confirm a pending agent action |
| POST | `/api/chat` | Legacy basic chat route — do not unintentionally change |
| POST | `/api/calendar/voice/draft` | Voice-to-event draft |
| POST | `/api/chat/voice/transcribe` | Transcribe audio for adviser |
| POST | `/api/reports/generate` | Generate Assistenzbeitrag / Transportkosten PDFs |
| GET | `/api/reports/download/...` | Download generated report |
| POST | `/api/reports/send` | Send generated report via connected mail provider |
| GET | `/api/storage/browser` | Raw Supabase storage browser |
| GET | `/api/documents/browser` | Structured document browser with buckets |
| GET | `/api/documents/<id>/file` | Serve a stored document |
| PATCH | `/api/documents/<id>/bucket` | Move document to a different bucket |
| GET/POST | `/api/invoices/<sid>/...` | Invoice capture session lifecycle |
| GET/POST/DELETE | `/api/reminders` | Reminder CRUD |
| POST | `/api/reminders/<id>/run` | Manually trigger a reminder |
| GET/POST | `/api/reminders/tick` | Tick-based reminder execution (cron-compatible) |
| POST | `/api/reminders/voice` | Voice-to-reminder draft |
| GET | `/api/ai/status` | OpenAI configuration status check |

## Environment Variables
```
# AI
OPENAI_API_KEY
OPENAI_AGENT_MODEL               # Orchestrator model
OPENAI_ORCHESTRATOR_MODEL        # Alias for OPENAI_AGENT_MODEL
OPENAI_CALENDAR_AGENT_MODEL      # Calendar sub-agent model
OPENAI_DOCUMENT_AGENT_MODEL      # Storage/document sub-agent model
OPENAI_STORAGE_AGENT_MODEL       # Alias for OPENAI_DOCUMENT_AGENT_MODEL
OPENAI_KNOWLEDGE_AGENT_MODEL     # Knowledge sub-agent model

# Database / Storage
DATABASE_URL
SUPABASE_URL
SUPABASE_SERVICE_ROLE_KEY
SUPABASE_STORAGE_DOCUMENTS_BUCKET
IV_AGENT_STORAGE_BACKEND          # local | postgres | supabase | auto

# App behaviour
IV_AGENT_CALENDAR_DEFAULT_TIMEZONE
IV_AGENT_ENABLE_EXTERNAL_KNOWLEDGE
IV_AGENT_DISABLE_OPENAI_AGENTS    # Set to "1" to disable SDK and use fallback
IV_AGENT_ENABLE_OPENAI_TRACING    # Set to "1" to enable OpenAI tracing
```
Never commit any of these values. Copy from `.env.example` into a local ignored file.

## Architecture Notes
- **Agent flow:** `/api/agent/chat` → `run_agent_chat` in orchestrator → either `_run_agents_sdk` (full multi-agent) or `_run_orchestrator_unavailable` (fallback) → specialized calendar/storage/knowledge agents via handoff. The orchestrator also builds local read-only tools (`list_calendar_range`, `list_user_documents`, `search_user_documents`, `sum_user_invoice_amounts`) that run without side effects.
- **Pending actions:** Side-effecting model outputs (create/update/delete events, send reports) are registered as pending actions and must be confirmed via `/api/agent/actions/<id>/confirm` before execution. This is enforced in `register_pending_actions` / `confirm_pending_action`.
- **Legacy route:** `/api/chat` is the basic non-agentic chat route. Do not unintentionally change it when working on the orchestrator.
- **Calendar categories:** `assistant`, `transport`, `other` (displayed as "Therapie" in the UI). The internal value `other` is preserved in the DB.
- **User-facing language:** German. Preserve German strings in UI and agent responses unless the surrounding code clearly uses another language.
- **Storage backends:** local filesystem, Postgres (via psycopg), or Supabase. The `IV_AGENT_STORAGE_BACKEND` env variable controls selection; tests patch env or fake stores directly.
- **Frontend:** Single-page app in `iv_agent/static/`. FullCalendar 6 (global bundle, includes interaction plugin). Calendar supports drag-to-create (select), drag-to-move (eventDrop), and resize (eventResize) — all persist via PUT `/api/events/<id>`. PDF export generates a formatted HTML document in a new browser tab (weekly or monthly table layout, data fetched from `/api/calendar-data`).

## Coding Guidelines
- Read the existing module before changing it; match local style (route conventions, error helpers, store access patterns).
- Keep edits scoped to the requested behavior — avoid broad rewrites of `iv_agent/app.py` unless required.
- Use `json_error()` for error responses; never return bare strings or unstructured dicts from routes.
- Prefer service/store abstractions over direct Supabase/Postgres/filesystem calls in route handlers.
- Preserve compatibility between local development and Vercel deployment.
- Do not introduce new dependencies without explicit approval.
- Do not hardcode absolute local paths except where the project already has legacy fallback paths for PDF templates.
- Do not log secrets, raw API keys, service-role keys, full database URLs, or raw uploaded document contents.

## Testing Expectations
- Run `python -m unittest discover -s tests` after code changes where feasible.
- For narrow route/service changes, add or update focused tests in the matching test file.
- For agent changes, cover the successful shape and the unavailable/missing-config fallback.
- For storage changes, test local/Postgres/Supabase selection with patched env or fake stores — not real external services.
- Do not delete or weaken tests to make a change pass.

## Git and Files
- `output/`, `__pycache__/`, `.pytest_cache/`, `.venv/`, local env files, and PDFs are gitignored.
- Do not revert unrelated working-tree changes.
- Do not delete generated/runtime files unless the path is verified to be inside the workspace and explicitly disposable.
- Keep commits small and descriptive. Never force-push without explicit permission.

## When Stuck
- If an external service is required, check whether the code has a local fake, patch point, or unavailable-state path first.
- If a live OpenAI / Supabase / Postgres / Vercel call is required, explain the required env/config and ask before using credentials or making deployment changes.
- If a failing behavior cannot be reproduced locally in two focused attempts, report the exact command, error, likely cause, and the smallest next diagnostic step.

## Response Style
- Be concise and specific.
- Mention files changed and any verification run.
- If tests were not run, say why.
- Use plain language with concrete next steps.
