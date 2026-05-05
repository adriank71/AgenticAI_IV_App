# AGENTS.md

## Project Overview
- **Project:** AgenticAI IV App, a Flask-based IV helper for calendar planning, reports, invoices, reminders, document storage, and agent chat workflows.
- **Target user:** A German-speaking IV assistance user/operator who manages appointments, assistant hours, invoices, reports, and stored documents.
- **Stack:** Python 3.12+, Flask, Flask-CORS, OpenAI Responses/Agents SDK, pypdf, psycopg, Supabase, Postgres-compatible storage, static HTML/CSS/JS frontend, Vercel Python Function deployment.
- **Primary app entry point:** `iv_agent.app:app`.
- **Vercel entry point:** `api/index.py`, which imports the Flask app.

## Commands
- **Install:** `python -m pip install -r requirements.txt`
- **Dev server:** `python -m flask --app iv_agent.app run --host 127.0.0.1 --port 5050`
- **Alternate local run:** `python app.py`
- **Tests:** `python -m unittest discover -s tests`
- **Single test file:** `python -m unittest tests.test_agent_api`
- **Build:** No separate build step; Vercel routes all requests to `api/index.py`.

## Important Paths
- `iv_agent/app.py` contains Flask routes, request validation, API orchestration, report generation, invoice capture, reminder routes, and static file serving.
- `iv_agent/agents/orchestrator.py` is the OpenAI Agents SDK orchestrator for `/api/agent/chat` and pending action registration/confirmation.
- `iv_agent/agents/calendar_agent.py` and `iv_agent/agents/storage_agent.py` define specialized handoff agents.
- `iv_agent/tools/` contains function tools exposed to agents.
- `iv_agent/services/` contains service-layer calendar, storage, knowledge, and automation logic.
- `iv_agent/storage.py` contains local, Postgres, and Supabase-backed stores for profiles, reports, templates, assets, and invoice captures.
- `iv_agent/static/` contains the browser UI.
- `tests/` contains unittest-based coverage for agent API, calendar API, form filling, and storage.
- `supabase/migrations/` contains schema migrations for Supabase/Postgres-backed data.
- `output/` is runtime/generated data and should stay untracked.

## Environment
- Copy values from `.env.example` into a local ignored env file such as `.env.local` when needed.
- Never commit `.env`, `.env.*`, `.env.local`, API keys, service-role keys, database URLs, PDFs with private data, or generated output.
- Core variables:
  - `OPENAI_API_KEY`
  - `OPENAI_AGENT_MODEL`
  - `OPENAI_CALENDAR_AGENT_MODEL`
  - `OPENAI_DOCUMENT_AGENT_MODEL`
  - `DATABASE_URL`
  - `SUPABASE_URL`
  - `SUPABASE_SERVICE_ROLE_KEY`
  - `SUPABASE_STORAGE_DOCUMENTS_BUCKET`
  - `IV_AGENT_STORAGE_BACKEND`
  - `IV_AGENT_CALENDAR_DEFAULT_TIMEZONE`
  - `IV_AGENT_ENABLE_EXTERNAL_KNOWLEDGE`
  - `IV_AGENT_CHAT_WEBHOOK_URL`
- Storage backends can be local, Postgres, Supabase, or auto-selected depending on env configuration. Tests often patch env values and stores directly.

## Architecture Notes
- `/api/agent/chat` is the new orchestrated agent route. It uploads chat attachments first, strips raw Base64 from model input, then calls `run_agent_chat`.
- `/api/chat` is the legacy/basic chat route and should not be unintentionally changed when working on the orchestrator.
- Side-effecting agent actions are not executed directly by model output. They must be registered as pending actions and confirmed through `/api/agent/actions/<action_id>/confirm`.
- Calendar and storage mutations should go through their service/tool layers so user scoping, timezone normalization, validation, and persistence stay consistent.
- User-facing app text and agent answers are mostly German. Preserve that unless the surrounding code or request clearly uses another language.
- Use structured JSON parsing and existing helper functions for payload validation. Do not add ad hoc parsing for existing payload shapes.

## Coding Guidelines
- Read the existing module before changing it and match local style.
- Keep edits scoped to the requested behavior; avoid broad rewrites of `iv_agent/app.py` unless the task requires it.
- Prefer existing store/service abstractions over direct filesystem, Postgres, or Supabase calls in route handlers.
- Keep validation explicit and return JSON errors with appropriate HTTP status codes.
- Preserve compatibility between local development and Vercel deployment.
- Avoid introducing new dependencies unless the user approves and the benefit is clear.
- Do not hardcode absolute local paths except where the project already has legacy fallback paths for local PDF templates.
- Do not log secrets, raw API keys, service-role keys, full database URLs, or full uploaded document contents.

## Testing Expectations
- Run `python -m unittest discover -s tests` after code changes when feasible.
- For narrow route/service changes, add or update focused tests in the matching test file.
- For agent changes, cover both the successful shape and unavailable/missing-configuration behavior where practical.
- For storage changes, test local/Postgres/Supabase selection logic with patched env or fake stores rather than real external services.
- Do not delete or weaken tests to make a change pass.

## Git And Files
- `output/`, `__pycache__/`, `.pytest_cache/`, `.venv/`, local env files, and PDFs are ignored and should remain generated/local-only.
- Do not revert unrelated user changes in the working tree.
- Do not delete generated/runtime files recursively unless the user explicitly asks or the target path is verified to be inside the workspace and disposable.
- Keep commits small and descriptive when asked to commit. Never force-push without explicit permission.

## When Stuck
- If an external service is required, first verify whether the code has a local fake, patch point, or unavailable-state path that can be tested without network access.
- If a live OpenAI, Supabase, Postgres, n8n, or Vercel call is required, explain the required env/config and ask before using credentials or making deployment changes.
- If a failing behavior cannot be reproduced locally in two focused attempts, report the exact command, error, likely cause, and the smallest next diagnostic step.

## Response Style For Future Agents
- Be concise and specific.
- Mention files changed and verification run.
- If tests were not run, say why.
- Use plain English with concrete next steps.
