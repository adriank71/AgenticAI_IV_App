"""Fuegt die fuer Supabase noetigen Env-Variablen zu Vercel hinzu.

Idempotent (nutzt upsert=true). Loescht KEINE Variablen.
Run-modes:
    --dry-run  -- nur anzeigen
    (ohne)     -- ausfuehren
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

PROJECT_ID = "prj_7osD9YgkzaDA416DZncb5hyZzsmN"
TEAM_ID = "team_v07nUThZAF6NWf3O964Rjegm"
TOKEN = os.environ.get("VERCEL_TOKEN", "")


def _load_dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        sys.exit(f"missing {path}")
    raw = path.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    out: dict[str, str] = {}
    for line in raw.decode("utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def upsert_env(key: str, value: str, var_type: str, dry_run: bool) -> None:
    targets = ["production", "preview", "development"]
    if dry_run:
        masked = value if var_type == "plain" else f"<{len(value)} chars hidden>"
        print(f"  [DRY] {key:42} ({var_type}) -> {masked}")
        return

    url = f"https://api.vercel.com/v10/projects/{PROJECT_ID}/env?upsert=true&teamId={TEAM_ID}"
    body = {"key": key, "value": value, "type": var_type, "target": targets}
    req = urllib.request.Request(
        url,
        method="POST",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        # Vercel returns either {"created": ...} or {"updated": [...]} depending on upsert
        action = "ok"
        if isinstance(data, dict):
            if "created" in data:
                action = "created"
            elif "updated" in data:
                action = "updated"
        print(f"  [{action:8}] {key}")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"  [FAIL]    {key}: HTTP {e.code} {body[:200]}")
    except Exception as e:
        print(f"  [FAIL]    {key}: {e}")


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    if not dry_run and not TOKEN:
        sys.exit("missing VERCEL_TOKEN environment variable")

    env = _load_dotenv(Path(__file__).resolve().parents[2] / ".env.local")

    plan = [
        ("DATABASE_URL",                       env["DATABASE_URL"],                  "encrypted"),
        ("SUPABASE_URL",                       env["SUPABASE_URL"],                  "plain"),
        ("SUPABASE_SERVICE_ROLE_KEY",          env["SUPABASE_SERVICE_ROLE_KEY"],     "sensitive"),
        ("SUPABASE_STORAGE_TEMPLATES_BUCKET",  env["SUPABASE_STORAGE_TEMPLATES_BUCKET"], "plain"),
        ("SUPABASE_STORAGE_REPORTS_BUCKET",    env["SUPABASE_STORAGE_REPORTS_BUCKET"],   "plain"),
        ("SUPABASE_STORAGE_INVOICES_BUCKET",   env["SUPABASE_STORAGE_INVOICES_BUCKET"],  "plain"),
        ("IV_AGENT_TEMPLATE_BACKEND",          env["IV_AGENT_TEMPLATE_BACKEND"],     "plain"),
        ("IV_AGENT_INVOICE_ASSET_BACKEND",     env["IV_AGENT_INVOICE_ASSET_BACKEND"],"plain"),
    ]

    print(f"upserting {len(plan)} env vars on Vercel project agenticai-iv-app{' (dry run)' if dry_run else ''}")
    print(f"target: production + preview + development")
    print()
    for key, value, var_type in plan:
        upsert_env(key, value, var_type, dry_run)


if __name__ == "__main__":
    main()
