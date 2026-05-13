import os


LOCAL_ENV_KEYS = {
    "DATABASE_URL",
    "OPEN_AI_KEY",
    "OPENAI_API_KEY",
    "OPENAI_KEY",
    "OPENAI_AGENT_MODEL",
    "OPENAI_ORCHESTRATOR_MODEL",
    "OPENAI_CALENDAR_AGENT_MODEL",
    "OPENAI_DOCUMENT_AGENT_MODEL",
    "OPENAI_AUTOMATION_MODEL",
    "OPENAI_VISION_MODEL",
    "IV_AGENT_DISABLE_OPENAI_AGENTS",
    "IV_AGENT_ENABLE_OPENAI_TRACING",
    "IV_AGENT_STORAGE_BACKEND",
    "IV_AGENT_REPORT_ASSET_BACKEND",
    "IV_AGENT_INVOICE_ASSET_BACKEND",
    "IV_AGENT_TEMPLATE_BACKEND",
    "IV_AGENT_DEFAULT_DOCUMENT_BUCKET",
    "IV_AGENT_DOCUMENT_BUCKETS",
    "IV_AGENT_DOCUMENT_MAX_BYTES",
    "IV_AGENT_DOCUMENT_SIGNED_URL_TTL_SECONDS",
    "IV_AGENT_CAMERA_CAPTURE_BUCKET",
    "IV_AGENT_INSURANCE_DOCUMENT_BUCKET",
    "SUPABASE_URL",
    "NEXT_PUBLIC_SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "SUPABASE_STORAGE_DOCUMENTS_BUCKET",
    "SUPABASE_STORAGE_TEMPLATES_BUCKET",
    "SUPABASE_STORAGE_REPORTS_BUCKET",
    "SUPABASE_STORAGE_INVOICES_BUCKET",
    "IV_AGENT_STUNDENBLATT_PDF",
    "IV_AGENT_RECHNUNG_PDF",
    "IV_AGENT_TRANSPORTKOSTEN_PDF",
    "WATSONX_ORCHESTRATE_BASE_URL",
    "WATSONX_ORCHESTRATE_API_KEY",
    "WATSONX_ORCHESTRATE_IV_ASSISTANT_AGENT_ID",
}

OPENAI_API_KEY_ALIASES = {
    "OPEN_AI_KEY": "OPENAI_API_KEY",
    "OPENAI_KEY": "OPENAI_API_KEY",
}


def _load_env_local() -> None:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_path = os.path.join(project_root, ".env.local")
    if not os.path.exists(env_path):
        return

    with open(env_path, "r", encoding="utf-8") as file:
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
            if key not in LOCAL_ENV_KEYS:
                continue

            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]

            target_key = OPENAI_API_KEY_ALIASES.get(key, key)
            os.environ.setdefault(target_key, value)


_load_env_local()
