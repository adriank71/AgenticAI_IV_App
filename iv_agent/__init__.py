import os


LOCAL_ENV_KEYS = {
    "OPEN_AI_KEY",
    "OPENAI_API_KEY",
    "OPENAI_KEY",
    "OPENAI_CALENDAR_AGENT_MODEL",
    "OPENAI_AUTOMATION_MODEL",
    "OPENAI_VISION_MODEL",
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
