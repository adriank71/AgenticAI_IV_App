import argparse
import json
import os
from typing import Any, Callable

try:
    from .calendar_manager import CALENDAR_PATH, DATA_DIR, JsonEventStore, PostgresEventStore
    from .storage import DEFAULT_PROFILE_ID, LocalProfileStore, PostgresProfileStore
except ImportError:
    from calendar_manager import CALENDAR_PATH, DATA_DIR, JsonEventStore, PostgresEventStore
    from storage import DEFAULT_PROFILE_ID, LocalProfileStore, PostgresProfileStore


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_PROFILE_PATH = os.path.join(DATA_DIR, "profile.json")
PROFILE_DIR = os.path.join(DATA_DIR, "profiles")


def migrate_local_data(
    *,
    database_url: str,
    calendar_path: str = CALENDAR_PATH,
    default_profile_path: str = DEFAULT_PROFILE_PATH,
    profile_dir: str = PROFILE_DIR,
    event_store_factory: Callable[[str], Any] | None = None,
    profile_store_factory: Callable[[str], Any] | None = None,
) -> dict[str, int]:
    if not str(database_url or "").strip():
        raise RuntimeError("DATABASE_URL is required for migration.")

    local_event_store = JsonEventStore(os.path.dirname(calendar_path), calendar_path)
    local_profile_store = LocalProfileStore(default_profile_path, profile_dir)
    target_event_store = event_store_factory(database_url) if event_store_factory else PostgresEventStore(database_url)
    target_profile_store = (
        profile_store_factory(database_url) if profile_store_factory else PostgresProfileStore(database_url)
    )

    events = local_event_store.load_all_events()
    target_event_store.replace_all_events(events)

    migrated_profiles = 0
    for profile_id, payload in local_profile_store.iter_profiles():
        target_profile_store.upsert_profile(profile_id, payload)
        migrated_profiles += 1

    return {
        "events": len(events),
        "profiles": migrated_profiles,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import local calendar/profile data into Postgres.")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", ""), help="Target Postgres URL")
    parser.add_argument("--calendar", default=CALENDAR_PATH, help="Path to local calendar.json")
    parser.add_argument("--profile", default=DEFAULT_PROFILE_PATH, help="Path to the default profile.json")
    parser.add_argument("--profile-dir", default=PROFILE_DIR, help="Path to additional profile JSON files")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    summary = migrate_local_data(
        database_url=args.database_url,
        calendar_path=args.calendar,
        default_profile_path=args.profile,
        profile_dir=args.profile_dir,
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
