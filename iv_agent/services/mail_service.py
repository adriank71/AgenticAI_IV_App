import base64
import json
import os
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Any, Callable, Protocol

try:
    from ..storage import _connect_postgres
except ImportError:
    from storage import _connect_postgres


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
MAIL_ACCOUNTS_PATH = os.path.join(DATA_DIR, "mail_accounts.json")

GMAIL_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GMAIL_TOKEN_URL = "https://oauth2.googleapis.com/token"
GMAIL_SEND_URL = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"
GMAIL_SCOPE = "https://www.googleapis.com/auth/gmail.send"

OUTLOOK_AUTH_URL = "https://login.microsoftonline.com/common/oauth2/v2.0/authorize"
OUTLOOK_TOKEN_URL = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
OUTLOOK_SEND_URL = "https://graph.microsoft.com/v1.0/me/sendMail"
OUTLOOK_SCOPE = "offline_access Mail.Send"

VALID_PROVIDERS = {"gmail", "outlook"}


class MailServiceError(RuntimeError):
    pass


class MailConfigurationError(MailServiceError):
    pass


class MailNotConnectedError(MailServiceError):
    pass


class MailAccountStore(Protocol):
    def load_all(self) -> dict[str, dict[str, Any]]:
        ...

    def save_account(self, provider: str, account: dict[str, Any]) -> dict[str, Any]:
        ...

    def delete_account(self, provider: str) -> bool:
        ...


class JsonMailAccountStore:
    def __init__(self, path: str = MAIL_ACCOUNTS_PATH):
        self.path = path

    def _ensure_storage(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        if not os.path.exists(self.path):
            with open(self.path, "w", encoding="utf-8") as file:
                json.dump({}, file)

    def load_all(self) -> dict[str, dict[str, Any]]:
        self._ensure_storage()
        try:
            with open(self.path, "r", encoding="utf-8") as file:
                payload = json.load(file)
        except (OSError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def _write_all(self, accounts: dict[str, dict[str, Any]]) -> None:
        self._ensure_storage()
        with open(self.path, "w", encoding="utf-8") as file:
            json.dump(accounts, file, indent=2)

    def save_account(self, provider: str, account: dict[str, Any]) -> dict[str, Any]:
        accounts = self.load_all()
        accounts[provider] = account
        self._write_all(accounts)
        return account

    def delete_account(self, provider: str) -> bool:
        accounts = self.load_all()
        if provider not in accounts:
            return False
        del accounts[provider]
        self._write_all(accounts)
        return True


class PostgresMailAccountStore:
    def __init__(self, database_url: str, connection_factory: Callable[[], Any] | None = None):
        self._database_url = database_url
        self._connection_factory = connection_factory or (lambda: _connect_postgres(database_url))
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS mail_oauth_accounts (
                        provider TEXT PRIMARY KEY,
                        account_email TEXT NOT NULL DEFAULT '',
                        token_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                        connected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )

    def load_all(self) -> dict[str, dict[str, Any]]:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT provider, account_email, token_json, connected_at, updated_at
                    FROM mail_oauth_accounts
                    ORDER BY updated_at DESC
                    """
                )
                rows = cursor.fetchall()
        accounts: dict[str, dict[str, Any]] = {}
        for row in rows:
            token_json = row.get("token_json") if isinstance(row, dict) else row[2]
            provider = row.get("provider") if isinstance(row, dict) else row[0]
            account_email = row.get("account_email") if isinstance(row, dict) else row[1]
            connected_at = row.get("connected_at") if isinstance(row, dict) else row[3]
            updated_at = row.get("updated_at") if isinstance(row, dict) else row[4]
            accounts[str(provider)] = {
                "provider": str(provider),
                "account_email": account_email or "",
                "token": token_json if isinstance(token_json, dict) else {},
                "connected_at": _iso(connected_at),
                "updated_at": _iso(updated_at),
            }
        return accounts

    def save_account(self, provider: str, account: dict[str, Any]) -> dict[str, Any]:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO mail_oauth_accounts (provider, account_email, token_json, connected_at, updated_at)
                    VALUES (%s, %s, %s::jsonb, %s::timestamptz, NOW())
                    ON CONFLICT (provider)
                    DO UPDATE SET
                        account_email = EXCLUDED.account_email,
                        token_json = EXCLUDED.token_json,
                        updated_at = NOW()
                    """,
                    (
                        provider,
                        account.get("account_email") or "",
                        json.dumps(account.get("token") or {}),
                        account.get("connected_at") or utc_timestamp(),
                    ),
                )
        return account

    def delete_account(self, provider: str) -> bool:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM mail_oauth_accounts WHERE provider = %s", (provider,))
                return bool(getattr(cursor, "rowcount", 0))


_STORE_CACHE: dict[tuple[str, str], MailAccountStore] = {}


def _iso(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_provider(provider: str) -> str:
    normalized = str(provider or "").strip().lower()
    if normalized not in VALID_PROVIDERS:
        raise ValueError("Unsupported mail provider")
    return normalized


def _client_config(provider: str) -> tuple[str, str]:
    provider = _normalize_provider(provider)
    if provider == "gmail":
        client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "").strip()
        client_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "").strip()
    else:
        client_id = os.environ.get("MICROSOFT_OAUTH_CLIENT_ID", "").strip()
        client_secret = os.environ.get("MICROSOFT_OAUTH_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise MailConfigurationError(f"{provider} OAuth is not configured")
    return client_id, client_secret


def get_mail_account_store() -> MailAccountStore:
    backend = str(os.environ.get("IV_AGENT_STORAGE_BACKEND", "auto") or "auto").strip().lower()
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if backend != "local" and database_url:
        cache_key = ("postgres", database_url)
        if cache_key not in _STORE_CACHE:
            _STORE_CACHE[cache_key] = PostgresMailAccountStore(database_url)
        return _STORE_CACHE[cache_key]
    cache_key = ("local", MAIL_ACCOUNTS_PATH)
    if cache_key not in _STORE_CACHE:
        _STORE_CACHE[cache_key] = JsonMailAccountStore()
    return _STORE_CACHE[cache_key]


def clear_mail_store_cache() -> None:
    _STORE_CACHE.clear()


def public_mail_status(store: MailAccountStore | None = None) -> dict[str, Any]:
    accounts = (store or get_mail_account_store()).load_all()
    providers = {}
    for provider in sorted(VALID_PROVIDERS):
        account = accounts.get(provider) if isinstance(accounts.get(provider), dict) else None
        token = account.get("token") if account else {}
        providers[provider] = {
            "connected": bool(account and token and token.get("refresh_token")),
            "account_email": account.get("account_email", "") if account else "",
            "connected_at": account.get("connected_at", "") if account else "",
            "updated_at": account.get("updated_at", "") if account else "",
        }
    default_provider = next((provider for provider, item in providers.items() if item["connected"]), "")
    return {
        "connected": bool(default_provider),
        "default_provider": default_provider,
        "providers": providers,
    }


def build_redirect_uri(provider: str, base_url: str) -> str:
    base = str(os.environ.get("MAIL_OAUTH_REDIRECT_BASE_URL") or base_url or "").strip().rstrip("/")
    if not base:
        raise MailConfigurationError("MAIL_OAUTH_REDIRECT_BASE_URL is required")
    return f"{base}/api/mail/oauth/{_normalize_provider(provider)}/callback"


def build_oauth_start_url(provider: str, base_url: str) -> str:
    provider = _normalize_provider(provider)
    client_id, _ = _client_config(provider)
    redirect_uri = build_redirect_uri(provider, base_url)
    state = f"{provider}:{uuid.uuid4().hex}"
    if provider == "gmail":
        params = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": GMAIL_SCOPE,
            "access_type": "offline",
            "prompt": "consent",
            "state": state,
        }
        return f"{GMAIL_AUTH_URL}?{urllib.parse.urlencode(params)}"
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": OUTLOOK_SCOPE,
        "response_mode": "query",
        "state": state,
    }
    return f"{OUTLOOK_AUTH_URL}?{urllib.parse.urlencode(params)}"


def exchange_oauth_code(provider: str, code: str, base_url: str, store: MailAccountStore | None = None) -> dict[str, Any]:
    provider = _normalize_provider(provider)
    if not str(code or "").strip():
        raise ValueError("OAuth code is required")
    token = _post_form(
        GMAIL_TOKEN_URL if provider == "gmail" else OUTLOOK_TOKEN_URL,
        {
            "client_id": _client_config(provider)[0],
            "client_secret": _client_config(provider)[1],
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": build_redirect_uri(provider, base_url),
        },
    )
    token = _normalize_token(token)
    account = {
        "provider": provider,
        "account_email": "",
        "token": token,
        "connected_at": utc_timestamp(),
        "updated_at": utc_timestamp(),
    }
    (store or get_mail_account_store()).save_account(provider, account)
    return public_mail_status(store)


def disconnect_mail(provider: str | None = None, store: MailAccountStore | None = None) -> dict[str, Any]:
    target_store = store or get_mail_account_store()
    providers = [_normalize_provider(provider)] if provider else sorted(VALID_PROVIDERS)
    for item in providers:
        target_store.delete_account(item)
    return public_mail_status(target_store)


def send_report_mail(
    *,
    to_email: str,
    subject: str,
    body: str,
    file_name: str,
    pdf_bytes: bytes,
    provider: str | None = None,
    store: MailAccountStore | None = None,
) -> dict[str, Any]:
    target_provider, account = _resolve_connected_account(provider, store)
    access_token = _valid_access_token(target_provider, account, store)
    if target_provider == "gmail":
        response = _send_gmail_message(
            access_token=access_token,
            to_email=to_email,
            subject=subject,
            body=body,
            file_name=file_name,
            pdf_bytes=pdf_bytes,
        )
    else:
        response = _send_outlook_message(
            access_token=access_token,
            to_email=to_email,
            subject=subject,
            body=body,
            file_name=file_name,
            pdf_bytes=pdf_bytes,
        )
    return {
        "sent": True,
        "provider": target_provider,
        "provider_response": response,
    }


def send_plain_mail(
    *,
    to_email: str,
    subject: str,
    body: str,
    provider: str | None = None,
    store: MailAccountStore | None = None,
) -> dict[str, Any]:
    target_provider, account = _resolve_connected_account(provider, store)
    access_token = _valid_access_token(target_provider, account, store)
    if target_provider == "gmail":
        response = _send_gmail_message(
            access_token=access_token,
            to_email=to_email,
            subject=subject,
            body=body,
        )
    else:
        response = _send_outlook_message(
            access_token=access_token,
            to_email=to_email,
            subject=subject,
            body=body,
        )
    return {
        "sent": True,
        "provider": target_provider,
        "provider_response": response,
    }


def build_gmail_raw_message(
    *,
    to_email: str,
    subject: str,
    body: str,
    file_name: str | None = None,
    pdf_bytes: bytes | None = None,
) -> str:
    message = EmailMessage()
    message["To"] = to_email
    message["Subject"] = subject
    message.set_content(body or "")
    if file_name and pdf_bytes is not None:
        message.add_attachment(
            pdf_bytes,
            maintype="application",
            subtype="pdf",
            filename=file_name,
        )
    return base64.urlsafe_b64encode(message.as_bytes()).decode("ascii").rstrip("=")


def build_outlook_send_payload(
    *,
    to_email: str,
    subject: str,
    body: str,
    file_name: str | None = None,
    pdf_bytes: bytes | None = None,
) -> dict[str, Any]:
    message: dict[str, Any] = {
        "subject": subject,
        "body": {
            "contentType": "Text",
            "content": body or "",
        },
        "toRecipients": [
            {
                "emailAddress": {
                    "address": to_email,
                }
            }
        ],
    }
    if file_name and pdf_bytes is not None:
        message["attachments"] = [
            {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": file_name,
                "contentType": "application/pdf",
                "contentBytes": base64.b64encode(pdf_bytes).decode("ascii"),
            }
        ]
    return {
        "message": message,
        "saveToSentItems": True,
    }


def _normalize_token(token: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(token)
    try:
        expires_in = int(normalized.get("expires_in") or 3600)
    except (TypeError, ValueError):
        expires_in = 3600
    normalized["expires_at"] = (
        datetime.now(timezone.utc) + timedelta(seconds=max(60, expires_in))
    ).isoformat()
    return normalized


def _resolve_connected_account(
    provider: str | None,
    store: MailAccountStore | None = None,
) -> tuple[str, dict[str, Any]]:
    accounts = (store or get_mail_account_store()).load_all()
    providers = [_normalize_provider(provider)] if provider else ["gmail", "outlook"]
    for candidate in providers:
        account = accounts.get(candidate)
        if isinstance(account, dict) and isinstance(account.get("token"), dict):
            if account["token"].get("refresh_token") or account["token"].get("access_token"):
                return candidate, account
    raise MailNotConnectedError("No Gmail or Outlook mailbox is connected")


def _valid_access_token(provider: str, account: dict[str, Any], store: MailAccountStore | None = None) -> str:
    token = account.get("token") if isinstance(account.get("token"), dict) else {}
    access_token = str(token.get("access_token") or "").strip()
    expires_at = str(token.get("expires_at") or "").strip()
    needs_refresh = not access_token
    if expires_at:
        try:
            expires_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            needs_refresh = needs_refresh or expires_dt <= datetime.now(timezone.utc) + timedelta(minutes=5)
        except ValueError:
            needs_refresh = True
    if needs_refresh:
        token = refresh_access_token(provider, token)
        account["token"] = token
        account["updated_at"] = utc_timestamp()
        (store or get_mail_account_store()).save_account(provider, account)
        access_token = str(token.get("access_token") or "").strip()
    if not access_token:
        raise MailNotConnectedError("Connected mailbox has no usable access token")
    return access_token


def refresh_access_token(provider: str, token: dict[str, Any]) -> dict[str, Any]:
    provider = _normalize_provider(provider)
    refresh_token = str(token.get("refresh_token") or "").strip()
    if not refresh_token:
        raise MailNotConnectedError("Connected mailbox has no refresh token")
    refreshed = _post_form(
        GMAIL_TOKEN_URL if provider == "gmail" else OUTLOOK_TOKEN_URL,
        {
            "client_id": _client_config(provider)[0],
            "client_secret": _client_config(provider)[1],
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
    )
    if "refresh_token" not in refreshed:
        refreshed["refresh_token"] = refresh_token
    return _normalize_token({**token, **refreshed})


def _post_form(url: str, payload: dict[str, str]) -> dict[str, Any]:
    body = urllib.parse.urlencode(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    return _open_json(request)


def _send_gmail_message(
    *,
    access_token: str,
    to_email: str,
    subject: str,
    body: str,
    file_name: str | None = None,
    pdf_bytes: bytes | None = None,
) -> dict[str, Any]:
    request = urllib.request.Request(
        GMAIL_SEND_URL,
        data=json.dumps(
            {
                "raw": build_gmail_raw_message(
                    to_email=to_email,
                    subject=subject,
                    body=body,
                    file_name=file_name,
                    pdf_bytes=pdf_bytes,
                )
            }
        ).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    return _open_json(request)


def _send_outlook_message(
    *,
    access_token: str,
    to_email: str,
    subject: str,
    body: str,
    file_name: str | None = None,
    pdf_bytes: bytes | None = None,
) -> dict[str, Any]:
    request = urllib.request.Request(
        OUTLOOK_SEND_URL,
        data=json.dumps(
            build_outlook_send_payload(
                to_email=to_email,
                subject=subject,
                body=body,
                file_name=file_name,
                pdf_bytes=pdf_bytes,
            )
        ).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    return _open_json(request, empty_ok=True)


def _open_json(request: urllib.request.Request, *, empty_ok: bool = False) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            raw = response.read()
            if response.status < 200 or response.status >= 300:
                raise MailServiceError(f"Mail provider returned status {response.status}")
            if not raw and empty_ok:
                return {}
            if not raw:
                return {}
            return json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise MailServiceError(f"Mail provider request failed with status {exc.code}: {detail[:300]}") from exc
    except urllib.error.URLError as exc:
        raise MailServiceError(f"Mail provider request failed: {exc}") from exc

