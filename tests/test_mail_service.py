import base64
import json
import os
import unittest
from unittest.mock import patch

from iv_agent.services import mail_service


class MemoryMailStore:
    def __init__(self, accounts=None):
        self.accounts = accounts or {}
        self.saved = []

    def load_all(self):
        return self.accounts

    def save_account(self, provider, account):
        self.accounts[provider] = account
        self.saved.append((provider, account))
        return account

    def delete_account(self, provider):
        return self.accounts.pop(provider, None) is not None


class MailServiceTests(unittest.TestCase):
    def test_gmail_mime_with_pdf_attachment_is_base64url_encoded(self):
        raw = mail_service.build_gmail_raw_message(
            to_email="iv@example.test",
            subject="Report",
            body="Hallo",
            file_name="report.pdf",
            pdf_bytes=b"%PDF-1.4\n",
        )

        self.assertNotIn("+", raw)
        self.assertNotIn("/", raw)
        decoded = base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)).decode("utf-8", errors="replace")
        self.assertIn("To: iv@example.test", decoded)
        self.assertIn("Subject: Report", decoded)
        self.assertIn("report.pdf", decoded)
        self.assertIn("application/pdf", decoded)

    def test_outlook_payload_contains_file_attachment(self):
        payload = mail_service.build_outlook_send_payload(
            to_email="iv@example.test",
            subject="Report",
            body="Hallo",
            file_name="report.pdf",
            pdf_bytes=b"%PDF-1.4\n",
        )

        self.assertTrue(payload["saveToSentItems"])
        self.assertEqual(payload["message"]["toRecipients"][0]["emailAddress"]["address"], "iv@example.test")
        attachment = payload["message"]["attachments"][0]
        self.assertEqual(attachment["@odata.type"], "#microsoft.graph.fileAttachment")
        self.assertEqual(attachment["contentBytes"], base64.b64encode(b"%PDF-1.4\n").decode("ascii"))

    def test_token_refresh_is_used_before_send(self):
        store = MemoryMailStore(
            {
                "gmail": {
                    "provider": "gmail",
                    "token": {
                        "access_token": "old-token",
                        "refresh_token": "refresh-token",
                        "expires_at": "2020-01-01T00:00:00+00:00",
                    },
                }
            }
        )

        with patch.dict(
            os.environ,
            {
                "GOOGLE_OAUTH_CLIENT_ID": "client",
                "GOOGLE_OAUTH_CLIENT_SECRET": "secret",
            },
            clear=False,
        ), patch.object(
            mail_service,
            "_post_form",
            return_value={"access_token": "new-token", "expires_in": 3600},
        ) as refresh_mock, patch.object(
            mail_service,
            "_send_gmail_message",
            return_value={"id": "msg-1"},
        ) as send_mock:
            result = mail_service.send_plain_mail(
                to_email="iv@example.test",
                subject="Reminder",
                body="Hallo",
                store=store,
            )

        self.assertTrue(result["sent"])
        refresh_mock.assert_called_once()
        self.assertEqual(send_mock.call_args.kwargs["access_token"], "new-token")
        self.assertEqual(store.accounts["gmail"]["token"]["access_token"], "new-token")

    def test_missing_oauth_connection_raises_clear_error(self):
        with self.assertRaises(mail_service.MailNotConnectedError):
            mail_service.send_plain_mail(
                to_email="iv@example.test",
                subject="Reminder",
                body="Hallo",
                store=MemoryMailStore({}),
            )

    def test_mail_status_does_not_expose_tokens(self):
        status = mail_service.public_mail_status(
            MemoryMailStore(
                {
                    "gmail": {
                        "provider": "gmail",
                        "token": {
                            "access_token": "secret-access",
                            "refresh_token": "secret-refresh",
                        },
                        "connected_at": "2026-05-14T12:00:00Z",
                    }
                }
            )
        )

        serialized = json.dumps(status)
        self.assertTrue(status["connected"])
        self.assertNotIn("secret-access", serialized)
        self.assertNotIn("secret-refresh", serialized)


if __name__ == "__main__":
    unittest.main()
