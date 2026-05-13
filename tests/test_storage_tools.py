import json
import unittest
from unittest.mock import patch

from iv_agent.tools.storage_tools import build_storage_tools


def identity_tool(func):
    return func


def make_event(name, status, message, **kwargs):
    return {"name": name, "status": status, "message": message, **kwargs}


class StorageToolsTests(unittest.TestCase):
    def _build_tools(self, artifacts=None):
        tool_events = []
        drafted_actions = []
        structured_actions = []
        tools = build_storage_tools(
            identity_tool,
            context_user_id="default",
            thread_id="thread-test",
            tool_events=tool_events,
            drafted_actions=drafted_actions,
            structured_actions=structured_actions,
            collected_artifacts=artifacts if artifacts is not None else [],
            register_pending_actions=lambda *args, **kwargs: [],
            make_json_safe=lambda value: value,
            tool_event_factory=make_event,
        )
        return {tool.__name__: tool for tool in tools}, tool_events

    def test_list_documents_passes_bucket_filter_and_collects_artifacts(self):
        artifacts = []
        document = {
            "document_id": "doc-1",
            "user_id": "default",
            "file_name": "rechnung.txt",
            "content_type": "text/plain",
            "storage_bucket": "IV",
            "summary": "Betrag CHF 12",
        }

        with patch("iv_agent.tools.storage_tools.service_list_documents", return_value=[document]) as list_mock:
            tools, _events = self._build_tools(artifacts)
            payload = json.loads(tools["list_documents"](storage_bucket="IV", tags_json='["rechnung"]'))

        self.assertEqual(payload["documents"][0]["document_id"], "doc-1")
        self.assertEqual(list_mock.call_args.kwargs["storage_bucket"], "IV")
        self.assertEqual(list_mock.call_args.kwargs["tags"], ["rechnung"])
        self.assertEqual(artifacts[0]["download_url"], "/api/documents/doc-1/file?profile_id=default&download=1")

    def test_get_document_details_deduplicates_collected_artifacts(self):
        document = {
            "document_id": "doc-1",
            "user_id": "default",
            "file_name": "rechnung.txt",
            "content_type": "text/plain",
            "storage_bucket": "IV",
        }
        artifacts = [{"id": "doc-1", "document_id": "doc-1", "type": "document"}]

        with patch("iv_agent.tools.storage_tools.service_get_document", return_value=document):
            tools, _events = self._build_tools(artifacts)
            tools["get_document_details"]("doc-1")

        self.assertEqual(len(artifacts), 1)

    def test_sum_invoice_amounts_tool_returns_sum_and_collects_documents(self):
        sum_payload = {
            "total_amount_chf": 42.5,
            "counted_documents": [
                {
                    "document_id": "doc-1",
                    "type": "document",
                    "title": "rechnung.txt",
                    "download_url": "/api/documents/doc-1/file?profile_id=default&download=1",
                }
            ],
            "documents_without_amount": [],
        }
        artifacts = []

        with patch("iv_agent.tools.storage_tools.service_sum_invoice_amounts", return_value=sum_payload) as sum_mock:
            tools, _events = self._build_tools(artifacts)
            payload = json.loads(tools["sum_invoice_amounts"](query="TixiTaxi Rechnungen"))

        self.assertEqual(payload["total_amount_chf"], 42.5)
        self.assertEqual(sum_mock.call_args.kwargs["storage_bucket"], "TixiTaxi")
        self.assertEqual(artifacts[0]["document_id"], "doc-1")

    def test_bundle_documents_tool_returns_zip_artifact(self):
        artifacts = []
        document = {
            "document_id": "doc-1",
            "user_id": "default",
            "file_name": "rechnung.txt",
            "content_type": "text/plain",
            "storage_bucket": "IV",
        }

        with patch("iv_agent.tools.storage_tools.service_get_document", return_value=document):
            tools, _events = self._build_tools(artifacts)
            payload = json.loads(tools["bundle_documents"](document_ids_json='["doc-1"]'))

        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["bundle"]["type"], "document_bundle")
        self.assertIn("/api/documents/bundle?", payload["bundle"]["download_url"])
        self.assertIn("document_ids=doc-1", payload["bundle"]["download_url"])
        self.assertEqual(artifacts[-1]["type"], "document_bundle")


if __name__ == "__main__":
    unittest.main()
