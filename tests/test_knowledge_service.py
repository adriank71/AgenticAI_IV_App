import json
import os
import unittest
from unittest.mock import patch

from iv_agent.services import knowledge_service
from iv_agent.tools.knowledge_tools import build_knowledge_tools


DOC_A = {
    "document_id": "doc-a",
    "user_id": "default",
    "file_name": "IV-Schreiben.txt",
    "title": "IV-Schreiben",
    "document_type": "letter",
    "institution": "IV-Stelle",
    "document_date": "2026-05-01",
    "tags": ["iv", "frist"],
    "summary": "IV-Stelle fordert Unterlagen bis 15.05.2026.",
    "extracted_text": "Bitte reichen Sie die Unterlagen bis 15.05.2026 ein. Betrag CHF 120.",
    "extraction_status": "completed",
    "metadata": {"title": "IV Schreiben", "notes": "Assistenzbeitrag"},
}

DOC_B = {
    "document_id": "doc-b",
    "user_id": "default",
    "file_name": "Rechnung.txt",
    "title": "Rechnung",
    "document_type": "invoice",
    "institution": "IV-Stelle",
    "document_date": "2026-05-02",
    "tags": ["rechnung"],
    "summary": "Rechnung fuer Assistenzleistung.",
    "extracted_text": "Rechnung der IV-Stelle. Total CHF 120. Referenz ABC123.",
    "extraction_status": "completed",
    "metadata": {},
}


class FakeHttpResponse:
    status = 200

    def __init__(self, payload):
        self.payload = payload

    def read(self):
        return json.dumps(self.payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class KnowledgeServiceTests(unittest.TestCase):
    def test_knowledge_tools_register_expected_tool_names(self):
        tools = build_knowledge_tools(
            lambda func: func,
            context_user_id="default",
            thread_id="thread-1",
            tool_events=[],
            make_json_safe=lambda value: value,
            tool_event_factory=lambda name, status, message, **kwargs: {
                "name": name,
                "status": status,
                "message": message,
            },
        )

        self.assertEqual(
            [tool.__name__ for tool in tools],
            [
                "search_internal_knowledge",
                "retrieve_relevant_documents",
                "ask_watsonx_iv_assistant",
                "summarize_document_context",
                "compare_documents",
                "extract_action_items",
                "synthesize_answer",
            ],
        )

    def test_watsonx_tool_passes_thread_and_user_context(self):
        tool_events = []
        tools = build_knowledge_tools(
            lambda func: func,
            context_user_id="profile_a",
            thread_id="thread-1",
            tool_events=tool_events,
            make_json_safe=lambda value: value,
            tool_event_factory=lambda name, status, message, **kwargs: {
                "name": name,
                "status": status,
                "message": message,
            },
        )
        ask_tool = {tool.__name__: tool for tool in tools}["ask_watsonx_iv_assistant"]

        with patch.object(
            knowledge_service,
            "ask_watsonx_iv_assistant",
            return_value={"available": True, "answer": "ok", "citations": []},
        ) as watsonx_mock:
            payload = json.loads(ask_tool("Frag den IV Assistant", context="lokaler Kontext"))

        self.assertEqual(payload["answer"], "ok")
        watsonx_mock.assert_called_once_with(
            question="Frag den IV Assistant",
            context="lokaler Kontext",
            thread_id="thread-1",
            user_id="profile_a",
        )
        self.assertEqual(tool_events[0]["name"], "ask_watsonx_iv_assistant")

    def test_internal_search_scopes_user_and_returns_bounded_document_fields(self):
        service = knowledge_service.KnowledgeService()

        with patch.object(
            knowledge_service.document_storage,
            "search_documents",
            return_value=[DOC_A],
        ) as search_mock:
            result = service.search_internal_knowledge(user_id="profile_a", query="Assistenzbeitrag", limit=3)

        search_mock.assert_called_once()
        self.assertEqual(search_mock.call_args.kwargs["user_id"], "profile_a")
        self.assertEqual(search_mock.call_args.kwargs["query"], "Assistenzbeitrag")
        self.assertIn("extracted_text", result["searched_fields"])
        self.assertIn("summary", result["searched_fields"])
        self.assertIn("tags", result["searched_fields"])
        self.assertIn("metadata.title", result["searched_fields"])
        self.assertEqual(result["documents"][0]["document_id"], "doc-a")
        self.assertLessEqual(len(result["documents"][0]["excerpt"]), knowledge_service.MAX_EXCERPT_CHARS + 3)

    def test_retrieve_relevant_documents_blocks_foreign_document_id(self):
        service = knowledge_service.KnowledgeService()

        def fake_get_document(*, user_id, document_id):
            if user_id == "default" and document_id == "doc-a":
                return DOC_A
            return None

        with patch.object(knowledge_service.document_storage, "get_document", side_effect=fake_get_document):
            result = service.retrieve_relevant_documents(
                user_id="default",
                document_ids=["doc-a", "foreign-doc"],
            )

        self.assertEqual([doc["document_id"] for doc in result["documents"]], ["doc-a"])
        self.assertEqual(result["missing_document_ids"], ["foreign-doc"])

    def test_compare_documents_returns_reasons_and_missing_docs(self):
        service = knowledge_service.KnowledgeService()

        def fake_get_document(*, user_id, document_id):
            return {"doc-a": DOC_A, "doc-b": DOC_B}.get(document_id)

        with patch.object(knowledge_service.document_storage, "get_document", side_effect=fake_get_document), patch.object(
            knowledge_service.document_storage,
            "match_documents",
            return_value={"matches": [{"document": DOC_B, "score": 0.8, "reason": "same institution"}]},
        ):
            result = service.compare_documents(user_id="default", document_ids=["doc-a", "doc-b", "missing-doc"])

        self.assertEqual(result["missing_document_ids"], ["missing-doc"])
        self.assertGreaterEqual(result["count"], 1)
        self.assertTrue(result["comparisons"])
        self.assertTrue(any("gleiche Institution" in item for item in result["comparisons"][0]["reasons"]))
        self.assertTrue(result["known_match_reasons"])

    def test_action_item_extraction_finds_deadlines_and_returns_empty_honestly(self):
        service = knowledge_service.KnowledgeService()

        with patch.object(knowledge_service.document_storage, "get_document", return_value=DOC_A):
            result = service.extract_action_items(user_id="default", document_ids=["doc-a"])

        self.assertGreaterEqual(result["count"], 1)
        self.assertTrue(any(item["type"] == "deadline" for item in result["action_items"]))

        calm_doc = {**DOC_A, "summary": "Nur Information.", "extracted_text": "Nur Information ohne Aufgabe."}
        with patch.object(knowledge_service.document_storage, "get_document", return_value=calm_doc):
            empty = service.extract_action_items(user_id="default", document_ids=["doc-a"])

        self.assertEqual(empty["action_items"], [])
        self.assertIn("Keine Fristen", empty["message"])

    def test_watsonx_client_uses_env_parses_response_and_does_not_return_secret(self):
        captured = {}

        def fake_urlopen(request, timeout):
            captured["url"] = request.full_url
            captured["headers"] = dict(request.header_items())
            captured["body"] = json.loads(request.data.decode("utf-8"))
            captured["timeout"] = timeout
            return FakeHttpResponse({"choices": [{"message": {"content": [{"text": "Antwort vom IV Assistant"}]}}]})

        api_token = "placeholder-token"
        env = {
            "WATSONX_ORCHESTRATE_BASE_URL": "https://watsonx.example",
            "WATSONX_ORCHESTRATE_API_KEY": api_token,
            "WATSONX_ORCHESTRATE_IV_ASSISTANT_AGENT_ID": "agent-123",
        }
        with patch.dict(os.environ, env, clear=False), patch.object(knowledge_service.urllib.request, "urlopen", side_effect=fake_urlopen):
            result = knowledge_service.WatsonXOrchestrateClient(timeout_seconds=7).chat(
                question="Was brauche ich?",
                context="Nur kurzer Kontext",
                thread_id="thread-1",
                user_id="default",
            )

        self.assertTrue(result["available"])
        self.assertEqual(result["answer"], "Antwort vom IV Assistant")
        self.assertEqual(captured["url"], "https://watsonx.example/api/v1/orchestrate/agent-123/chat/completions")
        self.assertEqual(captured["timeout"], 7)
        self.assertIn("Authorization", captured["headers"])
        self.assertEqual(captured["body"]["stream"], False)
        self.assertNotIn(api_token, json.dumps(result))

    def test_watsonx_client_handles_missing_env_and_http_failure(self):
        missing = knowledge_service.WatsonXOrchestrateClient(base_url="", api_key="", agent_id="").chat(question="x")
        self.assertFalse(missing["available"])
        self.assertIn("nicht konfiguriert", missing["reason"])

        client = knowledge_service.WatsonXOrchestrateClient(
            base_url="https://watsonx.example",
            api_key="placeholder-token",
            agent_id="agent-123",
        )
        with patch.object(
            knowledge_service.urllib.request,
            "urlopen",
            side_effect=knowledge_service.urllib.error.HTTPError("url", 500, "boom", {}, None),
        ):
            failed = client.chat(question="x")

        self.assertFalse(failed["available"])
        self.assertIn("HTTP-Fehler 500", failed["reason"])


if __name__ == "__main__":
    unittest.main()
