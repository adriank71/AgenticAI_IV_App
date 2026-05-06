import json
import os
import re
import socket
import urllib.error
import urllib.request
from datetime import date, datetime
from typing import Any

try:
    from . import storage_service as document_storage
    from .calendar_service import normalize_user_id
    from .storage_service import extract_structured_facts
except ImportError:
    import services.storage_service as document_storage
    from services.calendar_service import normalize_user_id
    from services.storage_service import extract_structured_facts


DEFAULT_WATSONX_TIMEOUT_SECONDS = 30
MAX_EXCERPT_CHARS = 1800
MAX_CONTEXT_CHARS = 6000


def _compact_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _bounded_text(value: Any, limit: int = MAX_EXCERPT_CHARS) -> str:
    compact = _compact_text(value)
    if len(compact) <= limit:
        return compact
    return f"{compact[: max(0, limit - 3)].rstrip()}..."


def _coerce_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        except json.JSONDecodeError:
            pass
        return [item.strip() for item in raw.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _optional_int(value: Any) -> int | None:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return None
    return parsed or None


def _safe_document_metadata(document: dict[str, Any], *, include_excerpt: bool = True) -> dict[str, Any]:
    payload = {
        "document_id": document.get("document_id") or "",
        "file_name": document.get("file_name") or document.get("safe_file_name") or "",
        "title": document.get("title") or document.get("file_name") or "",
        "document_type": document.get("document_type") or "",
        "institution": document.get("institution") or "",
        "document_date": document.get("document_date") or "",
        "tags": document.get("tags") or [],
        "summary": document.get("summary") or "",
        "metadata": document.get("metadata") if isinstance(document.get("metadata"), dict) else {},
        "extraction_status": document.get("extraction_status") or "",
        "extraction_error": document.get("extraction_error") or "",
        "created_at": document.get("created_at") or "",
        "updated_at": document.get("updated_at") or "",
    }
    if include_excerpt:
        payload["excerpt"] = _bounded_text(document.get("extracted_text") or document.get("summary") or "")
    return payload


def _find_query_excerpt(text: str, query: str, limit: int = MAX_EXCERPT_CHARS) -> str:
    compact = _compact_text(text)
    if not compact:
        return ""
    query_tokens = [token for token in re.findall(r"[\wÄÖÜäöüß]{3,}", query or "", flags=re.UNICODE)]
    start = 0
    lower = compact.lower()
    for token in query_tokens:
        index = lower.find(token.lower())
        if index >= 0:
            start = max(0, index - 220)
            break
    return _bounded_text(compact[start:], limit)


def _token_set(text: str) -> set[str]:
    return {
        token.lower()
        for token in re.findall(r"[A-Za-zÄÖÜäöüß0-9]{4,}", text or "", flags=re.UNICODE)
        if token.lower() not in {"diese", "dieser", "dieses", "nicht", "oder", "eine", "einer", "einen", "werden"}
    }


def _parse_date(value: Any) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d.%m.%y"):
        try:
            parsed = datetime.strptime(raw[:10], fmt)
            return parsed.date()
        except ValueError:
            continue
    return None


def _extract_amounts(text: str) -> list[str]:
    amounts = []
    for match in re.finditer(
        r"\b(?:CHF|Fr\.?)\s*([0-9][0-9'.,]*)\b|\b([0-9][0-9'.,]*)\s*(?:CHF|Fr\.?)\b",
        text or "",
        flags=re.IGNORECASE,
    ):
        amount = match.group(1) or match.group(2)
        value = f"CHF {amount}"
        if value not in amounts:
            amounts.append(value)
        if len(amounts) >= 8:
            break
    return amounts


def _extract_deadlines(text: str) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    patterns = (
        r"(?P<label>frist|einsprachefrist|zahlungsfrist|bis|bis spaetestens|bis spätestens|einzureichen bis|zahlbar bis|antwort bis|termin bis)\s*[:\-]?\s*(?P<date>\d{1,2}\.\d{1,2}\.\d{2,4}|\d{4}-\d{2}-\d{2})",
        r"(?P<date>\d{1,2}\.\d{1,2}\.\d{2,4}|\d{4}-\d{2}-\d{2}).{0,80}?(?P<label>frist|einreichen|bezahlen|nachreichen|einsprache|antworten)",
        r"(?P<label>innert|innerhalb)\s+(?P<relative>\d{1,3})\s+(?:tagen|tage|arbeitstagen|arbeitstage)",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, text or "", flags=re.IGNORECASE | re.DOTALL):
            start = max(0, match.start() - 110)
            end = min(len(text or ""), match.end() + 170)
            snippet = _bounded_text((text or "")[start:end], 360)
            item = {
                "label": _compact_text(match.groupdict().get("label") or "Frist"),
                "date": _compact_text(match.groupdict().get("date") or ""),
                "relative": _compact_text(match.groupdict().get("relative") or ""),
                "snippet": snippet,
            }
            if item not in results:
                results.append(item)
            if len(results) >= 12:
                return results
    return results


def _extract_todos(text: str) -> list[str]:
    todos = []
    patterns = (
        r"((?:bitte|wir bitten sie|reichen sie|senden sie|schicken sie|bezahlen sie|melden sie|antworten sie).{12,220}?[.!?])",
        r"((?:einzureichen|nachzureichen|zu bezahlen|zu unterschreiben|zurueckzusenden|zurückzusenden).{12,200}?[.!?])",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, text or "", flags=re.IGNORECASE | re.DOTALL):
            todo = _bounded_text(match.group(1), 300)
            if todo and todo not in todos:
                todos.append(todo)
            if len(todos) >= 10:
                return todos
    return todos


class WatsonXOrchestrateClient:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        api_key: str | None = None,
        agent_id: str | None = None,
        timeout_seconds: int = DEFAULT_WATSONX_TIMEOUT_SECONDS,
    ):
        self.base_url = (base_url if base_url is not None else os.environ.get("WATSONX_ORCHESTRATE_BASE_URL", "")).strip().rstrip("/")
        self.api_key = (api_key if api_key is not None else os.environ.get("WATSONX_ORCHESTRATE_API_KEY", "")).strip()
        self.agent_id = (
            agent_id
            if agent_id is not None
            else os.environ.get("WATSONX_ORCHESTRATE_IV_ASSISTANT_AGENT_ID", "")
        ).strip()
        self.timeout_seconds = max(1, int(timeout_seconds or DEFAULT_WATSONX_TIMEOUT_SECONDS))

    def configured(self) -> bool:
        return bool(self.base_url and self.api_key and self.agent_id)

    def chat(
        self,
        *,
        question: str,
        context: str = "",
        thread_id: str = "",
        user_id: str = "",
    ) -> dict[str, Any]:
        if not self.configured():
            return {
                "available": False,
                "reason": "WatsonX Orchestrate ist nicht konfiguriert.",
                "answer": "",
                "citations": [],
            }

        endpoint = f"{self.base_url}/api/v1/orchestrate/{self.agent_id}/chat/completions"
        messages = [
            {
                "role": "system",
                "content": (
                    "Du bist ein IV Assistant. Antworte knapp auf Deutsch. "
                    "Nutze bereitgestellten Kontext nur als Hintergrund und erfinde keine Akteninhalte."
                ),
            },
            {
                "role": "user",
                "content": _bounded_text(
                    f"Frage: {question}\n\nKontext aus der IV-Helper App:\n{context}",
                    MAX_CONTEXT_CHARS,
                ),
            },
        ]
        body = json.dumps(
            {
                "messages": messages,
                "stream": False,
                "metadata": {"source": "iv-helper", "user_id": normalize_user_id(user_id or "default")},
            }
        ).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        if thread_id:
            headers["X-IBM-THREAD-ID"] = str(thread_id)[:120]
        request = urllib.request.Request(endpoint, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                response_body = response.read()
                status = int(getattr(response, "status", 200) or 200)
        except (socket.timeout, TimeoutError):
            return {"available": False, "reason": "WatsonX Orchestrate hat nicht rechtzeitig geantwortet.", "answer": "", "citations": []}
        except urllib.error.HTTPError as exc:
            return {"available": False, "reason": f"WatsonX Orchestrate HTTP-Fehler {exc.code}.", "answer": "", "citations": []}
        except urllib.error.URLError:
            return {"available": False, "reason": "WatsonX Orchestrate ist aktuell nicht erreichbar.", "answer": "", "citations": []}
        except Exception as exc:
            return {"available": False, "reason": f"WatsonX Orchestrate Anfrage fehlgeschlagen: {type(exc).__name__}.", "answer": "", "citations": []}

        if status < 200 or status >= 300:
            return {"available": False, "reason": f"WatsonX Orchestrate HTTP-Status {status}.", "answer": "", "citations": []}
        try:
            parsed = json.loads(response_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            parsed = response_body.decode("utf-8", errors="replace")
        answer = self._extract_text(parsed)
        return {
            "available": True,
            "reason": "",
            "answer": answer,
            "citations": self._extract_citations(parsed),
        }

    def _extract_text(self, payload: Any) -> str:
        if payload is None:
            return ""
        if isinstance(payload, str):
            return payload.strip()
        if isinstance(payload, list):
            parts = [self._extract_text(item) for item in payload]
            return "\n".join(part for part in parts if part).strip()
        if isinstance(payload, dict):
            choices = payload.get("choices")
            if isinstance(choices, list) and choices:
                for choice in choices:
                    text = self._extract_text((choice or {}).get("message") if isinstance(choice, dict) else choice)
                    if text:
                        return text
            content = payload.get("content")
            if isinstance(content, list):
                parts = []
                for item in content:
                    if isinstance(item, dict):
                        parts.append(str(item.get("text") or item.get("content") or "").strip())
                    elif isinstance(item, str):
                        parts.append(item.strip())
                text = "\n".join(part for part in parts if part).strip()
                if text:
                    return text
            for key in ("text", "answer", "response", "output", "message", "content"):
                if key in payload:
                    text = self._extract_text(payload.get(key))
                    if text:
                        return text
        return ""

    def _extract_citations(self, payload: Any) -> list[dict[str, Any]]:
        if not isinstance(payload, dict):
            return []
        raw_items = payload.get("citations") or payload.get("sources") or payload.get("references") or []
        if not isinstance(raw_items, list):
            return []
        citations = []
        for index, item in enumerate(raw_items[:10], start=1):
            if isinstance(item, str):
                citations.append({"id": f"watsonx_{index}", "title": item, "url": "", "snippet": ""})
            elif isinstance(item, dict):
                citations.append(
                    {
                        "id": str(item.get("id") or f"watsonx_{index}"),
                        "title": str(item.get("title") or item.get("name") or item.get("source") or f"WatsonX {index}"),
                        "url": str(item.get("url") or item.get("href") or ""),
                        "snippet": _bounded_text(item.get("snippet") or item.get("text") or "", 500),
                    }
                )
        return citations


class KnowledgeService:
    def search_internal_knowledge(
        self,
        *,
        user_id: str,
        query: str,
        year: int | None = None,
        month: int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        document_type: str = "",
        institution: str = "",
        tags: list[str] | None = None,
        folder_id: str | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        normalized_user_id = normalize_user_id(user_id)
        documents = document_storage.search_documents(
            user_id=normalized_user_id,
            query=str(query or "").strip(),
            year=year,
            month=month,
            start_date=start_date,
            end_date=end_date,
            document_type=document_type,
            institution=institution,
            tags=tags or [],
            folder_id=folder_id,
            limit=max(1, min(int(limit or 10), 25)),
        )
        return {
            "user_id": normalized_user_id,
            "query": str(query or "").strip(),
            "documents": [
                {
                    **_safe_document_metadata(document),
                    "excerpt": _find_query_excerpt(document.get("extracted_text") or document.get("summary") or "", query),
                }
                for document in documents
            ],
            "count": len(documents),
            "searched_fields": ["file_name", "summary", "extracted_text", "document_type", "institution", "tags", "metadata.title", "metadata.notes"],
        }

    def retrieve_relevant_documents(
        self,
        *,
        user_id: str,
        document_ids: list[str] | None = None,
        query: str = "",
        limit: int = 5,
    ) -> dict[str, Any]:
        normalized_user_id = normalize_user_id(user_id)
        selected: list[dict[str, Any]] = []
        missing_ids: list[str] = []
        seen_ids: set[str] = set()
        for document_id in document_ids or []:
            normalized_id = str(document_id or "").strip()
            if not normalized_id or normalized_id in seen_ids:
                continue
            document = document_storage.get_document(user_id=normalized_user_id, document_id=normalized_id)
            if not document:
                missing_ids.append(normalized_id)
                continue
            seen_ids.add(normalized_id)
            selected.append(document)

        if query and len(selected) < max(1, int(limit or 5)):
            search_result = document_storage.search_documents(
                user_id=normalized_user_id,
                query=query,
                limit=max(1, min(int(limit or 5), 20)),
            )
            for document in search_result:
                document_id = str(document.get("document_id") or "")
                if document_id and document_id not in seen_ids:
                    seen_ids.add(document_id)
                    selected.append(document)
                if len(selected) >= max(1, min(int(limit or 5), 20)):
                    break

        documents = []
        for document in selected[: max(1, min(int(limit or 5), 20))]:
            documents.append(
                {
                    **_safe_document_metadata(document),
                    "excerpt": _find_query_excerpt(document.get("extracted_text") or document.get("summary") or "", query),
                    "text_char_count": len(document.get("extracted_text") or ""),
                }
            )
        return {
            "user_id": normalized_user_id,
            "documents": documents,
            "missing_document_ids": missing_ids,
            "count": len(documents),
            "truncated": any(len(document.get("extracted_text") or "") > MAX_EXCERPT_CHARS for document in selected),
        }

    def summarize_document_context(self, *, documents: list[dict[str, Any]], question: str = "") -> dict[str, Any]:
        summaries = []
        uncertainty_flags = []
        for document in documents:
            excerpt = _compact_text(document.get("excerpt") or document.get("extracted_text") or "")
            summary = _compact_text(document.get("summary") or "")
            facts = extract_structured_facts(f"{summary}\n{excerpt}")
            if not excerpt and document.get("extraction_status") != "completed":
                uncertainty_flags.append(f"Kein auswertbarer Text fuer {document.get('file_name') or document.get('document_id')}.")
            summaries.append(
                {
                    "document_id": document.get("document_id") or "",
                    "file_name": document.get("file_name") or "",
                    "institution": document.get("institution") or facts.get("institution") or "",
                    "document_date": document.get("document_date") or facts.get("document_date") or "",
                    "document_type": document.get("document_type") or "",
                    "amount": facts.get("amount") or "",
                    "deadline": facts.get("deadline") or "",
                    "reference": facts.get("reference") or "",
                    "known_todos": facts.get("todos") or [],
                    "summary": summary or _bounded_text(excerpt, 500),
                    "relevant_excerpt": _bounded_text(excerpt, 700),
                }
            )
        if not summaries:
            uncertainty_flags.append("Keine passenden Dokumente gefunden.")
        return {
            "question": question,
            "documents": summaries,
            "uncertainty_flags": uncertainty_flags,
        }

    def compare_documents(self, *, user_id: str, document_ids: list[str], query: str = "") -> dict[str, Any]:
        retrieved = self.retrieve_relevant_documents(user_id=user_id, document_ids=document_ids, query=query, limit=max(2, len(document_ids or [])))
        documents = retrieved["documents"]
        comparisons = []
        known_match_reasons: list[dict[str, Any]] = []
        normalized_user_id = normalize_user_id(user_id)
        for source in documents:
            try:
                match_payload = document_storage.match_documents(
                    user_id=normalized_user_id,
                    document_id=source["document_id"],
                    limit=10,
                )
            except Exception:
                continue
            for item in match_payload.get("matches") or []:
                target = item.get("document") if isinstance(item, dict) else {}
                target_id = str((target or {}).get("document_id") or "")
                if target_id in {document.get("document_id") for document in documents}:
                    known_match_reasons.append(
                        {
                            "source_document_id": source["document_id"],
                            "target_document_id": target_id,
                            "score": item.get("score"),
                            "reason": item.get("reason") or "",
                        }
                    )

        for index, left in enumerate(documents):
            for right in documents[index + 1:]:
                left_text = f"{left.get('summary')} {left.get('excerpt')}"
                right_text = f"{right.get('summary')} {right.get('excerpt')}"
                left_tokens = _token_set(left_text)
                right_tokens = _token_set(right_text)
                overlap = len(left_tokens & right_tokens) / max(1, len(left_tokens | right_tokens))
                left_facts = extract_structured_facts(left_text)
                right_facts = extract_structured_facts(right_text)
                reasons = []
                differences = []
                if left.get("document_type") and left.get("document_type") == right.get("document_type"):
                    reasons.append("gleicher Dokumenttyp")
                else:
                    differences.append("Dokumenttyp unterscheidet sich")
                if left.get("institution") and left.get("institution") == right.get("institution"):
                    reasons.append("gleiche Institution")
                elif left.get("institution") or right.get("institution"):
                    differences.append("Institution unterscheidet sich")
                left_amounts = _extract_amounts(left_text)
                right_amounts = _extract_amounts(right_text)
                if set(left_amounts) & set(right_amounts):
                    reasons.append("gleicher Betrag gefunden")
                elif left_amounts or right_amounts:
                    differences.append("Betrag unterscheidet sich oder ist nur in einem Dokument gefunden")
                if (left.get("document_date") or left_facts.get("document_date")) == (right.get("document_date") or right_facts.get("document_date")):
                    if left.get("document_date") or left_facts.get("document_date"):
                        reasons.append("gleiches Dokumentdatum")
                elif left.get("document_date") or right.get("document_date"):
                    differences.append("Dokumentdatum unterscheidet sich")
                if overlap:
                    reasons.append(f"Textueberlappung {overlap:.0%}")
                comparisons.append(
                    {
                        "left_document_id": left.get("document_id"),
                        "right_document_id": right.get("document_id"),
                        "left_file_name": left.get("file_name"),
                        "right_file_name": right.get("file_name"),
                        "reasons": reasons,
                        "differences": differences,
                        "text_overlap_score": round(overlap, 3),
                        "left_amounts": left_amounts,
                        "right_amounts": right_amounts,
                    }
                )
        return {
            "user_id": normalized_user_id,
            "documents": [_safe_document_metadata(document, include_excerpt=False) for document in documents],
            "missing_document_ids": retrieved.get("missing_document_ids", []),
            "comparisons": comparisons,
            "known_match_reasons": known_match_reasons,
            "count": len(documents),
        }

    def extract_action_items(
        self,
        *,
        user_id: str,
        document_ids: list[str] | None = None,
        query: str = "",
        limit: int = 10,
    ) -> dict[str, Any]:
        retrieved = self.retrieve_relevant_documents(
            user_id=user_id,
            document_ids=document_ids or [],
            query=query or ("" if document_ids else "frist einreichen zahlen nachreichen"),
            limit=limit,
        )
        action_items = []
        for document in retrieved["documents"]:
            text = f"{document.get('summary')}\n{document.get('excerpt')}"
            for deadline in _extract_deadlines(text):
                action_items.append(
                    {
                        "type": "deadline",
                        "document_id": document.get("document_id"),
                        "file_name": document.get("file_name"),
                        "label": deadline.get("label") or "Frist",
                        "date": deadline.get("date") or "",
                        "relative": deadline.get("relative") or "",
                        "snippet": deadline.get("snippet") or "",
                    }
                )
            for todo in _extract_todos(text):
                action_items.append(
                    {
                        "type": "todo",
                        "document_id": document.get("document_id"),
                        "file_name": document.get("file_name"),
                        "label": "To-do",
                        "date": "",
                        "relative": "",
                        "snippet": todo,
                    }
                )
        return {
            "user_id": normalize_user_id(user_id),
            "action_items": action_items,
            "count": len(action_items),
            "missing_document_ids": retrieved.get("missing_document_ids", []),
            "message": "" if action_items else "Keine Fristen oder Aufgaben in den gefundenen Dokumentauszuegen erkannt.",
        }

    def synthesize_answer(
        self,
        *,
        question: str,
        internal_findings: dict[str, Any] | None = None,
        watsonx_result: dict[str, Any] | None = None,
        action_items: dict[str, Any] | None = None,
        comparison: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        findings = internal_findings or {}
        watsonx = watsonx_result or {}
        uncertainty_flags: list[str] = []
        sources = []
        for document in findings.get("documents") or []:
            if document.get("document_id"):
                sources.append(
                    {
                        "document_id": document.get("document_id"),
                        "title": document.get("title") or document.get("file_name") or document.get("document_id"),
                        "institution": document.get("institution") or "",
                        "document_date": document.get("document_date") or "",
                    }
                )
            if not document.get("excerpt") and document.get("extraction_status") != "completed":
                uncertainty_flags.append(f"Textauszug fehlt fuer {document.get('file_name') or document.get('document_id')}.")
        if not sources and not watsonx.get("answer"):
            uncertainty_flags.append("Keine belastbaren Quellen gefunden.")
        if watsonx and not watsonx.get("available", True):
            uncertainty_flags.append(str(watsonx.get("reason") or "WatsonX Orchestrate nicht verfuegbar."))
        return {
            "question": question,
            "internal_findings": findings,
            "watsonx": watsonx,
            "action_items": action_items or {},
            "comparison": comparison or {},
            "sources": sources,
            "uncertainty_flags": uncertainty_flags,
            "answer_guidance": (
                "Antworte auf Deutsch, kurz und belegbezogen. Nenne zuerst, was aus lokalen Dokumenten hervorgeht. "
                "Wenn WatsonX beigetragen hat, trenne allgemeine IV-Auskunft klar von Akteninhalt. "
                "Bei Unsicherheit keine bindende medizinische, finanzielle oder rechtliche Empfehlung geben."
            ),
        }

    def ask_watsonx_iv_assistant(
        self,
        *,
        question: str,
        context: str = "",
        thread_id: str = "",
        user_id: str = "",
        client: WatsonXOrchestrateClient | None = None,
    ) -> dict[str, Any]:
        watsonx_client = client or WatsonXOrchestrateClient()
        return watsonx_client.chat(question=question, context=context, thread_id=thread_id, user_id=user_id)


def get_knowledge_service() -> KnowledgeService:
    return KnowledgeService()


def search_internal_knowledge(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return get_knowledge_service().search_internal_knowledge(*args, **kwargs)


def retrieve_relevant_documents(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return get_knowledge_service().retrieve_relevant_documents(*args, **kwargs)


def summarize_document_context(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return get_knowledge_service().summarize_document_context(*args, **kwargs)


def compare_documents(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return get_knowledge_service().compare_documents(*args, **kwargs)


def extract_action_items(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return get_knowledge_service().extract_action_items(*args, **kwargs)


def synthesize_answer(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return get_knowledge_service().synthesize_answer(*args, **kwargs)


def ask_watsonx_iv_assistant(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return get_knowledge_service().ask_watsonx_iv_assistant(*args, **kwargs)
