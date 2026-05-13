import json
import urllib.parse
from typing import Any

try:
    from ..services.storage_service import (
        build_chat_document_artifact as service_build_chat_document_artifact,
        classify_document as service_classify_document,
        count_documents as service_count_documents,
        get_document as service_get_document,
        group_documents as service_group_documents,
        infer_document_bucket_from_text,
        list_documents as service_list_documents,
        list_folders as service_list_folders,
        match_documents as service_match_documents,
        search_documents as service_search_documents,
        sum_invoice_amounts as service_sum_invoice_amounts,
        summarize_document as service_summarize_document,
    )
except ImportError:
    from services.storage_service import (
        build_chat_document_artifact as service_build_chat_document_artifact,
        classify_document as service_classify_document,
        count_documents as service_count_documents,
        get_document as service_get_document,
        group_documents as service_group_documents,
        infer_document_bucket_from_text,
        list_documents as service_list_documents,
        list_folders as service_list_folders,
        match_documents as service_match_documents,
        search_documents as service_search_documents,
        sum_invoice_amounts as service_sum_invoice_amounts,
        summarize_document as service_summarize_document,
    )


def _json_list(value: str) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except json.JSONDecodeError:
        pass
    return [item.strip() for item in raw.split(",") if item.strip()]


def _optional_int(value: int | str | None) -> int | None:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return None
    return parsed or None


def build_storage_tools(
    function_tool: Any,
    *,
    context_user_id: str,
    thread_id: str,
    tool_events: list[dict[str, Any]],
    drafted_actions: list[dict[str, Any]],
    structured_actions: list[dict[str, Any]],
    collected_artifacts: list[dict[str, Any]] | None = None,
    register_pending_actions: Any,
    make_json_safe: Any,
    tool_event_factory: Any,
) -> list[Any]:
    tools: list[Any] = []

    def _storage_tool_result(tool_name: str, callback: Any) -> str:
        tool_events.append(tool_event_factory(tool_name, "started", f"{tool_name} started"))
        try:
            payload = callback()
        except Exception as exc:
            tool_events.append(tool_event_factory(tool_name, "failed", str(exc)))
            raise
        tool_events.append(tool_event_factory(tool_name, "completed", f"{tool_name} completed"))
        return json.dumps(make_json_safe(payload), ensure_ascii=True)

    def _register_storage_pending_action(action_type: str, title: str, payload: dict[str, Any]) -> dict[str, Any]:
        action_payload = {**payload, "user_id": context_user_id}
        actions = register_pending_actions(
            [{"type": action_type, "title": title, "payload": action_payload, "user_id": context_user_id}],
            thread_id=thread_id,
            user_id=context_user_id,
        )
        drafted_actions.extend(actions)
        structured_actions.extend(actions)
        return {"pending_actions": actions}

    def _collect_document_artifacts(documents: Any) -> None:
        if collected_artifacts is None:
            return
        existing_ids = {
            str(item.get("document_id") or item.get("id"))
            for item in collected_artifacts
            if isinstance(item, dict)
        }
        for document in documents if isinstance(documents, list) else [documents]:
            if not isinstance(document, dict) or not document.get("document_id"):
                continue
            document_id = str(document.get("document_id"))
            if document_id in existing_ids:
                continue
            artifact = dict(document) if document.get("download_url") else service_build_chat_document_artifact(document)
            artifact.setdefault("id", document_id)
            artifact.setdefault("type", "document")
            collected_artifacts.append(artifact)
            existing_ids.add(document_id)

    @function_tool
    def list_documents(
        year: int = 0,
        month: int = 0,
        start_date: str = "",
        end_date: str = "",
        document_type: str = "",
        institution: str = "",
        tags_json: str = "[]",
        folder_id: str = "",
        storage_bucket: str = "",
        limit: int = 25,
    ) -> str:
        """List stored documents for the current user with optional year, month, type, institution, tags, folder, and storage bucket filters."""
        def read() -> dict[str, Any]:
            documents = service_list_documents(
                user_id=context_user_id,
                year=_optional_int(year),
                month=_optional_int(month),
                start_date=start_date or None,
                end_date=end_date or None,
                document_type=document_type,
                institution=institution,
                tags=_json_list(tags_json),
                folder_id=folder_id or None,
                storage_bucket=storage_bucket or infer_document_bucket_from_text(institution),
                limit=limit,
            )
            _collect_document_artifacts(documents)
            return {"documents": documents}

        return _storage_tool_result(
            "list_documents",
            read,
        )

    tools.append(list_documents)

    @function_tool
    def search_documents(
        query: str,
        year: int = 0,
        month: int = 0,
        start_date: str = "",
        end_date: str = "",
        document_type: str = "",
        institution: str = "",
        tags_json: str = "[]",
        storage_bucket: str = "",
        limit: int = 10,
    ) -> str:
        """Search stored documents for the current user by filename, summary, metadata, extracted text, and optional storage bucket."""
        def read() -> dict[str, Any]:
            documents = service_search_documents(
                user_id=context_user_id,
                query=query,
                year=_optional_int(year),
                month=_optional_int(month),
                start_date=start_date or None,
                end_date=end_date or None,
                document_type=document_type,
                institution=institution,
                tags=_json_list(tags_json),
                storage_bucket=storage_bucket or infer_document_bucket_from_text(query),
                limit=limit,
            )
            _collect_document_artifacts(documents)
            return {"query": query, "documents": documents}

        return _storage_tool_result(
            "search_documents",
            read,
        )

    tools.append(search_documents)

    @function_tool
    def count_documents(
        year: int = 0,
        month: int = 0,
        start_date: str = "",
        end_date: str = "",
        document_type: str = "",
        institution: str = "",
        tags_json: str = "[]",
        folder_id: str = "",
        storage_bucket: str = "",
    ) -> str:
        """Count stored documents for the current user with optional filters."""
        return _storage_tool_result(
            "count_documents",
            lambda: service_count_documents(
                user_id=context_user_id,
                year=_optional_int(year),
                month=_optional_int(month),
                start_date=start_date or None,
                end_date=end_date or None,
                document_type=document_type,
                institution=institution,
                tags=_json_list(tags_json),
                folder_id=folder_id or None,
                storage_bucket=storage_bucket or infer_document_bucket_from_text(institution),
            ),
        )

    tools.append(count_documents)

    @function_tool
    def get_document_details(document_id: str, include_signed_url: bool = False) -> str:
        """Get metadata, extracted text, and optionally a short-lived signed URL for one document."""
        def read() -> dict[str, Any]:
            document = service_get_document(
                user_id=context_user_id,
                document_id=document_id,
                include_signed_url=include_signed_url,
            )
            _collect_document_artifacts(document)
            return {"document": document}

        return _storage_tool_result(
            "get_document_details",
            read,
        )

    tools.append(get_document_details)

    @function_tool
    def summarize_document(document_id: str) -> str:
        """Summarize one stored document. Image-only documents return an honest no-text message."""
        return _storage_tool_result(
            "summarize_document",
            lambda: service_summarize_document(user_id=context_user_id, document_id=document_id),
        )

    tools.append(summarize_document)

    @function_tool
    def classify_document(document_id: str) -> str:
        """Classify one stored document and update its metadata if text is available."""
        return _storage_tool_result(
            "classify_document",
            lambda: service_classify_document(user_id=context_user_id, document_id=document_id),
        )

    tools.append(classify_document)

    @function_tool
    def group_documents(
        group_by: str = "month",
        year: int = 0,
        month: int = 0,
        start_date: str = "",
        end_date: str = "",
        document_type: str = "",
        institution: str = "",
        tags_json: str = "[]",
        storage_bucket: str = "",
    ) -> str:
        """Group the current user's documents by month, type, institution, or folder."""
        return _storage_tool_result(
            "group_documents",
            lambda: service_group_documents(
                user_id=context_user_id,
                group_by=group_by,
                year=_optional_int(year),
                month=_optional_int(month),
                start_date=start_date or None,
                end_date=end_date or None,
                document_type=document_type,
                institution=institution,
                tags=_json_list(tags_json),
                storage_bucket=storage_bucket or infer_document_bucket_from_text(institution),
            ),
        )

    tools.append(group_documents)

    @function_tool
    def sum_invoice_amounts(
        query: str = "",
        year: int = 0,
        month: int = 0,
        start_date: str = "",
        end_date: str = "",
        institution: str = "",
        tags_json: str = "[]",
        storage_bucket: str = "",
        limit: int = 100,
    ) -> str:
        """Sum stored invoice amounts after filtering documents first; exact duplicate checksums are ignored."""
        def read() -> dict[str, Any]:
            payload = service_sum_invoice_amounts(
                user_id=context_user_id,
                query=query,
                year=_optional_int(year),
                month=_optional_int(month),
                start_date=start_date or None,
                end_date=end_date or None,
                institution=institution,
                tags=_json_list(tags_json),
                storage_bucket=storage_bucket or infer_document_bucket_from_text(f"{query} {institution}"),
                limit=limit,
            )
            _collect_document_artifacts(payload.get("counted_documents") or [])
            _collect_document_artifacts(payload.get("documents_without_amount") or [])
            return payload

        return _storage_tool_result("sum_invoice_amounts", read)

    tools.append(sum_invoice_amounts)

    @function_tool
    def list_document_folders() -> str:
        """List folders for the current user's stored documents."""
        return _storage_tool_result(
            "list_document_folders",
            lambda: {"folders": service_list_folders(user_id=context_user_id)},
        )

    tools.append(list_document_folders)

    @function_tool
    def match_documents(document_id: str, limit: int = 5) -> str:
        """Find likely related documents for one stored document."""
        return _storage_tool_result(
            "match_documents",
            lambda: service_match_documents(user_id=context_user_id, document_id=document_id, limit=limit),
        )

    tools.append(match_documents)

    @function_tool
    def bundle_documents(
        document_ids_json: str = "[]",
        query: str = "",
        storage_bucket: str = "",
        document_type: str = "",
        institution: str = "",
        tags_json: str = "[]",
        limit: int = 10,
    ) -> str:
        """Create a downloadable ZIP artifact URL for selected or searched documents without changing storage."""

        def read() -> dict[str, Any]:
            document_ids = _json_list(document_ids_json)
            documents: list[dict[str, Any]] = []
            if document_ids:
                for document_id in document_ids[:20]:
                    document = service_get_document(user_id=context_user_id, document_id=document_id)
                    if document:
                        documents.append(document)
            else:
                documents = service_search_documents(
                    user_id=context_user_id,
                    query=query,
                    document_type=document_type,
                    institution=institution,
                    tags=_json_list(tags_json),
                    storage_bucket=storage_bucket or infer_document_bucket_from_text(f"{query} {institution}"),
                    limit=min(max(1, int(limit or 10)), 20),
                )
            selected_ids = [
                str(document.get("document_id") or "").strip()
                for document in documents
                if isinstance(document, dict) and str(document.get("document_id") or "").strip()
            ][:20]
            _collect_document_artifacts(documents)
            if not selected_ids:
                return {"bundle": None, "documents": [], "count": 0}
            query_params = urllib.parse.urlencode(
                {
                    "profile_id": context_user_id,
                    "document_ids": ",".join(selected_ids),
                }
            )
            bundle = {
                "id": f"document-bundle-{selected_ids[0]}-{len(selected_ids)}",
                "type": "document_bundle",
                "title": "Dokumentenpaket.zip",
                "file_name": "documents_bundle.zip",
                "content_type": "application/zip",
                "document_ids": selected_ids,
                "download_url": f"/api/documents/bundle?{query_params}",
            }
            if collected_artifacts is not None:
                collected_artifacts.append(bundle)
            return {"bundle": bundle, "documents": documents, "count": len(selected_ids)}

        return _storage_tool_result("bundle_documents", read)

    tools.append(bundle_documents)

    @function_tool
    def create_document_folder(name: str, parent_folder_id: str = "", color: str = "", document_ids_json: str = "[]") -> str:
        """Draft folder creation for user confirmation. Optionally include document_ids_json to assign documents after confirmation."""
        return _storage_tool_result(
            "create_document_folder",
            lambda: _register_storage_pending_action(
                "storage.create_folder",
                f"Ordner erstellen: {name}",
                {
                    "name": name,
                    "parent_folder_id": parent_folder_id or None,
                    "color": color,
                    "document_ids": _json_list(document_ids_json),
                },
            ),
        )

    tools.append(create_document_folder)

    @function_tool
    def move_document(document_id: str = "", folder_id: str = "", document_ids_json: str = "[]") -> str:
        """Draft moving one or more documents into a folder for user confirmation."""
        document_ids = _json_list(document_ids_json)
        if document_id:
            document_ids = [document_id, *[item for item in document_ids if item != document_id]]
        return _storage_tool_result(
            "move_document",
            lambda: _register_storage_pending_action(
                "storage.move_document",
                "Dokument verschieben",
                {"document_id": document_id, "document_ids": document_ids, "folder_id": folder_id or None},
            ),
        )

    tools.append(move_document)

    @function_tool
    def delete_document(document_id: str) -> str:
        """Draft deleting one document for user confirmation."""
        return _storage_tool_result(
            "delete_document",
            lambda: _register_storage_pending_action(
                "storage.delete_document",
                "Dokument loeschen",
                {"document_id": document_id},
            ),
        )

    tools.append(delete_document)

    @function_tool
    def update_document_metadata(document_id: str, updates_json: str) -> str:
        """Draft a document metadata update for user confirmation. updates_json must be a JSON object."""

        def draft() -> dict[str, Any]:
            updates = json.loads(updates_json or "{}")
            if not isinstance(updates, dict):
                raise ValueError("updates_json must be a JSON object")
            return _register_storage_pending_action(
                "storage.update_metadata",
                "Dokument-Metadaten aktualisieren",
                {"document_id": document_id, "updates": updates},
            )

        return _storage_tool_result("update_document_metadata", draft)

    tools.append(update_document_metadata)

    @function_tool
    def reassign_document_bucket(document_id: str, bucket: str, reason: str = "") -> str:
        """Draft moving a document into another canonical storage bucket for user confirmation."""
        return _storage_tool_result(
            "reassign_document_bucket",
            lambda: _register_storage_pending_action(
                "storage.reassign_bucket",
                f"Dokument in Bucket verschieben: {bucket}",
                {
                    "document_id": document_id,
                    "bucket": bucket,
                    "reason": reason,
                },
            ),
        )

    tools.append(reassign_document_bucket)

    return tools
