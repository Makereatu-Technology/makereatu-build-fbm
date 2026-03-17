from __future__ import annotations

import json
import os
import uuid
from typing import Any, Dict, Optional

from sqlalchemy import create_engine, text


def _safe_json(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False)


def _pick_field(fields: list[dict], field_name: str) -> Optional[dict]:
    for f in fields:
        if f.get("field_name") == field_name:
            return f
    return None


def _scalarize_for_document(value: Any) -> Optional[str]:
    """
    Convert normalized/field values into a safe scalar string for document-level columns.
    Avoid storing dict/list JSON blobs into project_id/project_name.
    """
    if value is None:
        return None

    if isinstance(value, dict):
        # e.g. currency normalization {"currency": "USD", "amount": 151.06}
        amount = value.get("amount")
        if amount is not None:
            return str(amount)
        return None

    if isinstance(value, list):
        return None

    if isinstance(value, (int, float)):
        return str(value)

    s = str(value).strip()
    return s if s else None


def _pick_nonempty_field_value(fields: list[dict], field_name: str) -> Optional[str]:
    row = _pick_field(fields, field_name)
    if not row:
        return None

    normalized = _scalarize_for_document(row.get("normalized_value"))
    raw = _scalarize_for_document(row.get("field_value"))

    return normalized or raw


def _count_non_null_fields(fields: list[dict]) -> int:
    count = 0
    for f in fields:
        if f.get("normalized_value") is not None or f.get("field_value") is not None:
            count += 1
    return count


def _count_non_null_indicators(indicators: Dict[str, Any]) -> int:
    return sum(1 for _, v in indicators.items() if v is not None)


def load_core_json_to_postgres(core_json_path: str, database_url: str) -> Dict[str, Any]:
    if not os.path.exists(core_json_path):
        raise FileNotFoundError(core_json_path)

    with open(core_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    classification = data.get("classification", {}) or {}
    fields = data.get("fields", []) or []
    indicators = data.get("indicators", {}) or {}
    artifact_path = data.get("artifact_path")

    project_id = (
        _pick_nonempty_field_value(fields, "project_id")
        or _pick_nonempty_field_value(fields, "project_number")
    )
    project_name = _pick_nonempty_field_value(fields, "project_name")

    bank = classification.get("bank")
    doc_type = classification.get("doc_type")
    doc_confidence = classification.get("confidence")

    engine = create_engine(database_url, future=True)

    with engine.begin() as conn:
        # --------------------------------------------------
        # 1. Look for an existing document with same source_ref
        # --------------------------------------------------
        existing = conn.execute(
            text("""
                SELECT doc_id
                FROM documents
                WHERE source_ref = :source_ref
                LIMIT 1
            """),
            {"source_ref": artifact_path},
        ).fetchone()

        if existing:
            existing_doc_id = existing[0]

            # delete child rows first
            conn.execute(
                text("DELETE FROM indicators WHERE doc_id = :doc_id"),
                {"doc_id": existing_doc_id},
            )
            conn.execute(
                text("DELETE FROM extracted_fields WHERE doc_id = :doc_id"),
                {"doc_id": existing_doc_id},
            )
            conn.execute(
                text("DELETE FROM documents WHERE doc_id = :doc_id"),
                {"doc_id": existing_doc_id},
            )

        # --------------------------------------------------
        # 2. Insert fresh document row
        # --------------------------------------------------
        doc_id = str(uuid.uuid4())

        conn.execute(
            text("""
                INSERT INTO documents (
                    doc_id,
                    source_ref,
                    bank,
                    doc_type,
                    project_id,
                    project_name,
                    classification_confidence
                )
                VALUES (
                    :doc_id,
                    :source_ref,
                    :bank,
                    :doc_type,
                    :project_id,
                    :project_name,
                    :classification_confidence
                )
            """),
            {
                "doc_id": doc_id,
                "source_ref": artifact_path,
                "bank": bank,
                "doc_type": doc_type,
                "project_id": project_id,
                "project_name": project_name,
                "classification_confidence": doc_confidence,
            }
        )

        # --------------------------------------------------
        # 3. Insert extracted fields
        # --------------------------------------------------
        for frow in fields:
            conn.execute(
                text("""
                    INSERT INTO extracted_fields (
                        doc_id,
                        field_name,
                        field_value,
                        normalized_value,
                        unit,
                        page,
                        source_type,
                        evidence_text,
                        confidence,
                        debug_json
                    )
                    VALUES (
                        :doc_id,
                        :field_name,
                        :field_value,
                        CAST(:normalized_value AS JSONB),
                        :unit,
                        :page,
                        :source_type,
                        :evidence_text,
                        :confidence,
                        CAST(:debug_json AS JSONB)
                    )
                """),
                {
                    "doc_id": doc_id,
                    "field_name": frow.get("field_name"),
                    "field_value": None if frow.get("field_value") is None else str(frow.get("field_value")),
                    "normalized_value": _safe_json(frow.get("normalized_value")),
                    "unit": frow.get("unit"),
                    "page": frow.get("page"),
                    "source_type": frow.get("source_type"),
                    "evidence_text": frow.get("evidence_text"),
                    "confidence": frow.get("confidence"),
                    "debug_json": _safe_json(frow.get("debug", {})),
                }
            )

        # --------------------------------------------------
        # 4. Insert indicators
        # --------------------------------------------------
        for indicator_name, indicator_value in indicators.items():
            conn.execute(
                text("""
                    INSERT INTO indicators (
                        doc_id,
                        indicator_name,
                        indicator_value,
                        unit,
                        method,
                        provenance_json
                    )
                    VALUES (
                        :doc_id,
                        :indicator_name,
                        :indicator_value,
                        :unit,
                        :method,
                        CAST(:provenance_json AS JSONB)
                    )
                """),
                {
                    "doc_id": doc_id,
                    "indicator_name": indicator_name,
                    "indicator_value": indicator_value,
                    "unit": None,
                    "method": "core_json_basic_indicator",
                    "provenance_json": _safe_json({
                        "artifact_path": artifact_path,
                        "classification": classification,
                    }),
                }
            )

    return {
        "status": "ok",
        "doc_id": doc_id,
        "project_id": project_id,
        "project_name": project_name,
        "fields_loaded": len(fields),
        "fields_non_null": _count_non_null_fields(fields),
        "indicators_loaded": len(indicators),
        "indicators_non_null": _count_non_null_indicators(indicators),
        "core_json_path": core_json_path,
    }