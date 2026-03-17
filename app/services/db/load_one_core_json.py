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


def load_core_json_to_postgres(core_json_path: str, database_url: str) -> Dict[str, Any]:
    if not os.path.exists(core_json_path):
        raise FileNotFoundError(core_json_path)

    with open(core_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    classification = data.get("classification", {}) or {}
    fields = data.get("fields", []) or []
    indicators = data.get("indicators", {}) or {}
    artifact_path = data.get("artifact_path")

    project_id_field = _pick_field(fields, "project_id") or _pick_field(fields, "project_number")
    project_name_field = _pick_field(fields, "project_name")

    project_id = project_id_field.get("normalized_value") or project_id_field.get("field_value") if project_id_field else None
    project_name = project_name_field.get("normalized_value") or project_name_field.get("field_value") if project_name_field else None

    bank = classification.get("bank")
    doc_type = classification.get("doc_type")
    doc_confidence = classification.get("confidence")

    doc_id = str(uuid.uuid4())

    engine = create_engine(database_url, future=True)

    with engine.begin() as conn:
        # 1. documents
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

        # 2. extracted_fields
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
                        :normalized_value,
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
                    "normalized_value": None if frow.get("normalized_value") is None else _safe_json(frow.get("normalized_value")),
                    "unit": frow.get("unit"),
                    "page": frow.get("page"),
                    "source_type": frow.get("source_type"),
                    "evidence_text": frow.get("evidence_text"),
                    "confidence": frow.get("confidence"),
                    "debug_json": _safe_json(frow.get("debug", {})),
                }
            )

        # 3. indicators
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
        "indicators_loaded": len(indicators),
        "core_json_path": core_json_path,
    }