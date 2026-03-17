from __future__ import annotations

import os
import sys
import glob
import json
import traceback
from typing import Dict, List, Any, Optional

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.services.extraction.classify_doc_type import classify_doc_type
from app.services.extraction.chunk_by_headings import chunk_artifact_by_headings
from app.services.extraction.extract_core_fields_adb import extract_core_fields_adb
from app.services.extraction.extract_core_fields_wb import extract_core_fields_wb
from app.services.extraction.normalize_fields import normalize_extracted_fields
from app.services.extraction.compute_basic_indicators import compute_basic_indicators

# Optional LLM fallback
try:
    from app.services.extraction.llm_fallback_core import llm_fallback_core_fields
    LLM_FALLBACK_AVAILABLE = True
except Exception:
    llm_fallback_core_fields = None
    LLM_FALLBACK_AVAILABLE = False


ARTIFACT_DIR = r"F:\D\Makereatu AI\Updated PAD_IEG_WB_ADB\_artifacts"
OUTDIR = os.path.join(ARTIFACT_DIR, "_core_extraction")

os.makedirs(OUTDIR, exist_ok=True)

ENABLE_LLM_FALLBACK = True


def _get_field_row(fields: List[Dict[str, Any]], field_name: str) -> Optional[Dict[str, Any]]:
    for f in fields:
        if f.get("field_name") == field_name:
            return f
    return None


def _get_field_value(fields: List[Dict[str, Any]], field_name: str, normalized: bool = False):
    for f in fields:
        if f.get("field_name") == field_name:
            return f.get("normalized_value") if normalized else f.get("field_value")
    return None


def _summarize_missing(fields: List[Dict[str, Any]], indicators: Dict[str, Any]) -> List[str]:
    warnings: List[str] = []

    critical_fields = [
        "project_name",
        "project_id",
        "approval_date",
        "planned_closing_date",
        "actual_closing_date",
        "output_target_km",
        "output_actual_km",
        "err_percent",
        "eirr_percent",
    ]

    for name in critical_fields:
        val = _get_field_value(fields, name, normalized=True)
        if val is None:
            warnings.append(f"missing:{name}")

    for key in ["delay_years", "delay_months", "delivery_gap_km_ratio", "economic_rate_percent"]:
        if indicators.get(key) is None:
            warnings.append(f"indicator_null:{key}")

    return warnings


def _count_non_null_fields(fields: List[Dict[str, Any]]) -> int:
    count = 0
    for f in fields:
        if f.get("normalized_value") is not None:
            count += 1
    return count


def _is_bad_project_name(name: Optional[str]) -> bool:
    if not name:
        return True

    s = str(name).strip().lower()

    if not s:
        return True

    bad_contains = [
        "has the development objective been changed",
        "project development objective",
        "board approval",
        "date posted",
        "report number",
        "implementation completion report",
        "independent evaluation group",
    ]
    if any(x in s for x in bad_contains):
        return True

    if len(s.split()) < 2:
        return True

    return False


def _needs_llm_repair(
    classification: Dict[str, Any],
    normalized_fields: List[Dict[str, Any]],
    indicators: Dict[str, Any],
) -> bool:
    bank = classification.get("bank")
    if bank not in {"ADB", "WB"}:
        return False

    project_name = _get_field_value(normalized_fields, "project_name", normalized=True)

    planned = _get_field_value(normalized_fields, "planned_closing_date", normalized=True)
    actual = _get_field_value(normalized_fields, "actual_closing_date", normalized=True)
    target_km = _get_field_value(normalized_fields, "output_target_km", normalized=True)
    actual_km = _get_field_value(normalized_fields, "output_actual_km", normalized=True)
    err = _get_field_value(normalized_fields, "err_percent", normalized=True)
    eirr = _get_field_value(normalized_fields, "eirr_percent", normalized=True)

    ratio = indicators.get("delivery_gap_km_ratio")

    # Trigger LLM only for unresolved or suspicious cases
    if _is_bad_project_name(project_name):
        return True

    if planned is None or actual is None:
        return True

    if target_km is None or actual_km is None:
        return True

    if err is None and eirr is None:
        return True

    # suspicious ratio guard
    if isinstance(ratio, (int, float)) and (ratio > 3 or ratio < -1):
        return True

    return False


def _build_llm_targets(
    normalized_fields: List[Dict[str, Any]],
    indicators: Dict[str, Any],
) -> List[str]:
    targets: List[str] = []

    project_name = _get_field_value(normalized_fields, "project_name", normalized=True)
    if _is_bad_project_name(project_name):
        targets.append("project_name")

    if _get_field_value(normalized_fields, "project_id", normalized=True) is None:
        targets.append("project_id")

    if _get_field_value(normalized_fields, "planned_closing_date", normalized=True) is None:
        targets.append("planned_closing_date")

    if _get_field_value(normalized_fields, "actual_closing_date", normalized=True) is None:
        targets.append("actual_closing_date")

    if _get_field_value(normalized_fields, "output_target_km", normalized=True) is None:
        targets.append("output_target_km")

    if _get_field_value(normalized_fields, "output_actual_km", normalized=True) is None:
        targets.append("output_actual_km")

    if _get_field_value(normalized_fields, "err_percent", normalized=True) is None:
        if _get_field_value(normalized_fields, "eirr_percent", normalized=True) is None:
            targets.append("err_percent")
            targets.append("eirr_percent")

    ratio = indicators.get("delivery_gap_km_ratio")
    if isinstance(ratio, (int, float)) and (ratio > 3 or ratio < -1):
        if "output_target_km" not in targets:
            targets.append("output_target_km")
        if "output_actual_km" not in targets:
            targets.append("output_actual_km")

    return targets


def _merge_llm_repairs(
    original_fields: List[Dict[str, Any]],
    llm_fields: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Replace only missing or obviously bad fields with LLM-repaired ones.
    """
    merged = [dict(x) for x in original_fields]

    for lf in llm_fields:
        fname = lf.get("field_name")
        if not fname:
            continue

        existing = _get_field_row(merged, fname)
        new_norm = lf.get("normalized_value")
        new_raw = lf.get("field_value")

        if existing is None:
            merged.append(lf)
            continue

        old_norm = existing.get("normalized_value")
        old_raw = existing.get("field_value")

        should_replace = False

        if old_norm is None and new_norm is not None:
            should_replace = True
        elif old_raw is None and new_raw is not None:
            should_replace = True
        elif fname == "project_name" and _is_bad_project_name(old_norm or old_raw):
            should_replace = True

        if should_replace:
            existing.update(lf)

    return merged


def main():
    artifact_files = sorted(glob.glob(os.path.join(ARTIFACT_DIR, "*.artifacts.json")))

    print("========================================")
    print(" Makereatu FBM Core Extraction ")
    print("========================================")
    print("ARTIFACT_DIR:", ARTIFACT_DIR)
    print("OUTDIR      :", OUTDIR)
    print("Found artifacts:", len(artifact_files))
    print("LLM fallback available:", LLM_FALLBACK_AVAILABLE)
    print("LLM fallback enabled  :", ENABLE_LLM_FALLBACK)
    print()

    if not artifact_files:
        print("No artifact files found.")
        return

    ok = 0
    failed = 0
    repaired = 0

    for path in artifact_files:
        try:
            cls = classify_doc_type(path)
            chunks = chunk_artifact_by_headings(path)

            chunk_dicts = [
                {
                    "chunk_id": c.chunk_id,
                    "label": c.label,
                    "pages": c.pages,
                    "text": c.text,
                    "tables": c.tables,
                }
                for c in chunks
            ]

            # -------------------------
            # Primary rule-based extraction
            # -------------------------
            if cls.bank == "ADB":
                fields = extract_core_fields_adb(chunk_dicts)
            elif cls.bank == "WB":
                fields = extract_core_fields_wb(chunk_dicts)
            else:
                fields = []

            field_dicts = [f.__dict__ for f in fields]
            normalized = normalize_extracted_fields(field_dicts)
            indicators = compute_basic_indicators(normalized)

            used_llm_fallback = False
            llm_targets: List[str] = []

            # -------------------------
            # Optional LLM repair pass
            # -------------------------
            if ENABLE_LLM_FALLBACK and LLM_FALLBACK_AVAILABLE:
                if _needs_llm_repair(cls.__dict__, normalized, indicators):
                    llm_targets = _build_llm_targets(normalized, indicators)

                    if llm_targets:
                        llm_repaired_fields = llm_fallback_core_fields(
                            artifact_path=path,
                            classification=cls.__dict__,
                            chunks=chunk_dicts,
                            existing_fields=normalized,
                            target_fields=llm_targets,
                        ) or []

                        if llm_repaired_fields:
                            normalized = _merge_llm_repairs(normalized, llm_repaired_fields)
                            indicators = compute_basic_indicators(normalized)
                            used_llm_fallback = True
                            repaired += 1

            payload = {
                "artifact_path": path,
                "classification": cls.__dict__,
                "fields": normalized,
                "indicators": indicators,
                "llm_fallback_used": used_llm_fallback,
                "llm_target_fields": llm_targets,
            }

            out_name = os.path.basename(path).replace(".artifacts.json", ".core.json")
            out_path = os.path.join(OUTDIR, out_name)

            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

            non_null_fields = _count_non_null_fields(normalized)
            project_name = _get_field_value(normalized, "project_name", normalized=True)
            warnings = _summarize_missing(normalized, indicators)

            print(
                "OK:",
                os.path.basename(path),
                "| bank:", cls.bank,
                "| doc_type:", cls.doc_type,
                "| confidence:", cls.confidence,
                "| project:", project_name,
                "| fields_non_null:", non_null_fields,
                "| warnings:", len(warnings),
                "| llm:", used_llm_fallback,
            )

            if llm_targets:
                print("   -> llm_targets:", ", ".join(llm_targets))

            if warnings:
                print("   ->", "; ".join(warnings[:12]))

            ok += 1

        except Exception as e:
            failed += 1
            print("FAIL:", os.path.basename(path))
            print("Error:", type(e).__name__, str(e))
            traceback.print_exc()
            print()

    print()
    print("========================================")
    print(" EXTRACTION SUMMARY ")
    print("========================================")
    print("OK       :", ok)
    print("FAILED   :", failed)
    print("REPAIRED :", repaired)
    print("TOTAL    :", len(artifact_files))
    print("========================================")


if __name__ == "__main__":
    main()