from __future__ import annotations

import os
import sys
import glob
import json
import traceback
from typing import Dict, List, Any

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.services.extraction.classify_doc_type import classify_doc_type
from app.services.extraction.chunk_by_headings import chunk_artifact_by_headings
from app.services.extraction.extract_core_fields_adb import extract_core_fields_adb
from app.services.extraction.extract_core_fields_wb import extract_core_fields_wb
from app.services.extraction.normalize_fields import normalize_extracted_fields
from app.services.extraction.compute_basic_indicators import compute_basic_indicators


ARTIFACT_DIR = r"F:\D\Makereatu AI\Updated PAD_IEG_WB_ADB\_artifacts"
OUTDIR = os.path.join(ARTIFACT_DIR, "_core_extraction")

os.makedirs(OUTDIR, exist_ok=True)


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


def main():
    artifact_files = sorted(glob.glob(os.path.join(ARTIFACT_DIR, "*.artifacts.json")))

    print("========================================")
    print(" Makereatu FBM Core Extraction ")
    print("========================================")
    print("ARTIFACT_DIR:", ARTIFACT_DIR)
    print("OUTDIR      :", OUTDIR)
    print("Found artifacts:", len(artifact_files))
    print()

    if not artifact_files:
        print("No artifact files found.")
        return

    ok = 0
    failed = 0

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

            if cls.bank == "ADB":
                fields = extract_core_fields_adb(chunk_dicts)
            elif cls.bank == "WB":
                fields = extract_core_fields_wb(chunk_dicts)
            else:
                fields = []

            field_dicts = [f.__dict__ for f in fields]
            normalized = normalize_extracted_fields(field_dicts)
            indicators = compute_basic_indicators(normalized)

            payload = {
                "artifact_path": path,
                "classification": cls.__dict__,
                "fields": normalized,
                "indicators": indicators,
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
            )

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
    print("OK     :", ok)
    print("FAILED :", failed)
    print("TOTAL  :", len(artifact_files))
    print("========================================")


if __name__ == "__main__":
    main()