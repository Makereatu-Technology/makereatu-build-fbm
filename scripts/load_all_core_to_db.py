import os
import sys
import glob
import traceback
from typing import List, Dict, Any

# ------------------------------------------------------
# Ensure project root is in Python path
# ------------------------------------------------------

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ------------------------------------------------------
# Import loader
# ------------------------------------------------------

from app.services.db.load_core_json import load_core_json_to_postgres


# ------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------

CORE_DIR = r"F:\D\Makereatu AI\Updated PAD_IEG_WB_ADB\_artifacts\_core_extraction"
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:kemboi@localhost:5432/makereatu_fbm",
)


# ------------------------------------------------------
# HELPERS
# ------------------------------------------------------

def _safe_int(v, default=0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _print_result(result: Dict[str, Any], path: str) -> None:
    doc_id = result.get("doc_id")
    project_name = result.get("project_name")
    fields_loaded = _safe_int(result.get("fields_loaded"), 0)
    indicators_loaded = _safe_int(result.get("indicators_loaded"), 0)

    print(
        "OK:",
        os.path.basename(path),
        "| doc_id:", doc_id,
        "| project:", project_name,
        "| fields:", fields_loaded,
        "| indicators:", indicators_loaded,
    )


def _print_warning_if_sparse(result: Dict[str, Any], path: str) -> None:
    """
    Warn when a file loaded successfully but still appears sparse.
    """
    fields_loaded = _safe_int(result.get("fields_loaded"), 0)
    indicators_loaded = _safe_int(result.get("indicators_loaded"), 0)
    project_name = result.get("project_name")

    warnings: List[str] = []

    if not project_name:
        warnings.append("missing project_name")
    if fields_loaded <= 2:
        warnings.append(f"very few fields_loaded={fields_loaded}")
    if indicators_loaded == 0:
        warnings.append("no indicators_loaded")

    if warnings:
        print(
            "WARN:",
            os.path.basename(path),
            "| " + "; ".join(warnings)
        )


# ------------------------------------------------------
# MAIN
# ------------------------------------------------------

def main():
    print("========================================")
    print(" Makereatu FBM Core JSON Loader ")
    print("========================================")
    print("CORE_DIR     :", CORE_DIR)
    print("DATABASE_URL :", DATABASE_URL)
    print()

    if not os.path.exists(CORE_DIR):
        print("ERROR: Core directory not found")
        print(CORE_DIR)
        return

    core_files = sorted(glob.glob(os.path.join(CORE_DIR, "*.core.json")))

    print("Core files found:", len(core_files))
    print()

    if not core_files:
        print("No .core.json files found.")
        print("Make sure you regenerate extraction outputs before loading.")
        return

    loaded = 0
    failed = 0
    sparse_loaded = 0

    for path in core_files:
        try:
            result = load_core_json_to_postgres(path, DATABASE_URL)

            loaded += 1
            _print_result(result, path)

            fields_loaded = _safe_int(result.get("fields_loaded"), 0)
            indicators_loaded = _safe_int(result.get("indicators_loaded"), 0)
            project_name = result.get("project_name")

            if not project_name or fields_loaded <= 2 or indicators_loaded == 0:
                sparse_loaded += 1
                _print_warning_if_sparse(result, path)

        except Exception as e:
            failed += 1

            print("FAILED:", os.path.basename(path))
            print("Error:", str(e))
            traceback.print_exc()
            print()

    print()
    print("========================================")
    print(" LOAD SUMMARY ")
    print("========================================")
    print("Loaded       :", loaded)
    print("Failed       :", failed)
    print("Sparse loaded:", sparse_loaded)
    print("Total        :", len(core_files))
    print("========================================")
    print()
    print("If extraction logic changed, regenerate the .core.json artifacts first.")


if __name__ == "__main__":
    main()