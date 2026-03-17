from __future__ import annotations

import os
import sys
import glob
import traceback

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.services.extraction.pdf_artifacts import build_artifacts, save_artifacts_json


FOLDER = r"F:\D\Makereatu AI\Updated PAD_IEG_WB_ADB"
OUTDIR = os.path.join(FOLDER, "_artifacts")

# Control whether to overwrite existing artifact json files
OVERWRITE = True

# OCR / extraction settings
DO_OCR = True
OCR_ONLY_IF_NEEDED = True
OCR_DPI = 250
EXTRACT_TABLES = True
TABLE_MAX_PAGES = 40
METADATA_PAGE_LIMIT = 5


def main() -> None:
    os.makedirs(OUTDIR, exist_ok=True)

    pdfs = sorted(glob.glob(os.path.join(FOLDER, "**", "*.pdf"), recursive=True))

    print("========================================")
    print(" Makereatu FBM PDF Artifact Extraction ")
    print("========================================")
    print("PROJECT_ROOT        :", PROJECT_ROOT)
    print("SOURCE FOLDER       :", FOLDER)
    print("OUTPUT DIR          :", OUTDIR)
    print("Found PDFs          :", len(pdfs))
    print("OVERWRITE           :", OVERWRITE)
    print("DO_OCR              :", DO_OCR)
    print("OCR_ONLY_IF_NEEDED  :", OCR_ONLY_IF_NEEDED)
    print("OCR_DPI             :", OCR_DPI)
    print("EXTRACT_TABLES      :", EXTRACT_TABLES)
    print("TABLE_MAX_PAGES     :", TABLE_MAX_PAGES)
    print("METADATA_PAGE_LIMIT :", METADATA_PAGE_LIMIT)
    print()

    if not pdfs:
        print("No PDFs found.")
        return

    ok = 0
    failed = 0
    skipped = 0

    for idx, p in enumerate(pdfs, start=1):
        try:
            print(f"[{idx}/{len(pdfs)}] Processing: {os.path.basename(p)}")

            art = build_artifacts(
                p,
                do_ocr=DO_OCR,
                ocr_only_if_needed=OCR_ONLY_IF_NEEDED,
                ocr_dpi=OCR_DPI,
                extract_tables=EXTRACT_TABLES,
                table_max_pages=TABLE_MAX_PAGES,
                metadata_page_limit=METADATA_PAGE_LIMIT,
            )

            out = os.path.join(OUTDIR, f"{art.sha256}.artifacts.json")

            if os.path.exists(out) and not OVERWRITE:
                skipped += 1
                print("  SKIP: artifact already exists ->", out)
                continue

            save_artifacts_json(art, out)

            print(
                "  OK:",
                os.path.basename(p),
                "->",
                out,
                "| pages:",
                art.num_pages,
                "| tables:",
                len(art.tables),
                "| warnings:",
                len(art.warnings),
            )

            if art.warnings:
                for w in art.warnings[:5]:
                    print("   warning:", w)

            ok += 1

        except Exception as e:
            failed += 1
            print("  FAIL:", p)
            print("  Error:", type(e).__name__, e)
            traceback.print_exc()
            print()

    print()
    print("========================================")
    print(" EXTRACTION SUMMARY ")
    print("========================================")
    print("OK      :", ok)
    print("FAILED  :", failed)
    print("SKIPPED :", skipped)
    print("TOTAL   :", len(pdfs))
    print("========================================")


if __name__ == "__main__":
    main()