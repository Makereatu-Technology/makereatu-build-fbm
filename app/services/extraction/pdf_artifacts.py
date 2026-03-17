from __future__ import annotations

import os
import re
import json
import hashlib
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

import fitz
import pdfplumber

# Optional OCR (requires: pytesseract + pillow + Tesseract EXE installed)
try:
    import pytesseract
    from PIL import Image

    TESSERACT_EXE = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    if os.path.exists(TESSERACT_EXE):
        pytesseract.pytesseract.tesseract_cmd = TESSERACT_EXE
        OCR_AVAILABLE = True
    else:
        OCR_AVAILABLE = False
except Exception:
    pytesseract = None
    Image = None
    OCR_AVAILABLE = False


# -------------------------
# Data models
# -------------------------
@dataclass
class PageArtifact:
    page: int                 # 1-based
    text: str
    is_scanned: bool
    used_ocr: bool
    raw_text_len: int
    ocr_text_len: int


@dataclass
class TableArtifact:
    page: int                 # 1-based
    table_index: int
    rows: List[List[str]]     # 2D cell matrix


@dataclass
class DocArtifacts:
    pdf_path: str
    file_name: str
    sha256: str
    num_pages: int
    pages: List[PageArtifact]
    tables: List[TableArtifact]
    warnings: List[str]


# -------------------------
# Helpers
# -------------------------
_WS = re.compile(r"[ \t]+")
_MANY_NL = re.compile(r"\n{3,}")


def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\x00", " ").replace("\u00a0", " ")
    s = _WS.sub(" ", s)
    s = _MANY_NL.sub("\n\n", s)
    return s.strip()


def sha256_file(path: str, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def text_quality_score(text: str) -> float:
    """
    Lightweight heuristic for deciding whether extracted text is usable.
    """
    if not text:
        return 0.0

    t = normalize_text(text)
    if not t:
        return 0.0

    non_ws = re.sub(r"\s+", "", t)
    length_score = min(len(non_ws) / 500.0, 1.0)

    alpha = sum(ch.isalpha() for ch in t)
    digit = sum(ch.isdigit() for ch in t)
    printable = sum(ch.isprintable() for ch in t)

    alpha_ratio = alpha / max(1, len(t))
    printable_ratio = printable / max(1, len(t))
    alnum_ratio = (alpha + digit) / max(1, len(t))

    score = (
        0.45 * length_score
        + 0.25 * alpha_ratio
        + 0.15 * printable_ratio
        + 0.15 * alnum_ratio
    )
    return round(score, 4)


def looks_scanned(text: str, min_chars: int = 50) -> bool:
    """
    More conservative than before.
    Avoid marking metadata-heavy pages as scanned too aggressively.
    """
    t = normalize_text(text)
    non_ws = re.sub(r"\s+", "", t)

    if len(non_ws) == 0:
        return True

    if len(non_ws) < min_chars:
        alpha = sum(ch.isalpha() for ch in t)
        return alpha < 10

    alpha = sum(ch.isalpha() for ch in t)
    alpha_ratio = alpha / max(1, len(t))

    return alpha_ratio < 0.08


def ocr_page(doc: fitz.Document, page_index0: int, dpi: int = 250) -> str:
    if not OCR_AVAILABLE:
        raise RuntimeError("OCR not available. Install pytesseract + pillow and Tesseract.")
    page = doc.load_page(page_index0)
    mat = fitz.Matrix(dpi / 72.0, dpi / 72.0)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    txt = pytesseract.image_to_string(img, lang="eng")
    return normalize_text(txt)


def extract_tables_pdfplumber(pdf_path: str, page_index0: int) -> List[List[List[str]]]:
    """
    Returns list of tables; each table is rows; each row is cells.
    Works best on digital PDFs; scanned PDFs usually will not yield tables here.
    """
    out: List[List[List[str]]] = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[page_index0]
            tables = page.extract_tables() or []

            for t in tables:
                cleaned: List[List[str]] = []
                for row in (t or []):
                    cleaned_row = [normalize_text(c or "") for c in (row or [])]
                    cleaned.append(cleaned_row)

                if len(cleaned) >= 2 and any(any(cell for cell in r) for r in cleaned):
                    out.append(cleaned)
    except Exception:
        return []
    return out


def _extract_key_value_lines_as_table(text: str) -> List[List[str]]:
    """
    Fallback pseudo-table for metadata pages when real table extraction fails.
    Converts lines like:
      Project Number: 30255
      Closing Date: 30 April 2005 9 April 2008
    into pseudo rows.
    """
    rows: List[List[str]] = []
    lines = [normalize_text(x) for x in (text or "").splitlines() if normalize_text(x)]

    for line in lines:
        if ":" in line:
            left, right = line.split(":", 1)
            left = normalize_text(left)
            right = normalize_text(right)
            if left and right:
                rows.append([left, right])

    return rows


# -------------------------
# Main extraction
# -------------------------
def build_artifacts(
    pdf_path: str,
    *,
    do_ocr: bool = True,
    ocr_only_if_needed: bool = True,
    ocr_dpi: int = 250,
    extract_tables: bool = True,
    table_max_pages: int = 40,
    metadata_page_limit: int = 5,
) -> DocArtifacts:
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(pdf_path)
    if not pdf_path.lower().endswith(".pdf"):
        raise ValueError(f"Not a PDF: {pdf_path}")

    warnings: List[str] = []
    doc = fitz.open(pdf_path)
    num_pages = doc.page_count

    pages: List[PageArtifact] = []
    tables: List[TableArtifact] = []

    for i in range(num_pages):
        raw = doc.load_page(i).get_text("text") or ""
        raw_text = normalize_text(raw)
        raw_score = text_quality_score(raw_text)

        scanned = looks_scanned(raw_text)
        used_ocr = False
        final_text = raw_text
        ocr_text = ""

        should_try_ocr = do_ocr and (
            (not ocr_only_if_needed)
            or scanned
            or (i < metadata_page_limit and raw_score < 0.35)
        )

        if should_try_ocr:
            if OCR_AVAILABLE:
                try:
                    ocr_text = ocr_page(doc, i, dpi=ocr_dpi)
                    ocr_score = text_quality_score(ocr_text)

                    # Prefer OCR when clearly better
                    if ocr_score > raw_score + 0.08:
                        final_text = ocr_text
                        used_ocr = True
                        scanned = True
                except Exception as e:
                    warnings.append(f"OCR failed p{i+1}: {type(e).__name__}: {e}")
            else:
                warnings.append("OCR requested but pytesseract/PIL/Tesseract not available; skipping OCR.")
                do_ocr = False

        pages.append(
            PageArtifact(
                page=i + 1,
                text=final_text,
                is_scanned=scanned,
                used_ocr=used_ocr,
                raw_text_len=len(raw_text),
                ocr_text_len=len(ocr_text),
            )
        )

        # Tables
        if extract_tables and i < table_max_pages:
            tlist = extract_tables_pdfplumber(pdf_path, i)
            for t_idx, t in enumerate(tlist):
                tables.append(TableArtifact(page=i + 1, table_index=t_idx, rows=t))

            # Metadata fallback pseudo-table for early pages
            if i < metadata_page_limit:
                pseudo_rows = _extract_key_value_lines_as_table(final_text)
                if pseudo_rows:
                    tables.append(
                        TableArtifact(
                            page=i + 1,
                            table_index=1000 + i,
                            rows=pseudo_rows,
                        )
                    )

    doc.close()

    return DocArtifacts(
        pdf_path=os.path.abspath(pdf_path),
        file_name=os.path.basename(pdf_path),
        sha256=sha256_file(pdf_path),
        num_pages=num_pages,
        pages=pages,
        tables=tables,
        warnings=warnings,
    )


def artifacts_to_json(artifacts: DocArtifacts) -> Dict[str, Any]:
    return {
        "pdf_path": artifacts.pdf_path,
        "file_name": artifacts.file_name,
        "sha256": artifacts.sha256,
        "num_pages": artifacts.num_pages,
        "warnings": artifacts.warnings,
        "pages": [asdict(p) for p in artifacts.pages],
        "tables": [asdict(t) for t in artifacts.tables],
    }


def save_artifacts_json(artifacts: DocArtifacts, out_path: str) -> str:
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(artifacts_to_json(artifacts), f, ensure_ascii=False, indent=2)
    return out_path