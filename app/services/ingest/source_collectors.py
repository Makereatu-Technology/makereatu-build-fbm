from __future__ import annotations

import os
import re
import hashlib
from dataclasses import dataclass
from typing import List, Optional, Set, Dict, Tuple

import pandas as pd

URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)


@dataclass
class PdfSource:
    source_type: str  # "local" | "url"
    value: str        # local path or URL
    origin: str       # where it came from (folder scan, csv file name, manual)


def _is_pdf_path(path: str) -> bool:
    return path.lower().endswith(".pdf")


def _safe_walk(folder: str, recursive: bool = True) -> List[str]:
    out: List[str] = []
    folder = os.path.abspath(folder)
    if recursive:
        for root, _, files in os.walk(folder):
            for fn in files:
                out.append(os.path.join(root, fn))
    else:
        for fn in os.listdir(folder):
            out.append(os.path.join(folder, fn))
    return out


def _extract_urls_from_text(text: str) -> List[str]:
    if not text:
        return []
    return [u.rstrip(").,;]}>\"'") for u in URL_RE.findall(str(text))]


def _extract_urls_from_table(df: pd.DataFrame) -> List[str]:
    urls: List[str] = []
    if df is None or df.empty:
        return urls
    # scan all cells as strings; fast enough for typical sheets
    for col in df.columns:
        ser = df[col].astype(str)
        for v in ser.values:
            urls.extend(_extract_urls_from_text(v))
    return urls


def collect_pdf_sources_from_folder(
    folder: str,
    recursive: bool = True,
    include_local_pdfs: bool = True,
    include_table_links: bool = True,
) -> List[PdfSource]:
    """
    Returns PDF sources from:
      - local PDFs found in folder
      - URLs found inside CSV/XLSX files in folder
    """
    sources: List[PdfSource] = []
    folder = os.path.abspath(folder)

    if not os.path.isdir(folder):
        raise FileNotFoundError(f"Folder not found: {folder}")

    paths = _safe_walk(folder, recursive=recursive)

    # 1) local PDFs
    if include_local_pdfs:
        for p in paths:
            if os.path.isfile(p) and _is_pdf_path(p):
                sources.append(PdfSource("local", os.path.abspath(p), origin="folder_scan"))

    # 2) CSV/XLSX links
    if include_table_links:
        table_paths = [
            p for p in paths
            if os.path.isfile(p) and p.lower().endswith((".csv", ".xlsx", ".xls"))
        ]

        for tp in table_paths:
            try:
                if tp.lower().endswith(".csv"):
                    df = pd.read_csv(tp)
                    urls = _extract_urls_from_table(df)
                else:
                    # read all sheets
                    xls = pd.ExcelFile(tp)
                    urls = []
                    for sheet in xls.sheet_names:
                        df = xls.parse(sheet)
                        urls.extend(_extract_urls_from_table(df))

                # keep only likely pdf links (you can loosen later)
                for u in urls:
                    if ".pdf" in u.lower():
                        sources.append(PdfSource("url", u, origin=os.path.basename(tp)))
            except Exception:
                # Don’t crash whole discovery; keep going
                continue

    # Deduplicate (prefer local over url if same file appears)
    seen: Set[Tuple[str, str]] = set()
    unique: List[PdfSource] = []
    for s in sources:
        key = (s.source_type, s.value.strip())
        if key in seen:
            continue
        seen.add(key)
        unique.append(s)
    return unique


def sha256_file(path: str, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()