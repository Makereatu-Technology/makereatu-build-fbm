from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

from app.services.extraction.utils import (
    ExtractedField,
    clean_ocr_text,
    extract_from_tables,
    extract_from_text,
    field_or_not_found,
    first_match,
    make_field,
    norm,
)


BAD_TITLE_EXACT = {
    "project name",
    "project title",
    "project number",
    "loan number",
    "project id",
    "basic data",
    "project data",
    "validation report",
    "performance evaluation report",
    "project completion report validation",
    "ieg icr review",
    "independent evaluation group",
    "independent evaluation department",
    "report number",
}

BAD_TITLE_CONTAINS = [
    "reference number",
    "prepared by",
    "reviewed by",
    "approval date",
    "closing date",
    "public disclosure authorized",
    "page ",
]


def clean_project_title(text: Optional[str]) -> Optional[str]:
    """
    Clean OCR/project-title candidates and reject obvious bad labels.
    """
    if not text:
        return None

    text = clean_ocr_text(text)

    # Remove leading labels
    text = re.sub(r"^(project name|project title)\s*[:\-]?\s*", "", text, flags=re.I)
    text = re.sub(r"^(country)\s*[:\-]?\s*", "", text, flags=re.I)

    # Remove trailing field fragments that OCR often glues on
    text = re.sub(r"\b(Project ID|Project Number|Loan Number)\b.*$", "", text, flags=re.I).strip(" :-|")

    text = re.sub(r"\s{2,}", " ", text).strip(" :-\t\r\n")
    if not text:
        return None

    low = text.lower().strip()

    if low in BAD_TITLE_EXACT:
        return None

    for bad in BAD_TITLE_CONTAINS:
        if bad in low:
            return None

    # reject pure ids like 29529 or P079935
    if re.fullmatch(r"[A-Z]?\d+(?:[-/][A-Z0-9]+)*", text, flags=re.I):
        return None

    # reject raw field label fragments
    if re.search(r"^(project number|loan number|project id)\b", text, flags=re.I):
        return None

    if len(text) < 5:
        return None

    return text


def choose_best_title(candidates: List[Tuple[str, float]]) -> Optional[str]:
    """
    candidates = [(title, score), ...]
    Prefer higher score, then longer cleaned title.
    """
    valid: List[Tuple[str, float]] = []

    for title, score in candidates:
        cleaned = clean_project_title(title)
        if cleaned:
            valid.append((cleaned, score))

    if not valid:
        return None

    valid.sort(key=lambda x: (x[1], len(x[0])), reverse=True)
    return valid[0][0]


def extract_first_regex(
    text: str,
    patterns: List[str],
    flags=re.IGNORECASE | re.S,
) -> Optional[str]:
    """
    Return the first non-empty captured group across patterns.
    """
    matched = first_match(patterns, text, flags=flags)
    if matched:
        value, _ = matched
        return clean_ocr_text(value)
    return None


def extract_all_regex(
    text: str,
    patterns: List[str],
    flags=re.IGNORECASE | re.S,
) -> List[str]:
    results: List[str] = []
    if not text:
        return results

    for pat in patterns:
        for m in re.finditer(pat, text, flags=flags):
            if m.groups():
                for g in m.groups():
                    if g is not None and str(g).strip():
                        results.append(clean_ocr_text(g.strip()))
                        break
            else:
                results.append(clean_ocr_text(m.group(0).strip()))
    return results


def extract_numeric_percent_near_keywords(text: str, keywords: List[str]) -> Optional[float]:
    """
    Search line-by-line first, then fallback to broader text regex.
    Useful for EIRR / ERR / economic rate extraction.
    """
    if not text:
        return None

    lines = text.splitlines()

    for line in lines:
        low = line.lower()
        if any(k.lower() in low for k in keywords):
            m = re.search(r"(-?\d+(?:\.\d+)?)\s*%", line)
            if m:
                try:
                    return float(m.group(1))
                except Exception:
                    pass

    pattern = r"(?:%s)[^.\n]{0,100}?(-?\d+(?:\.\d+)?)\s*%%" % "|".join(
        re.escape(k) for k in keywords
    )
    m = re.search(pattern, text, flags=re.I)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            return None

    return None


def find_label_value_pairs(lines: List[str], labels: List[str]) -> Dict[str, str]:
    """
    Detect:
      Project Name: Rural Access Roads Project
    and also:
      Project Name
      Rural Access Roads Project
    """
    found: Dict[str, str] = {}
    lower_labels = {lbl.lower(): lbl for lbl in labels}

    for i, line in enumerate(lines):
        line_clean = clean_ocr_text(line)
        low = line_clean.lower()

        # same-line label: value
        for lbl_low, lbl_orig in lower_labels.items():
            m = re.search(rf"\b{re.escape(lbl_low)}\b\s*[:\-]\s*(.+)$", line_clean, flags=re.I)
            if m and m.group(1).strip():
                found[lbl_orig] = clean_ocr_text(m.group(1).strip())

        # label-only line, value on next line
        for lbl_low, lbl_orig in lower_labels.items():
            if low.strip() == lbl_low and i + 1 < len(lines):
                nxt = clean_ocr_text(lines[i + 1].strip())
                if nxt:
                    found[lbl_orig] = nxt

    return found


def extract_field_from_tables_or_text(
    chunks: List[dict],
    field_name: str,
    row_aliases: List[str],
    text_patterns: List[str],
    *,
    value_regex: Optional[str] = None,
    pair_mode: Optional[str] = None,
    unit: Optional[str] = None,
    table_confidence: float = 0.92,
    text_confidence: float = 0.78,
) -> ExtractedField:
    """
    Convenience wrapper:
    first try tables, then text, else not_found.
    """
    from_table = extract_from_tables(
        chunks,
        field_name,
        row_aliases,
        value_regex=value_regex,
        pair_mode=pair_mode,
        unit=unit,
        confidence=table_confidence,
    )
    if from_table:
        return from_table

    from_text = extract_from_text(
        chunks,
        field_name,
        text_patterns,
        unit=unit,
        confidence=text_confidence,
    )
    return field_or_not_found(from_text, field_name)