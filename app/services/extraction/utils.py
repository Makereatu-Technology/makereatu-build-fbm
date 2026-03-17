from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class ExtractedField:
    field_name: str
    field_value: Optional[str]
    unit: Optional[str]
    page: Optional[int]
    source_type: str          # "table" | "text" | "not_found"
    evidence_text: Optional[str]
    confidence: float
    debug: Dict[str, Any]


def make_field(
    name: str,
    value: Optional[str],
    page: Optional[int],
    evidence: Optional[str],
    confidence: float,
    *,
    unit: Optional[str] = None,
    source_type: str = "text",
    debug: Optional[Dict[str, Any]] = None,
) -> ExtractedField:
    return ExtractedField(
        field_name=name,
        field_value=value,
        unit=unit,
        page=page,
        source_type=source_type,
        evidence_text=evidence,
        confidence=confidence,
        debug=debug or {},
    )


def normalize_whitespace(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\u00a0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def norm(s: str) -> str:
    return normalize_whitespace(s).lower()


def dedupe_adjacent_words(text: str) -> str:
    """
    Example:
    'Project Name Project Name' -> 'Project Name'
    """
    if not text:
        return ""
    words = normalize_whitespace(text).split()
    out: List[str] = []
    for w in words:
        if not out or out[-1].lower() != w.lower():
            out.append(w)
    return " ".join(out)


def dedupe_repeated_phrase(text: str) -> str:
    """
    Example:
    'Cambodia Cambodia Road Connectivity Improvement Project ID'
    -> 'Cambodia Road Connectivity Improvement Project ID'
    """
    if not text:
        return ""

    text = normalize_whitespace(text)
    parts = text.split()

    if len(parts) >= 2 and parts[0].lower() == parts[1].lower():
        return " ".join([parts[0]] + parts[2:])

    if len(parts) >= 4 and len(parts) % 2 == 0:
        half = len(parts) // 2
        left = [p.lower() for p in parts[:half]]
        right = [p.lower() for p in parts[half:]]
        if left == right:
            return " ".join(parts[:half])

    return dedupe_adjacent_words(text)


def clean_ocr_text(text: str) -> str:
    return dedupe_repeated_phrase(normalize_whitespace(text))


def first_match(
    patterns: List[str],
    text: str,
    flags=re.IGNORECASE,
) -> Optional[Tuple[str, str]]:
    """
    Returns (matched_value, matched_pattern).

    Uses the first non-empty capturing group.
    Falls back to full match if there are no explicit groups.
    """
    if not text:
        return None

    for p in patterns:
        m = re.search(p, text, flags)
        if not m:
            continue

        if m.groups():
            for group in m.groups():
                if group is not None and str(group).strip():
                    return clean_ocr_text(group.strip()), p
        else:
            return clean_ocr_text(m.group(0).strip()), p

    return None


def all_matches(
    patterns: List[str],
    text: str,
    flags=re.IGNORECASE,
) -> List[Tuple[str, str]]:
    results: List[Tuple[str, str]] = []
    if not text:
        return results

    for p in patterns:
        for m in re.finditer(p, text, flags):
            if m.groups():
                for group in m.groups():
                    if group is not None and str(group).strip():
                        results.append((clean_ocr_text(group.strip()), p))
                        break
            else:
                results.append((clean_ocr_text(m.group(0).strip()), p))
    return results


def flatten_table_rows(table_obj: dict) -> List[str]:
    rows = table_obj.get("rows", []) or []
    out = []
    for row in rows:
        row_text = " | ".join(normalize_whitespace(c or "") for c in row)
        row_text = clean_ocr_text(row_text)
        if row_text.strip():
            out.append(row_text)
    return out


def extract_date_candidates(text: str) -> List[str]:
    """
    Extract likely date strings from a row or paragraph.
    """
    if not text:
        return []

    patterns = [
        r"\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2,4}\b",   # 30 April 2005
        r"\b[A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}\b",    # April 30, 2005
        r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b",       # 30/04/2005
        r"\b\d{4}-\d{2}-\d{2}\b",                   # 2005-04-30
        r"\b\d{1,2}-[A-Za-z]{3}-\d{4}\b",           # 31-Dec-2012
    ]

    found: List[str] = []
    for pat in patterns:
        found.extend(re.findall(pat, text, flags=re.IGNORECASE))

    return [clean_ocr_text(x) for x in found]


def extract_numeric_candidates(text: str) -> List[str]:
    if not text:
        return []
    vals = re.findall(r"-?\d+(?:,\d{3})*(?:\.\d+)?", text)
    return [v.strip() for v in vals]


def extract_from_tables(
    chunks: List[dict],
    field_name: str,
    row_aliases: List[str],
    *,
    value_regex: Optional[str] = None,
    pair_mode: Optional[str] = None,   # "first_second" for appraisal/actual style
    unit: Optional[str] = None,
    confidence: float = 0.92,
) -> Optional[ExtractedField]:
    """
    pair_mode='first_second' expects something like:
    'Closing Date | 30 April 2005 | 9 April 2008'
    Caller decides whether field_name refers to the first or second value.
    """
    alias_norm = [norm(a) for a in row_aliases]

    for chunk in chunks:
        page = (chunk.get("pages") or [None])[0]

        for table in chunk.get("tables", []) or []:
            for row_text in flatten_table_rows(table):
                row_norm = norm(row_text)

                if not any(alias in row_norm for alias in alias_norm):
                    continue

                # Pair mode for date-like or appraisal/actual rows
                if pair_mode == "first_second":
                    date_vals = extract_date_candidates(row_text)
                    if len(date_vals) >= 2:
                        idx = 0 if ("planned" in field_name.lower() or "appraisal" in field_name.lower() or "original" in field_name.lower()) else 1
                        if idx < len(date_vals):
                            return make_field(
                                field_name,
                                date_vals[idx],
                                page,
                                row_text[:500],
                                confidence,
                                unit=unit,
                                source_type="table",
                                debug={
                                    "matched_row": row_text,
                                    "method": "pair_mode:first_second:dates",
                                    "values_found": date_vals,
                                },
                            )

                    numeric_vals = extract_numeric_candidates(row_text)
                    if len(numeric_vals) >= 2:
                        idx = 0 if ("planned" in field_name.lower() or "appraisal" in field_name.lower() or "original" in field_name.lower()) else 1
                        if idx < len(numeric_vals):
                            return make_field(
                                field_name,
                                numeric_vals[idx],
                                page,
                                row_text[:500],
                                confidence - 0.05,
                                unit=unit,
                                source_type="table",
                                debug={
                                    "matched_row": row_text,
                                    "method": "pair_mode:first_second:numeric",
                                    "values_found": numeric_vals,
                                },
                            )

                if value_regex:
                    m = re.search(value_regex, row_text, re.IGNORECASE)
                    if m:
                        value = None
                        if m.groups():
                            for g in m.groups():
                                if g is not None and str(g).strip():
                                    value = g.strip()
                                    break
                        else:
                            value = m.group(0).strip()

                        if value:
                            value = clean_ocr_text(value)
                            return make_field(
                                field_name,
                                value,
                                page,
                                row_text[:500],
                                confidence,
                                unit=unit,
                                source_type="table",
                                debug={
                                    "matched_row": row_text,
                                    "method": "table_regex",
                                    "pattern": value_regex,
                                },
                            )

                # fallback: use last numeric-ish token, not first
                numeric_vals = extract_numeric_candidates(row_text)
                if numeric_vals:
                    return make_field(
                        field_name,
                        numeric_vals[-1],
                        page,
                        row_text[:500],
                        confidence - 0.18,
                        unit=unit,
                        source_type="table",
                        debug={
                            "matched_row": row_text,
                            "method": "table_fallback_last_numeric",
                            "values_found": numeric_vals,
                        },
                    )

    return None


def extract_from_text(
    chunks: List[dict],
    field_name: str,
    patterns: List[str],
    *,
    unit: Optional[str] = None,
    confidence: float = 0.78,
) -> Optional[ExtractedField]:
    for chunk in chunks:
        text = clean_ocr_text(chunk.get("text", "") or "")
        page = (chunk.get("pages") or [None])[0]

        matched = first_match(patterns, text)
        if matched:
            value, pattern = matched
            return make_field(
                field_name,
                value,
                page,
                text[:600],
                confidence,
                unit=unit,
                source_type="text",
                debug={
                    "method": "text_regex",
                    "pattern": pattern,
                },
            )
    return None


def field_or_not_found(found: Optional[ExtractedField], field_name: str) -> ExtractedField:
    if found:
        return found
    return make_field(
        field_name,
        None,
        None,
        None,
        0.0,
        source_type="not_found",
        debug={"reason": "no_match_in_tables_or_text"},
    )