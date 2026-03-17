from __future__ import annotations

import re
from typing import List, Optional, Tuple

from app.services.extraction.field_utils import make_field, ExtractedField


BAD_TITLE_EXACT = {
    "project",
    "project name",
    "project title",
    "name",
    "country",
    "date posted",
    "project id",
    "project number",
    "loan number",
    "basic data",
    "project data",
    "validation report",
    "performance evaluation report",
    "project performance evaluation report",
    "project completion report validation",
    "ieg icr review",
    "independent evaluation group",
    "independent evaluation department",
    "implementation completion report",
}

BAD_TITLE_CONTAINS = [
    "implementation completion",
    "independent evaluation group",
    "independent evaluation department",
    "icr review",
    "report number",
    "document of the world bank",
    "public disclosure authorized",
    "prepared by",
    "reviewed by",
    "group:",
    "date posted",
    "project costs",
    "closing date",
    "board approval date",
    "reference number",
    "loan number",
    "project number",
]

GOOD_TITLE_HINTS = [
    "project",
    "transport",
    "road",
    "roads",
    "rural",
    "asset management",
    "maintenance",
    "connectivity",
    "highway",
    "infrastructure",
    "rehabilitation",
    "communications",
]

COUNTRY_WORDS = {
    "cambodia", "lao pdr", "laos", "viet nam", "vietnam", "thailand", "philippines",
    "indonesia", "myanmar", "timor-leste", "malaysia", "mongolia",
    "sri lanka", "nepal", "bangladesh", "india", "pakistan",
    "papua new guinea", "png",
}


def _clean_line(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s)
    s = s.strip(" |:-")

    # remove repeated first token: Cambodia Cambodia Road...
    parts = s.split()
    if len(parts) >= 2 and parts[0].lower() == parts[1].lower():
        s = " ".join([parts[0]] + parts[2:])

    # exact repeated half: Project Name Project Name
    parts = s.split()
    if len(parts) >= 4 and len(parts) % 2 == 0:
        half = len(parts) // 2
        if [p.lower() for p in parts[:half]] == [p.lower() for p in parts[half:]]:
            s = " ".join(parts[:half])

    return s.strip(" |:-")


def _clean_title_candidate(value: str) -> str:
    v = _clean_line(value)

    # strip leading labels
    v = re.sub(r"^(project name|project title)\s*[:\-]?\s*", "", v, flags=re.I)
    v = re.sub(r"^(country)\s*[:\-]?\s*", "", v, flags=re.I)

    # strip trailing OCR-glued field fragments
    v = re.sub(r"\b(Project ID|Project Number|Loan Number)\b.*$", "", v, flags=re.I)
    v = re.sub(r"\s{2,}", " ", v).strip(" |:-")
    return v


def _is_bad_title(value: str) -> bool:
    if not value:
        return True

    v = _clean_title_candidate(value)
    low = v.lower()

    if not v:
        return True
    if "|" in v:
        return True
    if low in BAD_TITLE_EXACT:
        return True
    if any(x in low for x in BAD_TITLE_CONTAINS):
        return True

    # reject pure numbers or ids
    if re.fullmatch(r"[0-9.,]+", v):
        return True
    if re.fullmatch(r"P\d{6}", v, flags=re.I):
        return True
    if re.fullmatch(r"[A-Z]?\d+(?:[-/][A-Z0-9]+)*", v, flags=re.I):
        return True

    # reject explicit field-value fragments
    if re.search(r"^(project id|project number|loan number)\b", low):
        return True

    if len(v) < 8:
        return True
    if len(v) > 180:
        return True

    # reject values with too little alphabetic content
    alpha = sum(ch.isalpha() for ch in v)
    if alpha < 5:
        return True

    return False


def _score_title(value: str) -> int:
    v = _clean_title_candidate(value)
    low = v.lower()
    score = 0

    if any(h in low for h in GOOD_TITLE_HINTS):
        score += 4

    if "project" in low:
        score += 4

    if any(c in low for c in COUNTRY_WORDS):
        score += 1

    # title-case-ish
    words = v.split()
    titled = sum(1 for w in words if w[:1].isupper())
    if words and titled / max(1, len(words)) > 0.5:
        score += 2

    # prefer fuller titles
    if len(words) >= 3:
        score += 2
    if len(words) >= 5:
        score += 1
    if len(words) == 1:
        score -= 3

    # discourage leftover labels
    if "project name" in low or "project title" in low:
        score -= 4

    # reward endings that look like real project names
    if re.search(r"\b(project|program|programme)$", low):
        score += 1

    return score


def _should_continue_title(line: str) -> bool:
    """
    Whether the next line likely continues a broken title.
    """
    c = _clean_line(line)
    if not c:
        return False

    low = c.lower()

    if _is_bad_title(c):
        return False

    # avoid metadata/headers
    if re.search(r"\b(project id|project number|loan number|country|approval date|closing date)\b", low):
        return False

    # short continuation lines are common in broken titles
    if len(c.split()) <= 6:
        return True

    # still allow if it obviously looks project-like
    if any(h in low for h in GOOD_TITLE_HINTS):
        return True

    return False


def _join_multiline_title(lines: List[str], idx: int) -> Optional[str]:
    """
    Join line idx with next 1-2 lines if it looks like a broken title block.
    """
    if idx >= len(lines):
        return None

    base = _clean_line(lines[idx])
    if not base:
        return None

    candidates = [base]

    if idx + 1 < len(lines) and _should_continue_title(lines[idx + 1]):
        c2 = _clean_line(base + " " + lines[idx + 1])
        candidates.append(c2)

        if idx + 2 < len(lines) and _should_continue_title(lines[idx + 2]):
            c3 = _clean_line(c2 + " " + lines[idx + 2])
            candidates.append(c3)

    best = None
    best_score = -999

    for c in candidates:
        cleaned = _clean_title_candidate(c)
        if _is_bad_title(cleaned):
            continue
        sc = _score_title(cleaned)
        if sc > best_score:
            best = cleaned
            best_score = sc

    return best


def _extract_from_project_name_label(chunks: List[dict]) -> Optional[ExtractedField]:
    """
    Handles:
      Project Name: Road Asset
      Management Project
    and similar table-like rows.
    """
    for chunk in chunks:
        page = (chunk.get("pages") or [None])[0]
        if page is None or page > 4:
            continue

        lines = [_clean_line(l) for l in (chunk.get("text") or "").splitlines() if _clean_line(l)]

        for i, line in enumerate(lines):
            low = line.lower()

            # Case 1: Project Name: X
            m = re.search(r"\bproject name\b\s*[:\-]?\s*(.+)?", line, re.IGNORECASE)
            if m:
                after = _clean_title_candidate(m.group(1) or "")

                if after and not _is_bad_title(after):
                    joined = _join_multiline_title([after] + lines[i + 1:i + 3], 0)
                    if joined:
                        return make_field(
                            "project_name",
                            joined,
                            page,
                            "\n".join(lines[max(0, i - 1):i + 3])[:500],
                            0.98,
                            source_type="first_page_project_name_label",
                            debug={"method": "project_name_label_multiline"},
                        )
                else:
                    if i + 1 < len(lines):
                        joined = _join_multiline_title(lines, i + 1)
                        if joined:
                            return make_field(
                                "project_name",
                                joined,
                                page,
                                "\n".join(lines[max(0, i - 1):i + 4])[:500],
                                0.98,
                                source_type="first_page_project_name_label",
                                debug={"method": "project_name_label_next_lines"},
                            )

            # Case 2: table-ish line with pipe
            if "project name" in low and "|" in line:
                cells = [_clean_line(c) for c in line.split("|") if _clean_line(c)]
                for j, c in enumerate(cells):
                    if c.lower().startswith("project name"):
                        rhs = cells[j + 1] if j + 1 < len(cells) else ""
                        joined = _join_multiline_title([rhs] + lines[i + 1:i + 3], 0)
                        if joined:
                            return make_field(
                                "project_name",
                                joined,
                                page,
                                line[:500],
                                0.97,
                                source_type="tableish_project_name",
                                debug={"method": "tableish_project_name"},
                            )
    return None


def _extract_from_table_rows(chunks: List[dict]) -> Optional[ExtractedField]:
    """
    Reads parsed tables and joins split project names across columns.
    Also handles header pairs like:
      Project ID | Project Name
      P079935    | PH- Natl Rds Improv. & Mgt Ph.2
    """
    for chunk in chunks:
        page = (chunk.get("pages") or [None])[0]
        if page is None or page > 5:
            continue

        for table in chunk.get("tables", []) or []:
            rows = table.get("rows", []) or []

            for r_idx, row in enumerate(rows):
                cells = [(_clean_line(c) if c is not None else "") for c in row]

                # explicit project name label
                for i, c in enumerate(cells):
                    low = c.lower()

                    if low == "project name" or low.startswith("project name"):
                        rhs_parts = []
                        if i + 1 < len(cells) and cells[i + 1]:
                            rhs_parts.append(cells[i + 1])

                        # include next-row continuation if needed
                        if r_idx + 1 < len(rows):
                            next_row = [(_clean_line(x) if x is not None else "") for x in rows[r_idx + 1]]
                            nonempty_next = [x for x in next_row if x]
                            if nonempty_next and len(nonempty_next[0].split()) <= 5 and not _is_bad_title(nonempty_next[0]):
                                rhs_parts.extend(nonempty_next[:1])

                        candidate = _clean_title_candidate(" ".join(rhs_parts))
                        if not _is_bad_title(candidate):
                            return make_field(
                                "project_name",
                                candidate,
                                page,
                                " | ".join(cells)[:500],
                                0.96,
                                source_type="table_project_name",
                                debug={"method": "table_project_name"},
                            )

                # header row like: Project ID | Project Name
                lowered = [c.lower() for c in cells if c]
                if "project id" in lowered and "project name" in lowered and r_idx + 1 < len(rows):
                    next_row = [(_clean_line(x) if x is not None else "") for x in rows[r_idx + 1]]
                    if len(next_row) >= 2:
                        # choose cell under project name column
                        try:
                            name_idx = lowered.index("project name")
                            candidate = _clean_title_candidate(next_row[name_idx])
                            if not _is_bad_title(candidate):
                                return make_field(
                                    "project_name",
                                    candidate,
                                    page,
                                    " | ".join(next_row)[:500],
                                    0.97,
                                    source_type="table_project_name_header_pair",
                                    debug={"method": "table_header_pair_project_name"},
                                )
                        except Exception:
                            pass
    return None


def _extract_from_early_page_headings(chunks: List[dict]) -> Optional[ExtractedField]:
    """
    Looks for standalone early-page headings containing likely title words.
    """
    candidates: List[Tuple[str, Optional[int], int, int]] = []

    for chunk in chunks:
        page = (chunk.get("pages") or [None])[0]
        if page is None or page > 4:
            continue

        lines = [_clean_line(l) for l in (chunk.get("text") or "").splitlines() if _clean_line(l)]

        for i, line in enumerate(lines[:50]):
            clean = _clean_title_candidate(line)
            low = clean.lower()

            if _is_bad_title(clean):
                continue

            # direct project-like heading
            if "project" in low or any(h in low for h in GOOD_TITLE_HINTS):
                joined = _join_multiline_title(lines, i)
                if joined and not _is_bad_title(joined):
                    candidates.append((joined, page, i, _score_title(joined)))

            # country line followed by project title
            if low in COUNTRY_WORDS and i + 1 < len(lines):
                joined = _join_multiline_title(lines, i + 1)
                if joined and "project" in joined.lower():
                    candidates.append((joined, page, i + 1, _score_title(joined) + 1))

    if not candidates:
        return None

    # prefer higher score, then earlier page, then earlier line
    candidates.sort(key=lambda x: (x[3], -(x[1] or 999), -x[2]), reverse=True)
    best, page, idx, score = candidates[0]

    if score < 4:
        return None

    return make_field(
        "project_name",
        best,
        page,
        best,
        0.93,
        source_type="early_page_heading",
        debug={"method": "early_page_heading", "score": score},
    )


def extract_project_title(chunks: List[dict]) -> Optional[ExtractedField]:
    """
    Strong hierarchical project title extraction.
    Order:
      1) explicit Project Name label in early pages
      2) parsed tables / header-pair tables
      3) early page headings
    """
    for fn in (
        _extract_from_project_name_label,
        _extract_from_table_rows,
        _extract_from_early_page_headings,
    ):
        out = fn(chunks)
        if out and not _is_bad_title(out.field_value or ""):
            out.field_value = _clean_title_candidate(out.field_value or "")
            if out.field_value and not _is_bad_title(out.field_value):
                return out

    return None