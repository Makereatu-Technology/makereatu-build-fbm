from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

from app.services.extraction.field_utils import (
    ExtractedField,
    clean_project_title,
    extract_field_from_tables_or_text,
)
from app.services.extraction.utils import (
    clean_ocr_text,
    extract_date_candidates,
    extract_from_tables,
    extract_from_text,
    extract_numeric_candidates,
    field_or_not_found,
    make_field,
    norm,
)
from app.services.extraction.extract_project_title import extract_project_title


WB_TABLE_ALIASES = {
    "project_id": ["project id"],
    "country": ["country"],
    "approval_date": ["bank approval date", "board approval date", "approval date", "expected approval date"],
    "planned_closing_date": ["closing date", "closing date (original)", "original closing date", "planned closing date", "expected closing date"],
    "actual_closing_date": ["closing date", "closing date (actual)", "actual closing date", "revised closing date", "operation closing/cancellation"],
    "appraisal_cost": [
        "project costs",
        "project costs (us$m)",
        "project cost at appraisal",
        "appraisal cost",
        "total project cost",
        "original commitment",
    ],
    "actual_cost": [
        "project costs",
        "project costs (us$m)",
        "actual cost",
        "total project cost",
        "actual",
        "revised commitment",
    ],
    "err_percent": ["err", "economic rate of return", "economic internal rate of return", "eirr"],
    "overall_rating": ["overall outcome", "outcome", "overall rating", "overall implementation progress", "progress towards achievement of pdo"],
}

WB_TEXT_PATTERNS = {
    "project_id": [
        r"\bproject id\s*[:\-]?\s*(P\d{6}|[A-Z0-9\-]+)",
        r"\bproject id\s+project name\s*\n\s*(P\d{6}|[A-Z0-9\-]+)\s+.+",
        r"\b\((P\d{6})\)\b",
    ],
    "country": [
        r"\bcountry\s*[:\-]?\s*([A-Za-z][A-Za-z'.,()\- ]+)",
    ],
    "approval_date": [
        r"\b(?:bank approval date|board approval date|approval date|expected approval date)\s*[:\-]?\s*([A-Za-z0-9, \-/]+)",
    ],
    "err_percent": [
        r"\b(?:err|economic rate of return|economic internal rate of return|eirr).{0,60}?(\d{1,3}(?:\.\d+)?)\s*%",
    ],
    "overall_rating": [
        r"\brated\s*([A-Za-z \-]{4,40})\.",
        r"\b(?:overall outcome|outcome|overall rating|overall implementation progress|progress towards achievement of pdo)\s*[:\-]?\s*([A-Za-z \-]{4,40})",
    ],
}


WB_BAD_TITLE_PHRASES = [
    "has the development objective been changed",
    "original development objective",
    "current development objective",
    "development objective",
    "objective",
    "components",
    "overall ratings",
    "key issues",
    "implementation status",
    "proposed development objective",
    "basic information",
    "datasheet",
    "project data",
]

WB_OUTPUT_FAMILY_PRIORITY = [
    "core_roads",
    "national_provincial_roads",
    "road_improvement",
    "rehabilitation",
    "maintenance",
    "periodic_maintenance",
    "preservation",
]


def _is_early_page(chunk: dict, max_page: int = 5) -> bool:
    pages = chunk.get("pages") or []
    if not pages:
        return True
    try:
        return pages[0] is not None and int(pages[0]) <= max_page
    except Exception:
        return False


def _filter_chunks(chunks: List[dict], *, early_only: bool = False) -> List[dict]:
    if not early_only:
        return chunks
    return [c for c in chunks if _is_early_page(c)]


def _valid_rate(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    try:
        x = float(str(value).replace(",", ""))
        if 0 <= x <= 100:
            if x.is_integer():
                return str(int(x))
            return str(x)
    except Exception:
        pass
    return None


def _to_float(v) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return None


def _clean_value(value: Optional[str]) -> str:
    return clean_ocr_text(value or "").strip(" |:-")


def _looks_like_bad_wb_title(value: Optional[str]) -> bool:
    v = _clean_value(value)
    low = v.lower()
    if not v:
        return True
    if len(v) < 6:
        return True
    if any(p in low for p in WB_BAD_TITLE_PHRASES):
        return True
    return False


def _extract_wb_header_title(chunks: List[dict]) -> Optional[ExtractedField]:
    """
    Handles:
      The World Bank
      Cambodia Road Connectivity Improvement (P169930)

      Independent Evaluation Group (IEG) Implementation Completion Report (ICR) Review
      PH- Natl Rds Improv. & Mgt Ph.2 (P079935)
    """
    for chunk in _filter_chunks(chunks, early_only=True):
        page = (chunk.get("pages") or [None])[0]
        text = clean_ocr_text(chunk.get("text") or "")
        if not text:
            continue

        lines = [clean_ocr_text(x) for x in text.splitlines() if clean_ocr_text(x)]
        for i, line in enumerate(lines[:25]):
            line_clean = line.strip()

            # direct title with project id in parentheses
            m = re.match(r"^(.+?)\s*\((P\d{6})\)$", line_clean)
            if m:
                candidate = clean_project_title(m.group(1).strip())
                if candidate and not _looks_like_bad_wb_title(candidate):
                    return make_field(
                        "project_name",
                        candidate,
                        page,
                        line_clean[:500],
                        0.995,
                        source_type="wb_header_title",
                        debug={"method": "wb_header_title_direct"},
                    )

            # line after "The World Bank" or after IEG report header
            if line_clean.lower() in {
                "the world bank",
                "independent evaluation group (ieg) implementation completion report (icr) review",
            }:
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    m2 = re.match(r"^(.+?)\s*\((P\d{6})\)$", next_line)
                    candidate = m2.group(1).strip() if m2 else next_line
                    candidate = clean_project_title(candidate)
                    if candidate and not _looks_like_bad_wb_title(candidate):
                        return make_field(
                            "project_name",
                            candidate,
                            page,
                            next_line[:500],
                            0.992,
                            source_type="wb_header_title",
                            debug={"method": "wb_header_title_after_header"},
                        )
    return None


def _extract_wb_project_name_from_table_header_pair(chunks: List[dict]) -> Optional[ExtractedField]:
    for chunk in _filter_chunks(chunks, early_only=True):
        page = (chunk.get("pages") or [None])[0]
        for table in chunk.get("tables", []) or []:
            rows = table.get("rows", []) or []
            for r_idx, row in enumerate(rows):
                cells = [_clean_value(c) for c in row]
                lowered = [c.lower() for c in cells if c]

                if "project id" in lowered and "project name" in lowered and r_idx + 1 < len(rows):
                    next_row = [_clean_value(x) for x in rows[r_idx + 1]]
                    try:
                        name_idx = lowered.index("project name")
                        if name_idx < len(next_row):
                            candidate = clean_project_title(next_row[name_idx])
                            if candidate and not _looks_like_bad_wb_title(candidate):
                                return make_field(
                                    "project_name",
                                    candidate,
                                    page,
                                    " | ".join(next_row)[:500],
                                    0.985,
                                    source_type="table_project_name_header_pair",
                                    debug={"method": "table_header_pair_project_name"},
                                )
                    except Exception:
                        pass
    return None


def _extract_wb_project_id_from_tables(chunks: List[dict]) -> Optional[ExtractedField]:
    for chunk in _filter_chunks(chunks, early_only=True):
        page = (chunk.get("pages") or [None])[0]

        for table in chunk.get("tables", []) or []:
            rows = table.get("rows", []) or []
            for row in rows:
                cells = [_clean_value(c) for c in row]
                if len(cells) < 2:
                    continue

                left = cells[0].lower().strip()
                right = cells[1].strip()

                if "project id" in left and re.fullmatch(r"P\d{6}|[A-Z0-9\-]+", right):
                    return make_field(
                        "project_id",
                        right,
                        page,
                        " | ".join(cells)[:400],
                        0.98,
                        source_type="table",
                        debug={"method": "wb_project_id_table_pair", "row": cells},
                    )
    return None


def _extract_wb_pair_from_metadata_row(
    chunks: List[dict],
    field_name: str,
    aliases: List[str],
    *,
    prefer_first: bool = True,
    value_kind: str = "date",
    confidence: float = 0.95,
) -> Optional[ExtractedField]:
    alias_norm = [norm(a) for a in aliases]

    for chunk in _filter_chunks(chunks, early_only=True):
        page = (chunk.get("pages") or [None])[0]

        for table in chunk.get("tables", []) or []:
            for row in table.get("rows", []) or []:
                cells = [_clean_value(c) for c in row]
                row_text = " | ".join(cells)
                row_norm = norm(row_text)

                if not any(a in row_norm for a in alias_norm):
                    continue

                values = extract_date_candidates(row_text) if value_kind == "date" else extract_numeric_candidates(row_text)

                if len(values) >= 2:
                    idx = 0 if prefer_first else 1
                    return make_field(
                        field_name,
                        values[idx],
                        page,
                        row_text[:500],
                        confidence,
                        source_type="table",
                        debug={
                            "method": f"wb_metadata_pair_{value_kind}",
                            "matched_row": row_text,
                            "values_found": values,
                            "selected_index": idx,
                        },
                    )

    return None


def _extract_wb_single_labeled_date(
    chunks: List[dict],
    field_name: str,
    labels: List[str],
    *,
    confidence: float = 0.96,
) -> Optional[ExtractedField]:
    label_norm = [norm(x) for x in labels]

    for chunk in _filter_chunks(chunks, early_only=True):
        page = (chunk.get("pages") or [None])[0]

        for table in chunk.get("tables", []) or []:
            for row in table.get("rows", []) or []:
                cells = [_clean_value(c) for c in row]
                row_text = " | ".join(cells)
                row_norm = norm(row_text)

                if not any(lbl in row_norm for lbl in label_norm):
                    continue

                dates = extract_date_candidates(row_text)
                if dates:
                    return make_field(
                        field_name,
                        dates[0],
                        page,
                        row_text[:500],
                        confidence,
                        source_type="table",
                        debug={"method": "wb_single_labeled_date_table", "matched_row": row_text, "dates_found": dates},
                    )

        text = clean_ocr_text(chunk.get("text", "") or "")
        lines = text.splitlines()
        for line in lines:
            line_clean = clean_ocr_text(line)
            line_norm = norm(line_clean)

            if any(lbl in line_norm for lbl in label_norm):
                dates = extract_date_candidates(line_clean)
                if dates:
                    return make_field(
                        field_name,
                        dates[0],
                        page,
                        line_clean[:500],
                        confidence - 0.03,
                        source_type="text",
                        debug={"method": "wb_single_labeled_date_text", "matched_line": line_clean, "dates_found": dates},
                    )

    return None


def _extract_wb_single_labeled_numeric(
    chunks: List[dict],
    field_name: str,
    labels: List[str],
    *,
    confidence: float = 0.94,
) -> Optional[ExtractedField]:
    label_norm = [norm(x) for x in labels]

    for chunk in _filter_chunks(chunks, early_only=True):
        page = (chunk.get("pages") or [None])[0]

        for table in chunk.get("tables", []) or []:
            for row in table.get("rows", []) or []:
                cells = [_clean_value(c) for c in row]
                row_text = " | ".join(cells)
                row_norm = norm(row_text)

                if not any(lbl in row_norm for lbl in label_norm):
                    continue

                nums = extract_numeric_candidates(row_text)
                if nums:
                    return make_field(
                        field_name,
                        nums[-1],
                        page,
                        row_text[:500],
                        confidence,
                        source_type="table",
                        debug={"method": "wb_single_labeled_numeric_table", "matched_row": row_text, "values_found": nums},
                    )

    return None


def _classify_wb_output_family(text_low: str) -> Optional[str]:
    if "core roads" in text_low:
        return "core_roads"
    if "national and provincial roads" in text_low:
        return "national_provincial_roads"
    if "road improvement" in text_low or "roads were improved" in text_low:
        return "road_improvement"
    if "rehabilitated" in text_low or "rehabilitation" in text_low:
        return "rehabilitation"
    if "periodic maintenance" in text_low:
        return "periodic_maintenance"
    if "maintenance" in text_low:
        return "maintenance"
    if "preservation" in text_low:
        return "preservation"
    return None


def _extract_wb_output_pairs_from_text(chunks: List[dict]) -> List[Tuple[str, ExtractedField, ExtractedField]]:
    """
    Extracts narrative target/actual pairs from IEG/ICR/ISR/PAD text.

    Examples handled:
      - 476 km ... out of 500 km
      - 753 (out of 800 planned)
      - Out of the planned 1,200 km ... for a total of 996 km
      - 295 kilometers ... compared to the revised target of 280 kilometers
      - 205.3km have been completed
    """
    candidates: List[Tuple[str, ExtractedField, ExtractedField]] = []

    patterns = [
        # actual out of target
        (r"\b(\d[\d,\.]*)\s*km.{0,80}?\(out of\s*(\d[\d,\.]*)\s*(?:km\s*)?(?:planned|target)?\)", "actual_first"),
        (r"\b(\d[\d,\.]*)\s*(?:km)?\s*out of\s*(\d[\d,\.]*)\s*(?:km\s*)?(?:planned|target)", "actual_first"),
        # out of planned target ... total actual
        (r"\bout of the planned\s*(\d[\d,\.]*)\s*km.{0,180}?for a total of\s*(\d[\d,\.]*)\s*km", "target_first"),
        (r"\bout of the planned\s*(\d[\d,\.]*)\s*km.{0,120}?(\d[\d,\.]*)\s*km were ongoing or completed", "target_first"),
        # revised target phrasing
        (r"\b(\d[\d,\.]*)\s*kilometers?.{0,100}?compared to the revised target of\s*(\d[\d,\.]*)\s*kilometers?", "actual_first"),
        (r"\b(\d[\d,\.]*)\s*km.{0,100}?compared to the revised target of\s*(\d[\d,\.]*)\s*km", "actual_first"),
        (r"\b(\d[\d,\.]*)\s*km.{0,100}?compared to the target of\s*(\d[\d,\.]*)\s*km", "actual_first"),
        # target first
        (r"\brevised target of\s*(\d[\d,\.]*)\s*kilometers?.{0,120}?(\d[\d,\.]*)\s*kilometers?.{0,80}?were improved", "target_first"),
        # completed sections with clear family but no target nearby are ignored here
    ]

    for chunk in chunks:
        page = (chunk.get("pages") or [None])[0]
        text = clean_ocr_text(chunk.get("text") or "")
        if not text:
            continue

        text_low = text.lower()
        family = _classify_wb_output_family(text_low) or "road_improvement"

        for pattern, order in patterns:
            for m in re.finditer(pattern, text, flags=re.I | re.S):
                a, b = m.group(1), m.group(2)

                if order == "actual_first":
                    actual_raw, target_raw = a, b
                else:
                    target_raw, actual_raw = a, b

                if _to_float(target_raw) is None or _to_float(actual_raw) is None:
                    continue

                target_field = make_field(
                    "output_target_km",
                    target_raw,
                    page,
                    m.group(0)[:500],
                    0.91,
                    unit="km",
                    source_type="text",
                    debug={"method": "wb_output_narrative_pair", "pattern": pattern, "family": family, "order": order},
                )
                actual_field = make_field(
                    "output_actual_km",
                    actual_raw,
                    page,
                    m.group(0)[:500],
                    0.91,
                    unit="km",
                    source_type="text",
                    debug={"method": "wb_output_narrative_pair", "pattern": pattern, "family": family, "order": order},
                )
                candidates.append((family, target_field, actual_field))

    return candidates


def _extract_wb_output_pairs_from_tables(chunks: List[dict]) -> List[Tuple[str, ExtractedField, ExtractedField]]:
    candidates: List[Tuple[str, ExtractedField, ExtractedField]] = []

    for chunk in chunks:
        page = (chunk.get("pages") or [None])[0]

        for table in chunk.get("tables", []) or []:
            rows = table.get("rows", []) or []

            for row in rows:
                cells = [_clean_value(c) for c in row]
                row_text = " | ".join(cells)
                row_low = row_text.lower()

                if "km" not in row_low and "kilometer" not in row_low:
                    continue

                family = _classify_wb_output_family(row_low)
                if not family:
                    continue

                nums = []
                for n in extract_numeric_candidates(row_text):
                    try:
                        x = float(n.replace(",", ""))
                        if 1 <= x <= 100000:
                            nums.append((n, x))
                    except Exception:
                        continue

                if len(nums) < 2:
                    continue

                # semantic orientation
                target_idx = None
                actual_idx = None

                if any(k in row_low for k in ["planned", "target", "original", "revised target"]):
                    target_idx = 0
                    actual_idx = 1

                if "out of" in row_low and ("planned" in row_low or "target" in row_low):
                    # rows like 753 (out of 800 planned)
                    actual_idx = 0
                    target_idx = 1

                if target_idx is None or actual_idx is None:
                    # fallback
                    target_idx = 0
                    actual_idx = 1

                if target_idx >= len(nums) or actual_idx >= len(nums):
                    continue

                target_raw = nums[target_idx][0]
                actual_raw = nums[actual_idx][0]

                target_field = make_field(
                    "output_target_km",
                    target_raw,
                    page,
                    row_text[:500],
                    0.86,
                    unit="km",
                    source_type="table",
                    debug={"method": "wb_output_table_pair", "family": family, "row": row_text, "values_found": [n[0] for n in nums]},
                )
                actual_field = make_field(
                    "output_actual_km",
                    actual_raw,
                    page,
                    row_text[:500],
                    0.86,
                    unit="km",
                    source_type="table",
                    debug={"method": "wb_output_table_pair", "family": family, "row": row_text, "values_found": [n[0] for n in nums]},
                )
                candidates.append((family, target_field, actual_field))

    return candidates


def _choose_best_wb_output_pair(
    project_name_value: Optional[str],
    text_pairs: List[Tuple[str, ExtractedField, ExtractedField]],
    table_pairs: List[Tuple[str, ExtractedField, ExtractedField]],
) -> Tuple[Optional[ExtractedField], Optional[ExtractedField]]:
    project_low = (project_name_value or "").lower()

    # Prefer narrative text evidence first
    ordered_text_pairs = list(text_pairs)

    # project-specific ranking
    preferred_priority = list(WB_OUTPUT_FAMILY_PRIORITY)

    if "road asset management project" in project_low:
        preferred_priority = ["core_roads", "rehabilitation", "periodic_maintenance", "maintenance", "preservation"]
    elif "natl rds improv" in project_low or "national roads improvement" in project_low:
        preferred_priority = ["road_improvement", "rehabilitation", "preservation", "maintenance", "periodic_maintenance"]
    elif "road connectivity improvement" in project_low:
        preferred_priority = ["national_provincial_roads", "road_improvement", "rehabilitation", "maintenance"]

    for family in preferred_priority:
        for fam, t, a in ordered_text_pairs:
            if fam == family:
                return t, a

    # fallback to any text pair
    if ordered_text_pairs:
        _, t, a = ordered_text_pairs[0]
        return t, a

    # then table pairs
    for family in preferred_priority:
        for fam, t, a in table_pairs:
            if fam == family:
                return t, a

    if table_pairs:
        _, t, a = table_pairs[0]
        return t, a

    return None, None


def _clean_rating(field: Optional[ExtractedField]) -> Optional[ExtractedField]:
    if not field or not field.field_value:
        return field

    value = clean_ocr_text(field.field_value)
    value = re.sub(r"\s{2,}", " ", value).strip(" :-|")
    value_low = value.lower()

    allowed = {
        "highly satisfactory",
        "satisfactory",
        "moderately satisfactory",
        "moderately unsatisfactory",
        "unsatisfactory",
        "highly unsatisfactory",
        "successful",
        "moderately successful",
        "unsuccessful",
        "substantial",
        "modest",
        "negligible",
        "high",
        "low",
        "moderate",
    }

    if value_low in allowed:
        field.field_value = value
        return field

    if 4 <= len(value) <= 40 and re.fullmatch(r"[A-Za-z \-]+", value):
        field.field_value = value
        return field

    return None


def extract_core_fields_wb(chunks: List[dict]) -> List[ExtractedField]:
    fields: List[ExtractedField] = []
    early_chunks = _filter_chunks(chunks, early_only=True)

    # -------------------------
    # Project title
    # Prefer explicit WB/IEG header and table header-pair before generic extractor
    # -------------------------
    project_name = (
        _extract_wb_header_title(early_chunks)
        or _extract_wb_project_name_from_table_header_pair(early_chunks)
        or extract_project_title(early_chunks)
        or extract_project_title(chunks)
    )
    if project_name and project_name.field_value:
        project_name.field_value = clean_project_title(project_name.field_value)
        if not project_name.field_value or _looks_like_bad_wb_title(project_name.field_value):
            project_name = None
    fields.append(field_or_not_found(project_name, "project_name"))
    project_name_value = project_name.field_value if project_name else None

    # -------------------------
    # Project ID
    # -------------------------
    project_id = _extract_wb_project_id_from_tables(early_chunks) or extract_from_text(
        early_chunks,
        "project_id",
        WB_TEXT_PATTERNS["project_id"],
        confidence=0.96,
    )
    fields.append(field_or_not_found(project_id, "project_id"))

    # -------------------------
    # Country
    # -------------------------
    country = extract_field_from_tables_or_text(
        early_chunks,
        "country",
        WB_TABLE_ALIASES["country"],
        WB_TEXT_PATTERNS["country"],
        value_regex=r"([A-Za-z][A-Za-z'.,()\- ]+)$",
        table_confidence=0.90,
        text_confidence=0.86,
    )
    fields.append(country)

    # -------------------------
    # Approval date
    # -------------------------
    approval_date = extract_field_from_tables_or_text(
        early_chunks,
        "approval_date",
        WB_TABLE_ALIASES["approval_date"],
        WB_TEXT_PATTERNS["approval_date"],
        value_regex=r"([A-Za-z0-9, \-/]+)$",
        table_confidence=0.92,
        text_confidence=0.90,
    )
    fields.append(approval_date)

    # -------------------------
    # Planned closing date
    # -------------------------
    planned_closing_date = (
        _extract_wb_single_labeled_date(
            early_chunks,
            "planned_closing_date",
            ["closing date (original)", "original closing date", "planned closing date", "expected closing date"],
            confidence=0.97,
        )
        or _extract_wb_pair_from_metadata_row(
            early_chunks,
            "planned_closing_date",
            WB_TABLE_ALIASES["planned_closing_date"],
            prefer_first=True,
            value_kind="date",
            confidence=0.94,
        )
        or extract_from_tables(
            early_chunks,
            "planned_closing_date",
            WB_TABLE_ALIASES["planned_closing_date"],
            pair_mode="first_second",
            confidence=0.90,
        )
    )
    fields.append(field_or_not_found(planned_closing_date, "planned_closing_date"))

    # -------------------------
    # Actual closing date
    # -------------------------
    actual_closing_date = (
        _extract_wb_single_labeled_date(
            early_chunks,
            "actual_closing_date",
            ["closing date (actual)", "actual closing date", "revised closing date", "operation closing/cancellation"],
            confidence=0.97,
        )
        or _extract_wb_pair_from_metadata_row(
            early_chunks,
            "actual_closing_date",
            WB_TABLE_ALIASES["actual_closing_date"],
            prefer_first=False,
            value_kind="date",
            confidence=0.94,
        )
        or extract_from_tables(
            early_chunks,
            "actual_closing_date",
            WB_TABLE_ALIASES["actual_closing_date"],
            pair_mode="first_second",
            confidence=0.90,
        )
    )
    fields.append(field_or_not_found(actual_closing_date, "actual_closing_date"))

    # -------------------------
    # Appraisal cost
    # -------------------------
    appraisal_cost = (
        _extract_wb_single_labeled_numeric(
            early_chunks,
            "appraisal_cost",
            ["project costs (usd)", "project costs (us$m)", "project cost at appraisal", "appraisal cost", "original commitment", "total project cost"],
            confidence=0.95,
        )
        or _extract_wb_pair_from_metadata_row(
            early_chunks,
            "appraisal_cost",
            WB_TABLE_ALIASES["appraisal_cost"],
            prefer_first=True,
            value_kind="numeric",
            confidence=0.93,
        )
        or extract_from_tables(
            early_chunks,
            "appraisal_cost",
            WB_TABLE_ALIASES["appraisal_cost"],
            pair_mode="first_second",
            confidence=0.88,
        )
    )
    fields.append(field_or_not_found(appraisal_cost, "appraisal_cost"))

    # -------------------------
    # Actual cost
    # -------------------------
    actual_cost = (
        _extract_wb_single_labeled_numeric(
            early_chunks,
            "actual_cost",
            ["actual project cost", "actual cost", "total project cost", "actual", "revised commitment"],
            confidence=0.95,
        )
        or _extract_wb_pair_from_metadata_row(
            early_chunks,
            "actual_cost",
            WB_TABLE_ALIASES["actual_cost"],
            prefer_first=False,
            value_kind="numeric",
            confidence=0.93,
        )
        or extract_from_tables(
            early_chunks,
            "actual_cost",
            WB_TABLE_ALIASES["actual_cost"],
            pair_mode="first_second",
            confidence=0.88,
        )
    )
    fields.append(field_or_not_found(actual_cost, "actual_cost"))

    # -------------------------
    # Output km
    # -------------------------
    text_output_pairs = _extract_wb_output_pairs_from_text(chunks)
    table_output_pairs = _extract_wb_output_pairs_from_tables(chunks)
    output_target_km, output_actual_km = _choose_best_wb_output_pair(
        project_name_value,
        text_output_pairs,
        table_output_pairs,
    )

    fields.append(field_or_not_found(output_target_km, "output_target_km"))
    fields.append(field_or_not_found(output_actual_km, "output_actual_km"))

    # -------------------------
    # ERR / EIRR
    # -------------------------
    err = extract_from_text(
        chunks,
        "err_percent",
        WB_TEXT_PATTERNS["err_percent"],
        unit="%",
        confidence=0.90,
    ) or extract_from_tables(
        chunks,
        "err_percent",
        WB_TABLE_ALIASES["err_percent"],
        value_regex=r"(\d{1,3}(?:\.\d+)?)\s*%",
        unit="%",
        confidence=0.88,
    )

    if err:
        cleaned = _valid_rate(err.field_value)
        if cleaned:
            err.field_value = cleaned
        else:
            err = None

    fields.append(field_or_not_found(err, "err_percent"))

    # -------------------------
    # Overall rating
    # -------------------------
    rating = extract_from_text(
        chunks,
        "overall_rating",
        WB_TEXT_PATTERNS["overall_rating"],
        confidence=0.68,
    ) or extract_from_tables(
        chunks,
        "overall_rating",
        WB_TABLE_ALIASES["overall_rating"],
        value_regex=r"([A-Za-z \-]{4,40})$",
        confidence=0.66,
    )
    rating = _clean_rating(rating)
    fields.append(field_or_not_found(rating, "overall_rating"))

    # -------------------------
    # Safeguards summary
    # -------------------------
    safeguards = extract_from_text(
        chunks,
        "safeguards_summary",
        [r"((?:safeguards?|environmental|resettlement|indigenous peoples).{0,450})"],
        confidence=0.60,
    )
    if safeguards and safeguards.field_value:
        safeguards.field_value = clean_ocr_text(safeguards.field_value)
    fields.append(field_or_not_found(safeguards, "safeguards_summary"))

    # -------------------------
    # Fiduciary summary
    # -------------------------
    fiduciary = extract_from_text(
        chunks,
        "fiduciary_summary",
        [r"((?:fiduciary|procurement|financial management|audit).{0,450})"],
        confidence=0.60,
    )
    if fiduciary and fiduciary.field_value:
        fiduciary.field_value = clean_ocr_text(fiduciary.field_value)
    fields.append(field_or_not_found(fiduciary, "fiduciary_summary"))

    # -------------------------
    # Lessons summary
    # -------------------------
    lessons = extract_from_text(
        chunks,
        "lessons_summary",
        [r"((?:lessons?.{0,450}))"],
        confidence=0.56,
    )
    if lessons and lessons.field_value:
        lessons.field_value = clean_ocr_text(lessons.field_value)
    fields.append(field_or_not_found(lessons, "lessons_summary"))

    return fields