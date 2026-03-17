from __future__ import annotations

import re
from typing import List, Optional, Tuple

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


ADB_TABLE_ALIASES = {
    "project_number": ["project number"],
    "loan_number": ["loan number"],
    "country": ["country"],
    "approval_date": ["approval date"],
    "planned_closing_date": ["closing date", "original closing date", "scheduled loan closing date"],
    "actual_closing_date": ["closing date", "actual closing date", "actual loan closing date"],
    "appraisal_cost": ["total project costs", "appraisal cost", "estimated project cost", "project cost at appraisal"],
    "actual_cost": ["total project costs", "actual cost", "project cost at completion", "completion cost"],
    "eirr_percent": ["eirr", "economic internal rate of return", "economic rate of return"],
    "overall_rating": ["overall assessment", "project performance", "overall rating"],
}

ADB_TEXT_PATTERNS = {
    "project_number": [
        r"\bproject number\s*[:\-]?\s*([A-Z0-9][A-Z0-9\-\/()]+)",
        r"\bproject number\s*\n\s*([A-Z0-9][A-Z0-9\-\/()]+)",
    ],
    "loan_number": [
        r"\bloan number\s*[:\-]?\s*([A-Z0-9][A-Z0-9\-\/() ]+)",
        r"\bloan number\s*\n\s*([A-Z0-9][A-Z0-9\-\/() ]+)",
    ],
    "country": [
        r"\bcountry\s*[:\-]?\s*([A-Za-z][A-Za-z'.,()\- ]+)",
    ],
    "approval_date": [
        r"\bapproval date\s*[:\-]?\s*([A-Za-z0-9, \-/]+)",
    ],
    "planned_closing_date": [
        r"\b(?:scheduled loan closing date|original closing date|planned closing date)\s*(?:of|was|:)?\s*([A-Za-z0-9, \-/]+)",
    ],
    "actual_closing_date": [
        r"\b(?:actual loan closing date|actual closing date|completion date)\s*(?:of|was|:)?\s*([A-Za-z0-9, \-/]+)",
    ],
    "appraisal_cost": [
        r"\bat appraisal, the project cost was estimated at\s*(?:us\$|\$)?\s*([0-9.,]+)",
        r"\b(?:appraisal cost|estimated project cost|project cost at appraisal)\s*[:\-]?\s*(?:us\$|\$)?\s*([0-9.,]+)",
    ],
    "actual_cost": [
        r"\bproject cost at completion was\s*(?:us\$|\$)?\s*([0-9.,]+)",
        r"\b(?:actual cost|project cost at completion|completion cost)\s*[:\-]?\s*(?:us\$|\$)?\s*([0-9.,]+)",
    ],

    # Broader narrative patterns for project-level target/actual km
    "output_target_km": [
        r"\b(?:target(?:ed)?|planned|expected).{0,120}?(\d[\d,\.]*)\s*km",
        r"\b(\d[\d,\.]*)\s*km.{0,80}?(?:target(?:ed)?|planned|expected)",
        r"\babout\s*(\d[\d,\.]*)\s*(?:kilometers|km)\b",
    ],
    "output_actual_km": [
        r"\b(?:actual|achieved|completed|rehabilitated|improved|constructed|maintained|carried out for).{0,120}?(\d[\d,\.]*)\s*km",
        r"\b(\d[\d,\.]*)\s*km.{0,80}?(?:actual|achieved|completed|rehabilitated|improved|constructed|maintained)",
        r"\bmajor output of\s*(\d[\d,\.]*)\s*km",
        r"\ba total of\s*(\d[\d,\.]*)\s*km",
    ],
    "eirr_percent": [
        r"\b(?:eirr|economic internal rate of return|economic rate of return).{0,40}?(\d{1,2}(?:\.\d+)?)\s*%",
    ],
    "overall_rating": [
        r"\boverall, .*? rates the project as\s*[\"“]?([A-Za-z \-]{4,40})[\"”]?",
        r"\b(?:overall assessment|overall rating|project performance)\s*[:\-]?\s*([A-Za-z \-]{4,40})",
    ],
}


OUTPUT_FAMILY_PRIORITY = [
    "feeder_roads",
    "road_improvement",
    "rural_roads_rehabilitated",
    "maintenance",
]


def _is_early_page(chunk: dict, max_page: int = 5) -> bool:
    pages = chunk.get("pages") or []
    if not pages:
        return True
    try:
        return pages[0] is not None and int(pages[0]) <= max_page
    except Exception:
        return False


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


def _filter_chunks(chunks: List[dict], *, early_only: bool = False) -> List[dict]:
    if not early_only:
        return chunks
    return [c for c in chunks if _is_early_page(c)]


def _to_float(v: Optional[str]) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return None


def _extract_from_adb_metadata_row(
    chunks: List[dict],
    field_name: str,
    aliases: List[str],
    *,
    prefer_first: bool = True,
    value_kind: str = "date",
    confidence: float = 0.93,
    unit: Optional[str] = None,
) -> Optional[ExtractedField]:
    alias_norm = [norm(a) for a in aliases]

    for chunk in _filter_chunks(chunks, early_only=True):
        page = (chunk.get("pages") or [None])[0]

        for table in chunk.get("tables", []) or []:
            for row in table.get("rows", []) or []:
                cells = [clean_ocr_text(c or "") for c in row]
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
                        unit=unit,
                        source_type="table",
                        debug={
                            "method": f"adb_metadata_pair_{value_kind}",
                            "matched_row": row_text,
                            "values_found": values,
                            "selected_index": idx,
                        },
                    )
    return None


def _classify_output_family(row_low: str) -> Optional[str]:
    if "feeder road" in row_low or "feeder roads" in row_low:
        return "feeder_roads"
    if "road improvement" in row_low or "rural access road" in row_low:
        return "road_improvement"
    if "rural roads rehabilitated" in row_low or "roads rehabilitated" in row_low:
        return "rural_roads_rehabilitated"
    if "periodic maintenance" in row_low or "maintenance works" in row_low:
        return "maintenance"
    return None


def _extract_semantic_output_pairs_from_tables(chunks: List[dict]) -> List[Tuple[str, ExtractedField, ExtractedField]]:
    candidates: List[Tuple[str, ExtractedField, ExtractedField]] = []

    for chunk in chunks:
        page = (chunk.get("pages") or [None])[0]

        for table in chunk.get("tables", []) or []:
            rows = table.get("rows", []) or []

            for row in rows:
                cells = [clean_ocr_text(c or "") for c in row]
                row_text = " | ".join(cells)
                row_low = row_text.lower()

                if "km" not in row_low:
                    continue

                family = _classify_output_family(row_low)
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

                # Prefer first two plausible values within the same semantic family row
                target_raw = nums[0][0]
                actual_raw = nums[1][0]

                target_field = make_field(
                    "output_target_km",
                    target_raw,
                    page,
                    row_text[:500],
                    0.88,
                    unit="km",
                    source_type="table",
                    debug={"method": "adb_semantic_output_target", "family": family, "row": row_text, "values_found": [n[0] for n in nums]},
                )
                actual_field = make_field(
                    "output_actual_km",
                    actual_raw,
                    page,
                    row_text[:500],
                    0.88,
                    unit="km",
                    source_type="table",
                    debug={"method": "adb_semantic_output_actual", "family": family, "row": row_text, "values_found": [n[0] for n in nums]},
                )
                candidates.append((family, target_field, actual_field))

    return candidates


def _extract_output_pair_from_narrative(chunks: List[dict]) -> Tuple[Optional[ExtractedField], Optional[ExtractedField]]:
    """
    Handles sentences like:
      - 545.3 km ... was higher than the 505.4 km targeted
      - 3,282 km ... exceeded the target of 3,150 km
      - carried out for 67 km instead of the expected 100 km
    """
    text_patterns = [
        (
            r"\b(\d[\d,\.]*)\s*km.{0,120}?(?:higher than|exceed(?:ed|ing)?|above).{0,80}?(\d[\d,\.]*)\s*km\s*(?:target(?:ed)?|planned|expected)",
            "actual_first"
        ),
        (
            r"\b(?:target(?:ed)?|planned|expected).{0,80}?(\d[\d,\.]*)\s*km.{0,120}?(?:actual|achieved|completed|rehabilitated|improved|constructed|maintained|carried out for|major output of)\s*(\d[\d,\.]*)\s*km",
            "target_first"
        ),
        (
            r"\b(?:carried out for|completed|rehabilitated|improved).{0,40}?(\d[\d,\.]*)\s*km.{0,80}?instead of the expected\s*(\d[\d,\.]*)\s*km",
            "actual_first"
        ),
    ]

    for chunk in chunks:
        page = (chunk.get("pages") or [None])[0]
        text = clean_ocr_text(chunk.get("text") or "")
        if not text:
            continue

        for pattern, order in text_patterns:
            m = re.search(pattern, text, flags=re.I | re.S)
            if not m:
                continue

            a, b = m.group(1), m.group(2)
            if order == "actual_first":
                actual_raw, target_raw = a, b
            else:
                target_raw, actual_raw = a, b

            target_field = make_field(
                "output_target_km",
                target_raw,
                page,
                text[:500],
                0.90,
                unit="km",
                source_type="text",
                debug={"method": "adb_narrative_pair", "pattern": pattern, "order": order},
            )
            actual_field = make_field(
                "output_actual_km",
                actual_raw,
                page,
                text[:500],
                0.90,
                unit="km",
                source_type="text",
                debug={"method": "adb_narrative_pair", "pattern": pattern, "order": order},
            )
            return target_field, actual_field

    return None, None


def _choose_best_output_pair(
    project_name_value: Optional[str],
    narrative_pair: Tuple[Optional[ExtractedField], Optional[ExtractedField]],
    table_pairs: List[Tuple[str, ExtractedField, ExtractedField]],
) -> Tuple[Optional[ExtractedField], Optional[ExtractedField]]:
    # 1. Prefer narrative pair when available
    n_target, n_actual = narrative_pair
    if n_target and n_actual:
        return n_target, n_actual

    # 2. Project-aware priority for ADB table families
    project_low = (project_name_value or "").lower()

    preferred_priority = list(OUTPUT_FAMILY_PRIORITY)

    if "rural access roads project" in project_low:
        preferred_priority = ["feeder_roads", "road_improvement", "maintenance", "rural_roads_rehabilitated"]

    if "rural roads improvement project" in project_low:
        preferred_priority = ["rural_roads_rehabilitated", "road_improvement", "maintenance", "feeder_roads"]

    for family in preferred_priority:
        for fam, target_field, actual_field in table_pairs:
            if fam == family:
                return target_field, actual_field

    # fallback to first table pair found
    if table_pairs:
        _, target_field, actual_field = table_pairs[0]
        return target_field, actual_field

    return None, None


def _clean_rating(field: Optional[ExtractedField]) -> Optional[ExtractedField]:
    if not field or not field.field_value:
        return field

    value = clean_ocr_text(field.field_value)
    value = re.sub(r"\s{2,}", " ", value).strip(" :-|")
    value_low = value.lower()

    allowed = {
        "highly successful",
        "successful",
        "less than successful",
        "unsuccessful",
        "highly relevant",
        "relevant",
        "less relevant",
        "inefficient",
        "efficient",
        "likely",
        "less likely",
        "unlikely",
    }

    if value_low in allowed:
        field.field_value = value
        return field

    if 4 <= len(value) <= 40 and re.fullmatch(r"[A-Za-z \-]+", value):
        field.field_value = value
        return field

    return None


def extract_core_fields_adb(chunks: List[dict]) -> List[ExtractedField]:
    fields: List[ExtractedField] = []
    early_chunks = _filter_chunks(chunks, early_only=True)

    project_name = extract_project_title(early_chunks) or extract_project_title(chunks)
    if project_name and project_name.field_value:
        project_name.field_value = clean_project_title(project_name.field_value)
        if not project_name.field_value:
            project_name = None
    fields.append(field_or_not_found(project_name, "project_name"))

    project_name_value = project_name.field_value if project_name else None

    project_number = extract_field_from_tables_or_text(
        early_chunks,
        "project_number",
        ADB_TABLE_ALIASES["project_number"],
        ADB_TEXT_PATTERNS["project_number"],
        value_regex=r"\b([A-Z0-9][A-Z0-9\-\/()]+)\b",
        table_confidence=0.95,
        text_confidence=0.92,
    )
    fields.append(project_number)

    loan_number = extract_field_from_tables_or_text(
        early_chunks,
        "loan_number",
        ADB_TABLE_ALIASES["loan_number"],
        ADB_TEXT_PATTERNS["loan_number"],
        value_regex=r"\b([A-Z0-9][A-Z0-9\-\/() ]+)\b",
        table_confidence=0.94,
        text_confidence=0.90,
    )
    fields.append(loan_number)

    country = extract_field_from_tables_or_text(
        early_chunks,
        "country",
        ADB_TABLE_ALIASES["country"],
        ADB_TEXT_PATTERNS["country"],
        value_regex=r"([A-Za-z][A-Za-z'.,()\- ]+)$",
        table_confidence=0.92,
        text_confidence=0.88,
    )
    fields.append(country)

    approval_date = extract_field_from_tables_or_text(
        early_chunks,
        "approval_date",
        ADB_TABLE_ALIASES["approval_date"],
        ADB_TEXT_PATTERNS["approval_date"],
        value_regex=r"([A-Za-z0-9, \-/]+)$",
        table_confidence=0.92,
        text_confidence=0.90,
    )
    fields.append(approval_date)

    planned_closing_date = (
        _extract_from_adb_metadata_row(
            early_chunks,
            "planned_closing_date",
            ADB_TABLE_ALIASES["planned_closing_date"],
            prefer_first=True,
            value_kind="date",
            confidence=0.95,
        )
        or extract_from_text(
            early_chunks,
            "planned_closing_date",
            ADB_TEXT_PATTERNS["planned_closing_date"],
            confidence=0.88,
        )
    )
    fields.append(field_or_not_found(planned_closing_date, "planned_closing_date"))

    actual_closing_date = (
        _extract_from_adb_metadata_row(
            early_chunks,
            "actual_closing_date",
            ADB_TABLE_ALIASES["actual_closing_date"],
            prefer_first=False,
            value_kind="date",
            confidence=0.95,
        )
        or extract_from_text(
            early_chunks,
            "actual_closing_date",
            ADB_TEXT_PATTERNS["actual_closing_date"],
            confidence=0.88,
        )
    )
    fields.append(field_or_not_found(actual_closing_date, "actual_closing_date"))

    appraisal_cost = (
        _extract_from_adb_metadata_row(
            early_chunks,
            "appraisal_cost",
            ADB_TABLE_ALIASES["appraisal_cost"],
            prefer_first=True,
            value_kind="numeric",
            confidence=0.93,
        )
        or extract_from_text(
            chunks,
            "appraisal_cost",
            ADB_TEXT_PATTERNS["appraisal_cost"],
            confidence=0.86,
        )
    )
    fields.append(field_or_not_found(appraisal_cost, "appraisal_cost"))

    actual_cost = (
        _extract_from_adb_metadata_row(
            early_chunks,
            "actual_cost",
            ADB_TABLE_ALIASES["actual_cost"],
            prefer_first=False,
            value_kind="numeric",
            confidence=0.93,
        )
        or extract_from_text(
            chunks,
            "actual_cost",
            ADB_TEXT_PATTERNS["actual_cost"],
            confidence=0.86,
        )
    )
    fields.append(field_or_not_found(actual_cost, "actual_cost"))

    # Updated output extraction
    narrative_target, narrative_actual = _extract_output_pair_from_narrative(chunks)
    table_pairs = _extract_semantic_output_pairs_from_tables(chunks)
    output_target_km, output_actual_km = _choose_best_output_pair(
        project_name_value,
        (narrative_target, narrative_actual),
        table_pairs,
    )

    fields.append(field_or_not_found(output_target_km, "output_target_km"))
    fields.append(field_or_not_found(output_actual_km, "output_actual_km"))

    eirr = extract_from_tables(
        chunks,
        "eirr_percent",
        ADB_TABLE_ALIASES["eirr_percent"],
        value_regex=r"(\d{1,2}(?:\.\d+)?)\s*%",
        unit="%",
        confidence=0.92,
    ) or extract_from_text(
        chunks,
        "eirr_percent",
        ADB_TEXT_PATTERNS["eirr_percent"],
        unit="%",
        confidence=0.90,
    )

    if eirr:
        cleaned = _valid_rate(eirr.field_value)
        if cleaned:
            eirr.field_value = cleaned
        else:
            eirr = None

    fields.append(field_or_not_found(eirr, "eirr_percent"))

    rating = extract_from_text(
        chunks,
        "overall_rating",
        ADB_TEXT_PATTERNS["overall_rating"],
        confidence=0.76,
    ) or extract_from_tables(
        chunks,
        "overall_rating",
        ADB_TABLE_ALIASES["overall_rating"],
        value_regex=r"([A-Za-z \-]{4,40})$",
        confidence=0.74,
    )
    rating = _clean_rating(rating)
    fields.append(field_or_not_found(rating, "overall_rating"))

    safeguards = extract_from_text(
        chunks,
        "safeguards_summary",
        [
            r"((?:environmental safeguards|resettlement|environmental concerns|safeguards).{0,350})",
        ],
        confidence=0.60,
    )
    if safeguards and safeguards.field_value:
        safeguards.field_value = clean_ocr_text(safeguards.field_value)
    fields.append(field_or_not_found(safeguards, "safeguards_summary"))

    lessons = extract_from_text(
        chunks,
        "lessons_summary",
        [
            r"((?:lessons?.{0,400}|recommendations?.{0,400}))",
        ],
        confidence=0.58,
    )
    if lessons and lessons.field_value:
        lessons.field_value = clean_ocr_text(lessons.field_value)
    fields.append(field_or_not_found(lessons, "lessons_summary"))

    return fields