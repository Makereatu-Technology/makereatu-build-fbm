from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, List, Optional


DATE_FORMATS = [
    "%d %B %Y",   # 30 April 2005
    "%d %b %Y",   # 30 Apr 2005
    "%B %d, %Y",  # April 30, 2005
    "%b %d, %Y",  # Apr 30, 2005
    "%Y-%m-%d",   # 2005-04-30
    "%d-%b-%Y",   # 31-Dec-2012
    "%d-%B-%Y",   # 31-December-2012
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%d %b, %Y",
    "%d %B, %Y",
]


def clean_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    v = str(value).replace("\u00a0", " ").strip()
    v = re.sub(r"\s+", " ", v)

    # remove obvious repeated first token
    parts = v.split()
    if len(parts) >= 2 and parts[0].lower() == parts[1].lower():
        v = " ".join([parts[0]] + parts[2:])

    # exact repeated half
    parts = v.split()
    if len(parts) >= 4 and len(parts) % 2 == 0:
        half = len(parts) // 2
        if [p.lower() for p in parts[:half]] == [p.lower() for p in parts[half:]]:
            v = " ".join(parts[:half])

    return v.strip(" |:-")


def clean_project_name(value: Optional[str]) -> Optional[str]:
    v = clean_text(value)
    if not v:
        return None

    v = re.sub(r"^(project name|project title)\s*[:\-]?\s*", "", v, flags=re.I)
    v = re.sub(r"\b(Project ID|Project Number|Loan Number)\b.*$", "", v, flags=re.I)
    v = re.sub(r"\s{2,}", " ", v).strip(" |:-")

    low = v.lower()
    bad_exact = {
        "project",
        "project name",
        "project title",
        "project id",
        "project number",
        "loan number",
        "basic data",
        "project data",
        "validation report",
        "performance evaluation report",
        "ieg icr review",
    }

    if low in bad_exact:
        return None

    if re.fullmatch(r"[A-Z]?\d+(?:[-/][A-Z0-9]+)*", v, flags=re.I):
        return None

    if len(v) < 5:
        return None

    return v


def normalize_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None

    v = clean_text(value)
    if not v:
        return None

    # Try direct formats first
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(v, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass

    # Common embedded date patterns
    date_patterns = [
        r"\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}\b",      # 30 April 2005
        r"\b[A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}\b",     # April 30, 2005
        r"\b\d{1,2}-[A-Za-z]{3}-\d{4}\b",            # 31-Dec-2012
        r"\b\d{1,2}-[A-Za-z]{4,9}-\d{4}\b",          # 31-December-2012
        r"\b\d{4}-\d{2}-\d{2}\b",                    # 2005-04-30
        r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b",        # 30/04/2005
    ]

    for pat in date_patterns:
        m = re.search(pat, v, flags=re.I)
        if m:
            candidate = m.group(0).strip()
            for fmt in DATE_FORMATS:
                try:
                    return datetime.strptime(candidate, fmt).strftime("%Y-%m-%d")
                except Exception:
                    pass

    # Month + year fallback
    m = re.search(r"\b([A-Za-z]{3,9})\s+(\d{4})\b", v, flags=re.I)
    if m:
        candidate = f"1 {m.group(1)} {m.group(2)}"
        for fmt in ("%d %B %Y", "%d %b %Y"):
            try:
                return datetime.strptime(candidate, fmt).strftime("%Y-%m-%d")
            except Exception:
                pass

    # Year only fallback
    m = re.fullmatch(r"\d{4}", v)
    if m:
        return f"{v}-01-01"

    return None


def normalize_numeric(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None

    v = clean_text(str(value))
    if not v:
        return None

    v = v.replace(",", "")
    m = re.search(r"-?\d+(?:\.\d+)?", v)
    if not m:
        return None

    try:
        return float(m.group(0))
    except Exception:
        return None


def normalize_currency(value: Optional[str]) -> Dict[str, Optional[float]]:
    if not value:
        return {"currency": None, "amount": None}

    raw = clean_text(value)
    if not raw:
        return {"currency": None, "amount": None}

    v = raw.upper()
    currency = None

    if "US$" in v or "USD" in v or "$" in v:
        currency = "USD"

    amount = normalize_numeric(raw)

    # optional: interpret "million" / "billion"
    if amount is not None:
        low = raw.lower()
        if "billion" in low:
            amount *= 1_000_000_000
        elif "million" in low:
            amount *= 1_000_000

    return {"currency": currency, "amount": amount}


def normalize_extracted_fields(fields: List[dict]) -> List[dict]:
    out: List[dict] = []

    for f in fields:
        item = dict(f)
        name = item.get("field_name")
        value = item.get("field_value")

        if name == "project_name":
            item["normalized_value"] = clean_project_name(value)

        elif name in {"approval_date", "planned_closing_date", "actual_closing_date"}:
            item["normalized_value"] = normalize_date(value)

        elif name in {"output_target_km", "output_actual_km"}:
            num = normalize_numeric(value)
            item["normalized_value"] = num if (num is None or 0 <= num <= 100000) else None

        elif name in {"eirr_percent", "err_percent"}:
            num = normalize_numeric(value)
            item["normalized_value"] = num if (num is None or 0 <= num <= 100) else None

        elif name in {"appraisal_cost", "actual_cost"}:
            item["normalized_value"] = normalize_currency(value)

        else:
            item["normalized_value"] = clean_text(value)

        out.append(item)

    return out