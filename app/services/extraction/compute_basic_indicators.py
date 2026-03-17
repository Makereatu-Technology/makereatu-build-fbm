from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional


def _get_normalized(fields: List[dict], field_name: str):
    for f in fields:
        if f.get("field_name") == field_name:
            return f.get("normalized_value")
    return None


def _parse_date(v: Optional[str]) -> Optional[datetime]:
    if not v or not isinstance(v, str):
        return None
    try:
        return datetime.strptime(v, "%Y-%m-%d")
    except Exception:
        return None


def _to_float(v) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).strip())
    except Exception:
        return None


def compute_basic_indicators(fields: List[dict]) -> Dict[str, Optional[float]]:
    indicators: Dict[str, Optional[float]] = {
        "delay_years": None,
        "delay_months": None,
        "delivery_gap_km_ratio": None,
        "economic_rate_percent": None,
    }

    # -------------------------
    # Delay indicators
    # -------------------------
    planned = _parse_date(_get_normalized(fields, "planned_closing_date"))
    actual = _parse_date(_get_normalized(fields, "actual_closing_date"))

    if planned and actual:
        days = (actual - planned).days

        # Keep only non-negative delays.
        # If a project finished earlier than planned, treat delay as 0.0.
        if days < 0:
            days = 0

        indicators["delay_years"] = round(days / 365.25, 3)
        indicators["delay_months"] = round(days / 30.4375, 2)

    # -------------------------
    # Delivery gap ratio
    # Formula: (actual - target) / target
    # -------------------------
    target_km = _to_float(_get_normalized(fields, "output_target_km"))
    actual_km = _to_float(_get_normalized(fields, "output_actual_km"))

    if (
        target_km is not None
        and actual_km is not None
        and target_km > 0
    ):
        # basic plausibility guard
        if 0 <= target_km <= 100000 and 0 <= actual_km <= 100000:
            indicators["delivery_gap_km_ratio"] = round((actual_km - target_km) / target_km, 4)

    # -------------------------
    # Economic rate percent
    # Prefer ERR if present, otherwise EIRR
    # -------------------------
    err = _to_float(_get_normalized(fields, "err_percent"))
    eirr = _to_float(_get_normalized(fields, "eirr_percent"))

    if err is not None and 0 <= err <= 100:
        indicators["economic_rate_percent"] = round(err, 4)
    elif eirr is not None and 0 <= eirr <= 100:
        indicators["economic_rate_percent"] = round(eirr, 4)

    return indicators