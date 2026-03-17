from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Set, Tuple
import csv
import io
import os
import time
import logging
import requests
from urllib.parse import quote

from app.services.discovery.filters import normalize, TARGET_COUNTRIES, any_country_in_text

log = logging.getLogger(__name__)

# ADB Sovereign Projects CSV download (stable public dataset)
ADB_SOVEREIGN_PROJECTS_CSV_URL = "https://data.adb.org/media/81/download"

CACHE_DIR = os.path.join(os.getcwd(), ".cache")
CACHE_FILE = os.path.join(CACHE_DIR, "adb_sovereign_projects.csv")
CACHE_TTL_SECONDS = 24 * 3600  # 1 day


@dataclass
class ADBHit:
    title: str
    url: str
    date: Optional[str] = None
    country: Optional[str] = None
    doc_type: Optional[str] = None
    project_id: Optional[str] = None


def _ensure_cache() -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_is_fresh(path: str, ttl: int) -> bool:
    if not os.path.exists(path):
        return False
    age = time.time() - os.path.getmtime(path)
    return age <= ttl


def _download_csv_to_cache(timeout: int = 60) -> None:
    _ensure_cache()
    log.info("ADB CSV download: %s", ADB_SOVEREIGN_PROJECTS_CSV_URL)
    r = requests.get(ADB_SOVEREIGN_PROJECTS_CSV_URL, timeout=timeout)
    log.info("ADB CSV status=%s bytes=%s", r.status_code, len(r.content or b""))
    r.raise_for_status()
    with open(CACHE_FILE, "wb") as f:
        f.write(r.content)


def _load_rows(timeout: int) -> List[Dict[str, str]]:
    if not _cache_is_fresh(CACHE_FILE, CACHE_TTL_SECONDS):
        _download_csv_to_cache(timeout=timeout)

    with open(CACHE_FILE, "rb") as f:
        raw = f.read()

    # Try UTF-8 first; fall back quietly
    try:
        text = raw.decode("utf-8")
    except Exception:
        text = raw.decode("latin-1", errors="replace")

    reader = csv.DictReader(io.StringIO(text))
    rows: List[Dict[str, str]] = []
    for row in reader:
        # Normalize keys (keep original values)
        cleaned: Dict[str, str] = {}
        for k, v in (row or {}).items():
            if k is None:
                continue
            cleaned[k.strip()] = (v or "").strip()
        if cleaned:
            rows.append(cleaned)
    return rows


def _pick_col(headers: List[str], candidates: List[str]) -> Optional[str]:
    """
    Pick a column by checking whether header contains any candidate substring.
    """
    hnorm = [(h, normalize(h)) for h in headers]
    for cand in candidates:
        c = normalize(cand)
        for h, hn in hnorm:
            if c in hn:
                return h
    return None


def _parse_year(value: str) -> Optional[int]:
    if not value:
        return None
    # common formats: YYYY-MM-DD or DD-Mon-YYYY etc
    for token in value.replace("/", "-").split():
        pass
    import re
    m = re.search(r"(19|20)\d{2}", value)
    return int(m.group(0)) if m else None


def _score_text_match(text: str, keywords: List[str]) -> int:
    blob = normalize(text)
    score = 0
    for kw in keywords:
        k = normalize(kw)
        if k and k in blob:
            score += 1
    return score


class ADBDiscoveryClient:
    """
    ADB discovery based on the ADB Sovereign Projects dataset (CSV).
    This is more reliable than scraping adb.org/search.
    """

    BASE_PROJECT = "https://www.adb.org/projects"

    def __init__(self, timeout: int = 60):
        self.timeout = timeout

    def search(
        self,
        keywords: str,
        year_min: int = 2010,
        year_max: int = 2020,
        max_hits: int = 20,
    ) -> List[ADBHit]:
        kws = [k.strip() for k in keywords.split() if k.strip()]
        # Also treat the whole phrase as a keyword
        kw_list = list({*kws, keywords})

        rows = _load_rows(timeout=self.timeout)
        if not rows:
            return []

        headers = list(rows[0].keys())

        # Heuristic column mapping (works even if ADB changes header text slightly)
        col_title = _pick_col(headers, ["project name", "title", "name"])
        col_country = _pick_col(headers, ["country", "borrower", "economy"])
        col_id = _pick_col(headers, ["project number", "project no", "project id", "project"])
        col_sector = _pick_col(headers, ["sector", "subsector", "industry"])
        col_status = _pick_col(headers, ["status"])
        col_completion = _pick_col(headers, ["completion date", "closing date", "completion"])
        col_approval = _pick_col(headers, ["approval date", "approval"])

        hits: List[Tuple[int, ADBHit]] = []
        seen: Set[str] = set()

        for r in rows:
            title = (r.get(col_title) if col_title else "") or ""
            country = (r.get(col_country) if col_country else "") or ""
            proj_id = (r.get(col_id) if col_id else "") or ""
            sector = (r.get(col_sector) if col_sector else "") or ""
            status = (r.get(col_status) if col_status else "") or ""

            # Prefer completion year; fall back to approval year
            y = None
            date_val = ""
            if col_completion:
                date_val = r.get(col_completion, "") or ""
                y = _parse_year(date_val)
            if y is None and col_approval:
                date_val = r.get(col_approval, "") or ""
                y = _parse_year(date_val)

            if y is None or y < year_min or y > year_max:
                continue

            blob = f"{title} {sector} {country} {status} {proj_id}"
            score = _score_text_match(blob, kw_list)

            # Require at least one keyword match to reduce noise
            if score <= 0:
                continue

            # Build a reasonable project URL
            # Many ADB project pages accept the project number format (e.g., 12345-001)
            proj_url = ""
            if proj_id:
                proj_url = f"{self.BASE_PROJECT}/{quote(proj_id)}"
            else:
                # Fallback: query page
                proj_url = f"{self.BASE_PROJECT}?keywords={quote(title)}"

            if proj_url in seen:
                continue
            seen.add(proj_url)

            hits.append((
                score,
                ADBHit(
                    title=title or proj_id or "ADB Project",
                    url=proj_url,
                    date=str(y),
                    country=country or None,
                    doc_type="Project",
                    project_id=proj_id or None,
                )
            ))

        hits.sort(key=lambda t: t[0], reverse=True)
        return [h for _, h in hits[:max_hits]]

    @staticmethod
    def filter_region(hits: List[ADBHit]) -> List[ADBHit]:
        out: List[ADBHit] = []
        for h in hits:
            # Use explicit country if present, else fallback to title/url text
            if h.country:
                if normalize(h.country) in TARGET_COUNTRIES:
                    out.append(h)
                continue
            if any_country_in_text(f"{h.title} {h.url}"):
                out.append(h)
        return out