from __future__ import annotations

from typing import List, Optional, Literal, Dict, Tuple
from datetime import datetime

from fastapi import APIRouter
from pydantic import BaseModel, Field
from fastapi.concurrency import run_in_threadpool

from app.services.discovery.filters import KEYWORDS_DEFAULT
from app.services.discovery.wb_discovery import WBDiscoveryClient
from app.services.discovery.adb_discovery import ADBDiscoveryClient

router = APIRouter(prefix="/discovery", tags=["discovery-search"])


class DiscoveryRequest(BaseModel):
    banks: List[Literal["WB", "ADB"]] = Field(default=["WB", "ADB"])
    keywords: List[str] = Field(default_factory=lambda: KEYWORDS_DEFAULT)
    year: Optional[int] = Field(default=2020, description="Prefer documents around this year")
    max_results_per_bank: int = Field(default=10, ge=1, le=50)
    region_filter: bool = Field(default=True, description="Restrict to SEA + Pacific heuristics")


class CandidateHit(BaseModel):
    bank: Literal["WB", "ADB"]
    title: str
    url: str
    date: Optional[str] = None
    country: Optional[str] = None
    doc_type: Optional[str] = None


class DiscoveryResponse(BaseModel):
    query_keywords: List[str]
    results: List[CandidateHit]


def _safe_year_from_date(date_str: Optional[str]) -> Optional[int]:
    if not date_str:
        return None
    # Try common formats; keep lightweight and forgiving
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(date_str[:10], fmt).year
        except Exception:
            pass
    # Fallback: extract 4-digit year anywhere
    import re

    m = re.search(r"(19|20)\d{2}", date_str)
    return int(m.group(0)) if m else None


def _keyword_score(title: str, keywords: List[str]) -> int:
    t = (title or "").lower()
    return sum(1 for k in keywords if k and k.lower() in t)


def _rank(hit: CandidateHit, keywords: List[str], preferred_year: Optional[int]) -> Tuple[int, int]:
    """
    Higher is better. Sort key:
      1) keyword matches (descending)
      2) closeness to preferred year (descending via negative distance)
    """
    ks = _keyword_score(hit.title, keywords)
    if preferred_year is None:
        return (ks, 0)
    hy = _safe_year_from_date(hit.date)
    if hy is None:
        return (ks, -9999)  # unknown year: lower priority
    dist = abs(hy - preferred_year)
    return (ks, -dist)


def _dedupe_by_url(hits: List[CandidateHit]) -> List[CandidateHit]:
    seen: Dict[str, CandidateHit] = {}
    for h in hits:
        key = (h.url or "").strip()
        if not key:
            continue
        # Keep the first; you could also keep the "best ranked" later
        if key not in seen:
            seen[key] = h
    return list(seen.values())


@router.post("/search", response_model=DiscoveryResponse)
async def discovery_search(payload: DiscoveryRequest):
    # Normalize query term a bit (avoid double spaces)
    keywords = [k.strip() for k in payload.keywords if k and k.strip()]
    qterm = " ".join(keywords)

    results: List[CandidateHit] = []

    async def fetch_wb() -> List[CandidateHit]:
        wb = WBDiscoveryClient()
        start_date = f"{payload.year - 1}-01-01" if payload.year else None
        end_date = f"{payload.year + 1}-12-31" if payload.year else None

        def _call():
            return wb.search(
                qterm=qterm,
                rows=payload.max_results_per_bank,
                os=0,
                start_date=start_date,
                end_date=end_date,
            )

        wb_hits = await run_in_threadpool(_call)
        if payload.region_filter:
            wb_hits = wb.filter_region(wb_hits)

        out: List[CandidateHit] = []
        for h in wb_hits[: payload.max_results_per_bank]:
            out.append(
                CandidateHit(
                    bank="WB",
                    title=h.title,
                    url=h.url,
                    date=getattr(h, "docdt", None),
                    country=getattr(h, "country", None),
                    doc_type=getattr(h, "docty", None),
                )
            )
        return out

    async def fetch_adb() -> List[CandidateHit]:
        adb = ADBDiscoveryClient()

        def _call():
            return adb.search(
                keywords=qterm,
                page=0,
                max_hits=payload.max_results_per_bank * 2,  # overfetch then filter
            )

        adb_hits = await run_in_threadpool(_call)
        if payload.region_filter:
            adb_hits = adb.filter_region(adb_hits)

        out: List[CandidateHit] = []
        for h in adb_hits[: payload.max_results_per_bank]:
            # If your ADBHit later includes metadata, wire it here.
            out.append(
                CandidateHit(
                    bank="ADB",
                    title=h.title,
                    url=h.url,
                    date=getattr(h, "date", None),
                    country=getattr(h, "country", None),
                    doc_type=getattr(h, "doc_type", None),
                )
            )
        return out

    # Fetch in parallel but isolate failures
    tasks = []
    if "WB" in payload.banks:
        tasks.append(fetch_wb())
    if "ADB" in payload.banks:
        tasks.append(fetch_adb())

    fetched: List[List[CandidateHit]] = []
    for coro in tasks:
        try:
            fetched.append(await coro)
        except Exception:
            # Don’t kill the whole response if one bank fails
            fetched.append([])

    for block in fetched:
        results.extend(block)

    # Dedupe + rank
    results = _dedupe_by_url(results)
    results.sort(key=lambda h: _rank(h, keywords, payload.year), reverse=True)

    return DiscoveryResponse(query_keywords=keywords, results=results)