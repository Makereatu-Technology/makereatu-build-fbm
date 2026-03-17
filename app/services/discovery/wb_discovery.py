from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import logging
import requests

from app.services.discovery.filters import normalize, TARGET_COUNTRIES

log = logging.getLogger(__name__)


@dataclass
class WBDocHit:
    title: str
    url: str
    docdt: Optional[str]
    country: Optional[str]
    language: Optional[str]
    docty: Optional[str]
    project_id: Optional[str]


class WBDiscoveryClient:
    """
    World Bank Documents & Reports API wrapper.
    Docs: https://documents.worldbank.org/en/publication/documents-reports/api
    """
    BASE = "https://search.worldbank.org/api/v3/wds"

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "makereatu-fbm/0.1",
            "Accept": "application/json",
        })

    def search(
        self,
        qterm: str,
        rows: int = 20,
        os: int = 0,
        start_date: Optional[str] = None,  # YYYY-MM-DD
        end_date: Optional[str] = None,    # YYYY-MM-DD
    ) -> List[WBDocHit]:
        params = {
            "format": "json",
            "apilang": "en",
            "qterm": qterm,
            "rows": rows,
            "os": os,
            "fl": "display_title,docdt,count,lang,docty,projectid,txturl,docurl",
        }
        if start_date:
            params["strdate"] = start_date
        if end_date:
            params["enddate"] = end_date

        r = self.session.get(self.BASE, params=params, timeout=self.timeout)
        log.info("WB request: %s status=%s", r.url, r.status_code)
        r.raise_for_status()

        data = r.json() if r.text else {}
        docs: Dict[str, Any] = (data or {}).get("documents") or {}
        hits: List[WBDocHit] = []

        if not isinstance(docs, dict):
            return hits

        for _, d in docs.items():
            # The "documents" object can include metadata keys (e.g., total/rows/os).
            if not isinstance(d, dict):
                continue

            title = d.get("display_title") or d.get("title") or ""
            url = d.get("docurl") or d.get("txturl") or d.get("url") or ""

            country = d.get("count")
            if isinstance(country, list):
                country = country[0] if country else None

            hits.append(WBDocHit(
                title=title,
                url=url,
                docdt=d.get("docdt"),
                country=country,
                language=d.get("lang"),
                docty=d.get("docty"),
                project_id=d.get("projectid"),
            ))

        return hits

    @staticmethod
    def filter_region(hits: List[WBDocHit]) -> List[WBDocHit]:
        out: List[WBDocHit] = []
        for h in hits:
            c = normalize(h.country or "")
            if c and c in TARGET_COUNTRIES:
                out.append(h)
        return out