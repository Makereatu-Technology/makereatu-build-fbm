from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Literal, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


ResolvedType = Literal["direct_pdf", "doi", "landing_page", "collection", "unknown"]


@dataclass
class ResolveResult:
    resolved_type: ResolvedType
    input_url: str
    final_url: str
    pdf_urls: List[str]
    notes: str


_PDF_EXT_RE = re.compile(r"\.pdf(\?|#|$)", re.IGNORECASE)

_META_PDF_KEYS = {
    "citation_pdf_url",
    "dc.identifier",
    "dc.relation",
}


def _normalize_url(u: str) -> str:
    return (u or "").strip().strip(' "\'')


def _is_doi_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return host in {"doi.org", "dx.doi.org"}


def _looks_like_pdf_bytes(b: bytes) -> bool:
    return b[:5] == b"%PDF-"


def _browser_headers() -> dict:
    # This header set avoids many 403s vs default python-requests fingerprints
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def _pdf_headers() -> dict:
    h = _browser_headers()
    h["Accept"] = "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8"
    return h


def _fetch_sniff(session: requests.Session, url: str, timeout: int = 30, max_bytes: int = 4096) -> Tuple[str, str, bytes]:
    """
    Fetches a small prefix of the response to classify content.
    Returns: (final_url, content_type_base, first_bytes)
    """
    r = session.get(url, timeout=timeout, headers=_pdf_headers(), allow_redirects=True, stream=True)
    r.raise_for_status()
    final_url = str(r.url)
    content_type = (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()

    first = b""
    for chunk in r.iter_content(chunk_size=max_bytes):
        first = chunk or b""
        break
    r.close()
    return final_url, content_type, first


def _extract_pdf_links_from_html(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: Set[str] = set()

    # a href
    for a in soup.find_all("a", href=True):
        href = _normalize_url(a.get("href"))
        if not href:
            continue
        full = urljoin(base_url, href)
        if _PDF_EXT_RE.search(full):
            candidates.add(full)

    # meta citation_pdf_url etc.
    for meta in soup.find_all("meta"):
        name = (meta.get("name") or meta.get("property") or "").strip().lower()
        content = _normalize_url(meta.get("content"))
        if not name or not content:
            continue
        if name in _META_PDF_KEYS:
            full = urljoin(base_url, content)
            if _PDF_EXT_RE.search(full):
                candidates.add(full)

    return sorted(candidates)


def _publisher_fallback_pdf_urls(final_url: str) -> List[str]:
    """
    Simple publisher heuristics that often work even when HTML fetch is blocked.
    """
    host = urlparse(final_url).netloc.lower()
    path = urlparse(final_url).path.rstrip("/")

    # MDPI: article page often -> /pdf works
    if "mdpi.com" in host:
        # Example: https://www.mdpi.com/2073-445X/10/3/330  -> append /pdf
        if re.search(r"/\d+/\d+/\d+$", path):
            return [final_url.rstrip("/") + "/pdf"]
        # sometimes already has structure where /pdf is valid anyway
        return [final_url.rstrip("/") + "/pdf"]

    return []


def resolve_link(url: str, timeout: int = 30, limit: int = 25) -> ResolveResult:
    input_url = _normalize_url(url)
    if not input_url:
        return ResolveResult("unknown", url, url, [], "Empty URL")

    session = requests.Session()
    session.headers.update(_browser_headers())

    # Step 1: sniff (follow redirects)
    try:
        final_url, content_type, first = _fetch_sniff(session, input_url, timeout=timeout)
    except requests.HTTPError as e:
        # If DOI redirects to publisher and publisher blocks HTML, try publisher fallback
        final_url = getattr(getattr(e, "response", None), "url", input_url) or input_url
        fallbacks = _publisher_fallback_pdf_urls(str(final_url))
        if fallbacks:
            rtype: ResolvedType = "doi" if _is_doi_url(input_url) else "landing_page"
            return ResolveResult(rtype, input_url, str(final_url), fallbacks[:limit], f"403/blocked; using publisher fallback for {urlparse(str(final_url)).netloc}")
        return ResolveResult("unknown", input_url, input_url, [], f"Fetch failed: {e}")
    except Exception as e:
        return ResolveResult("unknown", input_url, input_url, [], f"Fetch failed: {e}")

    # Direct PDF?
    if _looks_like_pdf_bytes(first) or content_type == "application/pdf":
        rtype: ResolvedType = "direct_pdf"
        if _is_doi_url(input_url):
            rtype = "doi"
        return ResolveResult(rtype, input_url, final_url, [final_url], "Direct PDF detected")

    # Step 2: try HTML parse
    try:
        r = session.get(final_url, timeout=timeout, headers=_browser_headers(), allow_redirects=True)
        r.raise_for_status()
        html = r.text or ""
    except requests.HTTPError as e:
        fallbacks = _publisher_fallback_pdf_urls(final_url)
        if fallbacks:
            rtype: ResolvedType = "doi" if _is_doi_url(input_url) else "landing_page"
            return ResolveResult(rtype, input_url, final_url, fallbacks[:limit], f"HTML blocked; using publisher fallback for {urlparse(final_url).netloc}")
        rtype = "doi" if _is_doi_url(input_url) else "unknown"
        return ResolveResult(rtype, input_url, final_url, [], f"Could not retrieve HTML for parsing: {e}")
    except Exception as e:
        rtype = "doi" if _is_doi_url(input_url) else "unknown"
        return ResolveResult(rtype, input_url, final_url, [], f"Could not retrieve HTML for parsing: {e}")

    pdf_urls = _extract_pdf_links_from_html(html, final_url)

    if not pdf_urls:
        # last chance: publisher heuristic
        pdf_urls = _publisher_fallback_pdf_urls(final_url)

    if pdf_urls:
        pdf_urls = pdf_urls[: max(1, limit)]
        if _is_doi_url(input_url):
            rtype = "doi"
        elif len(pdf_urls) == 1:
            rtype = "landing_page"
        else:
            rtype = "collection"
        return ResolveResult(rtype, input_url, final_url, pdf_urls, f"Found {len(pdf_urls)} PDF link(s)")

    rtype = "doi" if _is_doi_url(input_url) else "landing_page"
    return ResolveResult(rtype, input_url, final_url, [], "No PDF links found on page")