import hashlib
import os
import re
from dataclasses import dataclass
from typing import Optional, Dict

import requests


@dataclass
class DownloadResult:
    content: bytes
    sha256: str
    file_name: Optional[str]
    mime_type: Optional[str]
    size_bytes: int
    headers: Dict[str, str]


def _guess_filename(url: str, content_disposition: Optional[str]) -> Optional[str]:
    if content_disposition:
        m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', content_disposition, re.IGNORECASE)
        if m:
            return os.path.basename(m.group(1))
    tail = url.split("?")[0].rstrip("/").split("/")[-1]
    return tail or None


def _looks_like_pdf(content: bytes) -> bool:
    # PDFs start with %PDF-
    return content[:5] == b"%PDF-"


def download_pdf(url: str, timeout: int = 60) -> DownloadResult:
    headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

    r = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True)
    r.raise_for_status()

    content = r.content
    size_bytes = len(content)
    content_type = (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()

    # Validate: must be a real PDF
    # Some servers mislabel PDFs; the magic bytes check is the real gate.
    if not _looks_like_pdf(content):
        # capture a small snippet for debugging, without dumping the whole page
        snippet = content[:200].decode(errors="ignore")
        raise ValueError(
            f"Downloaded content is not a PDF. content-type='{content_type}'. "
            f"First bytes='{snippet[:120]}'"
        )

    sha256 = hashlib.sha256(content).hexdigest()
    file_name = _guess_filename(url, r.headers.get("Content-Disposition")) or "document.pdf"
    if not file_name.lower().endswith(".pdf"):
        file_name = f"{file_name}.pdf"

    return DownloadResult(
        content=content,
        sha256=sha256,
        file_name=file_name[:255],
        mime_type=content_type or "application/pdf",
        size_bytes=size_bytes,
        headers=dict(r.headers),
    )