from __future__ import annotations

import os
import re
import hashlib
from dataclasses import dataclass
from typing import Optional, Dict

import requests

PDF_NAME_SAFE = re.compile(r"[^a-zA-Z0-9._-]+")


@dataclass
class DownloadResult:
    url: str
    ok: bool
    path: Optional[str] = None
    error: Optional[str] = None
    content_type: Optional[str] = None
    bytes: Optional[int] = None


def _safe_filename(name: str) -> str:
    name = name.strip()
    name = PDF_NAME_SAFE.sub("_", name)
    return name[:180] if len(name) > 180 else name


def _hash_url(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()  # fine for filenames


def download_pdf_url(
    url: str,
    out_dir: str,
    timeout: int = 60,
    overwrite: bool = False,
) -> DownloadResult:
    os.makedirs(out_dir, exist_ok=True)
    url = url.strip()

    # filename strategy: hash + tail name
    tail = url.split("?")[0].split("/")[-1] or "document.pdf"
    if not tail.lower().endswith(".pdf"):
        tail = tail + ".pdf"
    fname = f"{_hash_url(url)}__{_safe_filename(tail)}"
    out_path = os.path.join(out_dir, fname)

    if os.path.exists(out_path) and not overwrite:
        return DownloadResult(url=url, ok=True, path=out_path, error=None, bytes=os.path.getsize(out_path))

    headers = {
        "User-Agent": "makereatu-fbm/0.1",
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
    }

    try:
        with requests.get(url, headers=headers, timeout=timeout, stream=True) as r:
            ct = r.headers.get("Content-Type", "")
            r.raise_for_status()

            total = 0
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 256):
                    if chunk:
                        f.write(chunk)
                        total += len(chunk)

        return DownloadResult(url=url, ok=True, path=out_path, content_type=ct, bytes=total)
    except Exception as e:
        # clean up partial file
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass
        return DownloadResult(url=url, ok=False, error=f"{type(e).__name__}: {e}")