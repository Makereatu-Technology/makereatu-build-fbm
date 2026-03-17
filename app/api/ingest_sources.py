from __future__ import annotations

import os
from typing import List, Optional
from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.ingest.source_collectors import collect_pdf_sources_from_folder, PdfSource
from app.services.ingest.pdf_downloader import download_pdf_url, DownloadResult

router = APIRouter(prefix="/ingest", tags=["ingest"])


class ListSourcesRequest(BaseModel):
    folder: str = Field(..., description="Local folder path")
    recursive: bool = True
    include_local_pdfs: bool = True
    include_table_links: bool = True
    extra_urls: List[str] = Field(default_factory=list)


class ListSourcesResponse(BaseModel):
    folder: str
    count: int
    sources: List[PdfSource]


@router.post("/list-sources", response_model=ListSourcesResponse)
def list_sources(payload: ListSourcesRequest):
    sources = collect_pdf_sources_from_folder(
        folder=payload.folder,
        recursive=payload.recursive,
        include_local_pdfs=payload.include_local_pdfs,
        include_table_links=payload.include_table_links,
    )

    # add manual URLs
    for u in payload.extra_urls:
        u = (u or "").strip()
        if u:
            sources.append(PdfSource("url", u, origin="manual"))

    # simple dedupe
    uniq = []
    seen = set()
    for s in sources:
        key = (s.source_type, s.value.strip())
        if key in seen:
            continue
        seen.add(key)
        uniq.append(s)

    return ListSourcesResponse(folder=os.path.abspath(payload.folder), count=len(uniq), sources=uniq)


class DownloadUrlsRequest(BaseModel):
    urls: List[str]
    out_dir: str = Field(default="./.cache/downloaded_pdfs")
    timeout: int = 60
    overwrite: bool = False


class DownloadUrlsResponse(BaseModel):
    out_dir: str
    ok: int
    failed: int
    results: List[DownloadResult]


@router.post("/download-urls", response_model=DownloadUrlsResponse)
def download_urls(payload: DownloadUrlsRequest):
    out_dir = os.path.abspath(payload.out_dir)

    results: List[DownloadResult] = []
    for u in payload.urls:
        if not u or not u.strip():
            continue
        results.append(download_pdf_url(
            url=u,
            out_dir=out_dir,
            timeout=payload.timeout,
            overwrite=payload.overwrite,
        ))

    ok = sum(1 for r in results if r.ok)
    failed = sum(1 for r in results if not r.ok)

    return DownloadUrlsResponse(out_dir=out_dir, ok=ok, failed=failed, results=results)