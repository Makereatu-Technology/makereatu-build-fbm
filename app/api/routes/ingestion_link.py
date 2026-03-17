from __future__ import annotations

from datetime import datetime
from typing import List, Optional, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, HttpUrl, Field
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.core.db import get_db
from app.models.raw.document import RawDocument
from app.services.ingestion.downloader import download_pdf
from app.services.ingestion.link_resolver import resolve_link

router = APIRouter(prefix="/ingestion", tags=["ingestion-link"])


class IngestFromLinkRequest(BaseModel):
    project_id: UUID
    source_system: Literal["WB", "ADB"]
    doc_type: str = Field(..., examples=["ICR", "PAD", "PCR", "RRP", "IEG", "PPER"])
    url: HttpUrl
    title: Optional[str] = None
    limit: int = Field(default=10, ge=1, le=50)
    store_file_bytes: bool = True  # local dev: True; later Azure blob: False


class IngestedItem(BaseModel):
    status: Literal["ingested", "exists", "skipped", "error"]
    document_id: Optional[UUID] = None
    source_url: str
    file_name: Optional[str] = None
    sha256: Optional[str] = None
    mime_type: Optional[str] = None
    size_bytes: Optional[int] = None
    message: Optional[str] = None


class IngestFromLinkResponse(BaseModel):
    resolved_type: str
    input_url: str
    final_url: str
    found_pdfs: int
    ingested: int
    exists: int
    errors: int
    items: List[IngestedItem]
    notes: str
    timestamp: datetime


def _upsert_document(
    db: Session,
    project_id: UUID,
    source_system: str,
    doc_type: str,
    source_url: str,
    title: Optional[str],
    store_file_bytes: bool,
) -> IngestedItem:
    # Download + validate PDF
    try:
        dl = download_pdf(source_url)
    except Exception as e:
        return IngestedItem(status="error", source_url=source_url, message=f"Download/validation failed: {e}")

    # Idempotency by (project_id, sha256)
    existing = (
        db.query(RawDocument)
        .filter(RawDocument.project_id == project_id)
        .filter(RawDocument.sha256 == dl.sha256)
        .first()
    )
    if existing:
        # Repair older bad mime_types if needed
        if existing.mime_type != "application/pdf":
            existing.mime_type = "application/pdf"
            db.commit()
            db.refresh(existing)

        return IngestedItem(
            status="exists",
            document_id=existing.id,
            source_url=existing.source_url or source_url,
            file_name=existing.file_name,
            sha256=existing.sha256,
            mime_type=existing.mime_type,
            size_bytes=existing.file_size_bytes,
        )

    doc = RawDocument(
        project_id=project_id,
        source_system=source_system,
        doc_type=doc_type,
        title=title,
        source_url=source_url,
        file_name=dl.file_name,
        mime_type="application/pdf",
        file_size_bytes=dl.size_bytes,
        sha256=dl.sha256,
        file_bytes=dl.content if store_file_bytes else None,
        extraction_status="PENDING",
    )
    db.add(doc)

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        # Another request inserted it first — fetch and return as exists
        existing = (
            db.query(RawDocument)
            .filter(RawDocument.project_id == project_id)
            .filter(RawDocument.sha256 == dl.sha256)
            .first()
        )
        if existing:
            return IngestedItem(
                status="exists",
                document_id=existing.id,
                source_url=existing.source_url or source_url,
                file_name=existing.file_name,
                sha256=existing.sha256,
                mime_type=existing.mime_type,
                size_bytes=existing.file_size_bytes,
            )
        return IngestedItem(status="error", source_url=source_url, message="Unique constraint hit but row not found")

    db.refresh(doc)

    return IngestedItem(
        status="ingested",
        document_id=doc.id,
        source_url=source_url,
        file_name=doc.file_name,
        sha256=doc.sha256,
        mime_type=doc.mime_type,
        size_bytes=doc.file_size_bytes,
    )


@router.post("/from_link", response_model=IngestFromLinkResponse)
def ingest_from_link(payload: IngestFromLinkRequest, db: Session = Depends(get_db)):
    # Verify project exists
    exists = db.execute(
        text("SELECT 1 FROM fbm_raw.projects WHERE id = :pid"),
        {"pid": payload.project_id},
    ).scalar()
    if not exists:
        raise HTTPException(status_code=404, detail="Project not found")

    # Resolve link to PDF(s)
    res = resolve_link(str(payload.url), limit=payload.limit)

    if not res.pdf_urls:
        raise HTTPException(
            status_code=400,
            detail=f"No PDFs found. resolved_type={res.resolved_type}. final_url={res.final_url}. notes={res.notes}",
        )

    items: List[IngestedItem] = []
    ingested = 0
    existed = 0
    errors = 0

    for pdf_url in res.pdf_urls:
        item = _upsert_document(
            db=db,
            project_id=payload.project_id,
            source_system=payload.source_system,
            doc_type=payload.doc_type,
            source_url=pdf_url,
            title=payload.title,
            store_file_bytes=payload.store_file_bytes,
        )
        items.append(item)
        if item.status == "ingested":
            ingested += 1
        elif item.status == "exists":
            existed += 1
        elif item.status == "error":
            errors += 1

    return IngestFromLinkResponse(
        resolved_type=res.resolved_type,
        input_url=res.input_url,
        final_url=res.final_url,
        found_pdfs=len(res.pdf_urls),
        ingested=ingested,
        exists=existed,
        errors=errors,
        items=items,
        notes=res.notes,
        timestamp=datetime.utcnow(),
    )