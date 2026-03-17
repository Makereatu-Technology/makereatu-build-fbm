from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.core.db import get_db
from app.schemas.raw import DocumentIngestFromUrl, DocumentIngestResult
from app.models.raw.document import RawDocument
from app.services.ingestion.downloader import download_pdf

router = APIRouter(prefix="/ingestion", tags=["ingestion"])


@router.post("/document_from_url", response_model=DocumentIngestResult)
def ingest_document_from_url(payload: DocumentIngestFromUrl, db: Session = Depends(get_db)):
    # Verify project exists
    exists = db.execute(
        text("SELECT 1 FROM fbm_raw.projects WHERE id = :pid"),
        {"pid": payload.project_id},
    ).scalar()

    if not exists:
        raise HTTPException(status_code=404, detail="Project not found")

    # Download + validate content
    try:
        dl = download_pdf(str(payload.source_url))
    except Exception as e:
        msg = str(e) or e.__class__.__name__
        raise HTTPException(status_code=400, detail=f"Failed to download a valid PDF: {msg}")

    # Idempotency check: same project + same file content (DO NOT filter on mime_type)
    existing = (
        db.query(RawDocument)
        .filter(RawDocument.project_id == payload.project_id)
        .filter(RawDocument.sha256 == dl.sha256)
        .first()
    )
    if existing:
        # Repair bad mime_type from older ingests (optional but recommended)
        updated = False
        if existing.mime_type != "application/pdf":
            existing.mime_type = "application/pdf"
            updated = True
        if not existing.file_name and dl.file_name:
            existing.file_name = dl.file_name
            updated = True
        if (existing.file_size_bytes is None) and dl.size_bytes:
            existing.file_size_bytes = dl.size_bytes
            updated = True
        if updated:
            db.commit()
            db.refresh(existing)

        return DocumentIngestResult(
            status="exists",
            document_id=existing.id,
            sha256=existing.sha256,
            file_name=existing.file_name,
            mime_type=existing.mime_type,
            size_bytes=existing.file_size_bytes,
        )

    # Create new document
    doc = RawDocument(
        project_id=payload.project_id,
        source_system=payload.source_system,
        doc_type=payload.doc_type,
        title=payload.title,
        source_url=str(payload.source_url),
        file_name=dl.file_name,
        mime_type="application/pdf",
        file_size_bytes=dl.size_bytes,
        sha256=dl.sha256,
        file_bytes=dl.content if payload.store_file_bytes else None,
        extraction_status="PENDING",
    )

    db.add(doc)

    # Protect against race conditions or previous inserts we didn't see
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        # Fetch the row that caused the unique violation and return it
        existing = (
            db.query(RawDocument)
            .filter(RawDocument.project_id == payload.project_id)
            .filter(RawDocument.sha256 == dl.sha256)
            .first()
        )
        if not existing:
            raise HTTPException(status_code=500, detail="Unique constraint hit but existing document not found.")
        return DocumentIngestResult(
            status="exists",
            document_id=existing.id,
            sha256=existing.sha256,
            file_name=existing.file_name,
            mime_type=existing.mime_type,
            size_bytes=existing.file_size_bytes,
        )

    db.refresh(doc)

    return DocumentIngestResult(
        status="ingested",
        document_id=doc.id,
        sha256=doc.sha256,
        file_name=doc.file_name,
        mime_type=doc.mime_type,
        size_bytes=doc.file_size_bytes,
    )