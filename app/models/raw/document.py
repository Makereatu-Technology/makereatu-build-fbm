import uuid
from sqlalchemy import (
    Column,
    String,
    Text,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    LargeBinary,
    UniqueConstraint,
    Index,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from app.models.base import Base


class RawDocument(Base):
    __tablename__ = "documents"
    __table_args__ = (
        # Prevent duplicates for the same project + same file content
        UniqueConstraint("project_id", "sha256", name="uq_documents_project_sha256"),
        # Helpful indexes for querying
        Index("ix_documents_project_id", "project_id"),
        Index("ix_documents_doc_type", "doc_type"),
        Index("ix_documents_source_system", "source_system"),
        {"schema": "fbm_raw"},
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # -----------------------------
    # Link to project
    # -----------------------------
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey("fbm_raw.projects.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Relationship (optional, but nice to have)
    project = relationship("RawProject", backref="documents")

    # -----------------------------
    # Document identity / provenance
    # -----------------------------
    source_system = Column(String(20), nullable=False)  # "WB" or "ADB"
    doc_type = Column(String(30), nullable=False)       # "ICR", "PAD", "PCR", "RRP", "IEG", "PPER", etc.
    title = Column(String(500), nullable=True)

    source_url = Column(Text, nullable=True)            # where we downloaded it from
    blob_url = Column(Text, nullable=True)              # if stored in Azure Blob later
    file_name = Column(String(255), nullable=True)
    mime_type = Column(String(120), nullable=True)      # "application/pdf"
    file_size_bytes = Column(Integer, nullable=True)

    # SHA256 hash of the file bytes (for dedupe + integrity)
    sha256 = Column(String(64), nullable=True)

    # Store raw bytes (local dev). For Azure, prefer blob_url and keep this NULL.
    file_bytes = Column(LargeBinary, nullable=True)

    # -----------------------------
    # OCR / Document Intelligence outputs
    # -----------------------------
    # Raw extracted text (full, concatenated) for quick search and retrieval
    full_text = Column(Text, nullable=True)

    # Optional structured outputs from Document Intelligence
    # - pages: per-page text, words, spans, confidence, page size, etc.
    # - tables: extracted tables with cells, bounding regions
    # - paragraphs/sections: if using Layout model
    docintel_result = Column(JSONB, nullable=True)

    # Useful summary fields for gating and monitoring
    page_count = Column(Integer, nullable=True)
    ocr_confidence = Column(Numeric(4, 3), nullable=True)  # e.g., 0.000–1.000
    extraction_status = Column(String(30), nullable=False, default="PENDING")
    # Suggested statuses: PENDING, OCR_OK, OCR_POOR, OCR_FAILED, PARSED_OK, PARSED_FAILED

    # -----------------------------
    # Timestamps
    # -----------------------------
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)