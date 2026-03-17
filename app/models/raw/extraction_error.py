import uuid
from sqlalchemy import (
    Column,
    String,
    Text,
    DateTime,
    ForeignKey,
    Index,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func

from app.models.base import Base


class ExtractionError(Base):
    __tablename__ = "extraction_errors"
    __table_args__ = (
        Index("ix_extraction_errors_run_id", "run_id"),
        Index("ix_extraction_errors_project_id", "project_id"),
        Index("ix_extraction_errors_document_id", "document_id"),
        Index("ix_extraction_errors_stage", "stage"),
        Index("ix_extraction_errors_prompt_id", "prompt_id"),
        {"schema": "fbm_raw"},
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Pipeline run correlation
    run_id = Column(UUID(as_uuid=True), nullable=False)

    # Optional links (may be NULL depending on where failure happens)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey("fbm_raw.projects.id", ondelete="SET NULL"),
        nullable=True,
    )

    document_id = Column(
        UUID(as_uuid=True),
        ForeignKey("fbm_raw.documents.id", ondelete="SET NULL"),
        nullable=True,
    )

    # Where it failed
    stage = Column(String(40), nullable=False)
    # Suggested stages: DISCOVERY, INGESTION, OCR, EXTRACTION, HARMONIZATION, ML, FBM

    # If an LLM prompt/extractor failed
    prompt_id = Column(String(80), nullable=True)       # e.g., "FP03_ICR_v1"
    doc_type = Column(String(30), nullable=True)        # ICR/PCR/PAD/RRP/IEG/PPER
    focal_point = Column(String(20), nullable=True)     # e.g., "FP3"

    # Error details
    error_type = Column(String(80), nullable=False)     # e.g., TimeoutError, JSONDecodeError, HTTPError
    error_message = Column(Text, nullable=False)
    stack_trace = Column(Text, nullable=True)

    # Optional payloads for debugging/audit
    request_payload = Column(JSONB, nullable=True)
    response_payload = Column(JSONB, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)