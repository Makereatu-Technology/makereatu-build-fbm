import uuid
from sqlalchemy import (
    Column,
    String,
    DateTime,
    Integer,
    Numeric,
    Text,
    Index,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func

from app.models.base import Base


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"
    __table_args__ = (
        Index("ix_pipeline_runs_status", "status"),
        Index("ix_pipeline_runs_stage", "stage"),
        Index("ix_pipeline_runs_started_at", "started_at"),
        {"schema": "fbm_raw"},
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Stage this run is executing (we can have one run per stage, or one run overall with checkpoints)
    stage = Column(String(40), nullable=False)
    # Suggested: DISCOVERY, INGESTION, OCR, EXTRACTION, HARMONIZATION, ML, FBM

    status = Column(String(20), nullable=False, default="RUNNING")
    # Suggested: RUNNING, SUCCESS, FAILED, PARTIAL, CANCELLED

    # Optional: scope information
    batch_name = Column(String(120), nullable=True)  # e.g., "pilot_2020_2projects"
    model_version = Column(String(80), nullable=True)  # extraction/model version tag if relevant

    # Checkpointing / progress
    checkpoint = Column(String(120), nullable=True)     # e.g., "doc_ocr_done", "fp05_done"
    progress_current = Column(Integer, nullable=True)
    progress_total = Column(Integer, nullable=True)

    # Metrics (helps with cost + rate limit management)
    docs_processed = Column(Integer, nullable=True)
    prompts_executed = Column(Integer, nullable=True)
    tokens_in = Column(Integer, nullable=True)
    tokens_out = Column(Integer, nullable=True)
    cost_usd_estimate = Column(Numeric(12, 4), nullable=True)

    # For recovery / debugging
    error_log = Column(JSONB, nullable=True)            # non-fatal warnings
    notes = Column(Text, nullable=True)

    started_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)