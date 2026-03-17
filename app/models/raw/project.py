import uuid
from sqlalchemy import Column, String, Integer, DateTime, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.models.base import Base


class RawProject(Base):
    __tablename__ = "projects"
    __table_args__ = {"schema": "fbm_raw"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Minimal discovery metadata
    bank = Column(String(20), nullable=False)          # "WB" or "ADB"
    bank_project_id = Column(String(80), nullable=True)  # e.g., P123456 or ADB project no.
    name = Column(String(300), nullable=False)

    country = Column(String(120), nullable=True)
    region = Column(String(120), nullable=True)        # e.g., "Southeast Asia", "Pacific Islands"

    # "completed in 2020" / closed year indexing
    completion_year = Column(Integer, nullable=True)

    # Useful pointers
    project_url = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)