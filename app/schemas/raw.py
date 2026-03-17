from pydantic import BaseModel, HttpUrl, Field
from typing import Optional, Literal
from uuid import UUID
from datetime import datetime

class ProjectCreate(BaseModel):
    bank: Literal["WB", "ADB"]
    bank_project_id: Optional[str] = None
    name: str
    country: Optional[str] = None
    region: Optional[str] = None
    completion_year: Optional[int] = Field(default=None, ge=1900, le=2100)
    project_url: Optional[HttpUrl] = None


class ProjectOut(BaseModel):
    id: UUID  # <-- change here
    bank: str
    bank_project_id: Optional[str]
    name: str
    country: Optional[str]
    region: Optional[str]
    completion_year: Optional[int]
    project_url: Optional[str]

    class Config:
        from_attributes = True

class DocumentIngestFromUrl(BaseModel):
    project_id: UUID
    source_system: Literal["WB", "ADB"]
    doc_type: str = Field(..., examples=["ICR", "PAD", "PCR", "RRP", "IEG", "PPER"])
    source_url: HttpUrl
    title: Optional[str] = None
    store_file_bytes: bool = True  # local dev True; later Azure False


class DocumentOut(BaseModel):
    id: UUID
    project_id: UUID
    source_system: str
    doc_type: str
    title: Optional[str]
    source_url: Optional[str]
    file_name: Optional[str]
    mime_type: Optional[str]
    file_size_bytes: Optional[int]
    sha256: Optional[str]
    extraction_status: str
    created_at: datetime

    class Config:
        from_attributes = True


class DocumentIngestResult(BaseModel):
    status: Literal["ingested", "exists"]
    document_id: UUID
    sha256: Optional[str] = None
    file_name: Optional[str] = None
    mime_type: Optional[str] = None
    size_bytes: Optional[int] = None 