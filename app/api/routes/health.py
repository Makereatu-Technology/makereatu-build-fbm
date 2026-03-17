from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.db import get_db

router = APIRouter(tags=["health"])


@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/health/db")
def health_db(db: Session = Depends(get_db)):
    # Minimal DB round-trip
    db.execute(text("SELECT 1")).scalar()

    # Quick check of core tables
    projects = db.execute(text("SELECT COUNT(*) FROM fbm_raw.projects")).scalar()
    documents = db.execute(text("SELECT COUNT(*) FROM fbm_raw.documents")).scalar()

    return {
        "status": "ok",
        "db": "connected",
        "counts": {"projects": int(projects), "documents": int(documents)},
    }