from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.schemas.raw import ProjectCreate, ProjectOut
from app.models.raw.project import RawProject

router = APIRouter(prefix="/discovery", tags=["discovery"])


@router.post("/manual_project", response_model=ProjectOut)
def create_project(payload: ProjectCreate, db: Session = Depends(get_db)):
    proj = RawProject(
        bank=payload.bank,
        bank_project_id=payload.bank_project_id,
        name=payload.name,
        country=payload.country,
        region=payload.region,
        completion_year=payload.completion_year,
        project_url=str(payload.project_url) if payload.project_url else None,
    )
    db.add(proj)
    db.commit()
    db.refresh(proj)
    return proj