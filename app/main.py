from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.health import router as health_router
from app.api.routes.discovery import router as discovery_router
from app.api.routes.ingestion import router as ingestion_router
from app.api.routes.ingestion_link import router as ingestion_link_router
from app.api.routes.discovery_search import router as discovery_search_router
from app.api.ingest_sources import router as ingest_router

app = FastAPI(title="Makereatu FBM API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(discovery_router)
app.include_router(ingestion_router)
app.include_router(ingestion_link_router)
app.include_router(discovery_search_router)
app.include_router(ingest_router)