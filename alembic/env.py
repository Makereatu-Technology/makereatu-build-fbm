from logging.config import fileConfig
import os
import sys

from sqlalchemy import engine_from_config, pool
from alembic import context

# Alembic Config object
config = context.config

# Logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# --- IMPORTANT: ensure app imports work on Windows ---
# Adds project root (the folder that contains "app/") to sys.path
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app.core.config import settings  # noqa: E402
from app.models.base import Base      # noqa: E402

# ------------------------------------------------------
# Import ALL models so Alembic sees them for autogenerate
# ------------------------------------------------------

# RAW models (exist now)
from app.models.raw.project import RawProject  # noqa: F401,E402
from app.models.raw.document import RawDocument  # noqa: F401,E402
from app.models.raw.extraction_error import ExtractionError  # noqa: F401,E402
from app.models.raw.pipeline_run import PipelineRun  # noqa: F401,E402

# STAGING models (create later, then uncomment)
# from app.models.staging.extracted_feature import ExtractedFeature  # noqa: F401,E402

# ANALYTICAL models (create later, then uncomment)
# from app.models.analytical.project_features import ProjectFeatures  # noqa: F401,E402
# from app.models.analytical.model_artifact import ModelArtifact  # noqa: F401,E402
# from app.models.analytical.paprika_weights import PaprikaWeights  # noqa: F401,E402
# ------------------------------------------------------
# Use DB URL from .env/config
# Support both DATABASE_URL (recommended) and database_url (legacy)
# ------------------------------------------------------
db_url = getattr(settings, "DATABASE_URL", None) or getattr(settings, "database_url", None)
if not db_url:
    raise ValueError("Database URL not found. Set DATABASE_URL in .env or config.py")

config.set_main_option("sqlalchemy.url", db_url)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
        include_schemas=True,  # important for fbm_raw/fbm_staging/fbm_analytical
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            include_schemas=True,  # important for fbm_raw/fbm_staging/fbm_analytical
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()