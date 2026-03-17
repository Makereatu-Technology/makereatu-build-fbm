"""init fbm schemas + raw tables

Revision ID: d2f552561dea
Revises: 
Create Date: 2026-03-05 00:13:54.322185

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'd2f552561dea'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

from alembic import op

def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS fbm_raw;")
    op.execute("CREATE SCHEMA IF NOT EXISTS fbm_staging;")
    op.execute("CREATE SCHEMA IF NOT EXISTS fbm_analytical;")
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    op.create_table('pipeline_runs',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('stage', sa.String(length=40), nullable=False),
    sa.Column('status', sa.String(length=20), nullable=False),
    sa.Column('batch_name', sa.String(length=120), nullable=True),
    sa.Column('model_version', sa.String(length=80), nullable=True),
    sa.Column('checkpoint', sa.String(length=120), nullable=True),
    sa.Column('progress_current', sa.Integer(), nullable=True),
    sa.Column('progress_total', sa.Integer(), nullable=True),
    sa.Column('docs_processed', sa.Integer(), nullable=True),
    sa.Column('prompts_executed', sa.Integer(), nullable=True),
    sa.Column('tokens_in', sa.Integer(), nullable=True),
    sa.Column('tokens_out', sa.Integer(), nullable=True),
    sa.Column('cost_usd_estimate', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('error_log', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('notes', sa.Text(), nullable=True),
    sa.Column('started_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_pipeline_runs')),
    schema='fbm_raw'
    )
    op.create_index('ix_pipeline_runs_stage', 'pipeline_runs', ['stage'], unique=False, schema='fbm_raw')
    op.create_index('ix_pipeline_runs_started_at', 'pipeline_runs', ['started_at'], unique=False, schema='fbm_raw')
    op.create_index('ix_pipeline_runs_status', 'pipeline_runs', ['status'], unique=False, schema='fbm_raw')
    op.create_table('projects',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('bank', sa.String(length=20), nullable=False),
    sa.Column('bank_project_id', sa.String(length=80), nullable=True),
    sa.Column('name', sa.String(length=300), nullable=False),
    sa.Column('country', sa.String(length=120), nullable=True),
    sa.Column('region', sa.String(length=120), nullable=True),
    sa.Column('completion_year', sa.Integer(), nullable=True),
    sa.Column('project_url', sa.Text(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_projects')),
    schema='fbm_raw'
    )
    op.create_table('documents',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('project_id', sa.UUID(), nullable=False),
    sa.Column('source_system', sa.String(length=20), nullable=False),
    sa.Column('doc_type', sa.String(length=30), nullable=False),
    sa.Column('title', sa.String(length=500), nullable=True),
    sa.Column('source_url', sa.Text(), nullable=True),
    sa.Column('blob_url', sa.Text(), nullable=True),
    sa.Column('file_name', sa.String(length=255), nullable=True),
    sa.Column('mime_type', sa.String(length=120), nullable=True),
    sa.Column('file_size_bytes', sa.Integer(), nullable=True),
    sa.Column('sha256', sa.String(length=64), nullable=True),
    sa.Column('file_bytes', sa.LargeBinary(), nullable=True),
    sa.Column('full_text', sa.Text(), nullable=True),
    sa.Column('docintel_result', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('page_count', sa.Integer(), nullable=True),
    sa.Column('ocr_confidence', sa.Numeric(precision=4, scale=3), nullable=True),
    sa.Column('extraction_status', sa.String(length=30), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['project_id'], ['fbm_raw.projects.id'], name=op.f('fk_documents_project_id_projects'), ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_documents')),
    sa.UniqueConstraint('project_id', 'sha256', name='uq_documents_project_sha256'),
    schema='fbm_raw'
    )
    op.create_index('ix_documents_doc_type', 'documents', ['doc_type'], unique=False, schema='fbm_raw')
    op.create_index('ix_documents_project_id', 'documents', ['project_id'], unique=False, schema='fbm_raw')
    op.create_index('ix_documents_source_system', 'documents', ['source_system'], unique=False, schema='fbm_raw')
    op.create_table('extraction_errors',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('run_id', sa.UUID(), nullable=False),
    sa.Column('project_id', sa.UUID(), nullable=True),
    sa.Column('document_id', sa.UUID(), nullable=True),
    sa.Column('stage', sa.String(length=40), nullable=False),
    sa.Column('prompt_id', sa.String(length=80), nullable=True),
    sa.Column('doc_type', sa.String(length=30), nullable=True),
    sa.Column('focal_point', sa.String(length=20), nullable=True),
    sa.Column('error_type', sa.String(length=80), nullable=False),
    sa.Column('error_message', sa.Text(), nullable=False),
    sa.Column('stack_trace', sa.Text(), nullable=True),
    sa.Column('request_payload', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('response_payload', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['document_id'], ['fbm_raw.documents.id'], name=op.f('fk_extraction_errors_document_id_documents'), ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['project_id'], ['fbm_raw.projects.id'], name=op.f('fk_extraction_errors_project_id_projects'), ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_extraction_errors')),
    schema='fbm_raw'
    )
    op.create_index('ix_extraction_errors_document_id', 'extraction_errors', ['document_id'], unique=False, schema='fbm_raw')
    op.create_index('ix_extraction_errors_project_id', 'extraction_errors', ['project_id'], unique=False, schema='fbm_raw')
    op.create_index('ix_extraction_errors_prompt_id', 'extraction_errors', ['prompt_id'], unique=False, schema='fbm_raw')
    op.create_index('ix_extraction_errors_run_id', 'extraction_errors', ['run_id'], unique=False, schema='fbm_raw')
    op.create_index('ix_extraction_errors_stage', 'extraction_errors', ['stage'], unique=False, schema='fbm_raw')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('ix_extraction_errors_stage', table_name='extraction_errors', schema='fbm_raw')
    op.drop_index('ix_extraction_errors_run_id', table_name='extraction_errors', schema='fbm_raw')
    op.drop_index('ix_extraction_errors_prompt_id', table_name='extraction_errors', schema='fbm_raw')
    op.drop_index('ix_extraction_errors_project_id', table_name='extraction_errors', schema='fbm_raw')
    op.drop_index('ix_extraction_errors_document_id', table_name='extraction_errors', schema='fbm_raw')
    op.drop_table('extraction_errors', schema='fbm_raw')
    op.drop_index('ix_documents_source_system', table_name='documents', schema='fbm_raw')
    op.drop_index('ix_documents_project_id', table_name='documents', schema='fbm_raw')
    op.drop_index('ix_documents_doc_type', table_name='documents', schema='fbm_raw')
    op.drop_table('documents', schema='fbm_raw')
    op.drop_table('projects', schema='fbm_raw')
    op.drop_index('ix_pipeline_runs_status', table_name='pipeline_runs', schema='fbm_raw')
    op.drop_index('ix_pipeline_runs_started_at', table_name='pipeline_runs', schema='fbm_raw')
    op.drop_index('ix_pipeline_runs_stage', table_name='pipeline_runs', schema='fbm_raw')
    op.drop_table('pipeline_runs', schema='fbm_raw')
    # ### end Alembic commands ###
