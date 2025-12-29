"""Initial Control Plane schema + worker contract tables.

Revision ID: 0001_control_plane_initial
Revises: 
Create Date: 2025-12-29
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "0001_control_plane_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- Core tables ---
    job_status = sa.Enum(
        "pending",
        "queued",
        "running",
        "completed",
        "failed",
        "cancelled",
        name="jobstatus",
    )

    op.create_table(
        "jobs",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("domain", sa.String(), nullable=False),
        sa.Column("url", sa.String(), nullable=False),
        sa.Column("job_type", sa.String(), nullable=False),
        sa.Column("strategy", sa.String(), nullable=False, server_default="vanilla"),
        sa.Column("payload", sa.Text(), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="2"),
        sa.Column("status", job_status, nullable=False, server_default="pending"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("timeout_seconds", sa.Integer(), nullable=False, server_default="300"),
        sa.Column("result", sa.Text(), nullable=True),
        sa.Column("artifacts", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
    )
    op.create_index("ix_jobs_domain", "jobs", ["domain"])
    op.create_index("ix_jobs_job_type", "jobs", ["job_type"])
    op.create_index("ix_jobs_priority", "jobs", ["priority"])
    op.create_index("ix_jobs_status", "jobs", ["status"])
    op.create_index("ix_jobs_created_at", "jobs", ["created_at"])

    op.create_table(
        "job_executions",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("job_id", sa.String(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("execution_time_ms", sa.Integer(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
    )
    op.create_index("ix_job_executions_job_id", "job_executions", ["job_id"])

    # --- Compliance tables ---
    op.create_table(
        "domain_policies",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("domain", sa.String(), nullable=False, unique=True),
        sa.Column("allowed", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("denied", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("rate_limit_per_minute", sa.Integer(), nullable=True),
        sa.Column("rate_limit_per_hour", sa.Integer(), nullable=True),
        sa.Column("max_concurrent_jobs", sa.Integer(), nullable=True),
        sa.Column("allowed_strategies", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
    )
    op.create_index("ix_domain_policies_domain", "domain_policies", ["domain"])

    authorization_mode = sa.Enum(
        "public",
        "customer-authorized",
        "internal",
        name="authorizationmode",
    )
    policy_action = sa.Enum(
        "allow",
        "deny",
        "rate_limit",
        "concurrency_limit",
        "strategy_restricted",
        name="policyaction",
    )

    op.create_table(
        "audit_logs",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("job_id", sa.String(), nullable=False),
        sa.Column("domain", sa.String(), nullable=False),
        sa.Column("policy_id", sa.String(), nullable=True),
        sa.Column("authorization_mode", authorization_mode, nullable=False),
        sa.Column("strategy", sa.String(), nullable=False),
        sa.Column("action", policy_action, nullable=False),
        sa.Column("allowed", sa.Boolean(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("rate_limit_applied", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("concurrency_limit_applied", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("strategy_restricted", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
        sa.Column("user_id", sa.String(), nullable=True),
        sa.Column("ip_address", sa.String(), nullable=True),
        sa.Column("context", sa.Text(), nullable=True),
    )
    op.create_index("ix_audit_logs_job_id", "audit_logs", ["job_id"])
    op.create_index("ix_audit_logs_domain", "audit_logs", ["domain"])
    op.create_index("ix_audit_logs_timestamp", "audit_logs", ["timestamp"])

    # --- Worker contract tables ---
    op.create_table(
        "job_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", sa.String(), nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("worker_id", sa.String(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("outcome", sa.String(), nullable=True),
        sa.Column("error_code", sa.String(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("error_stack", sa.Text(), nullable=True),
        sa.Column("meta", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
    )
    op.create_index("ix_job_attempts_job_id", "job_attempts", ["job_id"])
    op.create_index("ix_job_attempts_attempt_no", "job_attempts", ["attempt_no"])
    op.create_index("ix_job_attempts_worker_id", "job_attempts", ["worker_id"])
    op.create_index("ix_job_attempts_started_at", "job_attempts", ["started_at"])
    op.create_index("ix_job_attempts_finished_at", "job_attempts", ["finished_at"])
    op.create_index("ix_job_attempts_outcome", "job_attempts", ["outcome"])
    op.create_index("ix_job_attempts_error_code", "job_attempts", ["error_code"])

    op.create_table(
        "job_leases",
        sa.Column("job_id", sa.String(), primary_key=True),
        sa.Column("lease_token", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("worker_id", sa.String(), nullable=False),
        sa.Column("acquired_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attempt_id", postgresql.UUID(as_uuid=True), nullable=False),
    )
    op.create_index("ix_job_leases_lease_token", "job_leases", ["lease_token"])
    op.create_index("ix_job_leases_worker_id", "job_leases", ["worker_id"])
    op.create_index("ix_job_leases_acquired_at", "job_leases", ["acquired_at"])
    op.create_index("ix_job_leases_expires_at", "job_leases", ["expires_at"])
    op.create_index("ix_job_leases_attempt_id", "job_leases", ["attempt_id"])


def downgrade() -> None:
    op.drop_table("job_leases")
    op.drop_table("job_attempts")
    op.drop_table("audit_logs")
    op.drop_table("domain_policies")
    op.drop_table("job_executions")
    op.drop_table("jobs")

    op.execute("DROP TYPE IF EXISTS policyaction")
    op.execute("DROP TYPE IF EXISTS authorizationmode")
    op.execute("DROP TYPE IF EXISTS jobstatus")

