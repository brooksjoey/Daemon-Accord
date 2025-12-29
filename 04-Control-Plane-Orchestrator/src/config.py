"""
Control Plane Configuration

Settings and environment variable management.
"""
from pydantic import Field
from pydantic_settings import BaseSettings
from typing import Optional


class ControlPlaneSettings(BaseSettings):
    """Control Plane service settings."""
    
    # Database
    postgres_dsn: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/daemon_accord",
        validation_alias="DATABASE_URL",
        description="PostgreSQL connection string"
    )
    
    # Redis
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        validation_alias="REDIS_URL",
        description="Redis connection URL"
    )
    
    # Service config
    max_concurrent_jobs: int = Field(
        default=100,
        validation_alias="MAX_CONCURRENT_JOBS",
        description="Maximum concurrent job executions"
    )
    
    worker_count: int = Field(
        default=5,
        validation_alias="WORKER_COUNT",
        description="Number of worker processes"
    )

    # Worker ↔ Control Plane contract (internal)
    worker_lease_ttl_seconds: int = Field(
        default=60,
        validation_alias="WORKER_LEASE_TTL_SECONDS",
        description="DB lease TTL for claimed jobs (seconds)"
    )
    worker_heartbeat_interval_seconds: int = Field(
        default=20,
        validation_alias="WORKER_HEARTBEAT_INTERVAL_SECONDS",
        description="Recommended worker heartbeat interval (seconds)"
    )
    lease_reaper_interval_seconds: int = Field(
        default=15,
        validation_alias="LEASE_REAPER_INTERVAL_SECONDS",
        description="How often the control plane reaps expired leases (seconds)"
    )
    pel_reconcile_interval_seconds: int = Field(
        default=30,
        validation_alias="PEL_RECONCILE_INTERVAL_SECONDS",
        description="How often to reconcile Redis Streams PEL (seconds)"
    )
    pel_idle_threshold_seconds: int = Field(
        default=60,
        validation_alias="PEL_IDLE_THRESHOLD_SECONDS",
        description="Minimum idle time before a pending message can be reclaimed (seconds)"
    )
    
    # Memory Service integration
    memory_service_url: Optional[str] = Field(
        default=None,
        validation_alias="MEMORY_SERVICE_URL",
        description="Memory Service base URL (e.g., http://memory-service:8100)"
    )
    
    # API
    api_host: str = Field(
        default="0.0.0.0",
        validation_alias="API_HOST",
        description="API server host"
    )
    
    api_port: int = Field(
        default=8080,
        validation_alias="API_PORT",
        description="API server port"
    )
    
    # Auth (foundation - can be extended later)
    enable_auth: bool = Field(
        default=False,
        validation_alias="ENABLE_AUTH",
        description="Enable authentication (currently disabled for dev)"
    )
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        populate_by_name = True

