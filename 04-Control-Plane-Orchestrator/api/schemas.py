from pydantic import BaseModel, Field, validator
from typing import Dict, List, Optional, Any
from datetime import datetime
from enum import Enum

class JobStatus(str, Enum):
    PENDING = "pending"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"

class ExecuteRequest(BaseModel):
    yaml_content: str = Field(..., description="Workflow YAML content")
    priority: int = Field(1, ge=1, le=10, description="Job priority (1-10)")
    client_host: Optional[str] = Field(None, description="Client IP address")
    user_agent: Optional[str] = Field(None, description="User agent string")
    
    @validator('yaml_content')
    def validate_yaml_length(cls, v):
        if len(v) > 100000:  # 100KB limit
            raise ValueError('YAML content too large')
        return v

class ExecuteResponse(BaseModel):
    job_id: str
    status: str
    message: str
    websocket_url: str
    estimated_wait_time: int = Field(..., description="Estimated wait time in seconds")

class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    artifacts: List[Dict[str, Any]] = Field(default_factory=list)
    timestamp: datetime
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class JobListResponse(BaseModel):
    jobs: List[Dict[str, Any]]
    total: int
    limit: int
    offset: int
    has_more: bool

class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    services: List[Dict[str, Any]]
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class MetricsResponse(BaseModel):
    period: str
    job_metrics: Dict[str, Any]
    system_metrics: List[Dict[str, Any]]
    scheduler_stats: Dict[str, Any]
    timestamp: datetime
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class ConfigUpdateRequest(BaseModel):
    config: Dict[str, Any]
    notes: Optional[str] = None

class UserCreateRequest(BaseModel):
    email: str = Field(..., regex=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    plan: str = Field("free", regex=r'^(free|basic|pro|enterprise|admin)$')

class ArtifactResponse(BaseModel):
    id: str
    job_id: str
    artifact_type: str
    storage_key: str
    storage_url: str
    content_type: Optional[str]
    file_size: Optional[int]
    created_at: datetime
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class RateLimitConfig(BaseModel):
    minute: int = Field(10, ge=1, le=1000)
    hour: int = Field(100, ge=10, le=10000)
    day: int = Field(1000, ge=100, le=100000)

class BrowserPoolConfig(BaseModel):
    max_browsers: int = Field(10, ge=1, le=50)
    user_agents: List[str] = Field(default_factory=list)
    headless: bool = True
    stealth_mode: bool = True
    
    @validator('user_agents')
    def validate_user_agents(cls, v):
        if not v:
            raise ValueError('At least one user agent required')
        return v

class WorkflowDefaults(BaseModel):
    timeout: int = Field(300, ge=30, le=3600, description="Default timeout in seconds")
    retry_count: int = Field(3, ge=0, le=10)
    priority: int = Field(1, ge=1, le=10)
    capture_artifacts: bool = True
    rate_limit_delay: int = Field(2, ge=0, le=10, description="Delay between requests in seconds")

class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
