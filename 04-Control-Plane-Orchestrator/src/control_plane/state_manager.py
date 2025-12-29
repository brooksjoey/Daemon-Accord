"""
State Manager

Manages job state transitions and persistence.
Coordinates between Redis (for fast lookups) and PostgreSQL (for persistence).
"""
from typing import Optional, Dict, Any, TYPE_CHECKING
from datetime import datetime
import redis.asyncio as redis
from sqlmodel import select
import structlog
import json

from src.exceptions import DatabaseError, RedisError
from .models import Job, JobStatus

if TYPE_CHECKING:
    from ..database import Database

logger = structlog.get_logger(__name__)


class StateManager:
    """
    Manages job state transitions and provides fast state lookups.
    
    Uses Redis for fast state caching and PostgreSQL as source of truth.
    Ensures state consistency between cache and database.
    """
    
    def __init__(self, redis_client: redis.Redis, db: "Database") -> None:
        """
        Initialize state manager.
        
        Args:
            redis_client: Redis async client for caching
            db: Database instance (not just engine)
        """
        self.redis = redis_client
        self.db = db  # Database instance to get async sessions
        self.cache_prefix = "job:state:"
        self.cache_ttl = 3600  # 1 hour cache TTL
    
    async def get_job_state(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get job state (cached from Redis, fallback to DB).
        
        Args:
            job_id: The job ID
            
        Returns:
            Job state dict or None if not found
        """
        # Try Redis cache first
        try:
            cache_key = f"{self.cache_prefix}{job_id}"
            cached = await self.redis.get(cache_key)
            
            if cached:
                # Decode if bytes
                if isinstance(cached, bytes):
                    cached = cached.decode('utf-8')
                
                import json
                state = json.loads(cached)
                logger.debug("job_state_from_cache", job_id=job_id)
                return state
        except Exception as e:
            logger.warning(
                "error_reading_cache",
                job_id=job_id,
                error=str(e),
                exc_info=True
            )
            # Continue to database fallback
        
        # Fallback to database
        try:
            async with self.db.session() as session:
                job = await session.get(Job, job_id)
                if not job:
                    return None
                
                state = {
                    "id": job.id,
                    "status": job.status,
                    "domain": job.domain,
                    "job_type": job.job_type,
                    "strategy": job.strategy,
                    "priority": job.priority,
                    "attempts": job.attempts,
                    "max_attempts": job.max_attempts,
                    "created_at": job.created_at.isoformat() if job.created_at else None,
                    "started_at": job.started_at.isoformat() if job.started_at else None,
                    "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                    "error": job.error,
                }
                
                # Cache the result
                await self._cache_job_state(job_id, state)
                
                return state
                
        except Exception as e:
            logger.error(
                "error_getting_job_state",
                job_id=job_id,
                error=str(e),
                exc_info=True
            )
            raise DatabaseError(
                f"Failed to get job state for {job_id}",
                operation="get_job_state",
                details={"job_id": job_id}
            ) from e
    
    async def update_job_status(
        self,
        job_id: str,
        status: JobStatus,
        error: Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Update job status in both database and cache.
        
        Args:
            job_id: The job ID
            status: New status
            error: Optional error message
            **kwargs: Additional fields to update (e.g., started_at, completed_at)
            
        Returns:
            True if updated successfully, False otherwise
        """
        try:
            async with self.db.session() as session:
                job = await session.get(Job, job_id)
                if not job:
                    logger.error("job_not_found_status_update", job_id=job_id)
                    return False
                
                # Update status
                job.status = status.value if isinstance(status, JobStatus) else status
                if hasattr(job, "updated_at"):
                    job.updated_at = datetime.utcnow()
                
                # Update optional fields
                if error is not None:
                    job.error = error
                
                if "started_at" in kwargs:
                    job.started_at = kwargs["started_at"]
                elif status == JobStatus.RUNNING and not job.started_at:
                    job.started_at = datetime.utcnow()
                
                if "completed_at" in kwargs:
                    job.completed_at = kwargs["completed_at"]
                elif status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                    if not job.completed_at:
                        job.completed_at = datetime.utcnow()
                
                if "attempts" in kwargs:
                    job.attempts = kwargs["attempts"]

                # Optional result payload (stored as JSON-encoded string in current schema)
                if "result" in kwargs and kwargs["result"] is not None:
                    try:
                        job.result = json.dumps(kwargs["result"])
                    except Exception:
                        job.result = str(kwargs["result"])
                
                session.add(job)
                await session.commit()
                await session.refresh(job)
                
                # Invalidate cache (will be refreshed on next read)
                await self._invalidate_cache(job_id)
                
                logger.info("job_status_updated", job_id=job_id, status=status.value if isinstance(status, JobStatus) else status)
                return True
                
        except Exception as e:
            logger.error(
                "error_updating_job_status",
                job_id=job_id,
                error=str(e),
                exc_info=True
            )
            raise DatabaseError(
                f"Failed to update job status for {job_id}",
                operation="update_job_status",
                details={"job_id": job_id, "status": str(status)}
            ) from e
    
    async def increment_attempts(self, job_id: str) -> bool:
        """
        Increment job attempt counter.
        
        Args:
            job_id: The job ID
            
        Returns:
            True if updated successfully, False otherwise
        """
        try:
            async with self.db.session() as session:
                job = await session.get(Job, job_id)
                if not job:
                    return False
                
                job.attempts += 1
                if hasattr(job, "updated_at"):
                    job.updated_at = datetime.utcnow()
                session.add(job)
                await session.commit()
                
                # Invalidate cache
                await self._invalidate_cache(job_id)
                
                return True
                
        except Exception as e:
            logger.error(
                "error_incrementing_attempts",
                job_id=job_id,
                error=str(e),
                exc_info=True
            )
            raise DatabaseError(
                f"Failed to increment attempts for {job_id}",
                operation="increment_attempts",
                details={"job_id": job_id}
            ) from e
    
    async def _cache_job_state(self, job_id: str, state: Dict[str, Any]) -> None:
        """Cache job state in Redis."""
        try:
            import json
            cache_key = f"{self.cache_prefix}{job_id}"
            await self.redis.setex(
                cache_key,
                self.cache_ttl,
                json.dumps(state)
            )
        except Exception as e:
            logger.warning(
                "error_caching_job_state",
                job_id=job_id,
                error=str(e),
                exc_info=True
            )
            # Cache errors are non-fatal, continue
    
    async def _invalidate_cache(self, job_id: str) -> None:
        """Invalidate cached job state."""
        try:
            cache_key = f"{self.cache_prefix}{job_id}"
            await self.redis.delete(cache_key)
        except Exception as e:
            logger.warning(
                "error_invalidating_cache",
                job_id=job_id,
                error=str(e),
                exc_info=True
            )
            # Cache invalidation errors are non-fatal, continue
    
    async def get_jobs_by_status(self, status: JobStatus, limit: int = 100) -> list[Job]:
        """
        Get jobs by status from database.
        
        Args:
            status: The status to filter by
            limit: Maximum number of jobs to return
            
        Returns:
            List of Job objects
        """
        try:
            async with self.db.session() as session:
                statement = select(Job).where(
                    Job.status == status.value
                ).limit(limit)
                result = await session.execute(statement)
                return list(result.scalars().all())
        except Exception as e:
            logger.error(
                "error_getting_jobs_by_status",
                status=status.value if isinstance(status, JobStatus) else str(status),
                error=str(e),
                exc_info=True
            )
            raise DatabaseError(
                f"Failed to get jobs by status {status}",
                operation="get_jobs_by_status",
                details={"status": str(status)}
            ) from e



