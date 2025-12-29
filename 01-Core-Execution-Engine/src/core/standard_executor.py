import asyncio
import time
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import hashlib
from collections import defaultdict
import redis.asyncio as redis
from sqlmodel.ext.asyncio.session import AsyncSession
from .executor import BaseExecutor, JobResult, JobStatus
from .browser_pool import BrowserPool
import logging
from dataclasses import dataclass
import json

logger = logging.getLogger(__name__)

@dataclass
class RateLimitState:
    requests: int
    window_start: datetime

class StandardExecutor(BaseExecutor):
    def __init__(self, 
                 redis_client: redis.Redis,
                 db_session: AsyncSession,
                 browser_pool: BrowserPool,
                 max_failures: int = 3,
                 cooldown_hours: int = 6,
                 req_per_min: int = 5,
                 req_per_hour: int = 100):
        self.redis = redis_client
        self.db = db_session
        self.pool = browser_pool
        self.max_failures = max_failures
        self.cooldown_hours = cooldown_hours
        self.req_per_min = req_per_min
        self.req_per_hour = req_per_hour
        
        self.circuit_breaker = {}
        self.rate_limits_domain = defaultdict(lambda: RateLimitState(0, datetime.utcnow()))
        self.rate_limits_ip = defaultdict(lambda: RateLimitState(0, datetime.utcnow()))
        self.sessions = {}
        
        self.metrics = {
            'job_execution_duration_seconds': [],
            'job_success_total': 0,
            'circuit_breaker_state': 0
        }

    async def execute(self, job_data: Dict[str, Any]) -> JobResult:
        job_id = job_data.get('id', 'unknown')
        job_type = job_data.get('type', 'unknown')
        target = job_data.get('target', {})
        domain = target.get('domain', '')
        ip = target.get('ip', '')
        
        start_time = time.time()
        
        if await self._is_circuit_broken(domain):
            logger.warning(f"Circuit broken for {domain}")
            return JobResult(
                job_id=job_id,
                status=JobStatus.CIRCUIT_BROKEN,
                data={},
                artifacts={},
                error=f"Circuit broken for {domain}"
            )
        
        if not await self._check_rate_limit(domain, ip):
            logger.warning(f"Rate limit exceeded for {domain} from {ip}")
            return JobResult(
                job_id=job_id,
                status=JobStatus.RATE_LIMITED,
                data={},
                artifacts={},
                error="Rate limit exceeded"
            )
        
        try:
            result = await self._execute_job(job_data)
            await self._record_success(domain)
            execution_time = time.time() - start_time
            self.metrics['job_execution_duration_seconds'].append(execution_time)
            self.metrics['job_success_total'] += 1
            
            return result
            
        except Exception as e:
            logger.error(f"Job execution failed: {e}")
            await self._record_failure(domain, str(e))
            execution_time = time.time() - start_time
            self.metrics['job_execution_duration_seconds'].append(execution_time)
            
            return JobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                data={},
                artifacts={},
                error=str(e),
                execution_time=execution_time
            )

    async def _execute_job(self, job_data: Dict[str, Any]) -> JobResult:
        from core.action_adapter import execute_action_as_job_result

        return await execute_action_as_job_result(job_data, browser_pool=self.pool)

    async def _check_rate_limit(self, domain: str, ip: str) -> bool:
        now = datetime.utcnow()
        
        domain_state = self.rate_limits_domain[domain]
        ip_state = self.rate_limits_ip[ip]
        
        if now - domain_state.window_start > timedelta(minutes=1):
            domain_state.requests = 0
            domain_state.window_start = now
        
        if now - ip_state.window_start > timedelta(hours=1):
            ip_state.requests = 0
            ip_state.window_start = now
        
        if domain_state.requests >= self.req_per_min:
            return False
        
        if ip_state.requests >= self.req_per_hour:
            return False
        
        domain_state.requests += 1
        ip_state.requests += 1
        
        return True

    async def _is_circuit_broken(self, domain: str) -> bool:
        if domain not in self.circuit_breaker:
            return False
        
        state = self.circuit_breaker[domain]
        if state['failures'] < self.max_failures:
            return False
        
        if datetime.utcnow() - state['last_failure'] < timedelta(hours=self.cooldown_hours):
            self.metrics['circuit_breaker_state'] = 1
            return True
        
        return False

    async def _record_failure(self, domain: str, error: str):
        if domain not in self.circuit_breaker:
            self.circuit_breaker[domain] = {
                'failures': 0,
                'last_failure': datetime.utcnow()
            }
        
        self.circuit_breaker[domain]['failures'] += 1
        self.circuit_breaker[domain]['last_failure'] = datetime.utcnow()
        
        await self.redis.hset(f"circuit:{domain}", mapping={
            'failures': self.circuit_breaker[domain]['failures'],
            'last_failure': self.circuit_breaker[domain]['last_failure'].isoformat()
        })

    async def _record_success(self, domain: str):
        if domain in self.circuit_breaker:
            self.circuit_breaker[domain]['failures'] = 0
            self.metrics['circuit_breaker_state'] = 0
            
            await self.redis.hset(f"circuit:{domain}", mapping={
                'failures': 0,
                'last_failure': datetime.utcnow().isoformat()
            })

    async def health_check(self) -> bool:
        try:
            await self.redis.ping()
            return True
        except:
            return False

    async def cleanup(self):
        await self.pool.cleanup()
        await self.redis.close()
