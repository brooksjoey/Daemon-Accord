"""
Policy Enforcer

Enforces compliance policies at job submission and execution time.
"""
import json
from typing import Dict, Any, Optional, Tuple, Callable
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select
import redis.asyncio as redis
import structlog

from ..exceptions import (
    DatabaseError,
    RedisError,
    RateLimitExceededError,
    ConcurrencyLimitExceededError,
    StrategyNotAllowedError,
    DomainNotAllowedError,
)
from .models import (
    DomainPolicy,
    AuditLog,
    AuthorizationMode,
    PolicyAction
)

logger = structlog.get_logger(__name__)


class PolicyEnforcer:
    """
    Enforces compliance policies for job execution.
    
    Features:
    - Domain allowlist/denylist
    - Rate limiting per domain
    - Concurrency limiting per domain
    - Strategy restrictions based on authorization mode
    - Audit logging
    """
    
    def __init__(
        self,
        db_session_factory: Callable[[], AsyncSession],
        redis_client: redis.Redis,
    ) -> None:
        """
        Initialize Policy Enforcer.
        
        Args:
            db_session_factory: Callable that returns AsyncSession
            redis_client: Redis client for rate limiting and concurrency tracking
        """
        self.db_session_factory = db_session_factory
        self.redis_client = redis_client
        self.rate_limit_prefix = "rate_limit:domain:"
        self.concurrency_prefix = "concurrency:domain:"
    
    async def check_policy(
        self,
        job_id: str,
        domain: str,
        url: str,
        strategy: str,
        authorization_mode: AuthorizationMode = AuthorizationMode.PUBLIC,
        user_id: Optional[str] = None,
        ip_address: Optional[str] = None,
    ) -> Tuple[bool, PolicyAction, Optional[str], Dict[str, Any]]:
        """
        Check if a job is allowed by policy.
        
        Args:
            job_id: Job ID
            domain: Target domain
            url: Target URL
            strategy: Execution strategy
            authorization_mode: Authorization mode
            user_id: User/API key ID
            ip_address: Request IP address
            
        Returns:
            Tuple of (allowed, action, reason, context)
        """
        context = {
            "domain": domain,
            "strategy": strategy,
            "authorization_mode": authorization_mode.value,
            "timestamp": datetime.utcnow().isoformat(),
        }
        rate_limit_applied = False
        
        # Get domain policy
        policy = await self._get_domain_policy(domain)
        
        # Check allowlist/denylist
        if policy:
            if policy.denied:
                reason = f"Domain {domain} is on denylist"
                await self._log_audit(
                    job_id=job_id,
                    domain=domain,
                    policy_id=policy.id,
                    authorization_mode=authorization_mode,
                    strategy=strategy,
                    action=PolicyAction.DENY,
                    allowed=False,
                    reason=reason,
                    user_id=user_id,
                    ip_address=ip_address,
                    context=context,
                )
                return False, PolicyAction.DENY, reason, context
            
            if not policy.allowed:
                reason = f"Domain {domain} is not on allowlist"
                await self._log_audit(
                    job_id=job_id,
                    domain=domain,
                    policy_id=policy.id,
                    authorization_mode=authorization_mode,
                    strategy=strategy,
                    action=PolicyAction.DENY,
                    allowed=False,
                    reason=reason,
                    user_id=user_id,
                    ip_address=ip_address,
                    context=context,
                )
                return False, PolicyAction.DENY, reason, context
            
            # Check strategy restrictions
            if not policy.is_strategy_allowed(strategy):
                reason = f"Strategy '{strategy}' not allowed for domain {domain}. Allowed: {policy.get_allowed_strategies_list()}"
                context["strategy_restricted"] = True
                context["allowed_strategies"] = policy.get_allowed_strategies_list()
                
                await self._log_audit(
                    job_id=job_id,
                    domain=domain,
                    policy_id=policy.id,
                    authorization_mode=authorization_mode,
                    strategy=strategy,
                    action=PolicyAction.STRATEGY_RESTRICTED,
                    allowed=False,
                    reason=reason,
                    strategy_restricted=True,
                    user_id=user_id,
                    ip_address=ip_address,
                    context=context,
                )
                return False, PolicyAction.STRATEGY_RESTRICTED, reason, context
            
            # Check rate limits
            if policy.rate_limit_per_minute:
                if await self._check_rate_limit(domain, policy.rate_limit_per_minute, window_seconds=60):
                    rate_limit_applied = True
                    context["rate_limit_per_minute"] = policy.rate_limit_per_minute
                    context["rate_limit_window"] = "1 minute"
            
            if policy.rate_limit_per_hour and not rate_limit_applied:
                if await self._check_rate_limit(domain, policy.rate_limit_per_hour, window_seconds=3600):
                    rate_limit_applied = True
                    context["rate_limit_per_hour"] = policy.rate_limit_per_hour
                    context["rate_limit_window"] = "1 hour"
            
            if rate_limit_applied:
                reason = f"Rate limit exceeded for domain {domain}"
                await self._log_audit(
                    job_id=job_id,
                    domain=domain,
                    policy_id=policy.id,
                    authorization_mode=authorization_mode,
                    strategy=strategy,
                    action=PolicyAction.RATE_LIMIT,
                    allowed=False,
                    reason=reason,
                    rate_limit_applied=True,
                    user_id=user_id,
                    ip_address=ip_address,
                    context=context,
                )
                return False, PolicyAction.RATE_LIMIT, reason, context
            
            # Check concurrency limits
            if policy.max_concurrent_jobs:
                current_concurrency = await self._get_current_concurrency(domain)
                if current_concurrency >= policy.max_concurrent_jobs:
                    reason = f"Concurrency limit ({policy.max_concurrent_jobs}) exceeded for domain {domain}"
                    context["concurrency_limit"] = policy.max_concurrent_jobs
                    context["current_concurrency"] = current_concurrency
                    
                    await self._log_audit(
                        job_id=job_id,
                        domain=domain,
                        policy_id=policy.id,
                        authorization_mode=authorization_mode,
                        strategy=strategy,
                        action=PolicyAction.CONCURRENCY_LIMIT,
                        allowed=False,
                        reason=reason,
                        concurrency_limit_applied=True,
                        user_id=user_id,
                        ip_address=ip_address,
                        context=context,
                    )
                    return False, PolicyAction.CONCURRENCY_LIMIT, reason, context
                
                context["concurrency_limit"] = policy.max_concurrent_jobs
                context["current_concurrency"] = current_concurrency
        
        # Check authorization mode restrictions
        # Enterprise customers (INTERNAL) can use all strategies
        # Customer-authorized can use stealth/assault if authorized
        # Public can only use vanilla
        if authorization_mode == AuthorizationMode.PUBLIC:
            if strategy.lower() not in ["vanilla"]:
                reason = f"Strategy '{strategy}' requires customer authorization. Public mode only allows 'vanilla'."
                context["strategy_restricted"] = True
                
                await self._log_audit(
                    job_id=job_id,
                    domain=domain,
                    authorization_mode=authorization_mode,
                    strategy=strategy,
                    action=PolicyAction.STRATEGY_RESTRICTED,
                    allowed=False,
                    reason=reason,
                    strategy_restricted=True,
                    user_id=user_id,
                    ip_address=ip_address,
                    context=context,
                )
                return False, PolicyAction.STRATEGY_RESTRICTED, reason, context
        
        # All checks passed
        await self._log_audit(
            job_id=job_id,
            domain=domain,
            policy_id=policy.id if policy else None,
            authorization_mode=authorization_mode,
            strategy=strategy,
            action=PolicyAction.ALLOW,
            allowed=True,
            reason="Policy check passed",
            rate_limit_applied=rate_limit_applied,
            user_id=user_id,
            ip_address=ip_address,
            context=context,
        )
        
        return True, PolicyAction.ALLOW, "Policy check passed", context
    
    async def _get_domain_policy(self, domain: str) -> Optional[DomainPolicy]:
        """
        Get domain policy from database.
        
        Args:
            domain: Domain name to get policy for
            
        Returns:
            DomainPolicy if found, None otherwise
            
        Raises:
            DatabaseError: If database operation fails
        """
        try:
            async with self.db_session_factory() as session:
                statement = select(DomainPolicy).where(DomainPolicy.domain == domain)
                result = await session.execute(statement)
                return result.scalar_one_or_none()
        except Exception as e:
            logger.error(
                "error_getting_domain_policy",
                domain=domain,
                error=str(e),
                exc_info=True
            )
            raise DatabaseError(
                f"Failed to get domain policy for {domain}",
                operation="get_domain_policy",
                details={"domain": domain}
            ) from e
    
    async def _check_rate_limit(self, domain: str, limit: int, window_seconds: int) -> bool:
        """
        Check if rate limit is exceeded.
        
        Args:
            domain: Domain name
            limit: Rate limit (requests per window)
            window_seconds: Time window in seconds
            
        Returns:
            True if limit is exceeded (should deny), False otherwise
            
        Raises:
            RedisError: If Redis operation fails
        """
        try:
            key = f"{self.rate_limit_prefix}{domain}:{window_seconds}"
            current = await self.redis_client.get(key)
            
            if current is None:
                # First request in window
                await self.redis_client.setex(key, window_seconds, "1")
                return False
            
            count = int(current)
            if count >= limit:
                logger.warning(
                    "rate_limit_exceeded",
                    domain=domain,
                    limit=limit,
                    current=count,
                    window_seconds=window_seconds
                )
                return True  # Limit exceeded
            
            # Increment counter
            await self.redis_client.incr(key)
            return False
        except Exception as e:
            logger.error(
                "error_checking_rate_limit",
                domain=domain,
                limit=limit,
                window_seconds=window_seconds,
                error=str(e),
                exc_info=True
            )
            raise RedisError(
                f"Failed to check rate limit for {domain}",
                operation="check_rate_limit",
                details={"domain": domain, "limit": limit, "window_seconds": window_seconds}
            ) from e
    
    async def _get_current_concurrency(self, domain: str) -> int:
        """
        Get current number of running jobs for domain.
        
        Args:
            domain: Domain name
            
        Returns:
            Current concurrency count
            
        Raises:
            RedisError: If Redis operation fails
        """
        try:
            key = f"{self.concurrency_prefix}{domain}"
            count = await self.redis_client.get(key)
            return int(count) if count else 0
        except Exception as e:
            logger.error(
                "error_getting_concurrency",
                domain=domain,
                error=str(e),
                exc_info=True
            )
            raise RedisError(
                f"Failed to get concurrency for {domain}",
                operation="get_current_concurrency",
                details={"domain": domain}
            ) from e
    
    async def increment_concurrency(self, domain: str) -> None:
        """
        Increment concurrency counter for domain.
        
        Args:
            domain: Domain name
            
        Raises:
            RedisError: If Redis operation fails
        """
        try:
            key = f"{self.concurrency_prefix}{domain}"
            await self.redis_client.incr(key)
            logger.debug("concurrency_incremented", domain=domain)
        except Exception as e:
            logger.error(
                "error_incrementing_concurrency",
                domain=domain,
                error=str(e),
                exc_info=True
            )
            raise RedisError(
                f"Failed to increment concurrency for {domain}",
                operation="increment_concurrency",
                details={"domain": domain}
            ) from e
    
    async def decrement_concurrency(self, domain: str) -> None:
        """
        Decrement concurrency counter for domain.
        
        Args:
            domain: Domain name
            
        Raises:
            RedisError: If Redis operation fails
        """
        try:
            key = f"{self.concurrency_prefix}{domain}"
            await self.redis_client.decr(key)
            # Don't go below 0
            count = await self.redis_client.get(key)
            if count and int(count) < 0:
                await self.redis_client.set(key, "0")
            logger.debug("concurrency_decremented", domain=domain)
        except Exception as e:
            logger.error(
                "error_decrementing_concurrency",
                domain=domain,
                error=str(e),
                exc_info=True
            )
            raise RedisError(
                f"Failed to decrement concurrency for {domain}",
                operation="decrement_concurrency",
                details={"domain": domain}
            ) from e
    
    async def _log_audit(
        self,
        job_id: str,
        domain: str,
        authorization_mode: AuthorizationMode,
        strategy: str,
        action: PolicyAction,
        allowed: bool,
        reason: Optional[str] = None,
        policy_id: Optional[str] = None,
        rate_limit_applied: bool = False,
        concurrency_limit_applied: bool = False,
        strategy_restricted: bool = False,
        user_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log policy decision to audit log."""
        try:
            audit_log = AuditLog(
                id=f"audit_{job_id}_{datetime.utcnow().timestamp()}",
                job_id=job_id,
                domain=domain,
                policy_id=policy_id,
                authorization_mode=authorization_mode,
                strategy=strategy,
                action=action,
                allowed=allowed,
                reason=reason,
                rate_limit_applied=rate_limit_applied,
                concurrency_limit_applied=concurrency_limit_applied,
                strategy_restricted=strategy_restricted,
                user_id=user_id,
                ip_address=ip_address,
                context=json.dumps(context) if context else None,
            )
            
            async with self.db_session_factory() as session:
                session.add(audit_log)
                await session.commit()
        except Exception as e:
            logger.error(
                "error_logging_audit_entry",
                job_id=job_id,
                domain=domain,
                error=str(e),
                exc_info=True
            )
            # Don't raise exception for audit logging failures - log and continue
            # Audit logging is important but shouldn't break the main flow

