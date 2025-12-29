"""
Unit tests for Policy Enforcer
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from src.compliance.models import (
    DomainPolicy,
    AuditLog,
    AuthorizationMode,
    PolicyAction,
)
from src.compliance.policy_enforcer import PolicyEnforcer


@pytest.fixture
def mock_db_session_factory():
    """Mock database session factory."""
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.execute = AsyncMock()
    session.add = MagicMock()
    session.commit = AsyncMock()
    
    factory = MagicMock(return_value=session)
    return factory


@pytest.fixture
def mock_redis_client():
    """Mock Redis client."""
    redis_client = AsyncMock()
    redis_client.get = AsyncMock(return_value=None)
    redis_client.setex = AsyncMock()
    redis_client.incr = AsyncMock()
    redis_client.decr = AsyncMock()
    return redis_client


@pytest.fixture
def policy_enforcer(mock_db_session_factory, mock_redis_client):
    """Create PolicyEnforcer instance."""
    return PolicyEnforcer(
        db_session_factory=mock_db_session_factory,
        redis_client=mock_redis_client,
    )


@pytest.mark.asyncio
async def test_denylist_rejects_domain(policy_enforcer, mock_db_session_factory):
    """Test that denylisted domains are rejected."""
    # Create denylisted domain policy
    policy = DomainPolicy(
        id="policy_1",
        domain="blocked.com",
        allowed=True,
        denied=True,
    )
    
    # Mock database query to return policy
    result = MagicMock()
    result.scalar_one_or_none = MagicMock(return_value=policy)
    mock_db_session_factory.return_value.execute = AsyncMock(return_value=result)
    
    # Check policy
    allowed, action, reason, context = await policy_enforcer.check_policy(
        job_id="job_1",
        domain="blocked.com",
        url="https://blocked.com/page",
        strategy="vanilla",
        authorization_mode=AuthorizationMode.PUBLIC,
    )
    
    assert allowed is False
    assert action == PolicyAction.DENY
    assert "denylist" in reason.lower() or "denied" in reason.lower()
    
    # Verify audit log was created
    mock_db_session_factory.return_value.add.assert_called()
    mock_db_session_factory.return_value.commit.assert_called()


@pytest.mark.asyncio
async def test_rate_limit_enforced(policy_enforcer, mock_db_session_factory, mock_redis_client):
    """Test that rate limits are enforced."""
    # Create policy with rate limit
    policy = DomainPolicy(
        id="policy_1",
        domain="example.com",
        allowed=True,
        denied=False,
        rate_limit_per_minute=5,
    )
    
    # Mock database query
    result = MagicMock()
    result.scalar_one_or_none = MagicMock(return_value=policy)
    mock_db_session_factory.return_value.execute = AsyncMock(return_value=result)
    
    # Mock Redis to return count at limit
    mock_redis_client.get = AsyncMock(return_value="5")  # At limit
    
    # Check policy
    allowed, action, reason, context = await policy_enforcer.check_policy(
        job_id="job_1",
        domain="example.com",
        url="https://example.com/page",
        strategy="vanilla",
        authorization_mode=AuthorizationMode.PUBLIC,
    )
    
    assert allowed is False
    assert action == PolicyAction.RATE_LIMIT
    assert "rate limit" in reason.lower()
    
    # Verify audit log
    mock_db_session_factory.return_value.add.assert_called()
    audit_log = mock_db_session_factory.return_value.add.call_args[0][0]
    assert audit_log.rate_limit_applied is True


@pytest.mark.asyncio
async def test_strategy_restriction_public_mode(policy_enforcer, mock_db_session_factory):
    """Test that public mode only allows vanilla strategy."""
    # No domain policy (default allow)
    result = MagicMock()
    result.scalar_one_or_none = MagicMock(return_value=None)
    mock_db_session_factory.return_value.execute = AsyncMock(return_value=result)
    
    # Try to use stealth strategy in public mode
    allowed, action, reason, context = await policy_enforcer.check_policy(
        job_id="job_1",
        domain="example.com",
        url="https://example.com/page",
        strategy="stealth",
        authorization_mode=AuthorizationMode.PUBLIC,
    )
    
    assert allowed is False
    assert action == PolicyAction.STRATEGY_RESTRICTED
    assert "public" in reason.lower() or "vanilla" in reason.lower()
    
    # Verify vanilla is allowed
    allowed, action, reason, context = await policy_enforcer.check_policy(
        job_id="job_2",
        domain="example.com",
        url="https://example.com/page",
        strategy="vanilla",
        authorization_mode=AuthorizationMode.PUBLIC,
    )
    
    assert allowed is True
    assert action == PolicyAction.ALLOW


@pytest.mark.asyncio
async def test_internal_mode_allows_all_strategies(policy_enforcer, mock_db_session_factory):
    """Test that internal mode allows all strategies."""
    # No domain policy
    result = MagicMock()
    result.scalar_one_or_none = MagicMock(return_value=None)
    mock_db_session_factory.return_value.execute = AsyncMock(return_value=result)
    
    # Internal mode should allow all strategies
    for strategy in ["vanilla", "stealth", "assault"]:
        allowed, action, reason, context = await policy_enforcer.check_policy(
            job_id=f"job_{strategy}",
            domain="example.com",
            url="https://example.com/page",
            strategy=strategy,
            authorization_mode=AuthorizationMode.INTERNAL,
        )
        
        assert allowed is True, f"Strategy {strategy} should be allowed in internal mode"
        assert action == PolicyAction.ALLOW


@pytest.mark.asyncio
async def test_audit_log_created(policy_enforcer, mock_db_session_factory):
    """Test that audit logs are created for all policy decisions."""
    # No domain policy
    result = MagicMock()
    result.scalar_one_or_none = MagicMock(return_value=None)
    mock_db_session_factory.return_value.execute = AsyncMock(return_value=result)
    
    # Check policy
    await policy_enforcer.check_policy(
        job_id="job_1",
        domain="example.com",
        url="https://example.com/page",
        strategy="vanilla",
        authorization_mode=AuthorizationMode.PUBLIC,
        user_id="user_123",
        ip_address="192.168.1.1",
    )
    
    # Verify audit log was created
    mock_db_session_factory.return_value.add.assert_called()
    audit_log = mock_db_session_factory.return_value.add.call_args[0][0]
    
    assert isinstance(audit_log, AuditLog)
    assert audit_log.job_id == "job_1"
    assert audit_log.domain == "example.com"
    assert audit_log.authorization_mode == AuthorizationMode.PUBLIC
    assert audit_log.strategy == "vanilla"
    assert audit_log.user_id == "user_123"
    assert audit_log.ip_address == "192.168.1.1"
    assert audit_log.allowed is True


@pytest.mark.asyncio
async def test_concurrency_tracking(policy_enforcer, mock_redis_client):
    """Test concurrency counter tracking."""
    # Set initial concurrency
    mock_redis_client.get = AsyncMock(return_value="3")
    
    # Get current concurrency
    count = await policy_enforcer._get_current_concurrency("example.com")
    assert count == 3
    
    # Increment
    await policy_enforcer.increment_concurrency("example.com")
    mock_redis_client.incr.assert_called()
    
    # Decrement
    await policy_enforcer.decrement_concurrency("example.com")
    mock_redis_client.decr.assert_called()


@pytest.mark.asyncio
async def test_concurrency_limit_enforced(policy_enforcer, mock_db_session_factory, mock_redis_client):
    """Test that concurrency limits are enforced."""
    # Create policy with concurrency limit
    policy = DomainPolicy(
        id="policy_1",
        domain="example.com",
        allowed=True,
        denied=False,
        max_concurrent_jobs=5,
    )
    
    # Mock database query
    result = MagicMock()
    result.scalar_one_or_none = MagicMock(return_value=policy)
    mock_db_session_factory.return_value.execute = AsyncMock(return_value=result)
    
    # Mock Redis to return count at limit
    mock_redis_client.get = AsyncMock(return_value="5")  # At limit
    
    # Check policy
    allowed, action, reason, context = await policy_enforcer.check_policy(
        job_id="job_1",
        domain="example.com",
        url="https://example.com/page",
        strategy="vanilla",
        authorization_mode=AuthorizationMode.PUBLIC,
    )
    
    assert allowed is False
    assert action == PolicyAction.CONCURRENCY_LIMIT
    assert "concurrency" in reason.lower()
    
    # Verify audit log
    audit_log = mock_db_session_factory.return_value.add.call_args[0][0]
    assert audit_log.concurrency_limit_applied is True

