"""
Custom exception hierarchy for Daemon Accord.

Provides structured error handling with proper error propagation.
"""


class DaemonAccordException(Exception):
    """Base exception for all Daemon Accord errors."""
    
    def __init__(self, message: str, error_code: str | None = None, details: dict | None = None):
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        super().__init__(self.message)


class PolicyViolationError(DaemonAccordException):
    """Raised when a policy violation is detected."""
    
    def __init__(self, message: str, policy_action: str | None = None, domain: str | None = None, **kwargs):
        super().__init__(message, error_code="POLICY_VIOLATION", details=kwargs)
        self.policy_action = policy_action
        self.domain = domain


class RateLimitExceededError(PolicyViolationError):
    """Raised when rate limit is exceeded."""
    
    def __init__(self, domain: str, limit: int, window: str, **kwargs):
        message = f"Rate limit exceeded for domain {domain}: {limit} requests per {window}"
        super().__init__(message, policy_action="RATE_LIMIT", domain=domain, **kwargs)
        self.limit = limit
        self.window = window


class ConcurrencyLimitExceededError(PolicyViolationError):
    """Raised when concurrency limit is exceeded."""
    
    def __init__(self, domain: str, limit: int, current: int, **kwargs):
        message = f"Concurrency limit exceeded for domain {domain}: {limit} (current: {current})"
        super().__init__(message, policy_action="CONCURRENCY_LIMIT", domain=domain, **kwargs)
        self.limit = limit
        self.current = current


class StrategyNotAllowedError(PolicyViolationError):
    """Raised when execution strategy is not allowed."""
    
    def __init__(self, strategy: str, domain: str | None = None, allowed_strategies: list[str] | None = None, **kwargs):
        message = f"Strategy '{strategy}' not allowed"
        if domain:
            message += f" for domain {domain}"
        if allowed_strategies:
            message += f". Allowed strategies: {', '.join(allowed_strategies)}"
        super().__init__(message, policy_action="STRATEGY_RESTRICTED", domain=domain, **kwargs)
        self.strategy = strategy
        self.allowed_strategies = allowed_strategies or []


class DomainNotAllowedError(PolicyViolationError):
    """Raised when domain is not allowed."""
    
    def __init__(self, domain: str, reason: str = "Domain is not on allowlist", **kwargs):
        message = f"Domain {domain} is not allowed: {reason}"
        super().__init__(message, policy_action="DENY", domain=domain, **kwargs)
        self.reason = reason


class JobExecutionError(DaemonAccordException):
    """Raised when job execution fails."""
    
    def __init__(self, message: str, job_id: str | None = None, **kwargs):
        super().__init__(message, error_code="JOB_EXECUTION_ERROR", details=kwargs)
        self.job_id = job_id


class JobNotFoundError(DaemonAccordException):
    """Raised when a job is not found."""
    
    def __init__(self, job_id: str, **kwargs):
        message = f"Job {job_id} not found"
        super().__init__(message, error_code="JOB_NOT_FOUND", details=kwargs)
        self.job_id = job_id


class DatabaseError(DaemonAccordException):
    """Raised when a database operation fails."""
    
    def __init__(self, message: str, operation: str | None = None, **kwargs):
        super().__init__(message, error_code="DATABASE_ERROR", details=kwargs)
        self.operation = operation


class RedisError(DaemonAccordException):
    """Raised when a Redis operation fails."""
    
    def __init__(self, message: str, operation: str | None = None, **kwargs):
        super().__init__(message, error_code="REDIS_ERROR", details=kwargs)
        self.operation = operation


class ConfigurationError(DaemonAccordException):
    """Raised when there's a configuration error."""
    
    def __init__(self, message: str, config_key: str | None = None, **kwargs):
        super().__init__(message, error_code="CONFIGURATION_ERROR", details=kwargs)
        self.config_key = config_key

