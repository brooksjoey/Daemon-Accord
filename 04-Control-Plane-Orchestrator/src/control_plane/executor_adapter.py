"""
Execution Engine Adapter

Bridges Control Plane with Execution Engine.
Handles executor instantiation, job format conversion, and result mapping.
"""
import sys
import os
from typing import Dict, Any, Optional, TYPE_CHECKING
import structlog

from ..exceptions import ConfigurationError, JobExecutionError

# Add Execution Engine to path (for local development)
# In containerized deployments, Execution Engine runs as separate service
execution_engine_path = os.path.join(
    os.path.dirname(__file__),
    "..", "..", "..", "01-Core-Execution-Engine", "src"
)
EXECUTION_ENGINE_AVAILABLE = os.path.exists(execution_engine_path)
if EXECUTION_ENGINE_AVAILABLE and execution_engine_path not in sys.path:
    sys.path.insert(0, execution_engine_path)

logger = structlog.get_logger(__name__)

if not EXECUTION_ENGINE_AVAILABLE:
    logger.warning(
        "execution_engine_not_found",
        message=(
            "Execution Engine code not found at expected path. "
            "In containerized deployments, Execution Engine worker handles job execution via Redis Streams."
        ),
    )

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    import redis.asyncio as redis
    from core.browser_pool import BrowserPool


class _FallbackExecutor:
    """
    Minimal executor used when Execution Engine python modules aren't importable.

    It matches the `execute(job)` coroutine shape expected by the Control Plane.
    """

    async def execute(self, job: Any):
        class _Result:
            success = False
            data = {}
            artifacts = {}
            error = "Execution Engine unavailable"

        return _Result()


class ExecutorAdapter:
    """
    Adapter between Control Plane and Execution Engine.
    
    Responsibilities:
    1. Create appropriate executor based on strategy
    2. Convert Control Plane Job format to Execution Engine format
    3. Execute job and handle results
    4. Map Execution Engine results back to Control Plane format
    """
    
    def __init__(
        self,
        redis_client: "redis.Redis",
        db_session: Optional["AsyncSession"],
        browser_pool: Optional["BrowserPool"],
    ) -> None:
        """
        Initialize Executor Adapter.
        
        Args:
            redis_client: Redis client for executors
            db_session: Database session (optional)
            browser_pool: Browser pool instance (optional)
        """
        # Attributes expected by tests + wider codebase.
        self.redis_client = redis_client
        self.db_session = db_session
        self.browser_pool = browser_pool

        # Backward-compatible aliases.
        self.redis = redis_client
        self._executor_cache: Dict[str, Any] = {}  # Cache executors by strategy
    
    def _get_executor(self, strategy: Any):
        """
        Get or create executor for the given strategy.
        
        Strategies:
        - 'vanilla': Basic execution
        - 'stealth': Stealth execution with evasion
        - 'assault': Maximum evasion
        """
        # Accept either a string strategy or a Job-like object with `.strategy`.
        strategy_name = (
            strategy
            if isinstance(strategy, str)
            else getattr(strategy, "strategy", "vanilla")
        )

        if strategy_name in self._executor_cache:
            return self._executor_cache[strategy_name]
        
        try:
            # Import Execution Engine components if present.
            #
            # This mono-repo can be used in two modes:
            # - local dev: import execution engine python modules directly
            # - containerized: execution engine runs separately and consumes Redis Streams
            #
            # For unit tests (and for containerized mode), fall back to a lightweight executor.
            from core.standard_executor import StandardExecutor  # noqa: F401
            from core.enhanced_executor import EnhancedExecutor  # noqa: F401

            # If a proper strategies module exists, prefer it; otherwise fall back.
            try:
                from strategies.vanilla_executor import VanillaExecutor
                from strategies.stealth_executor import StealthExecutor
                from strategies.assault_executor import AssaultExecutor

                if strategy_name == "assault":
                    executor = AssaultExecutor(
                        browser_pool=self.browser_pool,
                        redis_client=self.redis_client,
                    )
                elif strategy_name == "stealth":
                    executor = StealthExecutor(
                        browser_pool=self.browser_pool,
                        redis_client=self.redis_client,
                    )
                else:
                    executor = VanillaExecutor(
                        browser_pool=self.browser_pool,
                        redis_client=self.redis_client,
                    )
            except Exception:
                executor = _FallbackExecutor()

            self._executor_cache[strategy_name] = executor
            logger.info("executor_created", strategy=strategy_name)
            return executor
            
        except ImportError as e:
            # Do not hard-fail: allow Control Plane to run/tests to pass even if the
            # Execution Engine python modules are not importable in this environment.
            logger.warning("execution_engine_import_failed", error=str(e))
            executor = _FallbackExecutor()
            self._executor_cache[strategy_name] = executor
            return executor

    async def execute(self, job: Any) -> Dict[str, Any]:
        """
        Execute a Job-like object via an executor.

        This wrapper exists for backward compatibility with unit tests and older code.
        """
        try:
            executor = self._get_executor(job)
            result = await executor.execute(job)
            return self._convert_result_to_control_plane_format(result)
        except Exception as e:
            logger.error("job_execution_failed", error=str(e), exc_info=True)
            return {
                "success": False,
                "data": {},
                "artifacts": {},
                "error": str(e),
                "execution_time": 0.0,
            }
    
    def _convert_job_to_execution_format(
        self,
        job_id: str,
        domain: str,
        url: str,
        job_type: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Convert Control Plane Job format to Execution Engine format.
        
        Control Plane format:
        - id, domain, url, job_type, strategy, payload (JSON string)
        
        Execution Engine format:
        - id, type, target: {domain, url, ip}, parameters: {...}
        """
        # Parse payload if it's a string
        if isinstance(payload, str):
            import json
            try:
                payload = json.loads(payload)
            except:
                payload = {}
        
        # Build execution engine job_data
        job_data = {
            "id": job_id,
            "type": job_type,
            "target": {
                "domain": domain,
                "url": url,
                "ip": payload.get("ip", ""),  # Can extract from payload if needed
            },
            "parameters": payload,  # Pass through all payload data
        }
        
        return job_data
    
    async def execute_job(
        self,
        job_id: str,
        domain: str,
        url: str,
        job_type: str,
        strategy: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Execute a job using the Execution Engine.
        
        Args:
            job_id: Job identifier
            domain: Target domain
            url: Target URL
            job_type: Type of job (navigate_extract, authenticate, etc.)
            strategy: Execution strategy (vanilla, stealth, assault)
            payload: Job payload data
            
        Returns:
            Execution result in Control Plane format:
            {
                "success": bool,
                "data": dict,
                "artifacts": dict,
                "error": str | None,
                "execution_time": float
            }
        """
        try:
            # Get executor for strategy
            executor = self._get_executor(strategy)
            
            # Convert job format
            job_data = self._convert_job_to_execution_format(
                job_id=job_id,
                domain=domain,
                url=url,
                job_type=job_type,
                payload=payload,
            )
            
            # Execute job
            logger.info("executing_job", job_id=job_id, strategy=strategy)
            result = await executor.execute(job_data)
            
            # Convert result to Control Plane format
            return self._convert_result_to_control_plane_format(result)
            
        except Exception as e:
            logger.error(
                "job_execution_failed",
                job_id=job_id,
                strategy=strategy,
                error=str(e),
                exc_info=True
            )
            raise JobExecutionError(
                f"Job execution failed: {str(e)}",
                job_id=job_id
            ) from e
    
    def _convert_result_to_control_plane_format(self, result) -> Dict[str, Any]:
        """
        Convert Execution Engine result to Control Plane format.
        
        Execution Engine returns JobResult or ExecutionResult:
        - JobResult: status (JobStatus enum), data, artifacts, error, execution_time
        - ExecutionResult: success (bool), data, error, timing
        
        Control Plane expects:
        - success: bool
        - data: dict
        - artifacts: dict
        - error: str | None
        - execution_time: float
        """
        # Handle JobResult (from StandardExecutor)
        if hasattr(result, "status"):
            # JobResult from core/executor.py
            success = result.status.value in ["success", "completed"]
            return {
                "success": success,
                "data": result.data or {},
                "artifacts": result.artifacts or {},
                "error": result.error,
                "execution_time": result.execution_time or 0.0,
            }
        
        # Handle ExecutionResult (from strategies)
        elif hasattr(result, "success"):
            # ExecutionResult from strategies
            execution_time = 0.0
            if hasattr(result, "timing") and result.timing:
                # Convert ms to seconds.
                execution_time = result.timing.get("total_ms", 0.0) / 1000.0
            
            return {
                "success": result.success,
                "data": result.data or {},
                "artifacts": getattr(result, "artifacts", {}),
                "error": getattr(result, "error", None),
                "execution_time": execution_time,
            }
        
        # Fallback
        else:
            return {
                "success": False,
                "data": {},
                "artifacts": {},
                "error": "Unknown result format",
                "execution_time": 0.0,
            }

