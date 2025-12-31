from typing import Any, Dict, Optional
from urllib.parse import urlparse
import json

from backend.app.models import Job
from .vanilla_executor import BaseExecutor, ExecutionResult, VanillaExecutor
from .stealth_executor import StealthExecutor
from .assault_executor import AssaultExecutor


class StrategyExecutor:
    def __init__(
        self,
        browser_pool: Any = None,
        redis_client: Any = None,
        metrics_client: Any = None,
    ):
        self.browser_pool = browser_pool
        self.redis = redis_client
        self.metrics = metrics_client

    def _infer_level_from_domain(self, job: Job) -> int:
        domain = job.target.get("domain") if job.target else None
        if not domain:
            parsed = urlparse(job.url)
            domain = parsed.hostname or ""

        domain = domain.lower()
        high_risk = ("cloudflare", "akamai", "datadome")
        medium_risk = ("login", "account", "auth")

        if any(marker in domain for marker in high_risk):
            return 2
        if any(marker in domain for marker in medium_risk):
            return 1
        return 0

    def get_executor(self, job: Job) -> BaseExecutor:
        level = job.payload.get("evasion_level")
        if level is None:
            level = self._infer_level_from_domain(job)

        if level >= 2:
            return AssaultExecutor(
                browser_pool=self.browser_pool,
                redis_client=self.redis,
                metrics_client=self.metrics,
            )
        if level >= 1:
            return StealthExecutor(
                browser_pool=self.browser_pool,
                redis_client=self.redis,
                metrics_client=self.metrics,
            )
        return VanillaExecutor(
            browser_pool=self.browser_pool,
            redis_client=self.redis,
            metrics_client=self.metrics,
        )

    async def dispatch(self, job: Job) -> ExecutionResult:
        executor = self.get_executor(job)
        return await executor.execute(job)

    async def poll_stream(
        self,
        consumer_group: str,
        consumer_name: str,
        block_ms: int = 1000,
    ) -> Optional[ExecutionResult]:
        if not self.redis:
            return None

        records = await self.redis.xreadgroup(
            groupname=consumer_group,
            consumername=consumer_name,
            streams={"jobs-stream": ">"},
            count=1,
            block=block_ms,
        )

        for _, entries in records or []:
            for message_id, raw_data in entries:
                data = self._normalize_stream_payload(raw_data)
                job = Job(
                    id=str(data.get("id") or message_id),
                    url=data.get("url", ""),
                    payload=data.get("payload", {}),
                    target=data.get("target", {}),
                )
                return await self.dispatch(job)
        return None

    def _normalize_stream_payload(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        parsed: Dict[str, Any] = {}
        for key, value in raw_data.items():
            decoded_key = key.decode() if isinstance(key, (bytes, bytearray)) else key
            decoded_value = (
                value.decode() if isinstance(value, (bytes, bytearray)) else value
            )
            if decoded_key in {"payload", "target"} and isinstance(decoded_value, str):
                try:
                    parsed[decoded_key] = json.loads(decoded_value)
                    continue
                except json.JSONDecodeError:
                    parsed[decoded_key] = {}
                    continue
            parsed[decoded_key] = decoded_value
        return parsed


__all__ = [
    "BaseExecutor",
    "ExecutionResult",
    "VanillaExecutor",
    "StealthExecutor",
    "AssaultExecutor",
    "StrategyExecutor",
]
