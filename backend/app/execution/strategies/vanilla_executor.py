import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Protocol
from urllib.parse import urlparse

from playwright.async_api import Browser, Page, async_playwright

from backend.app.models import Job


class BrowserPoolProtocol(Protocol):
    async def acquire(self) -> Browser: ...
    async def release(self, browser: Browser) -> None: ...
    async def cleanup(self) -> None: ...


@dataclass
class ExecutionResult:
    job_id: str
    success: bool
    duration: float
    error: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


class BaseExecutor(ABC):
    strategy_name: str = "base"

    def __init__(
        self,
        browser_pool: Optional[BrowserPoolProtocol] = None,
        redis_client: Any = None,
        metrics_client: Any = None,
        playwright_factory: Callable = async_playwright,
        default_timeout: int = 30,
    ):
        self.browser_pool = browser_pool
        self.redis = redis_client
        self.metrics = metrics_client
        self.default_timeout = default_timeout
        self._playwright_factory = playwright_factory
        self._ephemeral_playwright = None

    @abstractmethod
    async def execute(self, job: Job) -> ExecutionResult:
        raise NotImplementedError

    async def _acquire_browser(self) -> Browser:
        if self.browser_pool:
            return await self.browser_pool.acquire()

        self._ephemeral_playwright = await self._playwright_factory().start()
        browser = await self._ephemeral_playwright.chromium.launch(headless=True)
        return browser

    async def _release_browser(self, browser: Browser):
        if self.browser_pool:
            await self.browser_pool.release(browser)
        else:
            await browser.close()
            if self._ephemeral_playwright:
                await self._ephemeral_playwright.stop()
                self._ephemeral_playwright = None

    async def _emit_metrics(self, domain: str, success: bool, duration: float):
        if hasattr(self.metrics, "record_execution"):
            self.metrics.record_execution(domain, self.strategy_name, success, duration)

    def _extract_domain(self, job: Job) -> str:
        if job.target and isinstance(job.target, dict):
            domain = job.target.get("domain")
            if domain:
                return domain

        parsed = urlparse(job.url)
        return parsed.hostname or "unknown"

    def _resolve_timeout(self, job: Job) -> int:
        payload_timeout = job.payload.get("timeout")
        if isinstance(payload_timeout, (int, float)) and payload_timeout > 0:
            return int(payload_timeout)
        return self.default_timeout

    async def _read_next_job(
        self,
        consumer_group: str,
        consumer_name: str,
        block_ms: int = 1000,
    ):
        if not self.redis:
            return None

        records = await self.redis.xreadgroup(
            groupname=consumer_group,
            consumername=consumer_name,
            streams={"jobs-stream": ">"},
            count=1,
            block=block_ms,
        )
        return records

    async def cleanup(self):
        if self.browser_pool:
            await self.browser_pool.cleanup()


class VanillaExecutor(BaseExecutor):
    strategy_name = "vanilla"

    async def _before_navigation(self, job: Job, page: Page):
        return

    async def _after_navigation(self, job: Job, page: Page):
        return

    async def execute(self, job: Job) -> ExecutionResult:
        start_time = time.perf_counter()
        page: Optional[Page] = None
        browser: Optional[Browser] = None

        try:
            browser = await self._acquire_browser()
            page = await browser.new_page()

            await self._before_navigation(job, page)

            timeout_ms = self._resolve_timeout(job) * 1000
            response = await page.goto(
                job.url,
                wait_until="networkidle",
                timeout=timeout_ms,
            )

            await self._after_navigation(job, page)

            duration = time.perf_counter() - start_time
            domain = self._extract_domain(job)
            await self._emit_metrics(domain, True, duration)

            return ExecutionResult(
                job_id=job.id,
                success=True,
                duration=duration,
                error=None,
                details={"status": getattr(response, "status", None)},
            )
        except Exception as exc:
            duration = time.perf_counter() - start_time
            domain = self._extract_domain(job)
            await self._emit_metrics(domain, False, duration)

            return ExecutionResult(
                job_id=job.id,
                success=False,
                duration=duration,
                error=str(exc),
                details={},
            )
        finally:
            if page:
                await page.close()
            if browser:
                await self._release_browser(browser)
