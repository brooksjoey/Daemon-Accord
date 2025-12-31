import asyncio
import random
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.execution.strategies import (
    AssaultExecutor,
    BaseExecutor,
    ExecutionResult,
    StealthExecutor,
    StrategyExecutor,
    VanillaExecutor,
)
from backend.app.models import Job


class DummyMetrics:
    def __init__(self):
        self.records = []

    def record_execution(self, domain, strategy, success, duration):
        self.records.append((domain, strategy, success, duration))


class DummyPage:
    def __init__(self):
        self.viewport = None
        self.closed = False
        self.evaluated_script = None

    async def goto(self, url, wait_until=None, timeout=None):
        return type("Response", (), {"status": 200})

    async def close(self):
        self.closed = True

    async def set_viewport_size(self, viewport):
        self.viewport = viewport

    async def evaluate(self, script):
        self.evaluated_script = script


class DummyBrowser:
    def __init__(self, page: DummyPage):
        self.page = page
        self.closed = False

    async def new_page(self):
        return self.page

    async def close(self):
        self.closed = True


class DummyBrowserPool:
    def __init__(self):
        self.page = DummyPage()
        self.browser = DummyBrowser(self.page)
        self.released = False

    async def acquire(self):
        return self.browser

    async def release(self, browser):
        self.released = True

    async def cleanup(self):
        return


@pytest.mark.asyncio
async def test_strategy_hierarchy(monkeypatch):
    assert issubclass(VanillaExecutor, BaseExecutor)
    assert issubclass(StealthExecutor, VanillaExecutor)
    assert issubclass(AssaultExecutor, StealthExecutor)

    metrics = DummyMetrics()
    pool = DummyBrowserPool()

    job = Job(id="v1", url="http://example.com", payload={})
    vanilla = VanillaExecutor(browser_pool=pool, metrics_client=metrics)
    result = await vanilla.execute(job)

    assert isinstance(result, ExecutionResult)
    assert result.success is True
    assert pool.released is True
    assert pool.page.closed is True
    assert metrics.records

    pool = DummyBrowserPool()
    stealth_job = Job(id="s1", url="http://example.com", payload={"evasion_level": 1})
    stealth = StealthExecutor(browser_pool=pool)

    monkeypatch.setattr(random, "uniform", lambda a, b: 0.1)
    result = await stealth.execute(stealth_job)
    assert result.success is True
    assert pool.page.viewport in StealthExecutor.viewports

    pool = DummyBrowserPool()
    assault_job = Job(id="a1", url="http://example.com", payload={"evasion_level": 2})
    assault = AssaultExecutor(browser_pool=pool)
    result = await assault.execute(assault_job)
    assert result.success is True
    assert pool.page.evaluated_script is not None

    pool = DummyBrowserPool()
    no_evasion_job = Job(id="s0", url="http://example.com", payload={"evasion_level": 0})
    stealth = StealthExecutor(browser_pool=pool)
    result = await stealth.execute(no_evasion_job)
    assert result.success is True
    assert pool.page.viewport is None

    selector = StrategyExecutor(browser_pool=DummyBrowserPool())
    low = Job(id="low", url="http://example.com", payload={"evasion_level": 0})
    med = Job(id="med", url="http://example.com", payload={"evasion_level": 1})
    high = Job(id="high", url="http://example.com", payload={"evasion_level": 2})
    heuristic = Job(id="heuristic", url="http://cloudflare-guard.com", payload={})

    assert isinstance(selector.get_executor(low), VanillaExecutor)
    assert isinstance(selector.get_executor(med), StealthExecutor)
    assert isinstance(selector.get_executor(high), AssaultExecutor)
    assert isinstance(selector.get_executor(heuristic), AssaultExecutor)

    pool = DummyBrowserPool()
    failing_job = Job(id="fail", url="http://example.com", payload={})
    failing_pool = DummyBrowserPool()

    async def failing_goto(url, wait_until=None, timeout=None):
        raise RuntimeError("navigation failed")

    failing_pool.page.goto = failing_goto
    failing_executor = VanillaExecutor(browser_pool=failing_pool)
    result = await failing_executor.execute(failing_job)
    assert result.success is False
    assert "navigation failed" in result.error
