import asyncio
import random
from typing import Optional

from playwright.async_api import Page

from backend.app.models import Job
from .vanilla_executor import VanillaExecutor


class StealthExecutor(VanillaExecutor):
    strategy_name = "stealth"
    viewports = [
        {"width": 1920, "height": 1080},
        {"width": 1366, "height": 768},
        {"width": 1536, "height": 864},
    ]

    def _should_apply_evasion(self, job: Job) -> bool:
        payload_level = job.payload.get("evasion_level", 0)
        random_delay = job.payload.get("random_delay")
        return bool(payload_level >= 1 or random_delay)

    async def _before_navigation(self, job: Job, page: Page):
        if not self._should_apply_evasion(job):
            return

        delay = random.uniform(0.1, 0.3)
        await asyncio.sleep(delay)

        viewport = random.choice(self.viewports)
        await page.set_viewport_size(viewport)
