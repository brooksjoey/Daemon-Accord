from playwright.async_api import Page

from backend.app.models import Job
from .stealth_executor import StealthExecutor


class AssaultExecutor(StealthExecutor):
    strategy_name = "assault"

    def _should_apply_assault(self, job: Job) -> bool:
        return job.payload.get("evasion_level", 0) >= 2 or job.payload.get("assault")

    async def _before_navigation(self, job: Job, page: Page):
        await super()._before_navigation(job, page)

        if not self._should_apply_assault(job):
            return

        await self._apply_stealth_patches(page)

    async def _apply_stealth_patches(self, page: Page):
        await page.evaluate(
            """
            () => {
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = window.chrome || {};
                window.chrome.runtime = {};
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications'
                        ? Promise.resolve({ state: 'denied' })
                        : originalQuery(parameters)
                );
            }
            """
        )
