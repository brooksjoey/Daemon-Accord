from __future__ import annotations

import base64
import logging
from datetime import datetime
from typing import Any, Dict

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from .base_action import (
    ActionFatalError,
    ActionResult,
    ActionRetryableError,
    BaseAction,
)

logger = logging.getLogger(__name__)


class ScreenshotAction(BaseAction):
    name = "screenshot"

    async def execute(self, page: Page, params: Dict[str, Any]) -> ActionResult:
        """
        Navigate (optional) and capture screenshots.

        Expected params:
        - url: str (required)
        - capture_config:
          - full_page: bool (default True)
          - viewport: bool (default True)
          - trigger_selectors: List[str] (optional)
        """
        url = (params.get("url") or "").strip()
        if not url:
            raise ActionFatalError("No URL provided for screenshot capture")

        capture_config: Dict[str, Any] = params.get("capture_config") or {}
        full_page = bool(capture_config.get("full_page", True))
        viewport = bool(capture_config.get("viewport", True))
        trigger_selectors = capture_config.get("trigger_selectors", []) or []

        try:
            await page.goto(url, wait_until="networkidle")
        except PlaywrightTimeoutError as e:
            raise ActionRetryableError(f"Screenshot navigation timeout: {e}") from e
        except Exception as e:
            raise ActionRetryableError(f"Screenshot navigation failed: {e}") from e

        screenshots: Dict[str, str] = {}
        metadata: Dict[str, Any] = {
            "url": url,
            "timestamp": datetime.utcnow().isoformat(),
        }

        try:
            metadata["user_agent"] = await page.evaluate("navigator.userAgent")
        except Exception:
            pass

        if full_page:
            img = await page.screenshot(full_page=True)
            screenshots["full_page"] = base64.b64encode(img).decode("utf-8")

        if viewport:
            img = await page.screenshot()
            screenshots["viewport"] = base64.b64encode(img).decode("utf-8")

        for selector in trigger_selectors:
            try:
                await page.wait_for_selector(selector, timeout=5000)
                img = await page.screenshot()
                screenshots[f"trigger_{selector}"] = base64.b64encode(img).decode("utf-8")
            except Exception:
                continue

        return ActionResult(success=True, data={"screenshots": screenshots, "metadata": metadata})

