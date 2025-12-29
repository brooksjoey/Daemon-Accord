from __future__ import annotations

import logging
from typing import Any, Dict, List

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from .base_action import (
    ActionFatalError,
    ActionResult,
    ActionRetryableError,
    BaseAction,
)

logger = logging.getLogger(__name__)


class PriceExtractionAction(BaseAction):
    name = "price_extraction"

    async def execute(self, page: Page, params: Dict[str, Any]) -> ActionResult:
        """
        Specialized extraction: returns text for each selector as a list of strings.

        Expected params:
        - url: str (required)
        - selectors: List[str] (required)
        - wait_strategy: str ('networkidle' default)
        - timeout: int (30000 default)
        """
        url = (params.get("url") or "").strip()
        if not url:
            raise ActionFatalError("No URL provided for price extraction")

        selectors: List[str] = params.get("selectors") or []
        if not isinstance(selectors, list) or not selectors:
            raise ActionFatalError("No selectors provided for price extraction")

        wait_strategy = params.get("wait_strategy", "networkidle")
        timeout = int(params.get("timeout", 30000))

        try:
            await page.goto(url, wait_until=wait_strategy, timeout=timeout)
        except PlaywrightTimeoutError as e:
            raise ActionRetryableError(f"Navigation timeout: {e}") from e
        except Exception as e:
            raise ActionRetryableError(f"Navigation failed: {e}") from e

        extracted: Dict[str, Any] = {}

        for selector in selectors:
            try:
                elements = await page.query_selector_all(selector)
                values: List[str] = []
                for el in elements:
                    text = await el.text_content()
                    if text:
                        values.append(text.strip())
                extracted[selector] = values
            except Exception as e:
                logger.warning("Price selector extract failed (%s): %s", selector, e)
                extracted[selector] = []

        return ActionResult(success=True, data={"extracted": extracted, "url": url})

