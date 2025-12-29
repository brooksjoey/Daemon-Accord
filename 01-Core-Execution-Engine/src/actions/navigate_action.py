from __future__ import annotations

import logging
from typing import Any, Dict, List, Union

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from .base_action import (
    ActionFatalError,
    ActionResult,
    ActionRetryableError,
    BaseAction,
)

logger = logging.getLogger(__name__)

SelectorConfig = Union[str, Dict[str, Any]]


class NavigateExtractAction(BaseAction):
    name = "navigate"

    async def execute(self, page: Page, params: Dict[str, Any]) -> ActionResult:
        """
        Navigate to a URL and extract content via selectors.

        Expected params:
        - url: str (required)
        - selectors: List[str] or List[Dict[str, Any]]
          - selector: str
          - attribute: str ('text' default)
          - multiple: bool (False default)
        - wait_strategy: str ('networkidle' default)
        - timeout: int (30000 default)
        """
        url = (params.get("url") or "").strip()
        if not url:
            raise ActionFatalError("No URL provided for navigation")

        selectors: List[SelectorConfig] = params.get("selectors") or []
        wait_strategy = params.get("wait_strategy", "networkidle")
        timeout = int(params.get("timeout", 30000))

        try:
            logger.info("Navigating to %s", url)
            await page.goto(url, wait_until=wait_strategy, timeout=timeout)
        except PlaywrightTimeoutError as e:
            raise ActionRetryableError(f"Navigation timeout: {e}") from e
        except Exception as e:
            # DNS errors, network failures, etc.
            raise ActionRetryableError(f"Navigation failed: {e}") from e

        extracted: Dict[str, Any] = {}

        for selector_config in selectors:
            if isinstance(selector_config, str):
                selector = selector_config
                attribute = "text"
                multiple = False
            else:
                selector = (selector_config.get("selector") or "").strip()
                attribute = selector_config.get("attribute", "text")
                multiple = bool(selector_config.get("multiple", False))

            if not selector:
                continue

            try:
                if multiple:
                    elements = await page.query_selector_all(selector)
                    values: List[str] = []
                    for el in elements:
                        if attribute == "text":
                            value = await el.text_content()
                        else:
                            value = await el.get_attribute(attribute)
                        if value:
                            values.append(value.strip())
                    extracted[selector] = values
                else:
                    el = await page.query_selector(selector)
                    if not el:
                        continue
                    if attribute == "text":
                        value = await el.text_content()
                    else:
                        value = await el.get_attribute(attribute)
                    if value:
                        extracted[selector] = value.strip()
            except Exception as e:
                # Non-critical extraction failure. Keep going.
                logger.warning("Selector extract failed (%s): %s", selector, e)

        return ActionResult(
            success=True,
            data={"extracted": extracted, "url": url},
            error=None,
        )

