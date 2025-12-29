from __future__ import annotations

import logging
import time
from typing import Any, Dict

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from .base_action import (
    ActionFatalError,
    ActionResult,
    ActionRetryableError,
    BaseAction,
)

logger = logging.getLogger(__name__)


class FormSubmitAction(BaseAction):
    name = "form_submit"

    async def execute(self, page: Page, params: Dict[str, Any]) -> ActionResult:
        """
        Fill and submit a form.

        Expected params:
        - url: str (required)
        - form_config: dict (optional)
          - fields: Dict[field_name, {selector, value, type}]
          - form_selector, submit_selector
          - validation: {success_selectors, error_selectors, expected_text, max_wait}
        """
        url = (params.get("url") or "").strip()
        if not url:
            raise ActionFatalError("No URL provided for form submission")

        form_config: Dict[str, Any] = params.get("form_config") or {}
        fields: Dict[str, Any] = form_config.get("fields", {}) if isinstance(form_config, dict) else {}
        validation: Dict[str, Any] = (
            form_config.get("validation", {}) if isinstance(form_config, dict) else {}
        )

        try:
            await page.goto(url, wait_until="networkidle")
        except PlaywrightTimeoutError as e:
            raise ActionRetryableError(f"Form navigation timeout: {e}") from e
        except Exception as e:
            raise ActionRetryableError(f"Form navigation failed: {e}") from e

        ok = await self._fill_and_submit_form(page, fields, form_config)
        if not ok:
            raise ActionRetryableError("Form fill/submit failed")

        if validation:
            valid = await self._validate_response(page, validation)
            if not valid:
                raise ActionRetryableError("Form submission validation failed")

        response_data = await self._capture_response_data(page)
        return ActionResult(success=True, data={"submitted": True, "response": response_data})

    async def _fill_and_submit_form(
        self, page: Page, fields: Dict[str, Any], config: Dict[str, Any]
    ) -> bool:
        submit_selector = config.get(
            "submit_selector", 'button[type="submit"], input[type="submit"]'
        )

        try:
            for field_name, field_config in (fields or {}).items():
                selector = field_config.get("selector", f'[name="{field_name}"]')
                value = field_config.get("value", "")
                field_type = field_config.get("type", "text")

                el = await page.query_selector(selector)
                if not el:
                    continue

                if field_type == "select":
                    await el.select_option(value)
                elif field_type == "checkbox":
                    if value:
                        await el.check()
                    else:
                        await el.uncheck()
                else:
                    await el.fill(str(value))

            if submit_selector:
                await page.click(submit_selector)
                await page.wait_for_timeout(1000)
                await page.wait_for_load_state("networkidle")

            return True
        except Exception as e:
            logger.error("Form filling error: %s", e)
            return False

    async def _validate_response(self, page: Page, validation: Dict[str, Any]) -> bool:
        success_selectors = validation.get("success_selectors", []) or []
        error_selectors = validation.get("error_selectors", []) or []
        expected_text = validation.get("expected_text", "") or ""
        max_wait = int(validation.get("max_wait", 5000))

        try:
            for selector in success_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=max_wait)
                    return True
                except Exception:
                    continue

            for selector in error_selectors:
                try:
                    el = await page.wait_for_selector(selector, timeout=1000)
                    if el:
                        return False
                except Exception:
                    continue

            if expected_text:
                content = await page.content()
                return expected_text in content

            return True
        except Exception as e:
            logger.error("Validation error: %s", e)
            return False

    async def _capture_response_data(self, page: Page) -> Dict[str, Any]:
        try:
            url = page.url
            title = await page.title()
            status_elements = await page.query_selector_all(".status, .message, .alert")
            status_texts = []
            for el in status_elements:
                text = await el.text_content()
                if text:
                    status_texts.append(text.strip())
            return {"url": url, "title": title, "status_messages": status_texts, "timestamp": time.time()}
        except Exception:
            return {}

