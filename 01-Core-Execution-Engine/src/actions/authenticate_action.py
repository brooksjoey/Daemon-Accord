from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from .base_action import (
    ActionFatalError,
    ActionResult,
    ActionRetryableError,
    BaseAction,
)

logger = logging.getLogger(__name__)


class AuthenticateAction(BaseAction):
    name = "authenticate"

    async def execute(self, page: Page, params: Dict[str, Any]) -> ActionResult:
        """
        Perform a simple username/password login flow.

        Expected params:
        - url: str (required)
        - domain: str (optional; used for env credential lookup)
        - credentials: {username: str, password: str} (optional; can be resolved from env)
        - auth_config: dict (optional)
          - selectors: {username, password, submit}
          - success_indicator: str (CSS selector)
        """
        url = (params.get("url") or "").strip()
        if not url:
            raise ActionFatalError("No URL provided for authentication")

        domain = (params.get("domain") or "").strip()
        auth_config: Dict[str, Any] = params.get("auth_config") or {}

        credentials = await self._get_credentials(params, domain=domain)
        if not credentials:
            raise ActionFatalError("No credentials available for authentication")

        try:
            await page.goto(url, wait_until="networkidle")
        except PlaywrightTimeoutError as e:
            raise ActionRetryableError(f"Authentication navigation timeout: {e}") from e
        except Exception as e:
            raise ActionRetryableError(f"Authentication navigation failed: {e}") from e

        ok = await self._perform_login_flow(page, credentials, auth_config)
        if not ok:
            raise ActionRetryableError("Authentication flow did not complete successfully")

        cookies = await page.context.cookies()
        return ActionResult(success=True, data={"authenticated": True, "cookies": cookies})

    async def _get_credentials(
        self, params: Dict[str, Any], *, domain: str
    ) -> Optional[Dict[str, str]]:
        credentials = params.get("credentials") or {}
        if isinstance(credentials, dict) and credentials.get("username") and credentials.get(
            "password"
        ):
            return {"username": credentials["username"], "password": credentials["password"]}

        if domain:
            env_prefix = f"CRED_{domain.upper().replace('.', '_')}"
            username = os.getenv(f"{env_prefix}_USERNAME")
            password = os.getenv(f"{env_prefix}_PASSWORD")
            if username and password:
                return {"username": username, "password": password}

        return None

    async def _perform_login_flow(
        self, page: Page, credentials: Dict[str, str], config: Dict[str, Any]
    ) -> bool:
        selectors = config.get("selectors", {}) if isinstance(config, dict) else {}

        username_selector = selectors.get(
            "username",
            'input[name="username"], input[name="email"], input[type="email"]',
        )
        password_selector = selectors.get("password", 'input[type="password"]')
        submit_selector = selectors.get(
            "submit", 'button[type="submit"], input[type="submit"]'
        )

        try:
            if username_selector:
                await page.fill(username_selector, credentials.get("username", ""))
            if password_selector:
                await page.fill(password_selector, credentials.get("password", ""))

            if submit_selector:
                await page.click(submit_selector)
                await page.wait_for_timeout(2000)
                await page.wait_for_load_state("networkidle")

            success_indicator = config.get("success_indicator") if isinstance(config, dict) else None
            if success_indicator:
                try:
                    await page.wait_for_selector(success_indicator, timeout=5000)
                    return True
                except Exception:
                    return False

            return True
        except Exception as e:
            logger.error("Login flow error: %s", e)
            return False

