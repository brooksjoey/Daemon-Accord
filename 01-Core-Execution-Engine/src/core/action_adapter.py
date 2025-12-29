from __future__ import annotations

import time
from typing import Any, Dict, Optional

from playwright.async_api import Page

from actions import ACTION_REGISTRY
from actions.base_action import (
    ActionError,
    ActionFatalError,
    ActionRetryableError,
)
from core.executor import JobResult, JobStatus


def _resolve_action_key(job_type: str) -> str:
    jt = (job_type or "unknown").strip()
    # ACTION_REGISTRY already includes aliases, but keep a small mapping here
    # for safety in case callers use unexpected names.
    mapping = {
        "navigate_extract": "navigate",
        "data_scraping": "navigate",
        "navigate": "navigate",
        "price_extraction": "price_extraction",
        "login": "authenticate",
        "authentication": "authenticate",
        "authenticate": "authenticate",
        "form_submit": "form_submit",
        "form_fill": "form_submit",
        "screenshot": "screenshot",
        "screenshot_capture": "screenshot",
    }
    return mapping.get(jt, jt)


def build_action_params(job_data: Dict[str, Any]) -> Dict[str, Any]:
    target = job_data.get("target") or {}
    parameters = job_data.get("parameters") or {}
    action_params = job_data.get("action_params") or {}

    params: Dict[str, Any] = {
        "url": (target.get("url") or job_data.get("url") or "").strip(),
        "domain": (target.get("domain") or job_data.get("domain") or "").strip(),
        # common optional fields used by various actions
        "selectors": job_data.get("selectors") or parameters.get("selectors") or [],
        "credentials": job_data.get("credentials") or parameters.get("credentials") or {},
        "auth_config": job_data.get("auth_config") or parameters.get("auth_config") or {},
        "form_config": job_data.get("form_config") or parameters.get("form_config") or {},
        "capture_config": job_data.get("capture_config") or parameters.get("capture_config") or {},
        # allow any additional parameters to be passed through
        **(parameters if isinstance(parameters, dict) else {}),
        **(action_params if isinstance(action_params, dict) else {}),
    }
    return params


async def execute_action_as_job_result(
    job_data: Dict[str, Any],
    *,
    browser_pool: Any,
    page: Optional[Page] = None,
) -> JobResult:
    """
    Execute a job via the new action system and return a core.JobResult.

    If `page` is provided, the caller owns the page lifecycle.
    If not, this function will acquire/release a page from `browser_pool`.
    """
    job_id = job_data.get("id", "unknown")
    job_type = job_data.get("type", "unknown")

    action_key = _resolve_action_key(job_type)
    action = ACTION_REGISTRY.get(action_key) or ACTION_REGISTRY.get(job_type)
    if not action:
        return JobResult(
            job_id=job_id,
            status=JobStatus.FAILED,
            data={},
            artifacts={},
            error=f"Unknown job type: {job_type}",
            execution_time=0.0,
        )

    params = build_action_params(job_data)

    owned_page = False
    if page is None:
        page = await browser_pool.acquire_page()
        owned_page = True

    start = time.time()
    try:
        result = await action.execute(page, params)
        return JobResult(
            job_id=job_id,
            status=JobStatus.SUCCESS if result.success else JobStatus.FAILED,
            data=result.data or {},
            artifacts={},
            error=result.error,
            execution_time=time.time() - start,
        )
    except ActionRetryableError as e:
        return JobResult(
            job_id=job_id,
            status=JobStatus.FAILED,
            data={"retryable": True, "error_type": "retryable"},
            artifacts={},
            error=str(e),
            execution_time=time.time() - start,
        )
    except ActionFatalError as e:
        return JobResult(
            job_id=job_id,
            status=JobStatus.FAILED,
            data={"retryable": False, "error_type": "fatal"},
            artifacts={},
            error=str(e),
            execution_time=time.time() - start,
        )
    except ActionError as e:
        return JobResult(
            job_id=job_id,
            status=JobStatus.FAILED,
            data={"retryable": False, "error_type": "action_error"},
            artifacts={},
            error=str(e),
            execution_time=time.time() - start,
        )
    finally:
        if owned_page and page is not None:
            await browser_pool.release_page(page)

