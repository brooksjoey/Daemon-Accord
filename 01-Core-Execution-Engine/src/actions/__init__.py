"""
Actions module for Execution Engine.

This package contains:
- A new action contract (`BaseAction`, `ActionResult`, `ActionError` hierarchy)
- A registry of available actions (`ACTION_REGISTRY`)
- Legacy context-based action types kept in `legacy_base_action.py`
"""

from .base_action import (
    ActionError,
    ActionFatalError,
    ActionResult,
    ActionRetryableError,
    BaseAction,
)
from .authenticate_action import AuthenticateAction
from .form_submit_action import FormSubmitAction
from .navigate_action import NavigateExtractAction
from .price_extraction_action import PriceExtractionAction
from .screenshot_action import ScreenshotAction

ACTION_REGISTRY = {
    # canonical keys
    "navigate": NavigateExtractAction(),
    "authenticate": AuthenticateAction(),
    "form_submit": FormSubmitAction(),
    "screenshot": ScreenshotAction(),
    "price_extraction": PriceExtractionAction(),
    # aliases / compatibility keys
    "navigate_extract": NavigateExtractAction(),
    "data_scraping": NavigateExtractAction(),
    "login": AuthenticateAction(),
    "authentication": AuthenticateAction(),
    "form_fill": FormSubmitAction(),
    "screenshot_capture": ScreenshotAction(),
}

__all__ = [
    "ACTION_REGISTRY",
    "BaseAction",
    "ActionResult",
    "ActionError",
    "ActionRetryableError",
    "ActionFatalError",
]
