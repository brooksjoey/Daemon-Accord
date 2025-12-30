from __future__ import annotations

from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, Optional

from playwright.async_api import Page


class ActionError(Exception):
    """Base class for all action errors."""


class ActionRetryableError(ActionError):
    """An error that should be retried (network flake, transient timeouts, etc.)."""


class ActionFatalError(ActionError):
    """An error that should *not* be retried (bad params, permanent failure, etc.)."""


@dataclass(frozen=True)
class ActionResult:
    """
    Immutable action output.

    Note: `data` is deep-copied on creation to prevent accidental mutation.
    """

    success: bool
    data: Dict[str, Any]
    error: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "data", deepcopy(self.data))


class BaseAction(ABC):
    """
    New action contract: deterministic, dependency-injected, and easy to test.

    Implementers should raise ActionRetryableError / ActionFatalError on failures.
    """

    name: str = "base"

    @abstractmethod
    async def execute(self, page: Page, params: Dict[str, Any]) -> ActionResult:
        raise NotImplementedError
