from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class Job:
    """
    Lightweight job model used by the execution strategies.

    This mirrors the core Job definition while keeping dependencies minimal
    for the execution layer.
    """

    id: str
    url: str
    payload: Dict[str, Any] = field(default_factory=dict)
    target: Optional[Dict[str, Any]] = field(default_factory=dict)

