"""
Control Center entrypoint (scaffold).

This file is intentionally minimal: it provides a stable import target for future
API/UI implementations and integration wiring.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ControlCenterApp:
    """
    Minimal placeholder "app" object.

    Replace/extend with a concrete framework (e.g., FastAPI) once decisions are made.
    """

    name: str = "control-center"


def create_app() -> ControlCenterApp:
    """Factory kept stable for future framework migration."""

    return ControlCenterApp()


if __name__ == "__main__":
    # Placeholder run behavior; intentionally does not start a server.
    app = create_app()
    print(f"{app.name}: scaffold ready")

