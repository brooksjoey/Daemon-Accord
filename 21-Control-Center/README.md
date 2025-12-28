# Control Center

Control Center is the operator-facing “single pane of glass” for Daemon Accord: a place to view system health, jobs/workflows, and safety/compliance signals, and to perform approved operational actions.

This directory is intentionally a **scaffold** (minimal code + clear structure) so the Control Center can evolve without forcing premature framework decisions.

## Goals

- Provide a unified operational view across:
  - Execution Engine (`01-Core-Execution-Engine/`)
  - Safety/Observability (`02-Safety-Observability/`)
  - Control Plane Orchestrator (`04-Control-Plane-Orchestrator/`)
- Establish a stable module layout (API, UI, shared models) that can be implemented incrementally.
- Keep integration points explicit and documented via ADRs.

## Non-Goals (for this scaffold)

- Picking a final UI framework (React/Next, server-rendered, etc.)
- Implementing authentication/authorization flows
- Defining a full API surface or persistence layer

## Module layout

```
21-Control-Center/
  README.md
  docs/
    adr/
      0001-control-center-scaffold.md
  src/
    control_center/
      __init__.py
      main.py
  tests/
    test_smoke_imports.py
```

## Running (placeholder)

Once implemented, this module is expected to expose:

- A lightweight **API** (likely HTTP) to aggregate/read operational state
- A **UI** (served separately or bundled) for operators

For now, `src/control_center/main.py` is only a stub so downstream wiring can import it without errors.

## ADRs

See `docs/adr/` for local architecture decisions and evolution notes.

