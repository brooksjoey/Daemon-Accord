# ADR 0001: Control Center scaffold and module boundaries

## Status

Proposed

## Context

Daemon Accord has multiple modules that expose operationally relevant state:

- Execution Engine (jobs/executions, browser pool health, artifacts)
- Safety/Observability (circuit breakers, target policies, evidence packs, metrics)
- Control Plane Orchestrator (job queue, workflow lifecycle, compliance enforcement)

Operators need a single place to observe the system, triage issues, and (when explicitly allowed) take operational actions.

The repository currently lacks a dedicated “Control Center” module and an ADR trail describing how it should integrate with other services.

## Decision

Create a new top-level module directory: `21-Control-Center/` with:

- A minimal, stable module layout (`docs/`, `src/`, `tests/`)
- A tiny importable Python entrypoint (`src/control_center/main.py`) as a placeholder for future API/UI wiring
- A local ADR folder (`docs/adr/`) to capture Control Center decisions without imposing global repo conventions

This scaffold explicitly avoids committing to a UI framework or API technology until integration requirements are validated.

## Consequences

- **Positive**: Establishes a consistent place for Control Center code, docs, and tests; makes future integration work incremental.
- **Neutral**: No runtime service is provided yet; this is structural only.
- **Risk**: Without follow-up ADRs, the scaffold could drift. Mitigation: add ADRs as decisions are made (auth, API aggregation, UI choice).

## Alternatives considered

- Add Control Center directly under `04-Control-Plane-Orchestrator/`: rejected to keep “operator UI/ops surface” decoupled from orchestration internals.
- Add a top-level `docs/adr/` for the whole repo first: rejected for this change to keep scope small; can be introduced later if desired.

## Follow-ups

- Decide whether Control Center is:
  - a standalone service with its own API, or
  - a UI that consumes existing module APIs, or
  - both (API gateway + UI)
- Define authentication/authorization strategy and audit logging requirements.

