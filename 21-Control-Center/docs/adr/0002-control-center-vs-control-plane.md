# ADR 0002: Control Center responsibilities vs Control Plane

## Status

Proposed

## Context

`04-Control-Plane-Orchestrator/` is the system of record for job/workflow lifecycle and policy enforcement. `21-Control-Center/` is intended as the operator-facing surface.

Without a clear boundary, Control Center risks duplicating orchestration logic, creating conflicting sources of truth, or bypassing safety/compliance checks.

## Decision

Define the boundary as:

- **Control Plane Orchestrator**:
  - Owns job/workflow state machines, persistence, and policy enforcement
  - Exposes authoritative APIs/events for operational state and actions
  - Enforces authz, rate limits, and auditing for any state-changing operation

- **Control Center**:
  - Provides operator UX and read-only aggregation views by default
  - Performs state-changing operations only by calling Control Plane (or other module) APIs
  - Does not introduce independent “business truth” for jobs/workflows/policies
  - May include a thin aggregation layer (gateway/BFF) **only** to compose data for UI, not to replicate orchestration rules

## Consequences

- **Positive**: Prevents overlap and keeps “truth” centralized; simplifies compliance/audit posture.
- **Tradeoff**: Control Center may depend on multiple upstream APIs; aggregation needs careful caching/timeout strategy.

## Notes

If a future decision introduces a Control Center backend (BFF/API gateway), it must remain a composition layer and avoid re-implementing state transitions that belong in the Control Plane.

