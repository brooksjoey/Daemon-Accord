# Safety & Observability

Safety & Observability is the cross-cutting control layer for Daemon Accord. It provides runtime guardrails (rate limiting, circuit breaking), target-specific policy configuration, and evidence/artifact capture to support debugging, compliance, and auditability.

## What this module provides

- **Target registry + configurations**: Domain-specific safety configuration in `src/targets/configurations/`
- **Rate limiting**: Token-bucket style enforcement (Lua + Python glue) under `src/targets/` and `src/safety/`
- **Circuit breakers**: Defensive fail-fast mechanisms under `src/safety/`
- **Artifacts**: Capture and package evidence (snapshots/diffs) under `src/artifacts/`

## Layout

```
02-Safety-Observability/
  README.md
  src/
    artifacts/
    integration/
    safety/
    targets/
  tests/
```

## Testing

From this directory:

```bash
pytest -q
```

## Integration notes

This module is designed to be consumed by orchestration and execution layers:

- `04-Control-Plane-Orchestrator/` for policy enforcement and operational controls
- `01-Core-Execution-Engine/` for execution-time safety hooks (as integrated)

