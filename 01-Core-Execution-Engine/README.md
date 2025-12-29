Daemon Accord - Core Execution Module
=====================================

> A powerful, adaptive browser automation engine designed to maximize data extraction success rates.

## The Problem
Traditional automation crumbles against modern bot detection. Static fingerprints, predictable click paths, and noisy network patterns trigger blocks, CAPTCHAs, and throttling, leaving data pipelines unreliable and costly to maintain.

## The Solution
This module delivers three tiers of intelligent execution—Vanilla, Stealth, and Assault—that dynamically evade detection. It layers fingerprint randomization, behavioral modeling, and network pattern obfuscation to match the right tactics to each target surface without sacrificing throughput.

## Commercial Value
This is the core IP. Replicating it in-house would demand roughly nine months of senior engineering effort—over $200,000—before parity. It is the foundation for a licensable automation platform or a high-margin managed service.

## Architecture
- Base Executor → Vanilla (speed) → Stealth (evasion) → Assault (advanced counter-detection)

## Status
Proof-of-Concept / Architected. Code structure is complete and awaiting integration.

## Execution Engine worker (Control Plane contract)

`src/worker.py` runs a worker that:
- Claims work via Control Plane (`/internal/worker/claim`)
- Heartbeats DB leases (`/internal/worker/heartbeat`)
- Completes/fails jobs (`/internal/worker/complete` / `/internal/worker/fail`)
- ACKs Redis Streams **only when** Control Plane returns `ack=true`

### Run locally

```bash
export REDIS_URL="redis://localhost:6379/0"
export CONTROL_PLANE_URL="http://localhost:8080"
export WORKER_ID="execution-worker-1"
export JOBS_CONSUMER_GROUP="workers"

python src/worker.py
```
