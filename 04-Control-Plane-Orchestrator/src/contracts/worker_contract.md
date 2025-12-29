# Worker ↔ Control Plane Contract (v1)

## Goals
- Workers pull work without inbound ports (poll / outbound HTTP).
- Correctness under duplicates: at-least-once delivery with idempotent completion.
- Durable job lifecycle: DB is authoritative, Redis stream is transport.

## Entities
- Job: canonical record (DB).
- JobAttempt: each execution attempt (DB).
- JobLease: exclusive lock with TTL (DB).
- Stream message: transport envelope (Redis Streams).

## Job lifecycle
1) Control Plane enqueues stream message containing `job_id` and `enqueue_id`.
2) Worker claims a job:
   - Control Plane creates/renews a DB lease (exclusive) and transitions job to RUNNING.
   - Control Plane returns a `lease_token` and `attempt_id`.
3) Worker heartbeats periodically:
   - Control Plane extends lease TTL if token matches.
4) Worker completes or fails:
   - Control Plane atomically writes result/error + final status + closes attempt + releases lease.
   - Control Plane returns `ack=true` meaning worker should ACK the Redis stream message.
5) Control Plane reaper:
   - Detects expired leases for RUNNING jobs.
   - Requeues job (new stream message) or DLQs based on attempt count and policy.

## HTTP Endpoints
### POST /internal/worker/claim
Request:
- worker_id: string
- streams: list of stream names to poll (priority order)
- max_wait_ms: int (long-poll)
Response:
- claimed: bool
- if claimed:
  - job_id, attempt_id, lease_token
  - stream: name, message_id
  - payload: job payload snapshot for execution
  - lease_ttl_seconds, heartbeat_interval_seconds

### POST /internal/worker/heartbeat
Request:
- worker_id
- job_id
- lease_token
Response:
- ok: bool
- lease_expires_at

### POST /internal/worker/complete
Request:
- worker_id
- job_id
- attempt_id
- lease_token
- stream + message_id
- result: JSON (includes artifact pointers, timings, outputs)
Response:
- ok: bool
- ack: bool

### POST /internal/worker/fail
Request:
- worker_id
- job_id
- attempt_id
- lease_token
- stream + message_id
- error: { code, message, stack, retryable }
Response:
- ok: bool
- ack: bool
- requeued: bool
- dlq: bool

## Semantics (non-negotiable)
- DB state is the truth. Redis message may be duplicated.
- A stream message is ACKed only after DB transaction commits final state OR is safely requeued/DLQed.
- Completion/failure is idempotent by (job_id, attempt_id) and guarded by lease_token.
- Leases are short (e.g., 60s) and must be extended by heartbeat.

---

### Repo alignment notes
- `job_id` is represented as a UUID string in the current Control Plane schema (`jobs.id` is a string containing a UUID).

