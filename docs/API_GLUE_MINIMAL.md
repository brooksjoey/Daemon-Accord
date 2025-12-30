## Minimal API Glue (approved scope)

### What it is
**API Glue** is a tiny HTTP service that sits in front of the existing **Control Plane** and provides:
- **1 aggregate endpoint** for the GUI (`/api/v1/system/status`)
- **simple proxy endpoints** for workflows + jobs

No auth, no Redis features, no database changes, no Kubernetes/GCP requirements in the minimal version.

### Reality check against this repo
- Control Plane is **REST** and is expected at `http://localhost:8082` in repo scripts/docs.
- Execution Engine is primarily a **Redis Streams worker** (not a REST `/jobs` API).
- In this workspace snapshot, `21-Control-Center/` is **not present** (so GUI work is not verifiable here).

---

## Minimal API Glue API (exact)

### Config
- `CONTROL_PLANE_BASE_URL` (default `http://localhost:8082`)
- `API_GLUE_PORT` (default `8088`)

### Endpoints
1) `GET /health` → `{ "status": "healthy" }`

2) `GET /api/v1/system/status`
- Calls Control Plane in parallel:
  - `GET {CONTROL_PLANE_BASE_URL}/health`
  - `GET {CONTROL_PLANE_BASE_URL}/api/v1/queue/stats`
  - `GET {CONTROL_PLANE_BASE_URL}/api/v1/ops/status`
- Returns:
  - `control_plane_health`
  - `queue_stats`
  - `ops_status`
  - `timestamp` (ISO-8601)

3) `GET /api/v1/workflows` → proxy to Control Plane `/api/v1/workflows`

4) `POST /api/v1/jobs` → proxy to Control Plane `/api/v1/jobs`
- Forward query params unchanged
- Forward JSON body unchanged
- Return upstream status code + JSON unchanged

5) `GET /api/v1/jobs/{job_id}` → proxy to Control Plane `/api/v1/jobs/{job_id}`

### Error behavior
If Control Plane is unreachable, return **502** with a clear JSON error (don’t crash).

---

## Minimal tests (required)
- Unit tests using mocked upstream (e.g., `respx` or `httpx.MockTransport`)
- Must verify:
  - `/api/v1/system/status` returns required keys and non-empty objects when upstream is healthy
  - `/api/v1/workflows` proxy returns upstream JSON unchanged

