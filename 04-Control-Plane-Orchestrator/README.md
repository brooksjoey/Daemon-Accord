# Control Plane Orchestrator

Job orchestration and management service for Daemon Accord.

## Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL (with asyncpg support)
- Redis
- (Optional) Execution Engine for browser automation

### Installation

```bash
pip install -r requirements.txt
```

### Configuration

Set environment variables or create `.env` file:

```env
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/daemon_accord
REDIS_URL=redis://localhost:6379/0
API_HOST=0.0.0.0
API_PORT=8080
MAX_CONCURRENT_JOBS=100
WORKER_COUNT=5
SKIP_INIT_MODELS=false  # Set to true in production (use migrations)
```

### Run

```bash
# Development (with init_models)
uvicorn src.main:app --host 0.0.0.0 --port 8080 --reload

# Or using Python module
python -m src.main
```

### Production

```bash
# 1. Run migrations
alembic upgrade head

# 2. Set SKIP_INIT_MODELS=true
export SKIP_INIT_MODELS=true

# 3. Start service
uvicorn src.main:app --host 0.0.0.0 --port 8080 --workers 4
```

## Docker

```bash
# Build
docker build -t control-plane .

# Run (requires Redis and PostgreSQL)
docker run -p 8080:8080 \
  -e DATABASE_URL=postgresql+asyncpg://postgres:postgres@host.docker.internal:5432/daemon_accord \
  -e REDIS_URL=redis://host.docker.internal:6379/0 \
  control-plane
```

## API Endpoints

- `GET /health` - Health check
- `POST /api/v1/jobs` - Create job
- `GET /api/v1/jobs/{job_id}` - Get job status
- `GET /api/v1/queue/stats` - Queue statistics

## Worker ↔ Control Plane contract (Execution Engine workers)

The deployed architecture expects the **Execution Engine worker** to pull work via the Control Plane (no inbound ports to the worker).

- **Contract doc**: `src/contracts/worker_contract.md`
- **Worker APIs**:
  - `POST /internal/worker/claim`
  - `POST /internal/worker/heartbeat`
  - `POST /internal/worker/complete`
  - `POST /internal/worker/fail`
- **Admin / reconciliation**:
  - `POST /internal/admin/queue/reconcile` (requeues stuck PEL messages when there is no active DB lease)

### Tuning (env vars)

```env
WORKER_LEASE_TTL_SECONDS=60
WORKER_HEARTBEAT_INTERVAL_SECONDS=20
LEASE_REAPER_INTERVAL_SECONDS=15
PEL_RECONCILE_INTERVAL_SECONDS=30
PEL_IDLE_THRESHOLD_SECONDS=60
JOBS_CONSUMER_GROUP=workers
```

## Architecture

See:
- `ASYNC_SYNC_PATTERN.md` - Async architecture details
- `EXECUTION_ENGINE_INTEGRATION.md` - Execution Engine integration
- `MIGRATIONS.md` - Database migration guide

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test types
pytest tests/unit/        # Unit tests
pytest tests/integration/ # Integration tests
pytest tests/e2e/         # E2E tests
```

See `TESTING.md` for comprehensive testing guide.

## Development

See `MIGRATIONS.md` for database migration workflow.

## API Documentation

- **Interactive API Docs**: http://localhost:8080/docs (Swagger UI)
- **ReDoc**: http://localhost:8080/redoc
- **OpenAPI JSON**: http://localhost:8080/openapi.json
- **API Reference**: See `docs/API.md`

