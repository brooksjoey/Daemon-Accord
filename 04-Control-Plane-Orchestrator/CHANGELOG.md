# Changelog

All notable changes to the Control Plane Orchestrator.

## [Operational Release] - 2025

### Critical Architectural Fixes

#### Async/Sync DB Pattern Correction
- **Issue**: Codebase was using `AsyncEngine` but calling sync `Session()`, blocking the event loop
- **Fix**: Converted all database operations to use `AsyncSession` with proper `await` patterns
- **Impact**: 
  - Event loop no longer blocked by DB operations
  - Maximum concurrency and throughput achieved
  - Consistent async pattern throughout codebase
- **Files Modified**:
  - `job_orchestrator.py` - 9 Session calls converted to AsyncSession
  - `state_manager.py` - 4 Session calls converted to AsyncSession
  - `main.py` - Updated to pass Database instance
  - `database.py` - Ensured proper AsyncSession usage

### Added

- Production-ready Dockerfile
- Alembic migrations infrastructure
- Comprehensive operational documentation
- Health check endpoints
- Queue statistics API
- Idempotency engine
- Redis Streams queue implementation
- Execution Engine integration via ExecutorAdapter

### Infrastructure

- Full async architecture (FastAPI, AsyncSession, Redis async, Execution Engine)
- Database migrations via Alembic
- Structured logging with structlog
- Docker containerization ready
- docker-compose full-stack configuration

### Documentation

- `START_HERE.md` - Quick start guide
- `OPERATIONAL_STATUS.md` - System status report
- `OPERATIONAL_CHECKLIST.md` - Deployment checklist
- `MIGRATIONS.md` - Database migration guide
- `docs/architecture/ASYNC_DESIGN.md` - Async architecture documentation
- `docs/integration/EXECUTION_BRIDGE.md` - Execution Engine integration guide


