# Daemon Accord - Deployment Guide

Complete guide for deploying Daemon Accord in development and production environments.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Quick Start](#quick-start)
3. [Environment Variables](#environment-variables)
4. [Deployment Modes](#deployment-modes)
5. [Port Configuration](#port-configuration)
6. [Low-Resource Mode (8GB RAM)](#low-resource-mode-8gb-ram)
7. [Scaling](#scaling)
8. [Windows Management](#windows-management)
9. [Troubleshooting](#troubleshooting)
10. [Production Checklist](#production-checklist)

---

## Prerequisites

### System Requirements

**Development:**
- Docker Desktop (Windows/Mac) or Docker Engine (Linux)
- Docker Compose v2.0+
- 4GB RAM minimum
- 10GB disk space

**Production (Low-Resource Mode):**
- Docker Engine 20.10+
- Docker Compose v2.0+
- 8GB RAM minimum
- 20GB disk space
- Linux (recommended) or Windows Server

### Software Dependencies

- Docker and Docker Compose installed and running
- Network access to download Docker images
- Ports available (see [Port Configuration](#port-configuration))

---

## Quick Start

### Development Mode

```bash
# Start all services
docker compose up -d

# Or using Windows PowerShell script
.\scripts\manage.ps1 up

# Check status
.\scripts\manage.ps1 status

# View logs
.\scripts\manage.ps1 logs
```

### Production Mode

```bash
# Start in production mode (low-resource)
docker compose -f docker-compose.prod.yml up -d

# Or using Windows PowerShell script
.\scripts\manage.ps1 up -Prod

# Check status
.\scripts\manage.ps1 status
```

---

## Environment Variables

### Control Plane

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql+asyncpg://postgres:postgres@postgres:5432/daemon_accord` | PostgreSQL connection string |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection URL |
| `MAX_CONCURRENT_JOBS` | `100` (dev) / `10` (prod) | Maximum concurrent job executions |
| `WORKER_COUNT` | `5` (dev) / `1` (prod) | Number of worker processes |
| `API_HOST` | `0.0.0.0` | API server host |
| `API_PORT` | `8080` | API server port |
| `SKIP_INIT_MODELS` | `false` (dev) / `true` (prod) | Use migrations instead of auto-init |
| `ENABLE_AUTH` | `false` | Enable API key authentication |
| `API_KEY` | (none) | API key for authentication |
| `MEMORY_SERVICE_URL` | (none) | Memory Service base URL (optional) |

### Execution Engine

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_BROWSERS` | `5` (dev) / `1` (prod) | Maximum browser instances |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection URL |
| `DATABASE_URL` | `postgresql+asyncpg://postgres:postgres@postgres:5432/daemon_accord` | PostgreSQL connection string |

### PostgreSQL

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_USER` | `postgres` | Database user |
| `POSTGRES_PASSWORD` | `postgres` | Database password |
| `POSTGRES_DB` | `daemon_accord` | Database name |
| `POSTGRES_PORT` | `5432` | Database port |

### Redis

| Variable | Default | Description |
|----------|---------|-------------|
| (none) | - | Redis uses default configuration |

---

## Deployment Modes

### Development Mode (`docker-compose.yml`)

**Purpose:** Local development and testing

**Configuration:**
- Multiple browser instances (5)
- Multiple workers (3)
- Higher concurrency (50 jobs)
- Auto-initializes database schema
- All services enabled

**Start:**
```bash
docker compose up -d
```

**Use Cases:**
- Local development
- Testing new features
- Debugging
- Integration testing

### Production Mode (`docker-compose.prod.yml`)

**Purpose:** Production deployment on resource-constrained machines

**Configuration:**
- Single browser instance (1)
- Single worker (1)
- Low concurrency (10 jobs)
- Uses database migrations
- Resource limits enforced
- PostgreSQL optimized for low memory

**Start:**
```bash
docker compose -f docker-compose.prod.yml up -d
```

**Use Cases:**
- Production deployment
- 8GB RAM machines
- Cost-optimized deployments
- Single-tenant installations

---

## Port Configuration

### Default Ports

| Service | Port | Description |
|---------|------|-------------|
| Control Plane API | `8082` | Main API endpoint |
| Execution Engine | `8081` | Execution Engine API (internal) |
| PostgreSQL | `5432` | Database |
| Redis | `6379` | Cache and queue |
| Memory Service | `8100` | Intelligence service (optional) |

### Customizing Ports

Set environment variables before starting:

```bash
# Custom ports
export CONTROL_PLANE_PORT=9000
export EXECUTION_ENGINE_PORT=9001
export POSTGRES_PORT=5433

docker compose -f docker-compose.prod.yml up -d
```

Or create a `.env` file:

```env
CONTROL_PLANE_PORT=9000
EXECUTION_ENGINE_PORT=9001
POSTGRES_PORT=5433
```

---

## Low-Resource Mode (8GB RAM)

### Configuration

Production mode (`docker-compose.prod.yml`) is optimized for 8GB RAM machines:

**Resource Limits:**
- Redis: 256MB max
- PostgreSQL: 1GB max
- Execution Engine: 2GB max
- Control Plane: 512MB max
- **Total: ~4GB reserved**

**Concurrency Limits:**
- `MAX_BROWSERS=1` - Single browser instance
- `WORKER_COUNT=1` - Single worker process
- `MAX_CONCURRENT_JOBS=10` - Low concurrency

**PostgreSQL Tuning:**
- `shared_buffers=128MB`
- `effective_cache_size=512MB`
- `work_mem=16MB`
- Optimized for low-memory operation

### Enforcing Low-Resource Mode

Set these environment variables:

```bash
export MAX_BROWSERS=1
export WORKER_COUNT=1
export MAX_CONCURRENT_JOBS=10

docker compose -f docker-compose.prod.yml up -d
```

Or in `.env` file:

```env
MAX_BROWSERS=1
WORKER_COUNT=1
MAX_CONCURRENT_JOBS=10
```

### Memory Service (Optional)

Memory Service is disabled by default in production mode. To enable:

```bash
docker compose -f docker-compose.prod.yml --profile memory-service up -d
```

**Note:** This adds ~512MB memory usage.

---

## Scaling

### Horizontal Scaling

**Control Plane:**
- Run multiple Control Plane instances behind a load balancer
- Share the same Redis and PostgreSQL
- Use sticky sessions if needed

**Execution Engine:**
- Run multiple Execution Engine workers
- Each worker consumes from the same Redis Stream
- Automatically distributes load

**Example:**
```bash
# Scale Execution Engine to 3 workers
docker compose -f docker-compose.prod.yml up -d --scale execution-engine=3
```

### Vertical Scaling

**Increase Resources:**
1. Edit `docker-compose.prod.yml`
2. Update `deploy.resources.limits.memory`
3. Increase `MAX_BROWSERS`, `WORKER_COUNT`, `MAX_CONCURRENT_JOBS`
4. Restart services

**Example (16GB RAM machine):**
```yaml
execution-engine:
  environment:
    MAX_BROWSERS: 5
  deploy:
    resources:
      limits:
        memory: 4G

control-plane:
  environment:
    WORKER_COUNT: 3
    MAX_CONCURRENT_JOBS: 50
  deploy:
    resources:
      limits:
        memory: 1G
```

---

## Windows Management

### PowerShell Script (`scripts/manage.ps1`)

**Available Commands:**

```powershell
# Start services (dev mode)
.\scripts\manage.ps1 up

# Start services (production mode)
.\scripts\manage.ps1 up -Prod

# Stop services
.\scripts\manage.ps1 down

# View logs (all services)
.\scripts\manage.ps1 logs

# View logs (specific service)
.\scripts\manage.ps1 logs -Service control-plane

# Run production proof pack
.\scripts\manage.ps1 proof

# Show system status
.\scripts\manage.ps1 status

# Restart services
.\scripts\manage.ps1 restart

# Clean up (remove containers, volumes, images)
.\scripts\manage.ps1 clean
```

**Status Output:**
- Running services
- Control Plane health
- Queue depth
- Success rate
- Recent jobs

---

## Troubleshooting

### Services Won't Start

**Check Docker:**
```bash
docker ps
docker compose -f docker-compose.prod.yml ps
```

**Check Logs:**
```bash
docker compose -f docker-compose.prod.yml logs
```

**Common Issues:**
1. **Port conflicts:** Change ports in `.env` or `docker-compose.prod.yml`
2. **Insufficient memory:** Use production mode or increase Docker memory limits
3. **Database connection:** Verify `DATABASE_URL` format (must use `asyncpg://`)

### Control Plane Not Responding

**Check Health:**
```bash
curl http://localhost:8082/health
```

**Check Ops Status:**
```bash
curl http://localhost:8082/api/v1/ops/status
```

**Common Issues:**
1. **Database not ready:** Wait for PostgreSQL health check
2. **Redis not ready:** Wait for Redis health check
3. **Worker startup:** Check logs for initialization errors

### High Memory Usage

**Enable Low-Resource Mode:**
```bash
export MAX_BROWSERS=1
export WORKER_COUNT=1
export MAX_CONCURRENT_JOBS=10
docker compose -f docker-compose.prod.yml up -d
```

**Monitor Resources:**
```bash
docker stats
```

**Reduce PostgreSQL Memory:**
Edit `docker-compose.prod.yml`:
```yaml
postgres:
  command: >
    postgres
    -c shared_buffers=64MB
    -c effective_cache_size=256MB
```

### Database Migration Issues

**Run Migrations Manually:**
```bash
docker compose -f docker-compose.prod.yml exec control-plane alembic upgrade head
```

**Reset Database (Development Only):**
```bash
docker compose -f docker-compose.prod.yml down -v
docker compose -f docker-compose.prod.yml up -d
```

### Jobs Not Processing

**Check Queue:**
```bash
curl http://localhost:8082/api/v1/queue/stats
```

**Check Execution Engine:**
```bash
docker compose -f docker-compose.prod.yml logs execution-engine
```

**Common Issues:**
1. **Execution Engine not running:** Start with `docker compose up -d execution-engine`
2. **Redis connection:** Verify `REDIS_URL` in Execution Engine
3. **Browser pool:** Check Execution Engine logs for browser initialization errors

---

## Production Checklist

### Pre-Deployment

- [ ] Review and set environment variables
- [ ] Configure database credentials
- [ ] Set up API authentication (if enabled)
- [ ] Configure resource limits
- [ ] Set up monitoring/logging
- [ ] Test on staging environment

### Deployment

- [ ] Start infrastructure (PostgreSQL, Redis)
- [ ] Run database migrations
- [ ] Start Control Plane
- [ ] Start Execution Engine
- [ ] Verify health endpoints
- [ ] Test job creation
- [ ] Monitor resource usage

### Post-Deployment

- [ ] Verify all services healthy
- [ ] Check queue processing
- [ ] Monitor success rate
- [ ] Set up alerts
- [ ] Document custom configurations
- [ ] Backup database

### Security

- [ ] Change default passwords
- [ ] Enable API authentication
- [ ] Configure firewall rules
- [ ] Set up SSL/TLS (if exposed)
- [ ] Review access controls
- [ ] Enable rate limiting

---

## Additional Resources

- **API Documentation:** `04-Control-Plane-Orchestrator/docs/API.md`
- **Architecture:** `docs/ARCHITECTURE_CONTAINER.md`
- **Workflows:** `docs/WORKFLOWS.md`
- **Security:** `04-Control-Plane-Orchestrator/SECURITY.md`
- **Proof Pack:** `docs/PROOF_PACK.md`

---

## Support

For issues or questions:
1. Check logs: `.\scripts\manage.ps1 logs`
2. Check status: `.\scripts\manage.ps1 status`
3. Review troubleshooting section
4. Check GitHub issues

---

**Last Updated:** 2024-01-01  
**Version:** 1.0.0

