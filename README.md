<div align="center">

# Daemon Accord

**Web Automation Platform**

*Enterprise-grade automation with production-ready workflows and scalable architecture*

---

[![Production Ready](https://img.shields.io/badge/Status-Production%20Ready-28a745?style=for-the-badge)](docs/business/PRODUCTION_READINESS_REPORT.md)
[![License](https://img.shields.io/badge/License-See%20LICENSE-0078d4?style=for-the-badge)](LICENSE)
[![Deployment](https://img.shields.io/badge/Deploy-One%20Command-00a4ef?style=for-the-badge)](docs/DEPLOYMENT.md)
[![Python](https://img.shields.io/badge/Python-3.11+-3776ab?style=for-the-badge&logo=python)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ed?style=for-the-badge&logo=docker)](docs/DEPLOYMENT.md)

</div>

---

## 🎯 Overview

Daemon Accord is a **web automation platform** designed for enterprise deployment. Built with production-ready workflows, scalable architecture, and comprehensive monitoring, it transforms web automation from a technical capability into a **reliable, production-grade service**.

**Key Differentiators:**
- ✅ **Production-Ready:** Deploy in minutes, validated on fresh VMs
- ✅ **Enterprise-Grade:** Advanced features for enterprise customers
- ✅ **Scalable Architecture:** Flexible deployment tiers from starter to enterprise
- ✅ **Workflow-First:** Ready-to-use workflows for common automation tasks

---

## 🚀 Quick Start

### Prerequisites

| Requirement | Specification |
|------------|---------------|
| **Docker** | Docker Desktop (Windows/Mac) or Docker Engine (Linux) |
| **Docker Compose** | v2.0+ |
| **RAM** | 8GB minimum (Starter) • 16GB (Professional) • 32GB+ (Enterprise) |
| **Network** | Access to target domains |

### Deploy in Minutes

<details>
<summary><b>Development Environment</b></summary>

```bash
docker compose up -d
```

**Features:**
- Multiple workers and browsers
- Auto-initializes database schema
- All services enabled

</details>

<details>
<summary><b>Production - Starter Tier (8GB RAM)</b></summary>

```bash
docker compose -f docker-compose.prod.yml up -d
```

**Perfect for:**
- Small teams and proof-of-concept
- Low-volume monitoring workflows
- Single worker, single browser
- Resource-efficient deployment

</details>

<details>
<summary><b>Production - Professional Tier (16GB RAM)</b></summary>

```bash
docker compose -f docker-compose.prod-medium.yml up -d
```

**Ideal for:**
- Growing organizations
- Moderate workloads (30 concurrent jobs)
- 2 workers, 3 browsers
- Full Memory Service integration

</details>

<details>
<summary><b>Production - Enterprise Tier (32GB+ RAM)</b></summary>

```bash
docker compose -f docker-compose.prod-enterprise.yml up -d
```

**Built for:**
- High-volume, mission-critical operations
- Maximum throughput (100+ concurrent jobs)
- 5 workers, 10 browsers
- Enterprise-grade performance and security

</details>

<details>
<summary><b>Windows PowerShell</b></summary>

```powershell
.\scripts\manage.ps1 up -Prod
```

**Management Commands:**
- `.\scripts\manage.ps1 up` - Start services
- `.\scripts\manage.ps1 status` - Show system status
- `.\scripts\manage.ps1 logs` - View logs
- `.\scripts\manage.ps1 down` - Stop services

</details>

### Verify Deployment

```bash
# Health check
curl http://localhost:8082/health

# Operator dashboard
curl http://localhost:8082/api/v1/ops/status
```

**Expected Response:**
```json
{
  "status": "healthy",
  "service": "control-plane",
  "workers": 1
}
```

**See:** [Deployment Guide](docs/DEPLOYMENT.md) for complete instructions and troubleshooting.

---

## ✨ Core Features

### ⚡ Core Automation Features

<table>
<tr>
<td width="50%">

**Job Orchestration**
- Priority-based job queuing
- Idempotency support
- Real-time status tracking

</td>
<td width="50%">

**Execution Strategies**
- Multiple execution modes
- Browser automation
- Custom executors

</td>
</tr>
<tr>
<td>

**Queue Management**
- Redis Streams backend
- Priority queues
- Dead letter queue support

</td>
<td>

**Monitoring & Observability**
- Health checks
- Queue statistics
- Operational dashboards

</td>
</tr>
</table>

### 📋 Production-Ready Workflows

| Workflow | Use Case | Value |
|----------|----------|-------|
| **Page Change Detection** | Monitor public pages for changes | Eliminates 5-10 hours/week manual checking |
| **Job Posting Monitor** | Extract structured job data | Replaces 8-15 hours/week job board monitoring |
| **Uptime/UX Smoke Check** | Verify page loads and elements | Prevents downtime, reduces QA time by 50% |

**See:** [Workflows Documentation](docs/WORKFLOWS.md) for complete details and examples.

### ⚙️ Execution Strategies

<div align="center">

| Strategy | Tier | Status | Use Case |
|---------|------|--------|----------|
| **Vanilla** | All | ✅ Production-Ready | Standard automation, public pages |
| **Stealth** | Professional+ | ✅ Production-Ready | Protected sites, basic evasion |
| **Assault** | Enterprise | ✅ Production-Ready | Highly protected sites, maximum evasion |
| **Ultimate Stealth** | Enterprise | 🔷 Enterprise Feature | Maximum stealth, human behavior simulation |
| **Custom Executor** | Enterprise | 🔷 Enterprise Feature | Custom logic, specialized evasion |

</div>

**See:** [Execution Strategies Guide](docs/EXECUTION_STRATEGIES.md) for complete details.

### 🔒 Security & Monitoring

<div align="center">

| Feature | Description | Status |
|---------|-------------|--------|
| **API Authentication** | API key support | ✅ Available |
| **Rate Limiting** | Request throttling | ✅ Available |
| **Health Monitoring** | Service health checks | ✅ Active |
| **Operational Dashboards** | Real-time system metrics | ✅ Active |

</div>

**See:** [Security Documentation](04-Control-Plane-Orchestrator/SECURITY.md) for complete details.

### ✅ Production Validation

- **Proof Pack:** End-to-end validation on any fresh VM
- **Test Coverage:** 50+ tests, >60% coverage (CI/CD enforced)
- **CI/CD Pipeline:** Automated tests, security scanning, quality gates
- **One-Command Deployment:** Docker Compose for dev and production

**See:** [Production Readiness Report](docs/business/PRODUCTION_READINESS_REPORT.md) for complete validation evidence.

---

## 🏗️ Architecture

<div align="center">

```
┌─────────────────────────────────────────────────────────────┐
│                    Daemon Accord Platform                    │
└─────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
   ┌────▼────┐          ┌────▼────┐          ┌────▼────┐
   │ Control │          │ Safety │          │Memory   │
   │  Plane  │          │  Layer │          │Service  │
   │         │          │        │          │         │
   │ FastAPI │          │Circuit │          │   AI    │
   │  Redis  │          │Breakers│          │Learning │
   │   Jobs  │          │  Rate  │          │ Vectors │
   └────┬────┘          │ Limits │          └────┬────┘
        │               └────┬───┘               │
        │                     │                   │
        └──────────┬──────────┴──────────┬────────┘
                   │                     │
            ┌──────▼──────┐      ┌──────▼──────┐
            │  Execution  │      │  Deploy     │
            │   Engine    │      │  Infra      │
            │             │      │             │
            │  Playwright │      │  Docker     │
            │  Evasion    │      │ Kubernetes  │
            │  Strategies │      │ Monitoring  │
            └─────────────┘      └─────────────┘
```

</div>

### Component Overview

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **01-Core-Execution-Engine** | Playwright, Python | Browser automation, evasion techniques |
| **02-Safety-Observability** | Redis, Circuit Breakers | Rate limiting, artifact capture, safety |
| **03-Intelligence-Memory-Service** | PostgreSQL, pgvector | AI learning, strategy optimization |
| **04-Control-Plane-Orchestrator** | FastAPI, Redis Streams | Job orchestration, queue management |
| **05-Deploy-Monitoring-Infra** | Docker, Kubernetes | Containerization, monitoring |

**See:** [Architecture Documentation](docs/ARCHITECTURE_CONTAINER.md) for complete details.

---

## 📚 Documentation

### Getting Started

<div align="center">

| Document | Description | Link |
|----------|-------------|------|
| **Deployment Guide** | Complete deployment instructions | [📖 View](docs/DEPLOYMENT.md) |
| **API Usage** | API examples and integration | [📖 View](04-Control-Plane-Orchestrator/docs/API_USAGE.md) |
| **Workflows** | Workflow templates and usage | [📖 View](docs/WORKFLOWS.md) |

</div>

### Production

<div align="center">

| Document | Description | Link |
|----------|-------------|------|
| **Security** | Security features and best practices | [📖 View](04-Control-Plane-Orchestrator/SECURITY.md) |
| **Execution Strategies** | Strategy guide and positioning | [📖 View](docs/EXECUTION_STRATEGIES.md) |
| **Proof Pack** | Production validation guide | [📖 View](docs/PROOF_PACK.md) |

</div>

### Sales Materials

<div align="center">

| Document | Description | Link |
|----------|-------------|------|
| **One Pager** | Problem, solution, workflows, pricing | [📖 View](sales/ONE_PAGER.md) |
| **Pilot Proposal** | 2-week pilot scope and pricing | [📖 View](sales/PILOT_PROPOSAL.md) |
| **ROI Calculator** | ROI calculation model | [📖 View](sales/ROI_CALCULATOR.md) |

</div>

---

## 🔌 API Reference

### Core Operations

<div align="center">

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | `GET` | Health check |
| `/api/v1/jobs` | `POST` | Create job |
| `/api/v1/jobs/{job_id}` | `GET` | Get job status |
| `/api/v1/queue/stats` | `GET` | Queue statistics |
| `/api/v1/ops/status` | `GET` | Operator dashboard |

</div>

### Workflows

<div align="center">

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/workflows` | `GET` | List available workflows |
| `/api/v1/workflows/{name}` | `GET` | Get workflow details |
| `/api/v1/workflows/{name}/run` | `POST` | Execute workflow |

</div>

**Interactive Documentation:** `http://localhost:8082/docs` (Swagger UI)

**See:** [API Documentation](04-Control-Plane-Orchestrator/docs/API.md) for complete reference.

---

## 🧪 Production Proof Pack

Validate end-to-end functionality on any fresh VM:

```bash
python scripts/proof_pack/run_proof_pack.py
```

**Generates:** `proof_pack_artifacts/YYYYMMDD-HHMM/` with complete validation evidence:

- ✅ Run summary with pass/fail indicators
- ✅ E2E trace log with complete execution details
- ✅ Docker service status
- ✅ SHA256 manifest for integrity verification

**See:** [Proof Pack Documentation](docs/PROOF_PACK.md) for complete guide.

---

## 🧪 Testing

```bash
# Run all tests
pytest

# With coverage
pytest --cov=src --cov-report=html

# Specific test types
pytest tests/unit/        # Unit tests
pytest tests/integration/ # Integration tests
pytest tests/e2e/        # E2E tests
```

**Coverage:** >60% (enforced in CI/CD)

---

## 🎯 Use Cases

### Legal & Compliance

<div align="center">

| Use Case | Workflow | Value |
|----------|----------|-------|
| **Terms of Service Monitoring** | Page Change Detection | Detect legal document changes |
| **Compliance Verification** | Page Change Detection | Monitor regulatory pages |
| **Documentation Tracking** | Page Change Detection | Track public documentation changes |

</div>

### Business Intelligence

<div align="center">

| Use Case | Workflow | Value |
|----------|----------|-------|
| **Competitive Monitoring** | Page Change Detection | Track competitor pricing, features |
| **Job Market Analysis** | Job Posting Monitor | Monitor job postings across boards |
| **Market Research** | All Workflows | Automated data collection |

</div>

### Operations & Quality

<div align="center">

| Use Case | Workflow | Value |
|----------|----------|-------|
| **Uptime Monitoring** | Uptime Smoke Check | Verify critical pages load |
| **Quality Assurance** | Uptime Smoke Check | Automated smoke tests |
| **Performance Monitoring** | Uptime Smoke Check | Track page load times |

</div>

---

## 🔐 Security

### Security Features

<div align="center">

| Feature | Description | Status |
|---------|-------------|--------|
| **API Authentication** | API key authentication | ✅ Available |
| **Rate Limiting** | Request throttling | ✅ Available |
| **HTTPS Support** | TLS encryption | ✅ Configurable |
| **Input Validation** | Request validation | ✅ Active |

</div>

**See:** [Security Documentation](04-Control-Plane-Orchestrator/SECURITY.md) for complete details.

---

## 📊 Production Readiness

<div align="center">

| Validation | Status | Evidence |
|------------|--------|----------|
| **Production Proof Pack** | ✅ Validated | Fresh VM deployment verified |
| **Test Coverage** | ✅ >60% | 50+ tests, CI/CD enforced |
| **CI/CD Pipeline** | ✅ Active | Automated quality gates |
| **Documentation** | ✅ Complete | API, security, deployment guides |
| **Deployment** | ✅ Flexible Tiers | Starter (8GB) → Professional (16GB) → Enterprise (32GB+) |

</div>

**See:** [Production Readiness Report](docs/business/PRODUCTION_READINESS_REPORT.md) for complete validation.

---

## 🚀 Flexible Deployment Tiers

Daemon Accord scales from **starter deployments to enterprise-grade infrastructure**. Choose the tier that matches your workload and infrastructure.

<div align="center">

| Tier | RAM | Workers | Browsers | Concurrency | Use Case | Command |
|------|-----|---------|----------|-------------|----------|---------|
| **Starter** | 8GB | 1 | 1 | 10 jobs | Small teams, low-volume monitoring | `docker compose -f docker-compose.prod.yml up` |
| **Professional** | 16GB | 2 | 3 | 30 jobs | Growing teams, moderate workloads | `docker compose -f docker-compose.prod-medium.yml up` |
| **Enterprise** | 32GB+ | 5 | 10 | 100 jobs | High-volume, mission-critical operations | `docker compose -f docker-compose.prod-enterprise.yml up` |
| **Development** | Any | 3 | 5 | 50 jobs | Local development, testing | `docker compose up` |

</div>

### 🎯 Tier Capabilities

<div align="center">

| Feature | Starter | Professional | Enterprise |
|---------|---------|--------------|------------|
| **Concurrent Jobs** | 10 | 30 | 100+ |
| **Browser Instances** | 1 | 3 | 10 |
| **Worker Processes** | 1 | 2 | 5 |
| **PostgreSQL Memory** | 1GB | 2GB | 8GB |
| **Redis Memory** | 256MB | 512MB | 2GB |
| **Memory Service** | Optional | Enabled | Enabled + Optimized |
| **API Authentication** | Optional | Optional | Enabled by Default |
| **Horizontal Scaling** | Single Node | Single Node | Multi-Node Ready |
| **Advanced Executors** | ✅ | ✅ | ✅ |
| **AI/ML Features** | Basic | Full | Full + Optimized |

</div>

### ⚡ Performance Characteristics

**Starter Tier (8GB):**
- Perfect for small teams and proof-of-concept deployments
- Handles 10 concurrent jobs efficiently
- Ideal for monitoring workflows and low-volume automation
- **Deploy:** `docker compose -f docker-compose.prod.yml up -d`

**Professional Tier (16GB):**
- Balanced performance for growing organizations
- 3x browser capacity, 3x concurrency vs. Starter
- Full Memory Service integration for AI-powered optimization
- **Deploy:** `docker compose -f docker-compose.prod-medium.yml up -d`

**Enterprise Tier (32GB+):**
- Maximum throughput and scalability
- 10x browser capacity, 10x concurrency vs. Starter
- Optimized PostgreSQL and Redis for high-volume operations
- API authentication enabled by default
- Ready for horizontal scaling across multiple nodes
- **Deploy:** `docker compose -f docker-compose.prod-enterprise.yml up -d`

**Development Mode:**
- Full feature set for local development
- Auto-initializes database schema
- All services enabled for testing
- **Deploy:** `docker compose up -d`

### 🔄 Easy Tier Migration

Upgrading between tiers is **configuration-only**—no code changes required:

1. **Stop current deployment:** `docker compose down`
2. **Switch compose file:** Use the tier-specific compose file
3. **Deploy new tier:** `docker compose -f docker-compose.prod-[tier].yml up -d`

All tiers use the same codebase and support the same features. The difference is resource allocation and scaling parameters.

**See:** [Deployment Guide](docs/DEPLOYMENT.md) for complete configuration details and troubleshooting.

---

## 📦 What's Included

<div align="center">

| Category | Contents |
|----------|----------|
| **Source Code** | All 5 components (Core, Safety, Memory, Control Plane, Deploy) |
| **Workflows** | 3 production-ready workflows |
| **Security** | API authentication and rate limiting |
| **Tests** | Unit, integration, E2E tests (50+) |
| **Documentation** | Complete guides and API docs |
| **Deployment** | Docker Compose for dev and production |
| **Validation** | Production proof pack suite |

</div>

---

## 🛠️ Technology Stack

<div align="center">

| Layer | Technology |
|-------|-----------|
| **Backend** | Python 3.11+, FastAPI, SQLAlchemy, SQLModel |
| **Browser Automation** | Playwright |
| **Queue System** | Redis Streams |
| **Database** | PostgreSQL with asyncpg |
| **Containerization** | Docker, Docker Compose |
| **Testing** | pytest, pytest-asyncio, pytest-cov |
| **CI/CD** | GitHub Actions |

</div>

---

## 📄 License

See [LICENSE](LICENSE) file for details.

---

## 📞 Support & Resources

<div align="center">

| Resource | Description |
|----------|-------------|
| **Documentation** | See `docs/` directory |
| **Deployment Issues** | [Troubleshooting Guide](docs/DEPLOYMENT.md#troubleshooting) |
| **API Questions** | [API Documentation](04-Control-Plane-Orchestrator/docs/API.md) |
| **Security** | [Security & Compliance Guide](docs/SECURITY_AND_COMPLIANCE.md) |

</div>

---

<div align="center">

---

## Daemon Accord

**Web Automation Platform**

[Production Ready](docs/business/PRODUCTION_READINESS_REPORT.md) • [Documentation](docs/) • [Deployment](docs/DEPLOYMENT.md) • [Security](04-Control-Plane-Orchestrator/SECURITY.md)

**Version 1.0.0** • **Last Updated:** 2024-01-01

---

</div>
