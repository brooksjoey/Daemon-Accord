# Production Proof Pack

**Purpose:** Generate buyer-grade evidence bundle proving Daemon Accord works end-to-end on any fresh VM.

---

## Overview

The Production Proof Pack is a comprehensive validation suite that:
- ✅ Starts all services from scratch
- ✅ Verifies health of all components
- ✅ Submits jobs across all executor strategies (vanilla/stealth/assault)
- ✅ Waits for completion and verifies results
- ✅ Generates timestamped artifacts with SHA256 manifests

**Why This Matters:** Creates diligence-grade proof that survives a fresh VM deployment, directly supporting the $1.5M valuation.

---

## Quick Start

### Prerequisites

```bash
# Required
- Docker and Docker Compose
- Python 3.11+
- httpx library: pip install httpx

# Optional (for full validation)
- All services accessible (Control Plane, Memory Service, Execution Engine)
```

### Run Proof Pack

```bash
# Default: 10 jobs
python scripts/proof_pack/run_proof_pack.py

# Custom number of jobs
python scripts/proof_pack/run_proof_pack.py --jobs 20

# Custom output directory
python scripts/proof_pack/run_proof_pack.py --output-dir my_proof_pack
```

---

## What It Does

### Step 1: Start Services
- Runs `docker compose up --build -d`
- Waits for services to initialize
- **Exit Code 1:** If services fail to start

### Step 2: Health Checks
- Verifies Control Plane health endpoint
- Verifies Memory Service health endpoint (if available)
- Captures `docker ps` output
- **Exit Code 1:** If any service is unhealthy

### Step 3: Submit Jobs
- Submits N jobs (configurable, default 10)
- Distributes across all strategies:
  - Vanilla (fast, no evasion)
  - Stealth (basic evasion)
  - Assault (maximum evasion)
- Uses real targets:
  - `example.com` (standard)
  - `httpbin.org` (HTTP testing)
  - `jsonplaceholder.typicode.com` (JSON API)
- **Exit Code 2:** If job submission fails

### Step 4: Wait for Completion
- Polls job status every 2 seconds
- Timeout: 10 minutes
- Tracks completed/failed jobs
- **Exit Code 2:** If >50% jobs fail or timeout

### Step 5: Verify Persistence
- Verifies completed jobs have result data in database
- Checks via Control Plane API (reads from DB)
- Verifies Memory Service storage (if available)
- **Exit Code 3:** If <50% of completed jobs verified

### Step 6: Generate Artifacts
- Creates timestamped artifact directory
- Saves run summary, logs, manifests
- Generates SHA256 checksums
- **Exit Code 4:** If artifact generation fails

---

## Artifacts Generated

All artifacts are saved to: `proof_pack_artifacts/YYYYMMDD-HHMM/`

### Required Artifacts

1. **`run_summary.json`**
   - Start/end times
   - Environment information
   - Job details and statuses
   - Summary statistics
   - Pass/fail indicators

2. **`e2e_trace.log`**
   - Complete trace of all actions
   - Job IDs and statuses
   - Timestamps for each step
   - Error messages (if any)

3. **`docker_ps.txt`**
   - Output of `docker ps` command
   - Shows running containers
   - Service status

4. **`sha256_manifest.txt`**
   - SHA256 checksums of all artifacts
   - Verifies artifact integrity
   - Enables tamper detection

### Optional Artifacts

5. **`control-plane_logs.txt`**
   - Last 100 lines of Control Plane logs
   - Service startup and operation logs

6. **`execution-engine_logs.txt`**
   - Last 100 lines of Execution Engine logs
   - Job execution details

---

## Pass Criteria

### ✅ PASS Requirements

1. **Services Start:** All docker services start successfully
2. **Health Checks:** All services report healthy status
3. **Job Submission:** All jobs submitted successfully
4. **Job Completion:** At least 50% of jobs complete successfully
5. **Persistence:** At least 50% of completed jobs have verified results
6. **Artifacts:** All artifacts generated successfully

### ❌ FAIL Conditions

- **Exit Code 1:** Service startup or health check failure
- **Exit Code 2:** Job submission or execution failure (>50% fail)
- **Exit Code 3:** Persistence verification failure (<50% verified)
- **Exit Code 4:** Artifact generation failure

---

## Configuration

### Environment Variables

```bash
# Control Plane URL (default: http://localhost:8082)
export CONTROL_PLANE_URL=http://localhost:8082

# Memory Service URL (default: http://localhost:8100)
export MEMORY_SERVICE_URL=http://localhost:8100
```

### Command Line Options

```bash
--jobs N          # Number of jobs to submit (default: 10)
--output-dir DIR  # Output directory (default: proof_pack_artifacts)
```

### Test Targets

Test targets are defined in `run_proof_pack.py`:
- `example.com` - Standard example domain
- `httpbin.org` - HTTP testing service
- `jsonplaceholder.typicode.com` - JSON API service

To add custom targets, modify `TEST_TARGETS` in the script.

---

## Platform Support

### Windows

```powershell
# Install dependencies
pip install httpx

# Run proof pack
python scripts/proof_pack/run_proof_pack.py
```

**Notes:**
- Uses `docker compose` (Docker Desktop)
- Path handling is Windows-compatible
- All commands work in PowerShell

### Linux/macOS

```bash
# Install dependencies
pip install httpx

# Run proof pack
python3 scripts/proof_pack/run_proof_pack.py
```

**Notes:**
- Uses `docker-compose` (if installed) or `docker compose`
- Standard Unix path handling
- Works in bash/zsh

---

## Deterministic Behavior

The proof pack uses seeded randomness for repeatability:

- **Random Seed:** 42 (fixed)
- **Job Distribution:** Deterministic across strategies
- **Target Selection:** Round-robin through targets
- **Priority Selection:** Seeded random choice

This ensures:
- Same jobs submitted on each run
- Reproducible results
- Consistent artifact generation

---

## Example Output

```
============================================================
STEP 1: Starting services with docker compose
============================================================
[2024-12-24 10:00:00] [INFO] Building and starting services...
[2024-12-24 10:00:15] [INFO] Services started. Waiting for health checks...

============================================================
STEP 2: Health checks
============================================================
[2024-12-24 10:00:25] [INFO] Checking control_plane at http://localhost:8082/health...
[2024-12-24 10:00:25] [INFO] ✅ control_plane is healthy
[2024-12-24 10:00:26] [INFO] ✅ Docker services running

============================================================
STEP 3: Submitting 10 jobs across all strategies
============================================================
[2024-12-24 10:00:30] [INFO] Submitting 4 jobs with strategy 'vanilla'...
[2024-12-24 10:00:30] [INFO]   ✅ Job abc123 submitted (vanilla, example.com)
...
[2024-12-24 10:00:35] [INFO] ✅ All 10 jobs submitted

============================================================
STEP 4: Waiting for job completion
============================================================
[2024-12-24 10:00:40] [INFO] Waiting for 10 jobs to complete... (0 completed, 0 failed)
[2024-12-24 10:00:45] [INFO]   ✅ Job abc123 completed
...
[2024-12-24 10:02:00] [INFO] ✅ All jobs processed (9 completed, 1 failed)

============================================================
STEP 5: Verifying persistence
============================================================
[2024-12-24 10:02:05] [INFO] Verifying job abc123...
[2024-12-24 10:02:05] [INFO]   ✅ Job abc123 verified in database
...
[2024-12-24 10:02:10] [INFO] ✅ Verified 9/9 completed jobs

============================================================
STEP 6: Generating artifacts
============================================================
[2024-12-24 10:02:15] [INFO] ✅ Saved run summary to proof_pack_artifacts/20241224-100000/run_summary.json
[2024-12-24 10:02:16] [INFO] ✅ Generated SHA256 manifest

============================================================
PROOF PACK GENERATION COMPLETE
============================================================
[2024-12-24 10:02:20] [INFO] ✅ All checks passed
[2024-12-24 10:02:20] [INFO] ✅ Artifacts saved to: proof_pack_artifacts/20241224-100000
[2024-12-24 10:02:20] [INFO] ✅ Total time: 140.5s
[2024-12-24 10:02:20] [INFO] ✅ Jobs completed: 9
```

---

## Troubleshooting

### Services Won't Start

```bash
# Check docker compose file exists
ls 05-Deploy-Monitoring-Infra/src/deploy/docker-compose.full.yml

# Check docker is running
docker ps

# Try manual start
cd 05-Deploy-Monitoring-Infra/src/deploy
docker compose -f docker-compose.full.yml up -d
```

### Health Checks Fail

```bash
# Check service URLs
curl http://localhost:8082/health
curl http://localhost:8100/health

# Check service logs
docker compose -f docker-compose.full.yml logs control-plane
```

### Jobs Don't Complete

```bash
# Check Execution Engine worker
docker compose -f docker-compose.full.yml logs execution-engine

# Check Redis queue
docker compose -f docker-compose.full.yml exec redis redis-cli XINFO STREAM jobs:stream:normal

# Check database
docker compose -f docker-compose.full.yml exec postgres psql -U postgres -d accord_engine -c "SELECT status, COUNT(*) FROM jobs GROUP BY status;"
```

### Verification Fails

```bash
# Check job results directly
curl http://localhost:8082/api/v1/jobs/{job_id}

# Verify database directly
docker compose -f docker-compose.full.yml exec postgres psql -U postgres -d accord_engine -c "SELECT id, status, result IS NOT NULL as has_result FROM jobs WHERE status='completed' LIMIT 10;"
```

---

## Buyer Due Diligence

### What Buyers Can Do

1. **Run on Fresh VM:**
   ```bash
   git clone <repo>
   cd Accord-Engine
   python scripts/proof_pack/run_proof_pack.py
   ```

2. **Review Artifacts:**
   - Check `run_summary.json` for pass/fail
   - Review `e2e_trace.log` for detailed execution
   - Verify `sha256_manifest.txt` for integrity

3. **Validate Results:**
   - Verify job completion rates
   - Check result data exists
   - Confirm all services healthy

### Evidence Provided

- ✅ **Fresh VM Deployment:** Works from scratch
- ✅ **End-to-End Validation:** Complete flow verified
- ✅ **Multiple Strategies:** All executor modes tested
- ✅ **Real Targets:** Not just example.com
- ✅ **Persistence:** Database and storage verified
- ✅ **Reproducibility:** Deterministic, repeatable
- ✅ **Integrity:** SHA256 manifest for tamper detection

---

## Integration with CI/CD

The proof pack can be integrated into CI/CD:

```yaml
# .github/workflows/proof_pack.yml
name: Production Proof Pack
on:
  schedule:
    - cron: '0 0 * * 0'  # Weekly
  workflow_dispatch:

jobs:
  proof_pack:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install httpx
      - run: python scripts/proof_pack/run_proof_pack.py --jobs 20
      - uses: actions/upload-artifact@v3
        with:
          name: proof-pack-artifacts
          path: proof_pack_artifacts/
```

---

## Security Considerations

- ✅ **No Credentials:** Uses public test targets only
- ✅ **No Sensitive Data:** All artifacts are safe to share
- ✅ **Deterministic:** Seeded randomness, no secrets
- ✅ **Read-Only:** Only reads from services, doesn't modify

---

## Limitations

1. **Requires Docker:** Services must be containerized
2. **Network Access:** Needs internet for test targets
3. **Time:** Takes 5-15 minutes depending on job count
4. **Memory Service:** Optional, won't fail if unavailable

---

## Future Enhancements

- [ ] Support for custom test targets via config file
- [ ] Parallel job submission for faster execution
- [ ] Performance metrics collection
- [ ] Integration with monitoring stack
- [ ] Support for non-containerized deployments

---

**Last Updated:** 2024-12-24  
**Version:** 1.0.0

