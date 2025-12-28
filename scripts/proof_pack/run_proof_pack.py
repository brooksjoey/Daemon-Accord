#!/usr/bin/env python3
"""
Production Proof Pack Generator

Generates buyer-grade evidence bundle proving Daemon Accord works end-to-end.
Can be run on any fresh VM to demonstrate system functionality.

Usage:
    python scripts/proof_pack/run_proof_pack.py [--jobs N] [--output-dir DIR]

Exit Codes:
    0: All checks passed
    1: Setup/health check failed
    2: Job execution failed
    3: Verification failed
    4: Artifact generation failed
"""
import argparse
import asyncio
import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import hashlib
import httpx
import random

# Configuration
DEFAULT_JOBS = 10
DEFAULT_OUTPUT_DIR = "proof_pack_artifacts"
CONTROL_PLANE_URL = os.getenv("CONTROL_PLANE_URL", "http://localhost:8082")
MEMORY_SERVICE_URL = os.getenv("MEMORY_SERVICE_URL", "http://localhost:8100")
HEALTH_CHECK_TIMEOUT = 300  # 5 minutes
JOB_COMPLETION_TIMEOUT = 600  # 10 minutes
POLL_INTERVAL = 2  # seconds

# Test targets (stable, legal to access)
TEST_TARGETS = [
    {
        "domain": "example.com",
        "url": "https://example.com",
        "description": "Example domain"
    },
    {
        "domain": "httpbin.org",
        "url": "https://httpbin.org/html",
        "description": "HTTP testing service"
    },
    {
        "domain": "jsonplaceholder.typicode.com",
        "url": "https://jsonplaceholder.typicode.com/",
        "description": "JSON API testing service"
    }
]

# Seed for deterministic randomness
RANDOM_SEED = 42
random.seed(RANDOM_SEED)


class ProofPackRunner:
    """Production Proof Pack generator."""
    
    def __init__(self, num_jobs: int, output_dir: str):
        self.num_jobs = num_jobs
        self.output_dir = Path(output_dir)
        self.artifact_dir = self.output_dir / datetime.now().strftime("%Y%m%d-%H%M%S")
        self.start_time = time.time()
        self.results = {
            "start_time": datetime.now().isoformat(),
            "environment": self._get_environment(),
            "jobs": [],
            "summary": {}
        }
        self.failed = False
        self.error_messages = []
    
    def _get_environment(self) -> Dict:
        """Get environment information."""
        return {
            "platform": platform.system(),
            "platform_version": platform.version(),
            "python_version": platform.python_version(),
            "control_plane_url": CONTROL_PLANE_URL,
            "memory_service_url": MEMORY_SERVICE_URL,
            "num_jobs": self.num_jobs,
            "random_seed": RANDOM_SEED
        }
    
    def log(self, message: str, level: str = "INFO"):
        """Log message to console and trace file."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"[{timestamp}] [{level}] {message}"
        print(log_line)
        
        # Also write to trace log
        trace_file = self.artifact_dir / "e2e_trace.log"
        trace_file.parent.mkdir(parents=True, exist_ok=True)
        with open(trace_file, "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
    
    def error(self, message: str, exit_code: int = 1):
        """Log error and set failure flag."""
        self.log(message, "ERROR")
        self.error_messages.append(message)
        self.failed = True
        return exit_code
    
    def run_command(self, cmd: List[str], check: bool = True, capture_output: bool = True) -> Tuple[int, str, str]:
        """Run shell command."""
        self.log(f"Running: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=capture_output,
                text=True,
                timeout=300,
                check=False
            )
            
            stdout = result.stdout if capture_output else ""
            stderr = result.stderr if capture_output else ""
            
            if check and result.returncode != 0:
                self.error(f"Command failed: {' '.join(cmd)}")
                self.log(f"STDOUT: {stdout}", "ERROR")
                self.log(f"STDERR: {stderr}", "ERROR")
            
            return result.returncode, stdout, stderr
        except subprocess.TimeoutExpired:
            self.error(f"Command timed out: {' '.join(cmd)}")
            return 1, "", "Timeout"
        except Exception as e:
            self.error(f"Command error: {e}")
            return 1, "", str(e)
    
    def _get_compose_cmd(self) -> List[str]:
        """Get docker compose command for current platform."""
        # Try docker compose (newer, Docker Desktop)
        result = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            return ["docker", "compose"]
        
        # Fall back to docker-compose (older, standalone)
        result = subprocess.run(
            ["docker-compose", "version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            return ["docker-compose"]
        
        # Default to docker compose
        return ["docker", "compose"]
    
    def step_1_start_services(self) -> int:
        """Step 1: Start services with docker compose."""
        self.log("=" * 60)
        self.log("STEP 1: Starting services with docker compose")
        self.log("=" * 60)
        
        # Determine docker compose command
        compose_cmd = self._get_compose_cmd()
        self.log(f"Using docker compose command: {' '.join(compose_cmd)}")
        
        compose_file = Path("05-Deploy-Monitoring-Infra/src/deploy/docker-compose.full.yml")
        if not compose_file.exists():
            return self.error(f"Docker compose file not found: {compose_file}")
        
        # Change to compose file directory
        original_dir = os.getcwd()
        compose_dir = compose_file.parent
        
        try:
            os.chdir(compose_dir)
            
            # Build and start services
            self.log("Building and starting services...")
            returncode, stdout, stderr = self.run_command(
                compose_cmd + ["-f", compose_file.name, "up", "--build", "-d"],
                check=True
            )
            
            if returncode != 0:
                return self.error("Failed to start services", 1)
            
            self.log("Services started. Waiting for health checks...")
            time.sleep(10)  # Give services time to start
            
            return 0
        finally:
            os.chdir(original_dir)
    
    async def step_2_health_checks(self) -> int:
        """Step 2: Verify all services are healthy."""
        self.log("=" * 60)
        self.log("STEP 2: Health checks")
        self.log("=" * 60)
        
        services = {
            "control_plane": CONTROL_PLANE_URL + "/health",
            "memory_service": MEMORY_SERVICE_URL + "/health",
        }
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            for service_name, url in services.items():
                self.log(f"Checking {service_name} at {url}...")
                
                try:
                    response = await client.get(url)
                    response.raise_for_status()
                    data = response.json()
                    
                    if data.get("status") == "healthy":
                        self.log(f"✅ {service_name} is healthy")
                    else:
                        return self.error(f"{service_name} health check failed: {data}", 1)
                        
                except Exception as e:
                    return self.error(f"{service_name} health check failed: {e}", 1)
        
        # Check docker services
        self.log("Checking docker services...")
        returncode, stdout, _ = self.run_command(["docker", "ps"], check=False)
        
        if returncode == 0:
            docker_ps_file = self.artifact_dir / "docker_ps.txt"
            docker_ps_file.parent.mkdir(parents=True, exist_ok=True)
            with open(docker_ps_file, "w", encoding="utf-8") as f:
                f.write(stdout)
            self.log(f"✅ Docker services running (saved to {docker_ps_file})")
        else:
            self.log("⚠️  Could not capture docker ps output", "WARN")
        
        return 0
    
    async def step_3_submit_jobs(self) -> int:
        """Step 3: Submit jobs across all executor levels."""
        self.log("=" * 60)
        self.log(f"STEP 3: Submitting {self.num_jobs} jobs across all strategies")
        self.log("=" * 60)
        
        strategies = ["vanilla", "stealth", "assault"]
        jobs_per_strategy = max(1, self.num_jobs // len(strategies))
        remaining = self.num_jobs - (jobs_per_strategy * len(strategies))
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            job_id = 0
            
            for strategy in strategies:
                count = jobs_per_strategy + (1 if remaining > 0 else 0)
                remaining -= 1
                
                self.log(f"Submitting {count} jobs with strategy '{strategy}'...")
                
                for i in range(count):
                    # Select target deterministically
                    target = TEST_TARGETS[job_id % len(TEST_TARGETS)]
                    
                    payload = {
                        "selector": "h1, title",
                        "extract": ["text"]
                    }
                    
                    params = {
                        "domain": target["domain"],
                        "url": target["url"],
                        "job_type": "navigate_extract",
                        "strategy": strategy,
                        "priority": random.choice([0, 1, 2, 3]),
                        "idempotency_key": f"proof-pack-{job_id}-{RANDOM_SEED}"
                    }
                    
                    try:
                        response = await client.post(
                            f"{CONTROL_PLANE_URL}/api/v1/jobs",
                            params=params,
                            json=payload
                        )
                        response.raise_for_status()
                        data = response.json()
                        job_id_value = data.get("job_id")
                        
                        self.results["jobs"].append({
                            "job_id": job_id_value,
                            "strategy": strategy,
                            "target": target,
                            "status": "submitted",
                            "submitted_at": datetime.now().isoformat()
                        })
                        
                        self.log(f"  ✅ Job {job_id_value} submitted ({strategy}, {target['domain']})")
                        job_id += 1
                        
                    except Exception as e:
                        return self.error(f"Failed to submit job {job_id}: {e}", 2)
        
        self.log(f"✅ All {self.num_jobs} jobs submitted")
        return 0
    
    async def step_4_wait_for_completion(self) -> int:
        """Step 4: Wait for all jobs to complete."""
        self.log("=" * 60)
        self.log("STEP 4: Waiting for job completion")
        self.log("=" * 60)
        
        start_time = time.time()
        completed_count = 0
        failed_count = 0
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            while time.time() - start_time < JOB_COMPLETION_TIMEOUT:
                pending = [j for j in self.results["jobs"] if j["status"] not in ["completed", "failed", "cancelled"]]
                
                if not pending:
                    break
                
                self.log(f"Waiting for {len(pending)} jobs to complete... ({completed_count} completed, {failed_count} failed)")
                
                for job in pending:
                    try:
                        response = await client.get(f"{CONTROL_PLANE_URL}/api/v1/jobs/{job['job_id']}")
                        response.raise_for_status()
                        data = response.json()
                        
                        status = data.get("status", "").lower()
                        job["status"] = status
                        job["status_data"] = data
                        
                        if status == "completed":
                            completed_count += 1
                            self.log(f"  ✅ Job {job['job_id']} completed")
                        elif status == "failed":
                            failed_count += 1
                            error_msg = data.get("error", "Unknown error")
                            self.log(f"  ❌ Job {job['job_id']} failed: {error_msg}", "WARN")
                        elif status in ["pending", "queued", "running"]:
                            pass  # Still processing
                        else:
                            self.log(f"  ⚠️  Job {job['job_id']} status: {status}", "WARN")
                            
                    except Exception as e:
                        self.log(f"  ⚠️  Error checking job {job['job_id']}: {e}", "WARN")
                
                await asyncio.sleep(POLL_INTERVAL)
            
            # Check if all completed
            still_pending = [j for j in self.results["jobs"] if j["status"] not in ["completed", "failed", "cancelled"]]
            
            if still_pending:
                return self.error(f"Timeout: {len(still_pending)} jobs did not complete", 2)
            
            self.log(f"✅ All jobs processed ({completed_count} completed, {failed_count} failed)")
            
            # Record summary
            self.results["summary"]["jobs_completed"] = completed_count
            self.results["summary"]["jobs_failed"] = failed_count
            self.results["summary"]["jobs_total"] = len(self.results["jobs"])
            
            # Consider it a failure if >50% failed
            if failed_count > len(self.results["jobs"]) * 0.5:
                return self.error(f"Too many job failures: {failed_count}/{len(self.results['jobs'])}", 2)
            
            return 0
    
    async def step_5_verify_persistence(self) -> int:
        """Step 5: Verify DB persistence and memory service storage."""
        self.log("=" * 60)
        self.log("STEP 5: Verifying persistence")
        self.log("=" * 60)
        
        completed_jobs = [j for j in self.results["jobs"] if j["status"] == "completed"]
        
        if not completed_jobs:
            return self.error("No completed jobs to verify", 3)
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            verified_count = 0
            
            for job in completed_jobs:
                job_id = job["job_id"]
                self.log(f"Verifying job {job_id}...")
                
                # Verify via API (which reads from DB)
                try:
                    response = await client.get(f"{CONTROL_PLANE_URL}/api/v1/jobs/{job_id}")
                    response.raise_for_status()
                    data = response.json()
                    
                    # Check result data exists
                    result = data.get("result")
                    if not result:
                        self.log(f"  ⚠️  Job {job_id} has no result data", "WARN")
                        continue
                    
                    # Verify result is not empty
                    if isinstance(result, dict) and len(result) == 0:
                        self.log(f"  ⚠️  Job {job_id} has empty result", "WARN")
                        continue
                    elif isinstance(result, str):
                        try:
                            parsed = json.loads(result)
                            if not parsed:
                                self.log(f"  ⚠️  Job {job_id} has empty parsed result", "WARN")
                                continue
                        except:
                            pass
                    
                    verified_count += 1
                    self.log(f"  ✅ Job {job_id} verified in database")
                    
                except Exception as e:
                    self.log(f"  ⚠️  Error verifying job {job_id}: {e}", "WARN")
            
            self.results["summary"]["jobs_verified"] = verified_count
            
            # Verify at least 50% of completed jobs have results
            if verified_count < len(completed_jobs) * 0.5:
                return self.error(f"Insufficient verification: {verified_count}/{len(completed_jobs)}", 3)
            
            self.log(f"✅ Verified {verified_count}/{len(completed_jobs)} completed jobs")
            
            # Try memory service (optional)
            try:
                response = await client.get(f"{MEMORY_SERVICE_URL}/health")
                if response.status_code == 200:
                    self.log("✅ Memory service is available (optional verification)")
                else:
                    self.log("ℹ️  Memory service not available (optional)", "INFO")
            except:
                self.log("ℹ️  Memory service not available (optional)", "INFO")
        
        return 0
    
    def step_6_generate_artifacts(self) -> int:
        """Step 6: Generate proof pack artifacts."""
        self.log("=" * 60)
        self.log("STEP 6: Generating artifacts")
        self.log("=" * 60)
        
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        
        # Calculate timings
        elapsed_time = time.time() - self.start_time
        self.results["end_time"] = datetime.now().isoformat()
        self.results["elapsed_seconds"] = round(elapsed_time, 2)
        self.results["summary"]["total_time_seconds"] = round(elapsed_time, 2)
        self.results["summary"]["success"] = not self.failed
        
        # Save run summary
        summary_file = self.artifact_dir / "run_summary.json"
        try:
            with open(summary_file, "w", encoding="utf-8") as f:
                json.dump(self.results, f, indent=2)
            self.log(f"✅ Saved run summary to {summary_file}")
        except Exception as e:
            return self.error(f"Failed to save run summary: {e}", 4)
        
        # Capture service logs
        self.log("Capturing service logs...")
        compose_file = Path("05-Deploy-Monitoring-Infra/src/deploy/docker-compose.full.yml")
        compose_dir = compose_file.parent
        compose_cmd = self._get_compose_cmd()
        
        original_dir = os.getcwd()
        try:
            os.chdir(compose_dir)
            
            services = ["control-plane", "execution-engine"]
            for service in services:
                log_file = self.artifact_dir / f"{service}_logs.txt"
                returncode, stdout, _ = self.run_command(
                    compose_cmd + ["-f", compose_file.name, "logs", "--tail=100", service],
                    check=False
                )
                if returncode == 0:
                    with open(log_file, "w", encoding="utf-8") as f:
                        f.write(stdout)
                    self.log(f"  ✅ Captured {service} logs")
        finally:
            os.chdir(original_dir)
        
        # Generate SHA256 manifest
        self.log("Generating SHA256 manifest...")
        manifest_file = self.artifact_dir / "sha256_manifest.txt"
        try:
            with open(manifest_file, "w", encoding="utf-8") as f:
                f.write(f"Proof Pack Artifacts - {datetime.now().isoformat()}\n")
                f.write("=" * 60 + "\n\n")
                
                for file_path in sorted(self.artifact_dir.rglob("*")):
                    if file_path.is_file() and file_path.name != "sha256_manifest.txt":
                        try:
                            with open(file_path, "rb") as file:
                                file_hash = hashlib.sha256(file.read()).hexdigest()
                            relative_path = file_path.relative_to(self.artifact_dir)
                            f.write(f"{file_hash}  {relative_path}\n")
                        except Exception as e:
                            f.write(f"ERROR: {relative_path} - {e}\n")
            
            self.log(f"✅ Generated SHA256 manifest: {manifest_file}")
        except Exception as e:
            return self.error(f"Failed to generate manifest: {e}", 4)
        
        self.log(f"✅ All artifacts saved to {self.artifact_dir}")
        return 0
    
    async def run(self) -> int:
        """Run the complete proof pack generation."""
        try:
            # Step 1: Start services
            if (code := self.step_1_start_services()) != 0:
                return code
            
            # Step 2: Health checks
            if (code := await self.step_2_health_checks()) != 0:
                return code
            
            # Step 3: Submit jobs
            if (code := await self.step_3_submit_jobs()) != 0:
                return code
            
            # Step 4: Wait for completion
            if (code := await self.step_4_wait_for_completion()) != 0:
                return code
            
            # Step 5: Verify persistence
            if (code := await self.step_5_verify_persistence()) != 0:
                return code
            
            # Step 6: Generate artifacts
            if (code := self.step_6_generate_artifacts()) != 0:
                return code
            
            # Final summary
            self.log("=" * 60)
            self.log("PROOF PACK GENERATION COMPLETE")
            self.log("=" * 60)
            self.log(f"✅ All checks passed")
            self.log(f"✅ Artifacts saved to: {self.artifact_dir}")
            self.log(f"✅ Total time: {self.results['summary']['total_time_seconds']}s")
            self.log(f"✅ Jobs completed: {self.results['summary'].get('jobs_completed', 0)}")
            
            return 0
            
        except KeyboardInterrupt:
            self.log("Interrupted by user", "ERROR")
            return 1
        except Exception as e:
            self.error(f"Unexpected error: {e}")
            return 1


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate Production Proof Pack for Daemon Accord"
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=DEFAULT_JOBS,
        help=f"Number of jobs to submit (default: {DEFAULT_JOBS})"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})"
    )
    
    args = parser.parse_args()
    
    runner = ProofPackRunner(args.jobs, args.output_dir)
    exit_code = asyncio.run(runner.run())
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

