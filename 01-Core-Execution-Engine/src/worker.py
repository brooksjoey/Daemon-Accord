#!/usr/bin/env python3
"""
Execution Engine Worker

Consumes jobs from Redis Streams and executes them using the Execution Engine.
"""
import asyncio
import os
import sys
import json
import logging
from typing import Dict, Any, Optional
import redis.asyncio as redis
import httpx

# Add src to path
sys.path.insert(0, os.path.dirname(__file__))

from core.browser_pool import BrowserPool
from core.standard_executor import StandardExecutor
from strategies import StrategyExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ControlPlaneClient:
    """
    Minimal client for the Worker ↔ Control Plane contract (v1).

    Worker never ACKs transport until Control Plane returns ack=true
    after committing DB state.
    """

    def __init__(self, base_url: str, worker_id: str, timeout_s: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.worker_id = worker_id
        self._client = httpx.AsyncClient(timeout=timeout_s)

    async def claim(self, streams: list[str], max_wait_ms: int = 5000) -> Optional[Dict[str, Any]]:
        r = await self._client.post(
            f"{self.base_url}/internal/worker/claim",
            json={"worker_id": self.worker_id, "streams": streams, "max_wait_ms": max_wait_ms},
        )
        r.raise_for_status()
        data = r.json()
        return data if data.get("claimed") else None

    async def heartbeat(self, job_id: str, lease_token: str) -> None:
        r = await self._client.post(
            f"{self.base_url}/internal/worker/heartbeat",
            json={"worker_id": self.worker_id, "job_id": job_id, "lease_token": lease_token},
        )
        r.raise_for_status()

    async def complete(
        self,
        *,
        job_id: str,
        attempt_id: str,
        lease_token: str,
        stream: str,
        message_id: str,
        result: Dict[str, Any],
    ) -> Dict[str, Any]:
        r = await self._client.post(
            f"{self.base_url}/internal/worker/complete",
            json={
                "worker_id": self.worker_id,
                "job_id": job_id,
                "attempt_id": attempt_id,
                "lease_token": lease_token,
                "stream": stream,
                "message_id": message_id,
                "result": result,
            },
        )
        r.raise_for_status()
        return r.json()

    async def fail(
        self,
        *,
        job_id: str,
        attempt_id: str,
        lease_token: str,
        stream: str,
        message_id: str,
        error: Dict[str, Any],
    ) -> Dict[str, Any]:
        r = await self._client.post(
            f"{self.base_url}/internal/worker/fail",
            json={
                "worker_id": self.worker_id,
                "job_id": job_id,
                "attempt_id": attempt_id,
                "lease_token": lease_token,
                "stream": stream,
                "message_id": message_id,
                "error": error,
            },
        )
        r.raise_for_status()
        return r.json()

    async def aclose(self) -> None:
        await self._client.aclose()


class ExecutionWorker:
    """Worker that consumes jobs from Redis and executes them."""
    
    def __init__(
        self,
        redis_url: str,
        max_browsers: int = 20,
        worker_id: str = "execution-worker",
        control_plane_url: str = "http://localhost:8080",
    ):
        self.redis_url = redis_url
        self.max_browsers = max_browsers
        self.worker_id = worker_id
        self.control_plane_url = control_plane_url
        self.consumer_group = os.getenv("JOBS_CONSUMER_GROUP", "workers")
        self.redis_client = None
        self.browser_pool = None
        self.strategy_executor = None
        self.running = False
        self.cp: ControlPlaneClient | None = None
    
    async def initialize(self):
        """Initialize connections and resources."""
        # Redis
        self.redis_client = await redis.from_url(self.redis_url, decode_responses=True)
        self.cp = ControlPlaneClient(base_url=self.control_plane_url, worker_id=self.worker_id)
        
        # Browser pool
        self.browser_pool = BrowserPool(max_instances=self.max_browsers)
        await self.browser_pool.initialize()
        
        # Strategy executor
        self.strategy_executor = StrategyExecutor(
            browser_pool=self.browser_pool,
            redis_client=self.redis_client
        )
        
        logger.info("Execution worker initialized")
    
    async def process_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single job."""
        job_id = job_data.get('id', 'unknown')
        logger.info(f"Processing job {job_id}")
        
        try:
            # Create a simple job-like object for StrategyExecutor and executors
            # Executors expect a Job object with: id, url, type, payload attributes
            class JobObj:
                def __init__(self, data):
                    self.id = data.get('id', 'unknown')
                    self.url = data.get('url', '')
                    self.type = data.get('type', 'navigate_extract')
                    # payload can be a dict or needs to be parsed from string
                    payload = data.get('payload', {})
                    if isinstance(payload, str):
                        try:
                            payload = json.loads(payload)
                        except:
                            payload = {}
                    self.payload = payload
                    self.strategy = data.get('strategy', 'vanilla')
            
            job_obj = JobObj(job_data)
            
            # Get executor based on strategy
            executor = self.strategy_executor.get_executor(job_obj)
            
            # Execute job - pass the Job object, not a dict
            result = await executor.execute(job_obj)
            
            # Return result
            return {
                'success': result.success if hasattr(result, 'success') else True,
                'data': result.data if hasattr(result, 'data') else {},
                'artifacts': result.artifacts if hasattr(result, 'artifacts') else {},
                'error': result.error if hasattr(result, 'error') else None,
                'execution_time': result.execution_time if hasattr(result, 'execution_time') else 0.0
            }
        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}", exc_info=True)
            return {
                'success': False,
                'data': {},
                'artifacts': {},
                'error': str(e),
                'execution_time': 0.0
            }
    
    async def _run_once(self) -> None:
        """
        One claim → execute → complete/fail cycle.

        The worker does NOT read/ACK streams directly until Control Plane
        returns ack=true (after DB commit).
        """
        if not self.cp or not self.redis_client:
            return

        streams = ["jobs:stream:emergency", "jobs:stream:high", "jobs:stream:normal", "jobs:stream:low"]
        claim = await self.cp.claim(streams=streams, max_wait_ms=5000)
        if not claim:
            return

        job_id = claim["job_id"]
        attempt_id = claim["attempt_id"]
        lease_token = claim["lease_token"]
        stream = claim["stream"]
        message_id = claim["message_id"]
        job_snapshot = claim.get("payload") or {}

        job_data = {
            "id": job_snapshot.get("id", job_id),
            "domain": job_snapshot.get("domain", ""),
            "url": job_snapshot.get("url", ""),
            "type": job_snapshot.get("type", "navigate_extract"),
            "strategy": job_snapshot.get("strategy", "vanilla"),
            "payload": job_snapshot.get("payload", {}),
            "priority": job_snapshot.get("priority", 2),
            "timeout_seconds": job_snapshot.get("timeout_seconds", 300),
        }

        hb_interval = int(claim.get("heartbeat_interval_seconds") or 20)
        hb_sleep = max(1, hb_interval)
        hb_running = True

        async def heartbeat_loop():
            nonlocal hb_running
            while hb_running:
                try:
                    await asyncio.sleep(hb_sleep)
                    if not hb_running:
                        break
                    await self.cp.heartbeat(job_id=job_id, lease_token=lease_token)
                except Exception as e:
                    logger.warning(f"Heartbeat failed for {job_id}: {e}")

        hb_task = asyncio.create_task(heartbeat_loop())
        try:
            result = await self.process_job(job_data)
            if result.get("success"):
                resp = await self.cp.complete(
                    job_id=job_id,
                    attempt_id=attempt_id,
                    lease_token=lease_token,
                    stream=stream,
                    message_id=message_id,
                    result=result,
                )
            else:
                resp = await self.cp.fail(
                    job_id=job_id,
                    attempt_id=attempt_id,
                    lease_token=lease_token,
                    stream=stream,
                    message_id=message_id,
                    error={
                        "code": "EXECUTION_FAILED",
                        "message": str(result.get("error") or "Execution failed"),
                        "stack": None,
                        "retryable": True,
                    },
                )

            if resp.get("ack"):
                await self.redis_client.xack(stream, self.consumer_group, message_id)
            else:
                logger.warning(f"Control Plane did not authorize ACK for {job_id}: {resp}")
        finally:
            hb_running = False
            hb_task.cancel()
            try:
                await hb_task
            except Exception:
                pass
    
    async def run(self):
        """Run the worker loop."""
        self.running = True
        logger.info("Execution worker started")
        
        while self.running:
            try:
                await self._run_once()
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Worker error: {e}", exc_info=True)
                await asyncio.sleep(5)
    
    async def shutdown(self):
        """Shutdown the worker."""
        self.running = False
        if self.browser_pool:
            await self.browser_pool.cleanup()
        if self.redis_client:
            await self.redis_client.aclose()
        if self.cp:
            await self.cp.aclose()
        logger.info("Execution worker shut down")


async def main():
    """Main entry point."""
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    max_browsers = int(os.getenv("MAX_BROWSERS", "20"))
    control_plane_url = os.getenv("CONTROL_PLANE_URL", "http://localhost:8080")
    worker_id = os.getenv("WORKER_ID", "execution-worker")
    
    worker = ExecutionWorker(
        redis_url=redis_url,
        max_browsers=max_browsers,
        worker_id=worker_id,
        control_plane_url=control_plane_url,
    )
    
    try:
        await worker.initialize()
        await worker.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await worker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

