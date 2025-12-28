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
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Add src to path
sys.path.insert(0, os.path.dirname(__file__))

from core.browser_pool import BrowserPool
from core.standard_executor import StandardExecutor
from strategies import StrategyExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExecutionWorker:
    """Worker that consumes jobs from Redis and executes them."""
    
    def __init__(
        self,
        redis_url: str,
        database_url: str,
        max_browsers: int = 20,
        consumer_name: str = "execution-worker"
    ):
        self.redis_url = redis_url
        self.database_url = database_url
        self.max_browsers = max_browsers
        self.consumer_name = consumer_name
        self.redis_client = None
        self.db_engine = None
        self.browser_pool = None
        self.strategy_executor = None
        self.running = False
    
    async def initialize(self):
        """Initialize connections and resources."""
        # Redis
        self.redis_client = await redis.from_url(self.redis_url, decode_responses=True)
        
        # Database
        self.db_engine = create_async_engine(self.database_url)
        async_session = sessionmaker(
            self.db_engine, class_=AsyncSession, expire_on_commit=False
        )
        
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
    
    async def consume_jobs(self, stream_name: str = "jobs:stream:normal", timeout: int = 1000):
        """Consume jobs from Redis Stream."""
        group_name = "execution-workers"
        
        try:
            # Create consumer group if it doesn't exist
            try:
                await self.redis_client.xgroup_create(
                    name=stream_name,
                    groupname=group_name,
                    id="0",
                    mkstream=True
                )
            except redis.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise
            
            # Read from stream
            messages = await self.redis_client.xreadgroup(
                groupname=group_name,
                consumername=self.consumer_name,
                streams={stream_name: ">"},
                count=1,
                block=timeout
            )
            
            if messages:
                for stream, msgs in messages:
                    for msg_id, data in msgs:
                        # Parse job_data from message (Control Plane now includes it)
                        job_data_str = data.get('job_data')
                        if job_data_str:
                            job_data = json.loads(job_data_str)
                        else:
                            # Fallback: construct from message fields (backward compatibility)
                            logger.warning(f"Message {msg_id} missing job_data, using fallback")
                            job_data = {
                                'id': data.get('job_id', msg_id),
                                'domain': data.get('domain', ''),
                                'url': '',  # Not available in old format
                                'type': 'navigate_extract',  # Default
                                'strategy': 'vanilla',  # Default
                                'payload': {},
                                'priority': int(data.get('priority', 2))
                            }
                        
                        # Ensure job_id is set
                        if 'id' not in job_data:
                            job_data['id'] = data.get('job_id', msg_id)
                        
                        # Process job
                        result = await self.process_job(job_data)
                        
                        # Acknowledge message
                        await self.redis_client.xack(stream_name, group_name, msg_id)
                        
                        # Update Control Plane job status via database
                        await self._update_job_status(job_data['id'], result)
                        
                        logger.info(f"Job {job_data['id']} completed: {result['success']}")
            
        except Exception as e:
            logger.error(f"Error consuming jobs: {e}", exc_info=True)
    
    async def run(self):
        """Run the worker loop."""
        self.running = True
        logger.info("Execution worker started")
        
        while self.running:
            try:
                # Try all priority streams
                for stream in ["jobs:stream:emergency", "jobs:stream:high", "jobs:stream:normal", "jobs:stream:low"]:
                    await self.consume_jobs(stream, timeout=1000)
            except Exception as e:
                logger.error(f"Worker error: {e}", exc_info=True)
                await asyncio.sleep(5)
    
    async def _update_job_status(self, job_id: str, result: Dict[str, Any]):
        """Update job status in Control Plane database."""
        try:
            from sqlalchemy import text
            from datetime import datetime
            
            async_session = sessionmaker(
                self.db_engine, class_=AsyncSession, expire_on_commit=False
            )
            
            async with async_session() as session:
                # Update job status based on result
                # Database enum values are UPPERCASE: PENDING, QUEUED, RUNNING, COMPLETED, FAILED, CANCELLED
                if result.get('success'):
                    status = 'COMPLETED'
                    error = None
                else:
                    status = 'FAILED'
                    error = str(result.get('error', 'Execution failed'))
                
                # Update job in database
                # PostgreSQL enum: just pass the string value, it should match the enum
                await session.execute(
                    text("""
                        UPDATE jobs 
                        SET status = :status,
                            result = :result,
                            artifacts = :artifacts,
                            error = :error,
                            completed_at = :completed_at
                        WHERE id = :job_id
                    """),
                    {
                        "job_id": job_id,
                        "status": status,
                        "result": json.dumps(result.get('data', {})),
                        "artifacts": json.dumps(result.get('artifacts', {})),
                        "error": error,
                        "completed_at": datetime.utcnow()
                    }
                )
                await session.commit()
                logger.info(f"Updated job {job_id} status to {status}")
        except Exception as e:
            logger.error(f"Failed to update job {job_id} status: {e}", exc_info=True)
    
    async def shutdown(self):
        """Shutdown the worker."""
        self.running = False
        if self.browser_pool:
            await self.browser_pool.cleanup()
        if self.redis_client:
            await self.redis_client.aclose()
        if self.db_engine:
            await self.db_engine.dispose()
        logger.info("Execution worker shut down")


async def main():
    """Main entry point."""
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    database_url = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/daemon_accord")
    max_browsers = int(os.getenv("MAX_BROWSERS", "20"))
    
    worker = ExecutionWorker(
        redis_url=redis_url,
        database_url=database_url,
        max_browsers=max_browsers
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

