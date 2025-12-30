import asyncio
import heapq
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import uuid

class PriorityScheduler:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.priority_queue = []
        self.running_tasks: Dict[str, asyncio.Task] = {}
        self.max_concurrent = 50
        self.task_timeout = 300  # 5 minutes
        
    async def schedule(self, job_id: str, workflow: Dict, priority: int = 1) -> bool:
        """Schedule job with priority"""
        job_data = {
            'id': job_id,
            'workflow': workflow,
            'priority': priority,
            'created_at': datetime.utcnow().isoformat(),
            'status': 'pending'
        }
        
        # Store in Redis
        await self.redis.setex(f"job:data:{job_id}", 3600, json.dumps(job_data))
        
        # Add to priority queue (min-heap, lower number = higher priority)
        heapq.heappush(self.priority_queue, (priority, datetime.utcnow().timestamp(), job_id))
        await self.redis.zadd("scheduler:queue", {job_id: priority})
        
        # Trigger execution
        asyncio.create_task(self._process_queue())
        return True
    
    async def _process_queue(self):
        """Process jobs from priority queue"""
        while len(self.running_tasks) < self.max_concurrent and self.priority_queue:
            _, _, job_id = heapq.heappop(self.priority_queue)
            await self.redis.zrem("scheduler:queue", job_id)
            
            # Create execution task
            task = asyncio.create_task(self._execute_job(job_id))
            self.running_tasks[job_id] = task
            task.add_done_callback(lambda t, jid=job_id: self.running_tasks.pop(jid, None))
    
    async def _execute_job(self, job_id: str):
        """Execute a single job"""
        from executor import WorkflowExecutor
        from main import app_state
        
        try:
            # Get job data
            job_data = await self.redis.get(f"job:data:{job_id}")
            if not job_data:
                return
            
            workflow = json.loads(job_data)['workflow']
            
            # Update status
            await self.redis.hset(f"job:status:{job_id}", 
                "status", "executing")
            await self.redis.hset(f"job:status:{job_id}",
                "started_at", datetime.utcnow().isoformat())
            
            # Execute with timeout
            executor = WorkflowExecutor()
            results = await asyncio.wait_for(
                executor.execute(workflow, job_id),
                timeout=self.task_timeout
            )
            
            # Store results
            await self._store_results(job_id, results)
            await self.redis.hset(f"job:status:{job_id}",
                "status", "completed")
            await self.redis.hset(f"job:status:{job_id}",
                "completed_at", datetime.utcnow().isoformat())
            
        except asyncio.TimeoutError:
            await self.redis.hset(f"job:status:{job_id}",
                "status", "timeout")
        except Exception as e:
            await self.redis.hset(f"job:status:{job_id}",
                "status", f"failed: {str(e)}")
        finally:
            # Cleanup
            await self.redis.delete(f"job:data:{job_id}")
    
    async def _store_results(self, job_id: str, results: Dict):
        """Store job results in database and S3"""
        from main import app_state
        
        # Store in PostgreSQL
        async with app_state["pg_pool"].acquire() as conn:
            await conn.execute('''
                INSERT INTO job_results 
                (id, results, created_at, completed_at)
                VALUES ($1, $2, $3, $4)
            ''', job_id, json.dumps(results), 
                datetime.utcnow(), datetime.utcnow())
        
        # Store artifacts metadata
        for artifact in results.get("artifacts", []):
            await conn.execute('''
                INSERT INTO artifacts 
                (job_id, type, storage_key, url, created_at)
                VALUES ($1, $2, $3, $4, $5)
            ''', job_id, artifact["type"], 
                artifact["key"], artifact["url"],
                datetime.utcnow())
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job"""
        if job_id in self.running_tasks:
            self.running_tasks[job_id].cancel()
            await self.redis.hset(f"job:status:{job_id}",
                "status", "cancelled")
            return True
        return False
    
    async def get_queue_stats(self) -> Dict:
        """Get scheduler statistics"""
        return {
            "pending": len(self.priority_queue),
            "running": len(self.running_tasks),
            "max_concurrent": self.max_concurrent,
            "queue_size": await self.redis.zcard("scheduler:queue")
        }
