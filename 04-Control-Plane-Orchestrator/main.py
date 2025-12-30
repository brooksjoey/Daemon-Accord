from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import yaml
import uuid
import json
import asyncio
from typing import Dict, Any, List
from datetime import datetime
import logging

# Import control plane components
from control_plane.scheduler import PriorityScheduler
from control_plane.monitor import SystemMonitor
from control_plane.rate_limiter import DistributedRateLimiter
from control_plane.health_check import HealthChecker
from control_plane.config_manager import ConfigManager
from storage.redis_client import RedisClient
from storage.s3_client import S3StorageClient
from database.models import get_all_table_sql
from api.middleware import request_logging_middleware, cors_middleware, error_handling_middleware

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Configuration
DATABASE_URL = "postgresql://user:pass@localhost/browser_automation"
REDIS_URL = "redis://localhost:6379"
S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"
S3_BUCKET = "artifacts"
MAX_CONCURRENT_JOBS = 50

# Global state
app_state = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("Initializing Browser Automation Engine...")
    
    # Initialize Redis
    redis_client = RedisClient(REDIS_URL)
    await redis_client.connect()
    app_state["redis"] = redis_client
    
    # Initialize PostgreSQL pool
    import asyncpg
    app_state["pg_pool"] = await asyncpg.create_pool(DATABASE_URL, min_size=5, max_size=20)
    
    # Initialize database schema
    async with app_state["pg_pool"].acquire() as conn:
        for table_name, sql in get_all_table_sql().items():
            try:
                await conn.execute(sql)
                print(f"Created table: {table_name}")
            except Exception as e:
                print(f"Table {table_name} may already exist: {e}")
    
    # Initialize S3 storage
    s3_client = S3StorageClient(
        endpoint_url=S3_ENDPOINT,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        bucket=S3_BUCKET
    )
    app_state["storage"] = s3_client
    
    # Initialize control plane components
    app_state["scheduler"] = PriorityScheduler(redis_client)
    app_state["rate_limiter"] = DistributedRateLimiter(redis_client)
    app_state["config_manager"] = ConfigManager(redis_client, app_state["pg_pool"])
    await app_state["config_manager"].load_configs()
    
    # Initialize monitor
    app_state["monitor"] = SystemMonitor(redis_client)
    asyncio.create_task(app_state["monitor"].start_monitoring())
    
    # Initialize health checker
    health_checker = HealthChecker(redis_client, app_state["pg_pool"], s3_client)
    
    # Register health checks
    await health_checker.register_service("redis", health_checker.check_redis, 30)
    await health_checker.register_service("postgres", health_checker.check_postgres, 30)
    await health_checker.register_service("s3", health_checker.check_s3, 30)
    
    app_state["health_checker"] = health_checker
    asyncio.create_task(health_checker.start_checks())
    
    # Initialize browser manager
    from browser_manager import BrowserManager
    app_state["browser_manager"] = BrowserManager()
    await app_state["browser_manager"].initialize()
    
    # Register browser pool health check
    await health_checker.register_service(
        "browser_pool", 
        lambda: health_checker.check_browser_pool(app_state["browser_manager"]),
        60
    )
    
    print("Browser Automation Engine initialized successfully")
    
    yield
    
    # Shutdown
    print("Shutting down Browser Automation Engine...")
    await app_state["browser_manager"].shutdown()
    await redis_client.disconnect()
    await app_state["pg_pool"].close()
    print("Shutdown complete")

app = FastAPI(
    title="Browser Automation Engine",
    description="Professional-grade browser automation system with control plane",
    version="1.0.0",
    lifespan=lifespan
)

# Add middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    return await request_logging_middleware(request, call_next)

@app.middleware("http")
async def add_cors_header(request: Request, call_next):
    return await cors_middleware(request, call_next)

@app.middleware("http")
async def handle_errors(request: Request, call_next):
    return await error_handling_middleware(request, call_next)

# WebSocket connections
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
    
    async def connect(self, websocket: WebSocket, job_id: str):
        await websocket.accept()
        self.active_connections[job_id] = websocket
    
    def disconnect(self, job_id: str):
        self.active_connections.pop(job_id, None)
    
    async def send_update(self, job_id: str, message: Dict):
        if job_id in self.active_connections:
            try:
                await self.active_connections[job_id].send_json(message)
            except:
                self.disconnect(job_id)

manager = ConnectionManager()

# Import API endpoints
from api.endpoints import router as api_router
app.include_router(api_router, prefix="/api/v1")

# Legacy endpoints for backward compatibility
@app.post("/execute")
async def execute_workflow_legacy(yaml_content: str, background_tasks: BackgroundTasks):
    """Legacy endpoint for backward compatibility"""
    from api.endpoints import execute_workflow
    from api.schemas import ExecuteRequest
    from fastapi.security import HTTPBearer
    
    # Create mock request for compatibility
    class MockCredentials:
        def __init__(self):
            self.credentials = "legacy-key"
    
    request = ExecuteRequest(
        yaml_content=yaml_content,
        priority=1,
        client_host="127.0.0.1",
        user_agent="Legacy Client"
    )
    
    return await execute_workflow(
        request=request,
        background_tasks=background_tasks,
        credentials=MockCredentials()
    )

@app.websocket("/ws/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    await manager.connect(websocket, job_id)
    try:
        while True:
            # Send periodic updates
            status = await app_state["redis"].hget(f"job:status:{job_id}", "status")
            if status:
                await websocket.send_json({
                    "job_id": job_id,
                    "status": status,
                    "timestamp": datetime.utcnow().isoformat()
                })
            else:
                # Check database
                async with app_state["pg_pool"].acquire() as conn:
                    from database.queries import JobQueries
                    job = await JobQueries.get_job(conn, job_id)
                    if job:
                        await websocket.send_json({
                            "job_id": job_id,
                            "status": job["status"],
                            "timestamp": datetime.utcnow().isoformat()
                        })
            
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        manager.disconnect(job_id)

# Admin endpoints
@app.post("/admin/restart_browser_pool")
async def restart_browser_pool():
    """Restart browser pool (admin only)"""
    await app_state["browser_manager"].shutdown()
    await app_state["browser_manager"].initialize()
    return {"status": "browser_pool_restarted"}

@app.post("/admin/flush_cache")
async def flush_cache():
    """Flush Redis cache (admin only)"""
    await app_state["redis"].flush_db()
    return {"status": "cache_flushed"}

@app.get("/admin/stats")
async def get_admin_stats():
    """Get system statistics (admin only)"""
    stats = {
        "system": {
            "timestamp": datetime.utcnow().isoformat(),
            "uptime": "todo",  # Would track from startup
        },
        "jobs": await app_state["scheduler"].get_queue_stats(),
        "browser_pool": {
            "total_browsers": len(app_state["browser_manager"].browsers),
            "available_contexts": len(app_state["browser_manager"].browser_pool),
            "max_browsers": app_state["browser_manager"].max_browsers
        },
        "storage": app_state["storage"].get_usage_stats(),
        "redis": {
            "memory": await app_state["redis"].get_memory_usage(),
            "clients": await app_state["redis"].get_client_stats()
        }
    }
    return stats

# Internal job execution (called by scheduler)
async def execute_job_internal(job_id: str, workflow: Dict[str, Any]):
    """Internal job execution function"""
    from executor import WorkflowExecutor
    
    await app_state["redis"].hset(f"job:status:{job_id}", "status", "executing")
    await app_state["redis"].hset(f"job:status:{job_id}", "started_at", datetime.utcnow().isoformat())
    
    # Send WebSocket update
    await manager.send_update(job_id, {
        "job_id": job_id,
        "status": "executing",
        "timestamp": datetime.utcnow().isoformat()
    })
    
    executor = WorkflowExecutor()
    try:
        results = await executor.execute(workflow, job_id)
        
        # Store results
        from database.queries import JobQueries
        async with app_state["pg_pool"].acquire() as conn:
            await JobQueries.store_job_results(conn, job_id, results)
            await JobQueries.update_job_status(conn, job_id, "completed")
        
        await app_state["redis"].hset(f"job:status:{job_id}", "status", "completed")
        await app_state["redis"].hset(f"job:status:{job_id}", "completed_at", datetime.utcnow().isoformat())
        
        # Send completion update
        await manager.send_update(job_id, {
            "job_id": job_id,
            "status": "completed",
            "results": results,
            "timestamp": datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Job {job_id} failed: {e}")
        error_msg = str(e)
        
        async with app_state["pg_pool"].acquire() as conn:
            from database.queries import JobQueries
            await JobQueries.update_job_status(conn, job_id, "failed", error_msg)
        
        await app_state["redis"].hset(f"job:status:{job_id}", "status", f"failed: {error_msg}")
        
        # Send failure update
        await manager.send_update(job_id, {
            "job_id": job_id,
            "status": "failed",
            "error": error_msg,
            "timestamp": datetime.utcnow().isoformat()
        })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000, 
        workers=4,
        log_level="info"
    )
