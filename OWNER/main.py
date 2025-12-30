from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import yaml
import uuid
import json
import asyncio
from typing import Dict, Any
import aioredis
import asyncpg
from datetime import datetime
import boto3
from botocore.config import Config
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Database and storage configurations
DATABASE_URL = "postgresql://user:pass@localhost/db"
REDIS_URL = "redis://localhost:6379"
S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"
S3_BUCKET = "artifacts"

# Global state
app_state = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    app_state["redis"] = await aioredis.from_url(REDIS_URL, decode_responses=True)
    app_state["pg_pool"] = await asyncpg.create_pool(DATABASE_URL)

    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )
    app_state["s3"] = s3_client

    try:
        s3_client.create_bucket(Bucket=S3_BUCKET)
    except Exception:
        pass

    # Initialize browser pool
    from browser_manager import BrowserManager

    app_state["browser_manager"] = BrowserManager()
    await app_state["browser_manager"].initialize()

    yield

    # Shutdown
    await app_state["browser_manager"].shutdown()
    await app_state["redis"].close()
    await app_state["pg_pool"].close()


app = FastAPI(lifespan=lifespan)

# Rate limiting storage
rate_limits: Dict[str, Dict] = {}


async def store_results(job_id: str, results: Dict[str, Any]):
    async with app_state["pg_pool"].acquire() as conn:
        await conn.execute(
            """
            INSERT INTO results (id, data, created_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (id) DO UPDATE SET data = $2
            """,
            job_id,
            json.dumps(results),
            datetime.utcnow(),
        )


async def upload_to_s3(data: bytes, key: str) -> str:
    app_state["s3"].put_object(Bucket=S3_BUCKET, Key=key, Body=data)
    url = app_state["s3"].generate_presigned_url(
        "get_object", Params={"Bucket": S3_BUCKET, "Key": key}, ExpiresIn=3600
    )
    return url


@app.post("/execute")
async def execute_workflow(yaml_content: str, background_tasks: BackgroundTasks):
    try:
        workflow = yaml.safe_load(yaml_content)
        job_id = str(uuid.uuid4())

        # Queue job
        priority = workflow.get("priority", 1)
        await app_state["redis"].zadd("job_queue", {job_id: priority})

        # Start execution
        background_tasks.add_task(execute_job, job_id, workflow)

        return JSONResponse({"job_id": job_id, "status": "queued", "ws_url": f"/ws/{job_id}"})
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.websocket("/ws/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    await websocket.accept()
    try:
        while True:
            # Send updates
            status = await app_state["redis"].hget(f"job:{job_id}", "status")
            await websocket.send_json({"status": status or "unknown"})
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass


async def execute_job(job_id: str, workflow: Dict[str, Any]):
    from executor import WorkflowExecutor
    from browser_manager import BrowserManager

    await app_state["redis"].hset(f"job:{job_id}", "status", "executing")

    executor = WorkflowExecutor()
    try:
        results = await executor.execute(workflow, job_id)
        await store_results(job_id, results)
        await app_state["redis"].hset(f"job:{job_id}", "status", "completed")
    except Exception as e:
        logger.error(f"Job {job_id} failed: {e}")
        await app_state["redis"].hset(f"job:{job_id}", "status", f"failed: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, workers=4)
