# 04-Control-Plane-Orchestrator/src/control_plane/queue_manager.py
import asyncio
import json
import time
from typing import Optional, Dict, Any, List
from datetime import datetime
import redis.asyncio as redis
import os

class QueueManager:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.stream_key = "jobs:stream"
        self.consumer_group = os.getenv("JOBS_CONSUMER_GROUP", "workers")
        self.consumer_name = None
        self.dead_letter_key = "jobs:dlq"

    # -------------------------
    # Streams / consumer groups
    # -------------------------

    async def ensure_consumer_groups(self, streams: List[str]) -> None:
        """Ensure consumer group exists for each stream."""
        for stream in streams:
            try:
                await self.redis.xgroup_create(
                    name=stream,
                    groupname=self.consumer_group,
                    id="0",
                    mkstream=True,
                )
            except Exception:
                # Group might already exist, which is fine
                continue

    async def read_one_from_any_stream(
        self,
        streams: List[str],
        max_wait_ms: int,
        consumer: str,
    ) -> Optional[tuple[str, str, Dict[str, Any]]]:
        """
        Read exactly one message from the first stream that has one (priority order).

        IMPORTANT: Does NOT ACK. Caller must ACK after durable DB state change.
        Returns: (stream, message_id, fields)
        """
        await self.ensure_consumer_groups(streams)

        # One XREADGROUP call across all streams so total block time is max_wait_ms (not N * max_wait_ms).
        stream_map = {s: ">" for s in streams}
        resp = await self.redis.xreadgroup(
            groupname=self.consumer_group,
            consumername=consumer,
            streams=stream_map,
            count=1,
            block=max_wait_ms,
        )
        if resp:
            # resp: [(stream, [(message_id, fields)]) ...]
            stream_name, entries = resp[0]
            message_id, fields = entries[0]
            return str(stream_name), str(message_id), dict(fields)
        return None

    async def ack(self, stream: str, message_id: str) -> None:
        """ACK a message in the configured consumer group."""
        await self.redis.xack(stream, self.consumer_group, message_id)

    async def dlq_add(self, fields: Dict[str, Any]) -> str:
        """Add an entry to the DLQ stream."""
        return await self.redis.xadd(self.dead_letter_key, fields, maxlen=10000)

    async def pel_reconcile_requeue(
        self,
        *,
        stream: str,
        idle_ms: int,
        check_active_lease,
        batch_size: int = 100,
        requeue_maxlen: int = 10000,
        requeue_fields_extra: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Reconcile pending entries list (PEL) for a stream.

        Rule:
          - If message idle > idle_ms AND there is NO active DB lease for that job_id,
            reclaim and requeue (XADD new message) then XACK old message.
          - If active lease exists, do not touch the message (leave it pending).

        Uses XPENDING + XCLAIM for portability; prefers correctness over speed.
        """
        requeued = 0
        pending: list[dict[str, Any]] = []
        # Prefer redis-py helper if available; otherwise use raw XPENDING command.
        try:
            if hasattr(self.redis, "xpending_range"):
                pending = await self.redis.xpending_range(
                    stream,
                    self.consumer_group,
                    min="-",
                    max="+",
                    count=batch_size,
                    idle=idle_ms,
                )
            else:
                # XPENDING <key> <group> IDLE <ms> - + <count>
                raw = await self.redis.execute_command(
                    "XPENDING",
                    stream,
                    self.consumer_group,
                    "IDLE",
                    idle_ms,
                    "-",
                    "+",
                    batch_size,
                )
                # raw entries: [ [id, consumer, idle_ms, deliveries], ... ]
                pending = [
                    {
                        "message_id": r[0],
                        "consumer": r[1],
                        "time_since_delivered": r[2],
                        "times_delivered": r[3],
                    }
                    for r in (raw or [])
                ]
        except Exception:
            return 0

        if not pending:
            return 0

        for p in pending:
            msg_id = p.get("message_id") if isinstance(p, dict) else None
            if not msg_id:
                continue

            # Load message fields
            msg = await self.redis.xrange(stream, min=msg_id, max=msg_id, count=1)
            if not msg:
                # Message no longer exists; ack defensively.
                await self.ack(stream, msg_id)
                continue
            _, fields = msg[0]
            fields = dict(fields)
            job_id = fields.get("job_id")
            if not job_id:
                # Poison message: ack and drop.
                await self.ack(stream, msg_id)
                continue

            has_lease = await check_active_lease(str(job_id))
            if has_lease:
                # Do not reclaim; leave pending under original consumer.
                continue

            # Claim to a dedicated reconciler consumer (so we can ACK it).
            try:
                await self.redis.xclaim(
                    stream,
                    self.consumer_group,
                    consumername="reconciler",
                    min_idle_time=idle_ms,
                    message_ids=[msg_id],
                )
            except Exception:
                continue

            # Requeue by adding a fresh message, then ACK old.
            extra = requeue_fields_extra or {}
            await self.redis.xadd(
                stream,
                {**fields, **extra, "requeued_from_pel": str(msg_id)},
                maxlen=requeue_maxlen,
            )
            await self.ack(stream, msg_id)
            requeued += 1

        return requeued
        
    async def enqueue(self, job_id: str, priority: int, domain: str, 
                     job_data: Optional[Dict[str, Any]] = None,
                     dedupe_key: Optional[str] = None) -> str:
        """
        Enqueue a job with priority.
        
        Args:
            job_id: Unique job identifier
            priority: Priority level (0=emergency, 1=high, 2=normal, 3=low)
            domain: Target domain
            job_data: Full job data (url, type, payload, strategy) for Execution Engine worker
            dedupe_key: Optional deduplication key
        """
        
        # Check deduplication
        if dedupe_key:
            existing = await self.redis.get(f"dedupe:{dedupe_key}")
            if existing:
                return existing
        
        # Create message
        message = {
            "job_id": job_id,
            "priority": priority,
            "domain": domain,
            "timestamp": time.time(),
            "attempts": 0
        }
        
        # Include full job data for Execution Engine worker
        if job_data:
            message["job_data"] = json.dumps(job_data)
        
        # Add to appropriate priority stream
        if priority == 0:  # Emergency
            stream = f"{self.stream_key}:emergency"
        elif priority == 1:  # High
            stream = f"{self.stream_key}:high"
        elif priority == 2:  # Normal
            stream = f"{self.stream_key}:normal"
        else:  # Low
            stream = f"{self.stream_key}:low"
        
        # Add to stream
        message_id = await self.redis.xadd(
            stream,
            message,
            maxlen=10000  # Keep last 10k messages
        )
        
        # Store deduplication key if provided
        if dedupe_key:
            await self.redis.setex(f"dedupe:{dedupe_key}", 86400, job_id)
        
        return message_id
    
    async def dequeue(self, timeout: float = 5.0) -> Optional[str]:
        """Dequeue next job with priority ordering."""
        
        # Try emergency queue first
        for stream in [
            f"{self.stream_key}:emergency",
            f"{self.stream_key}:high",
            f"{self.stream_key}:normal",
            f"{self.stream_key}:low"
        ]:
            messages = await self.redis.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.consumer_name or "default",
                streams={stream: ">"},
                count=1,
                block=int(timeout * 1000)
            )
            
            if messages:
                for stream_name, message_list in messages:
                    for message_id, message_data in message_list:
                        # Acknowledge message
                        await self.redis.xack(stream_name, self.consumer_group, message_id)
                        job_id = None
                        if isinstance(message_data, dict):
                            job_id = message_data.get("job_id")
                            if job_id is None:
                                job_id = message_data.get(b"job_id")
                        if isinstance(job_id, bytes):
                            job_id = job_id.decode("utf-8")
                        return job_id
        
        return None
    
    async def requeue(self, job_id: str, priority: int, domain: str, 
                     job_data: Optional[Dict[str, Any]] = None,
                     delay_seconds: int = 0) -> str:
        """Requeue a failed job with delay."""
        
        if delay_seconds > 0:
            # Use Redis delayed queue
            score = time.time() + delay_seconds
            await self.redis.zadd(
                "jobs:delayed",
                {job_id: score}
            )
            return f"delayed:{job_id}"
        else:
            # Requeue immediately
            return await self.enqueue(job_id, priority, domain, job_data=job_data)
    
    async def move_to_dlq(self, message_id: str, error: str):
        """Move failed message to dead letter queue."""
        # Get message
        message = await self.redis.xrange(self.stream_key, min=message_id, max=message_id, count=1)
        if message:
            msg_id, msg_data = message[0]
            msg_data["error"] = error
            msg_data["dlq_time"] = time.time()
            
            # Add to DLQ
            await self.redis.xadd(self.dead_letter_key, msg_data)
            
            # Remove from original stream
            await self.redis.xdel(self.stream_key, msg_id)
    
    async def get_position(self, job_id: str) -> Optional[int]:
        """Get queue position for a job."""
        # This is simplified - in production would need proper queue scanning
        return None
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics."""
        stats = {}
        
        for stream_name in ["emergency", "high", "normal", "low"]:
            stream = f"{self.stream_key}:{stream_name}"
            length = await self.redis.xlen(stream)
            try:
                pending_info = await self.redis.xpending(stream, self.consumer_group)
                # xpending returns tuple (count, min_id, max_id, consumers)
                pending_count = pending_info[0] if isinstance(pending_info, (list, tuple)) and len(pending_info) > 0 else 0
            except Exception:
                pending_count = 0
            stats[stream_name] = {
                "length": length,
                "pending": pending_count
            }
        
        # DLQ stats
        dlq_length = await self.redis.xlen(self.dead_letter_key)
        stats["dlq"] = {"length": dlq_length}
        
        # Delayed jobs
        delayed_count = await self.redis.zcard("jobs:delayed")
        stats["delayed"] = {"count": delayed_count}
        
        return stats
    
    async def get_depth(self) -> int:
        """Get total queue depth."""
        total = 0
        for stream_name in ["emergency", "high", "normal", "low"]:
            stream = f"{self.stream_key}:{stream_name}"
            total += await self.redis.xlen(stream)
        return total
    
    async def remove(self, job_id: str) -> bool:
        """Remove a job from queue (for cancellation)."""
        # Search all priority streams for the job
        for stream_name in ["emergency", "high", "normal", "low"]:
            stream = f"{self.stream_key}:{stream_name}"
            # Scan stream for job_id (simplified - production might need better approach)
            try:
                messages = await self.redis.xrange(stream, count=1000)
                for msg_id, msg_data in messages:
                    found = None
                    if isinstance(msg_data, dict):
                        found = msg_data.get("job_id")
                        if found is None:
                            found = msg_data.get(b"job_id")
                    if isinstance(found, bytes):
                        found = found.decode("utf-8")
                    if found == job_id:
                        await self.redis.xdel(stream, msg_id)
                        return True
            except Exception:
                continue
        return False
    
    async def initialize_consumer_group(self, consumer_name: str):
        """Initialize consumer group for Redis Streams."""
        self.consumer_name = consumer_name
        # Create consumer group for each priority stream if it doesn't exist
        for stream_name in ["emergency", "high", "normal", "low"]:
            stream = f"{self.stream_key}:{stream_name}"
            try:
                await self.redis.xgroup_create(
                    name=stream,
                    groupname=self.consumer_group,
                    id="0",
                    mkstream=True
                )
            except Exception:
                # Group might already exist, which is fine
                pass


