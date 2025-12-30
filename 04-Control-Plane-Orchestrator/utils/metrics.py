import time
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import asyncio
from contextlib import contextmanager, asynccontextmanager

class MetricsCollector:
    """Collect and report system metrics"""
    
    def __init__(self, redis_client, pg_pool):
        self.redis = redis_client
        self.pg_pool = pg_pool
        self.metrics = {}
        self.histograms = {}
    
    @asynccontextmanager
    async def measure_execution(self, metric_name: str, labels: Dict = None):
        """Measure execution time of async code block"""
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            await self.record_timer(metric_name, duration, labels)
    
    @contextmanager
    def measure_sync(self, metric_name: str, labels: Dict = None):
        """Measure execution time of sync code block"""
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            asyncio.create_task(self.record_timer(metric_name, duration, labels))
    
    async def increment_counter(self, metric_name: str, value: int = 1, labels: Dict = None):
        """Increment counter metric"""
        key = self._build_metric_key(metric_name, labels)
        await self.redis.incr(key, value)
        await self.redis.expire(key, 86400)  # 24 hours
    
    async def record_gauge(self, metric_name: str, value: float, labels: Dict = None):
        """Record gauge metric"""
        key = self._build_metric_key(metric_name, labels)
        await self.redis.set(key, value)
        await self.redis.expire(key, 86400)
    
    async def record_timer(self, metric_name: str, duration: float, labels: Dict = None):
        """Record timer metric"""
        # Store duration
        await self.record_gauge(f"{metric_name}_duration", duration, labels)
        
        # Increment count
        await self.increment_counter(f"{metric_name}_count", 1, labels)
        
        # Update histogram
        await self._update_histogram(metric_name, duration, labels)
    
    async def _update_histogram(self, metric_name: str, value: float, labels: Dict = None):
        """Update histogram data"""
        key = self._build_metric_key(f"{metric_name}_histogram", labels)
        
        # Store in sorted set for percentile calculation
        timestamp = int(time.time() * 1000)
        await self.redis.zadd(key, {str(timestamp): value})
        
        # Keep only last 1000 values
        await self.redis.zremrangebyrank(key, 0, -1001)
        await self.redis.expire(key, 86400)
    
    def _build_metric_key(self, metric_name: str, labels: Dict = None) -> str:
        """Build Redis key for metric"""
        if labels:
            label_str = ":".join(f"{k}={v}" for k, v in sorted(labels.items()))
            return f"metrics:{metric_name}:{label_str}"
        return f"metrics:{metric_name}"
    
    async def get_metric(self, metric_name: str, labels: Dict = None) -> Optional[float]:
        """Get current metric value"""
        key = self._build_metric_key(metric_name, labels)
        value = await self.redis.get(key)
        return float(value) if value else None
    
    async def get_histogram(self, metric_name: str, labels: Dict = None) -> Dict[str, float]:
        """Get histogram statistics"""
        key = self._build_metric_key(f"{metric_name}_histogram", labels)
        values = await self.redis.zrange(key, 0, -1, withscores=True)
        
        if not values:
            return {}
        
        numeric_values = [float(score) for _, score in values]
        numeric_values.sort()
        
        n = len(numeric_values)
        return {
            "count": n,
            "min": min(numeric_values),
            "max": max(numeric_values),
            "mean": sum(numeric_values) / n,
            "p50": numeric_values[int(n * 0.5)] if n > 0 else 0,
            "p95": numeric_values[int(n * 0.95)] if n > 1 else numeric_values[0],
            "p99": numeric_values[int(n * 0.99)] if n > 1 else numeric_values[0],
        }
    
    async def record_job_metrics(self, job_id: str, workflow_name: str, 
                                duration: float, status: str, user_id: str = None):
        """Record job execution metrics"""
        labels = {
            "workflow": workflow_name,
            "status": status,
            "user_id": user_id or "anonymous"
        }
        
        await self.record_timer("job_execution", duration, labels)
        await self.increment_counter(f"job_{status}", 1, labels)
    
    async def record_browser_metrics(self, action: str, duration: float, 
                                    success: bool, context_id: str = None):
        """Record browser operation metrics"""
        labels = {
            "action": action,
            "success": str(success),
            "context_id": context_id or "unknown"
        }
        
        await self.record_timer("browser_operation", duration, labels)
        
        if success:
            await self.increment_counter("browser_success", 1, labels)
        else:
            await self.increment_counter("browser_failure", 1, labels)
    
    async def record_api_metrics(self, endpoint: str, method: str, 
                                duration: float, status_code: int):
        """Record API request metrics"""
        labels = {
            "endpoint": endpoint,
            "method": method,
            "status_code": str(status_code)
        }
        
        await self.record_timer("api_request", duration, labels)
        await self.increment_counter(f"http_{status_code}", 1, labels)
    
    async def get_system_metrics(self) -> Dict[str, Any]:
        """Get comprehensive system metrics"""
        metrics = {
            "timestamp": datetime.utcnow().isoformat(),
            "counters": {},
            "gauges": {},
            "histograms": {}
        }
        
        # Get all metric keys
        keys = await self.redis.scan("metrics:*", count=1000)
        
        for key in keys:
            if key.endswith("_histogram"):
                metric_name = key.split(":")[1]
                hist_data = await self.get_histogram(metric_name.replace("_histogram", ""))
                metrics["histograms"][metric_name] = hist_data
            elif key.endswith("_duration") or key.endswith("_count"):
                # Already captured in histograms
                continue
            else:
                value = await self.redis.get(key)
                if value:
                    metric_name = key.split(":")[1]
                    if "." in value:
                        metrics["gauges"][metric_name] = float(value)
                    else:
                        metrics["counters"][metric_name] = int(value)
        
        return metrics
    
    async def export_metrics(self, format: str = "json") -> str:
        """Export metrics in specified format"""
        metrics = await self.get_system_metrics()
        
        if format == "json":
            import json
            return json.dumps(metrics, indent=2)
        elif format == "prometheus":
            return self._to_prometheus_format(metrics)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _to_prometheus_format(self, metrics: Dict[str, Any]) -> str:
        """Convert metrics to Prometheus format"""
        lines = []
        
        # Add timestamp comment
        lines.append(f"# HELP system_metrics System metrics")
        lines.append(f"# TYPE system_metrics gauge")
        lines.append(f"system_metrics_timestamp {time.time()}")
        
        # Add counters
        for name, value in metrics.get("counters", {}).items():
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name} {value}")
        
        # Add gauges
        for name, value in metrics.get("gauges", {}).items():
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name} {value}")
        
        # Add histogram summaries
        for name, hist in metrics.get("histograms", {}).items():
            if hist:
                lines.append(f"# TYPE {name} summary")
                lines.append(f'{name}{{quantile="0.5"}} {hist.get("p50", 0)}')
                lines.append(f'{name}{{quantile="0.95"}} {hist.get("p95", 0)}')
                lines.append(f'{name}{{quantile="0.99"}} {hist.get("p99", 0)}')
                lines.append(f'{name}_count {hist.get("count", 0)}')
                lines.append(f'{name}_sum {hist.get("mean", 0) * hist.get("count", 0)}')
        
        return "\n".join(lines)
