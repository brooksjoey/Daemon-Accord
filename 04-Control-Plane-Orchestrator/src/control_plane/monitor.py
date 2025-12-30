import asyncio
from datetime import datetime, timedelta
from typing import Dict, List
import psutil
import json

class SystemMonitor:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.metrics_interval = 30  # seconds
        self.retention_days = 7
    
    async def start_monitoring(self):
        """Start continuous monitoring"""
        while True:
            try:
                metrics = await self._collect_metrics()
                await self._store_metrics(metrics)
                await self._check_alerts(metrics)
            except Exception as e:
                print(f"Monitoring error: {e}")
            
            await asyncio.sleep(self.metrics_interval)
    
    async def _collect_metrics(self) -> Dict:
        """Collect system and application metrics"""
        from main import app_state
        
        metrics = {
            "timestamp": datetime.utcnow().isoformat(),
            "system": {
                "cpu_percent": psutil.cpu_percent(interval=1),
                "memory_percent": psutil.virtual_memory().percent,
                "disk_percent": psutil.disk_usage('/').percent,
                "network_io": psutil.net_io_counters()._asdict()
            },
            "application": {
                "active_jobs": len(app_state.get("browser_manager", {}).get("browser_pool", [])),
                "browser_pool_size": len(app_state.get("browser_manager", {}).get("browser_pool", [])),
                "redis_connections": await self._get_redis_connections(),
                "pg_connections": await self._get_pg_connections()
            },
            "jobs": await self._get_job_metrics()
        }
        
        return metrics
    
    async def _get_redis_connections(self) -> int:
        """Get active Redis connections"""
        try:
            info = await self.redis.info('clients')
            return int(info.get('connected_clients', 0))
        except:
            return 0
    
    async def _get_pg_connections(self) -> int:
        """Get PostgreSQL connections"""
        from main import app_state
        try:
            async with app_state["pg_pool"].acquire() as conn:
                result = await conn.fetchval('''
                    SELECT count(*) FROM pg_stat_activity 
                    WHERE datname = current_database()
                ''')
                return result
        except:
            return 0
    
    async def _get_job_metrics(self) -> Dict:
        """Get job execution metrics"""
        from main import app_state
        
        now = datetime.utcnow()
        hour_ago = now - timedelta(hours=1)
        
        async with app_state["pg_pool"].acquire() as conn:
            # Get job counts
            total_jobs = await conn.fetchval(
                'SELECT COUNT(*) FROM job_results'
            )
            
            recent_jobs = await conn.fetchval('''
                SELECT COUNT(*) FROM job_results 
                WHERE created_at > $1
            ''', hour_ago)
            
            # Get success rate
            success_rate = await conn.fetchval('''
                SELECT 
                    COUNT(CASE WHEN results->>'errors' = '[]' THEN 1 END) * 100.0 / 
                    NULLIF(COUNT(*), 0) as success_rate
                FROM job_results 
                WHERE created_at > $1
            ''', hour_ago)
            
            # Get average execution time
            avg_time = await conn.fetchval('''
                SELECT AVG(
                    EXTRACT(EPOCH FROM (completed_at - created_at))
                ) FROM job_results 
                WHERE completed_at IS NOT NULL 
                AND created_at > $1
            ''', hour_ago)
        
        return {
            "total_jobs": total_jobs or 0,
            "recent_jobs": recent_jobs or 0,
            "success_rate": success_rate or 0,
            "avg_execution_time": avg_time or 0
        }
    
    async def _store_metrics(self, metrics: Dict):
        """Store metrics in time-series database (Redis)"""
        key = f"metrics:{datetime.utcnow().strftime('%Y%m%d:%H%M')}"
        await self.redis.setex(key, 604800, json.dumps(metrics))  # 7 days
        
        # Store aggregated metrics
        await self.redis.hset("metrics:daily", 
            datetime.utcnow().strftime('%Y%m%d'),
            json.dumps(metrics))
    
    async def _check_alerts(self, metrics: Dict):
        """Check for alert conditions"""
        alerts = []
        
        # CPU alert
        if metrics["system"]["cpu_percent"] > 80:
            alerts.append({
                "type": "high_cpu",
                "value": metrics["system"]["cpu_percent"],
                "threshold": 80,
                "timestamp": metrics["timestamp"]
            })
        
        # Memory alert
        if metrics["system"]["memory_percent"] > 85:
            alerts.append({
                "type": "high_memory",
                "value": metrics["system"]["memory_percent"],
                "threshold": 85,
                "timestamp": metrics["timestamp"]
            })
        
        # Browser pool alert
        if metrics["application"]["browser_pool_size"] < 3:
            alerts.append({
                "type": "low_browser_pool",
                "value": metrics["application"]["browser_pool_size"],
                "threshold": 3,
                "timestamp": metrics["timestamp"]
            })
        
        # Store alerts
        if alerts:
            for alert in alerts:
                await self.redis.lpush("alerts", json.dumps(alert))
                await self.redis.ltrim("alerts", 0, 999)  # Keep last 1000 alerts
    
    async def get_metrics(self, period: str = "hour") -> List[Dict]:
        """Retrieve metrics for specified period"""
        now = datetime.utcnow()
        
        if period == "hour":
            start = now - timedelta(hours=1)
            pattern = "metrics:*"
        elif period == "day":
            start = now - timedelta(days=1)
            pattern = "metrics:*"
        elif period == "week":
            start = now - timedelta(days=7)
            pattern = "metrics:daily"
        else:
            return []
        
        keys = await self.redis.keys(pattern)
        metrics = []
        
        for key in keys:
            data = await self.redis.get(key)
            if data:
                metric = json.loads(data)
                metric_time = datetime.fromisoformat(metric["timestamp"])
                if metric_time >= start:
                    metrics.append(metric)
        
        return sorted(metrics, key=lambda x: x["timestamp"])
