import time
from typing import Dict, List, Set
import asyncio
from datetime import datetime, timedelta
import hashlib

class DistributedRateLimiter:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.windows = {
            "second": 1,
            "minute": 60,
            "hour": 3600,
            "day": 86400
        }
        
    async def check_limit(self, key: str, limit: int, window: str = "minute") -> bool:
        """Check if request is within rate limit"""
        if window not in self.windows:
            raise ValueError(f"Invalid window: {window}")
        
        window_seconds = self.windows[window]
        current_time = int(time.time())
        window_key = f"ratelimit:{key}:{window}"
        
        # Remove old entries
        await self.redis.zremrangebyscore(
            window_key, 0, current_time - window_seconds
        )
        
        # Count requests in window
        count = await self.redis.zcard(window_key)
        
        if count >= limit:
            return False
        
        # Add new request
        await self.redis.zadd(window_key, {str(current_time): current_time})
        await self.redis.expire(window_key, window_seconds)
        
        return True
    
    async def get_remaining(self, key: str, limit: int, window: str = "minute") -> int:
        """Get remaining requests in window"""
        window_seconds = self.windows[window]
        current_time = int(time.time())
        window_key = f"ratelimit:{key}:{window}"
        
        await self.redis.zremrangebyscore(
            window_key, 0, current_time - window_seconds
        )
        
        count = await self.redis.zcard(window_key)
        return max(0, limit - count)
    
    async def domain_limiter(self, domain: str, limits: Dict[str, int] = None) -> bool:
        """Rate limiter for domains"""
        if limits is None:
            limits = {"minute": 10, "hour": 100}
        
        for window, limit in limits.items():
            allowed = await self.check_limit(
                f"domain:{domain}", limit, window
            )
            if not allowed:
                return False
        
        return True
    
    async def ip_limiter(self, ip: str, limits: Dict[str, int] = None) -> bool:
        """Rate limiter for IP addresses"""
        if limits is None:
            limits = {"minute": 60, "hour": 1000}
        
        for window, limit in limits.items():
            allowed = await self.check_limit(
                f"ip:{ip}", limit, window
            )
            if not allowed:
                return False
        
        return True
    
    async def user_limiter(self, user_id: str, plan: str = "free") -> bool:
        """Rate limiter based on user plan"""
        plans = {
            "free": {"minute": 5, "hour": 50},
            "basic": {"minute": 30, "hour": 500},
            "pro": {"minute": 100, "hour": 5000},
            "enterprise": {"minute": 500, "hour": 50000}
        }
        
        if plan not in plans:
            plan = "free"
        
        limits = plans[plan]
        
        for window, limit in limits.items():
            allowed = await self.check_limit(
                f"user:{user_id}", limit, window
            )
            if not allowed:
                return False
        
        return True
    
    async def get_limits(self, identifier: str) -> Dict:
        """Get current limit status for identifier"""
        status = {}
        
        for window in self.windows:
            for prefix in ["domain:", "ip:", "user:"]:
                if identifier.startswith(prefix):
                    key = identifier
                    break
            else:
                continue
            
            window_key = f"ratelimit:{key}:{window}"
            current_time = int(time.time())
            
            await self.redis.zremrangebyscore(
                window_key, 0, current_time - self.windows[window]
            )
            
            count = await self.redis.zcard(window_key)
            status[window] = {
                "count": count,
                "window_seconds": self.windows[window]
            }
        
        return status
