import aioredis
import json
from typing import Any, Optional, Dict, List, Union
from datetime import timedelta
import asyncio

class RedisClient:
    def __init__(self, url: str, decode_responses: bool = True):
        self.url = url
        self.decode_responses = decode_responses
        self.redis: Optional[aioredis.Redis] = None
    
    async def connect(self):
        """Connect to Redis"""
        self.redis = await aioredis.from_url(
            self.url,
            decode_responses=self.decode_responses,
            max_connections=100
        )
    
    async def disconnect(self):
        """Disconnect from Redis"""
        if self.redis:
            await self.redis.close()
    
    async def set(self, key: str, value: Any,
                 expire: Optional[int] = None) -> bool:
        """Set key-value pair"""
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        
        if expire:
            return await self.redis.setex(key, expire, value)
        else:
            return await self.redis.set(key, value)
    
    async def get(self, key: str, 
                 parse_json: bool = True) -> Optional[Any]:
        """Get value by key"""
        value = await self.redis.get(key)
        
        if value is None:
            return None
        
        if parse_json and value.startswith(('{', '[')):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        
        return value
    
    async def hset(self, key: str, field: str, value: Any) -> bool:
        """Set hash field"""
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        
        return await self.redis.hset(key, field, value)
    
    async def hget(self, key: str, field: str,
                  parse_json: bool = True) -> Optional[Any]:
        """Get hash field"""
        value = await self.redis.hget(key, field)
        
        if value is None:
            return None
        
        if parse_json and value.startswith(('{', '[')):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        
        return value
    
    async def hgetall(self, key: str,
                     parse_json: bool = True) -> Dict[str, Any]:
        """Get all hash fields"""
        data = await self.redis.hgetall(key)
        
        if parse_json:
            result = {}
            for field, value in data.items():
                if value.startswith(('{', '[')):
                    try:
                        result[field] = json.loads(value)
                    except json.JSONDecodeError:
                        result[field] = value
                else:
                    result[field] = value
            return result
        
        return data
    
    async def zadd(self, key: str, mapping: Dict[str, float]) -> int:
        """Add sorted set members with scores"""
        return await self.redis.zadd(key, mapping)
    
    async def zrange(self, key: str, start: int, end: int,
                    withscores: bool = False) -> List:
        """Get sorted set range"""
        return await self.redis.zrange(key, start, end, 
                                      withscores=withscores)
    
    async def zrem(self, key: str, *members) -> int:
        """Remove sorted set members"""
        return await self.redis.zrem(key, *members)
    
    async def lpush(self, key: str, *values) -> int:
        """Push to list"""
        processed = []
        for value in values:
            if isinstance(value, (dict, list)):
                processed.append(json.dumps(value))
            else:
                processed.append(str(value))
        
        return await self.redis.lpush(key, *processed)
    
    async def lrange(self, key: str, start: int, end: int) -> List:
        """Get list range"""
        values = await self.redis.lrange(key, start, end)
        
        result = []
        for value in values:
            if value.startswith(('{', '[')):
                try:
                    result.append(json.loads(value))
                except json.JSONDecodeError:
                    result.append(value)
            else:
                result.append(value)
        
        return result
    
    async def ltrim(self, key: str, start: int, end: int) -> bool:
        """Trim list"""
        return await self.redis.ltrim(key, start, end)
    
    async def delete(self, *keys) -> int:
        """Delete keys"""
        return await self.redis.delete(*keys)
    
    async def exists(self, key: str) -> bool:
        """Check if key exists"""
        return await self.redis.exists(key) > 0
    
    async def expire(self, key: str, seconds: int) -> bool:
        """Set key expiration"""
        return await self.redis.expire(key, seconds)
    
    async def incr(self, key: str, amount: int = 1) -> int:
        """Increment counter"""
        return await self.redis.incrby(key, amount)
    
    async def decr(self, key: str, amount: int = 1) -> int:
        """Decrement counter"""
        return await self.redis.decrby(key, amount)
    
    async def publish(self, channel: str, message: Any) -> int:
        """Publish to channel"""
        if isinstance(message, (dict, list)):
            message = json.dumps(message)
        
        return await self.redis.publish(channel, message)
    
    async def subscribe(self, channel: str):
        """Subscribe to channel"""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(channel)
        return pubsub
    
    async def scan(self, pattern: str = "*",
                  count: int = 100) -> List[str]:
        """Scan for keys matching pattern"""
        keys = []
        cursor = 0
        
        while True:
            cursor, found_keys = await self.redis.scan(
                cursor, match=pattern, count=count
            )
            keys.extend(found_keys)
            
            if cursor == 0:
                break
        
        return keys
    
    async def flush_db(self):
        """Flush database"""
        await self.redis.flushdb()
    
    async def info(self, section: Optional[str] = None) -> Dict:
        """Get Redis info"""
        info = await self.redis.info(section)
        return info
    
    async def ping(self) -> bool:
        """Ping Redis server"""
        try:
            return await self.redis.ping()
        except:
            return False
    
    async def get_memory_usage(self) -> Dict[str, Any]:
        """Get memory usage statistics"""
        info = await self.info('memory')
        
        return {
            'used_memory': info.get('used_memory'),
            'used_memory_human': info.get('used_memory_human'),
            'used_memory_peak': info.get('used_memory_peak'),
            'used_memory_peak_human': info.get('used_memory_peak_human'),
            'memory_fragmentation_ratio': info.get('mem_fragmentation_ratio'),
            'total_system_memory': info.get('total_system_memory'),
            'total_system_memory_human': info.get('total_system_memory_human')
        }
    
    async def get_client_stats(self) -> Dict[str, Any]:
        """Get client statistics"""
        info = await self.info('clients')
        
        return {
            'connected_clients': info.get('connected_clients'),
            'client_longest_output_list': info.get('client_longest_output_list'),
            'client_biggest_input_buf': info.get('client_biggest_input_buf'),
            'blocked_clients': info.get('blocked_clients')
        }
