import os
import json
import pickle
import hashlib
from typing import Optional, Any, Dict, List
from datetime import datetime, timedelta
import asyncio
from pathlib import Path

class LocalCache:
    """Local filesystem cache for temporary storage"""
    
    def __init__(self, cache_dir: str = "/tmp/browser_automation_cache", 
                 max_size_mb: int = 1024,  # 1GB default
                 default_ttl: int = 3600):  # 1 hour default
        self.cache_dir = Path(cache_dir)
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.default_ttl = default_ttl
        
        # Create cache directory
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Cache metadata
        self.metadata_file = self.cache_dir / "metadata.json"
        self.metadata = self._load_metadata()
        
        # Start cleanup task
        asyncio.create_task(self._periodic_cleanup())
    
    def _load_metadata(self) -> Dict:
        """Load cache metadata"""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def _save_metadata(self):
        """Save cache metadata"""
        with open(self.metadata_file, 'w') as f:
            json.dump(self.metadata, f, indent=2)
    
    def _get_cache_path(self, key: str) -> Path:
        """Get filesystem path for cache key"""
        # Create hash of key for filename
        key_hash = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{key_hash}.cache"
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set cache value"""
        cache_path = self._get_cache_path(key)
        
        try:
            # Serialize value
            if isinstance(value, (dict, list)):
                data = json.dumps(value).encode()
            else:
                data = pickle.dumps(value)
            
            # Write to file
            with open(cache_path, 'wb') as f:
                f.write(data)
            
            # Update metadata
            self.metadata[key] = {
                "size": len(data),
                "created_at": datetime.utcnow().isoformat(),
                "expires_at": (datetime.utcnow() + timedelta(seconds=ttl or self.default_ttl)).isoformat(),
                "access_count": 0
            }
            
            self._save_metadata()
            
            # Check cache size
            self._enforce_size_limit()
            
            return True
        except Exception as e:
            print(f"Cache set error: {e}")
            return False
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get cache value"""
        cache_path = self._get_cache_path(key)
        
        if not cache_path.exists():
            return default
        
        # Check expiration
        if key in self.metadata:
            expires_at = datetime.fromisoformat(self.metadata[key]["expires_at"])
            if datetime.utcnow() > expires_at:
                self.delete(key)
                return default
        
        try:
            # Read from file
            with open(cache_path, 'rb') as f:
                data = f.read()
            
            # Update access metadata
            if key in self.metadata:
                self.metadata[key]["access_count"] += 1
                self.metadata[key]["last_accessed"] = datetime.utcnow().isoformat()
                self._save_metadata()
            
            # Deserialize based on content
            try:
                # Try JSON first
                return json.loads(data.decode())
            except:
                # Fall back to pickle
                return pickle.loads(data)
        except Exception as e:
            print(f"Cache get error: {e}")
            return default
    
    def delete(self, key: str) -> bool:
        """Delete cache entry"""
        cache_path = self._get_cache_path(key)
        
        try:
            if cache_path.exists():
                cache_path.unlink()
            
            if key in self.metadata:
                del self.metadata[key]
                self._save_metadata()
            
            return True
        except:
            return False
    
    def exists(self, key: str) -> bool:
        """Check if key exists and is not expired"""
        cache_path = self._get_cache_path(key)
        
        if not cache_path.exists():
            return False
        
        if key in self.metadata:
            expires_at = datetime.fromisoformat(self.metadata[key]["expires_at"])
            if datetime.utcnow() > expires_at:
                self.delete(key)
                return False
        
        return True
    
    def clear(self) -> bool:
        """Clear entire cache"""
        try:
            # Delete all cache files
            for cache_file in self.cache_dir.glob("*.cache"):
                cache_file.unlink()
            
            # Delete metadata
            if self.metadata_file.exists():
                self.metadata_file.unlink()
            
            self.metadata = {}
            
            return True
        except Exception as e:
            print(f"Cache clear error: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total_size = 0
        total_files = 0
        expired_files = 0
        
        for cache_file in self.cache_dir.glob("*.cache"):
            total_size += cache_file.stat().st_size
            total_files += 1
        
        # Count expired entries
        now = datetime.utcnow()
        for key, meta in list(self.metadata.items()):
            expires_at = datetime.fromisoformat(meta["expires_at"])
            if now > expires_at:
                expired_files += 1
        
        return {
            "total_size_bytes": total_size,
            "total_size_mb": total_size / (1024 * 1024),
            "total_files": total_files,
            "expired_files": expired_files,
            "metadata_entries": len(self.metadata),
            "cache_dir": str(self.cache_dir),
            "max_size_mb": self.max_size_bytes / (1024 * 1024)
        }
    
    def _enforce_size_limit(self):
        """Enforce cache size limit"""
        stats = self.get_stats()
        
        if stats["total_size_bytes"] > self.max_size_bytes:
            # Need to free space - remove least recently accessed
            self._remove_oldest()
    
    def _remove_oldest(self, target_size_percent: float = 0.8):
        """Remove oldest cache entries until under size limit"""
        # Sort by last accessed (oldest first)
        entries = []
        for key, meta in self.metadata.items():
            last_accessed = meta.get("last_accessed", meta["created_at"])
            entries.append({
                "key": key,
                "last_accessed": last_accessed,
                "size": meta["size"]
            })
        
        entries.sort(key=lambda x: x["last_accessed"])
        
        # Remove until under target size
        target_size = self.max_size_bytes * target_size_percent
        stats = self.get_stats()
        
        removed_size = 0
        for entry in entries:
            if stats["total_size_bytes"] - removed_size <= target_size:
                break
            
            self.delete(entry["key"])
            removed_size += entry["size"]
    
    async def _periodic_cleanup(self, interval: int = 300):
        """Periodic cleanup of expired entries"""
        while True:
            try:
                self._cleanup_expired()
            except Exception as e:
                print(f"Cache cleanup error: {e}")
            
            await asyncio.sleep(interval)
    
    def _cleanup_expired(self):
        """Cleanup expired cache entries"""
        now = datetime.utcnow()
        expired_keys = []
        
        for key, meta in self.metadata.items():
            expires_at = datetime.fromisoformat(meta["expires_at"])
            if now > expires_at:
                expired_keys.append(key)
        
        for key in expired_keys:
            self.delete(key)
        
        if expired_keys:
            print(f"Cleaned up {len(expired_keys)} expired cache entries")
    
    def list_keys(self, pattern: str = "*") -> List[str]:
        """List cache keys matching pattern"""
        import fnmatch
        return [k for k in self.metadata.keys() if fnmatch.fnmatch(k, pattern)]
    
    def get_metadata(self, key: str) -> Optional[Dict]:
        """Get metadata for cache key"""
        return self.metadata.get(key)
    
    def set_ttl(self, key: str, ttl: int) -> bool:
        """Set TTL for existing cache entry"""
        if key not in self.metadata:
            return False
        
        self.metadata[key]["expires_at"] = (
            datetime.utcnow() + timedelta(seconds=ttl)
        ).isoformat()
        
        self._save_metadata()
        return True
    
    def increment(self, key: str, amount: int = 1) -> int:
        """Increment integer value"""
        current = self.get(key, 0)
        if isinstance(current, (int, float)):
            new_value = current + amount
            self.set(key, new_value)
            return new_value
        return 0
    
    def decrement(self, key: str, amount: int = 1) -> int:
        """Decrement integer value"""
        return self.increment(key, -amount)
