import json
import yaml
from typing import Dict, Any, Optional
from datetime import datetime
import hashlib

class ConfigManager:
    def __init__(self, redis_client, pg_pool):
        self.redis = redis_client
        self.pg_pool = pg_pool
        self.configs = {}
        self.config_versions = {}
        
    async def load_configs(self):
        """Load configurations from database"""
        async with self.pg_pool.acquire() as conn:
            # Load configs from database
            rows = await conn.fetch('''
                SELECT name, config, version, is_active 
                FROM configurations 
                WHERE is_active = true
            ''')
            
            for row in rows:
                self.configs[row['name']] = {
                    "config": json.loads(row['config']),
                    "version": row['version'],
                    "hash": self._calculate_hash(row['config'])
                }
            
            # Store in Redis for fast access
            for name, config in self.configs.items():
                await self.redis.setex(
                    f"config:{name}",
                    3600,  # 1 hour TTL
                    json.dumps(config)
                )
    
    def _calculate_hash(self, config_data: str) -> str:
        """Calculate hash of configuration"""
        return hashlib.sha256(config_data.encode()).hexdigest()[:16]
    
    async def get_config(self, name: str, version: Optional[str] = None) -> Dict:
        """Get configuration by name and optional version"""
        # Try Redis first
        cached = await self.redis.get(f"config:{name}")
        if cached:
            config = json.loads(cached)
            if not version or config["version"] == version:
                return config["config"]
        
        # Fallback to database
        async with self.pg_pool.acquire() as conn:
            query = '''
                SELECT config FROM configurations 
                WHERE name = $1 AND is_active = true
            '''
            params = [name]
            
            if version:
                query += ' AND version = $2'
                params.append(version)
            
            row = await conn.fetchrow(query, *params)
            if row:
                config = json.loads(row['config'])
                
                # Update cache
                await self.redis.setex(
                    f"config:{name}",
                    3600,
                    json.dumps({
                        "config": config,
                        "version": version or "latest",
                        "hash": self._calculate_hash(json.dumps(config))
                    })
                )
                
                return config
        
        raise KeyError(f"Configuration not found: {name}")
    
    async def update_config(self, name: str, config: Dict, 
                          user: str = "system") -> str:
        """Update configuration with versioning"""
        config_str = json.dumps(config, indent=2)
        config_hash = self._calculate_hash(config_str)
        
        async with self.pg_pool.acquire() as conn:
            # Get current version
            current = await conn.fetchrow('''
                SELECT version FROM configurations 
                WHERE name = $1 AND is_active = true
            ''', name)
            
            if current:
                # Deactivate old version
                await conn.execute('''
                    UPDATE configurations 
                    SET is_active = false 
                    WHERE name = $1 AND is_active = true
                ''', name)
                
                # Parse version (format: v1.2.3)
                version_parts = current['version'][1:].split('.')
                new_version = f"v{int(version_parts[0])}.{int(version_parts[1])}.{int(version_parts[2]) + 1}"
            else:
                new_version = "v1.0.0"
            
            # Insert new version
            await conn.execute('''
                INSERT INTO configurations 
                (name, config, version, created_by, hash, is_active)
                VALUES ($1, $2, $3, $4, $5, true)
            ''', name, config_str, new_version, user, config_hash)
            
            # Update cache
            await self.redis.setex(
                f"config:{name}",
                3600,
                json.dumps({
                    "config": config,
                    "version": new_version,
                    "hash": config_hash
                })
            )
            
            # Log change
            await self._log_config_change(name, new_version, user)
            
            return new_version
    
    async def _log_config_change(self, name: str, version: str, user: str):
        """Log configuration change"""
        async with self.pg_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO config_changes 
                (config_name, version, changed_by, changed_at)
                VALUES ($1, $2, $3, $4)
            ''', name, version, user, datetime.utcnow())
    
    async def rollback_config(self, name: str, version: str) -> bool:
        """Rollback to previous configuration version"""
        async with self.pg_pool.acquire() as conn:
            # Check if version exists
            exists = await conn.fetchval('''
                SELECT 1 FROM configurations 
                WHERE name = $1 AND version = $2
            ''', name, version)
            
            if not exists:
                return False
            
            # Deactivate current version
            await conn.execute('''
                UPDATE configurations 
                SET is_active = false 
                WHERE name = $1 AND is_active = true
            ''', name)
            
            # Activate specified version
            await conn.execute('''
                UPDATE configurations 
                SET is_active = true 
                WHERE name = $1 AND version = $2
            ''', name, version)
            
            # Update cache
            config_row = await conn.fetchrow('''
                SELECT config FROM configurations 
                WHERE name = $1 AND version = $2
            ''', name, version)
            
            if config_row:
                config = json.loads(config_row['config'])
                await self.redis.setex(
                    f"config:{name}",
                    3600,
                    json.dumps({
                        "config": config,
                        "version": version,
                        "hash": self._calculate_hash(config_row['config'])
                    })
                )
            
            return True
    
    async def get_config_history(self, name: str, limit: int = 10) -> List[Dict]:
        """Get configuration change history"""
        async with self.pg_pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT 
                    c.version,
                    c.config,
                    c.hash,
                    c.created_at,
                    c.created_by,
                    cc.changed_at,
                    cc.changed_by
                FROM configurations c
                LEFT JOIN config_changes cc ON c.name = cc.config_name 
                    AND c.version = cc.version
                WHERE c.name = $1
                ORDER BY c.created_at DESC
                LIMIT $2
            ''', name, limit)
            
            history = []
            for row in rows:
                history.append({
                    "version": row["version"],
                    "hash": row["hash"],
                    "created_at": row["created_at"].isoformat(),
                    "created_by": row["created_by"],
                    "last_change": row["changed_at"].isoformat() if row["changed_at"] else None,
                    "changed_by": row["changed_by"]
                })
            
            return history
    
    async def validate_config(self, config_type: str, config: Dict) -> List[str]:
        """Validate configuration"""
        errors = []
        
        if config_type == "rate_limits":
            if "limits" not in config:
                errors.append("Missing 'limits' section")
            else:
                for window, limit in config["limits"].items():
                    if not isinstance(limit, int) or limit <= 0:
                        errors.append(f"Invalid limit for {window}: {limit}")
        
        elif config_type == "browser_pool":
            if "max_browsers" not in config:
                errors.append("Missing 'max_browsers'")
            elif config["max_browsers"] > 50:
                errors.append("max_browsers cannot exceed 50")
            
            if "user_agents" in config:
                if not isinstance(config["user_agents"], list):
                    errors.append("user_agents must be a list")
                elif len(config["user_agents"]) == 0:
                    errors.append("user_agents cannot be empty")
        
        elif config_type == "workflow_defaults":
            required = ["timeout", "retry_count", "priority"]
            for field in required:
                if field not in config:
                    errors.append(f"Missing required field: {field}")
        
        return errors
    
    async def export_configs(self, format: str = "json") -> str:
        """Export all configurations"""
        configs = {}
        
        for name in self.configs:
            configs[name] = {
                "config": self.configs[name]["config"],
                "version": self.configs[name]["version"],
                "hash": self.configs[name]["hash"]
            }
        
        if format == "json":
            return json.dumps(configs, indent=2)
        elif format == "yaml":
            return yaml.dump(configs, default_flow_style=False)
        else:
            raise ValueError(f"Unsupported format: {format}")
