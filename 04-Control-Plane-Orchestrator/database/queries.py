from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import json

class JobQueries:
    @staticmethod
    async def create_job(conn, job_id: str, workflow: Dict, 
                        user_id: Optional[str] = None,
                        priority: int = 1) -> None:
        """Create a new job record"""
        await conn.execute('''
            INSERT INTO job_results 
            (id, workflow_name, results, status, user_id, priority)
            VALUES ($1, $2, $3, $4, $5, $6)
        ''', job_id, 
            workflow.get("name", "unnamed"),
            json.dumps({"workflow": workflow}),
            "pending",
            user_id,
            priority
        )
    
    @staticmethod
    async def update_job_status(conn, job_id: str, 
                               status: str,
                               error: Optional[str] = None) -> None:
        """Update job status"""
        if status == "executing":
            await conn.execute('''
                UPDATE job_results 
                SET status = $1, started_at = NOW()
                WHERE id = $2
            ''', status, job_id)
        elif status in ["completed", "failed", "cancelled", "timeout"]:
            await conn.execute('''
                UPDATE job_results 
                SET status = $1, completed_at = NOW(),
                    error_message = $3
                WHERE id = $2
            ''', status, job_id, error)
        else:
            await conn.execute('''
                UPDATE job_results 
                SET status = $1 
                WHERE id = $2
            ''', status, job_id)
    
    @staticmethod
    async def store_job_results(conn, job_id: str, 
                               results: Dict) -> None:
        """Store job execution results"""
        await conn.execute('''
            UPDATE job_results 
            SET results = $1 
            WHERE id = $2
        ''', json.dumps(results), job_id)
    
    @staticmethod
    async def get_job(conn, job_id: str) -> Optional[Dict]:
        """Get job by ID"""
        row = await conn.fetchrow('''
            SELECT 
                id,
                workflow_name,
                results,
                status,
                created_at,
                started_at,
                completed_at,
                execution_time,
                error_message,
                user_id,
                priority
            FROM job_results 
            WHERE id = $1
        ''', job_id)
        
        if row:
            return dict(row)
        return None
    
    @staticmethod
    async def list_jobs(conn, 
                       limit: int = 100,
                       offset: int = 0,
                       user_id: Optional[str] = None,
                       status: Optional[str] = None,
                       from_date: Optional[datetime] = None,
                       to_date: Optional[datetime] = None) -> List[Dict]:
        """List jobs with filters"""
        query = '''
            SELECT 
                id,
                workflow_name,
                status,
                created_at,
                started_at,
                completed_at,
                execution_time,
                user_id,
                priority
            FROM job_results 
            WHERE 1=1
        '''
        params = []
        param_count = 0
        
        if user_id:
            param_count += 1
            query += f" AND user_id = ${param_count}"
            params.append(user_id)
        
        if status:
            param_count += 1
            query += f" AND status = ${param_count}"
            params.append(status)
        
        if from_date:
            param_count += 1
            query += f" AND created_at >= ${param_count}"
            params.append(from_date)
        
        if to_date:
            param_count += 1
            query += f" AND created_at <= ${param_count}"
            params.append(to_date)
        
        query += f" ORDER BY created_at DESC LIMIT ${param_count + 1} OFFSET ${param_count + 2}"
        params.extend([limit, offset])
        
        rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]
    
    @staticmethod
    async def get_job_stats(conn, 
                           time_period: str = "day") -> Dict[str, Any]:
        """Get job statistics"""
        if time_period == "hour":
            interval = "1 hour"
        elif time_period == "day":
            interval = "24 hours"
        elif time_period == "week":
            interval = "7 days"
        elif time_period == "month":
            interval = "30 days"
        else:
            interval = "24 hours"
        
        # Get counts by status
        status_counts = await conn.fetch('''
            SELECT 
                status,
                COUNT(*) as count
            FROM job_results 
            WHERE created_at >= NOW() - $1::interval
            GROUP BY status
        ''', interval)
        
        # Get average execution time
        avg_time = await conn.fetchval('''
            SELECT AVG(EXTRACT(EPOCH FROM execution_time))
            FROM job_results 
            WHERE status = 'completed' 
            AND created_at >= NOW() - $1::interval
        ''', interval)
        
        # Get success rate
        total = await conn.fetchval('''
            SELECT COUNT(*)
            FROM job_results 
            WHERE created_at >= NOW() - $1::interval
        ''', interval)
        
        successful = await conn.fetchval('''
            SELECT COUNT(*)
            FROM job_results 
            WHERE status = 'completed' 
            AND created_at >= NOW() - $1::interval
        ''', interval)
        
        success_rate = (successful / total * 100) if total > 0 else 0
        
        # Get hourly/daily distribution
        time_series = await conn.fetch('''
            SELECT 
                date_trunc('hour', created_at) as time_bucket,
                COUNT(*) as count,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as success_count
            FROM job_results 
            WHERE created_at >= NOW() - $1::interval
            GROUP BY time_bucket
            ORDER BY time_bucket
        ''', interval)
        
        return {
            "status_counts": {row["status"]: row["count"] for row in status_counts},
            "avg_execution_time": avg_time or 0,
            "success_rate": success_rate,
            "time_series": [
                {
                    "time": row["time_bucket"].isoformat(),
                    "total": row["count"],
                    "successful": row["success_count"]
                }
                for row in time_series
            ],
            "time_period": time_period,
            "total_jobs": total or 0
        }

class ArtifactQueries:
    @staticmethod
    async def create_artifact(conn, job_id: str, 
                             artifact_type: str,
                             storage_key: str,
                             storage_url: str,
                             metadata: Optional[Dict] = None,
                             content_type: Optional[str] = None,
                             file_size: Optional[int] = None) -> str:
        """Create artifact record"""
        result = await conn.fetchrow('''
            INSERT INTO artifacts 
            (job_id, artifact_type, storage_key, storage_url,
             content_type, file_size, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
        ''', job_id, artifact_type, storage_key, storage_url,
            content_type, file_size, json.dumps(metadata or {}))
        
        return result["id"]
    
    @staticmethod
    async def get_job_artifacts(conn, job_id: str) -> List[Dict]:
        """Get artifacts for a job"""
        rows = await conn.fetch('''
            SELECT 
                id,
                artifact_type,
                storage_key,
                storage_url,
                content_type,
                file_size,
                created_at,
                metadata
            FROM artifacts 
            WHERE job_id = $1
            ORDER BY created_at
        ''', job_id)
        
        return [dict(row) for row in rows]
    
    @staticmethod
    async def delete_artifact(conn, artifact_id: str) -> bool:
        """Delete artifact record"""
        result = await conn.execute('''
            DELETE FROM artifacts 
            WHERE id = $1
        ''', artifact_id)
        
        return result == "DELETE 1"
    
    @staticmethod
    async def cleanup_old_artifacts(conn, 
                                   days_old: int = 30) -> int:
        """Cleanup artifacts older than specified days"""
        result = await conn.execute('''
            DELETE FROM artifacts 
            WHERE created_at < NOW() - $1::interval
            RETURNING id
        ''', f"{days_old} days")
        
        # Parse DELETE result to get count
        if isinstance(result, str) and "DELETE" in result:
            return int(result.split()[1])
        return 0

class UserQueries:
    @staticmethod
    async def create_user(conn, email: str, 
                         api_key: str,
                         plan: str = "free") -> str:
        """Create new user"""
        result = await conn.fetchrow('''
            INSERT INTO users (email, api_key, plan)
            VALUES ($1, $2, $3)
            RETURNING id
        ''', email, api_key, plan)
        
        return result["id"]
    
    @staticmethod
    async def get_user_by_api_key(conn, api_key: str) -> Optional[Dict]:
        """Get user by API key"""
        row = await conn.fetchrow('''
            SELECT 
                id,
                email,
                api_key,
                plan,
                rate_limit_config,
                is_active,
                created_at,
                last_login,
                metadata
            FROM users 
            WHERE api_key = $1 AND is_active = true
        ''', api_key)
        
        if row:
            return dict(row)
        return None
    
    @staticmethod
    async def update_user_last_login(conn, user_id: str) -> None:
        """Update user last login time"""
        await conn.execute('''
            UPDATE users 
            SET last_login = NOW() 
            WHERE id = $1
        ''', user_id)
    
    @staticmethod
    async def update_user_plan(conn, user_id: str, 
                              plan: str) -> None:
        """Update user plan"""
        await conn.execute('''
            UPDATE users 
            SET plan = $1 
            WHERE id = $2
        ''', plan, user_id)
        
        # Update rate limit config based on plan
        rate_limits = {
            "free": {"minute": 5, "hour": 50},
            "basic": {"minute": 30, "hour": 500},
            "pro": {"minute": 100, "hour": 5000},
            "enterprise": {"minute": 500, "hour": 50000}
        }
        
        await conn.execute('''
            UPDATE users 
            SET rate_limit_config = $1 
            WHERE id = $2
        ''', json.dumps(rate_limits.get(plan, rate_limits["free"])), user_id)
    
    @staticmethod
    async def validate_api_key(conn, api_key: str) -> bool:
        """Validate API key"""
        count = await conn.fetchval('''
            SELECT COUNT(*) 
            FROM users 
            WHERE api_key = $1 AND is_active = true
        ''', api_key)
        
        return count > 0

class AuditQueries:
    @staticmethod
    async def log_action(conn, 
                        user_id: Optional[str],
                        action: str,
                        resource_type: Optional[str] = None,
                        resource_id: Optional[str] = None,
                        details: Optional[Dict] = None,
                        ip_address: Optional[str] = None,
                        user_agent: Optional[str] = None) -> None:
        """Log audit action"""
        await conn.execute('''
            INSERT INTO audit_logs 
            (user_id, action, resource_type, resource_id,
             details, ip_address, user_agent)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        ''', user_id, action, resource_type, resource_id,
            json.dumps(details or {}), ip_address, user_agent)
    
    @staticmethod
    async def get_audit_logs(conn,
                            limit: int = 100,
                            offset: int = 0,
                            user_id: Optional[str] = None,
                            action: Optional[str] = None,
                            from_date: Optional[datetime] = None,
                            to_date: Optional[datetime] = None) -> List[Dict]:
        """Get audit logs with filters"""
        query = '''
            SELECT 
                id,
                user_id,
                action,
                resource_type,
                resource_id,
                details,
                ip_address,
                user_agent,
                timestamp
            FROM audit_logs 
            WHERE 1=1
        '''
        params = []
        param_count = 0
        
        if user_id:
            param_count += 1
            query += f" AND user_id = ${param_count}"
            params.append(user_id)
        
        if action:
            param_count += 1
            query += f" AND action = ${param_count}"
            params.append(action)
        
        if from_date:
            param_count += 1
            query += f" AND timestamp >= ${param_count}"
            params.append(from_date)
        
        if to_date:
            param_count += 1
            query += f" AND timestamp <= ${param_count}"
            params.append(to_date)
        
        query += f" ORDER BY timestamp DESC LIMIT ${param_count + 1} OFFSET ${param_count + 2}"
        params.extend([limit, offset])
        
        rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]
