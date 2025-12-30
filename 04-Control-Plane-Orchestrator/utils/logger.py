import logging
import sys
from logging.handlers import RotatingFileHandler
import json
from datetime import datetime
from typing import Dict, Any
import traceback

class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_object = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        if hasattr(record, 'context'):
            log_object["context"] = record.context
        
        if record.exc_info:
            log_object["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info)
            }
        
        return json.dumps(log_object)

def setup_logger(name: str, level: str = "INFO", log_file: str = None) -> logging.Logger:
    """Setup logger with JSON formatting"""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(JSONFormatter())
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
        )
        file_handler.setFormatter(JSONFormatter())
        logger.addHandler(file_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger

def log_with_context(logger: logging.Logger, level: str, message: str, **context):
    """Log message with additional context"""
    log_method = getattr(logger, level.lower(), logger.info)
    
    # Create log record with extra context
    extra = {'context': context}
    log_method(message, extra=extra)

# Module-specific loggers
execution_logger = setup_logger("execution", "INFO")
api_logger = setup_logger("api", "INFO")
database_logger = setup_logger("database", "INFO")
control_plane_logger = setup_logger("control_plane", "INFO")
browser_logger = setup_logger("browser", "INFO")

class AuditLogger:
    """Specialized logger for audit trails"""
    
    def __init__(self, pg_pool):
        self.pg_pool = pg_pool
        self.logger = setup_logger("audit", "INFO")
    
    async def log_action(self, action: str, user_id: str = None, 
                        resource_type: str = None, resource_id: str = None,
                        details: Dict[str, Any] = None, ip: str = None,
                        user_agent: str = None):
        """Log audit action to database and logger"""
        # Log to database
        if self.pg_pool:
            try:
                async with self.pg_pool.acquire() as conn:
                    await conn.execute('''
                        INSERT INTO audit_logs 
                        (user_id, action, resource_type, resource_id,
                         details, ip_address, user_agent)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ''', user_id, action, resource_type, resource_id,
                        json.dumps(details or {}), ip, user_agent)
            except Exception as e:
                self.logger.error(f"Failed to log audit action to database: {e}")
        
        # Log to structured logger
        log_with_context(
            self.logger,
            "info",
            f"Audit action: {action}",
            user_id=user_id,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details,
            ip=ip,
            user_agent=user_agent
        )
    
    async def query_audit_logs(self, filters: Dict = None, limit: int = 100, offset: int = 0):
        """Query audit logs"""
        if not self.pg_pool:
            return []
        
        try:
            async with self.pg_pool.acquire() as conn:
                query = '''
                    SELECT * FROM audit_logs 
                    WHERE 1=1
                '''
                params = []
                param_count = 0
                
                if filters:
                    for key, value in filters.items():
                        if value is not None:
                            param_count += 1
                            query += f" AND {key} = ${param_count}"
                            params.append(value)
                
                query += f" ORDER BY timestamp DESC LIMIT ${param_count + 1} OFFSET ${param_count + 2}"
                params.extend([limit, offset])
                
                rows = await conn.fetch(query, *params)
                return [dict(row) for row in rows]
        except Exception as e:
            self.logger.error(f"Failed to query audit logs: {e}")
            return []
