import asyncio
import time
from typing import Dict, Any, List
from datetime import datetime, timedelta
from .standard_executor import StandardExecutor
from .executor import JobResult, JobStatus
import hashlib
import logging
from dataclasses import dataclass
from playwright.async_api import Page, Request, Response
import json
import os

logger = logging.getLogger(__name__)

@dataclass
class ArtifactConfig:
    capture_screenshots: bool = True
    capture_har: bool = True
    capture_console: bool = True
    capture_dom: bool = True
    full_page: bool = True

class EnhancedExecutor(StandardExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.artifact_queue = asyncio.Queue()
        self.active_sessions = {}
        
    async def _execute_job(self, job_data: Dict[str, Any]) -> JobResult:
        job_id = job_data.get('id', 'unknown')
        job_type = job_data.get('type', 'unknown')
        
        artifacts = {}
        session_key = await self._get_session_key(job_data)
        
        if session_key in self.active_sessions:
            page = self.active_sessions[session_key]
            logger.info(f"Reusing session for {session_key}")
        else:
            page = await self.pool.acquire_page()
            self.active_sessions[session_key] = page
            logger.info(f"Created new session for {session_key}")
        
        try:
            await self._setup_artifact_capture(page, job_id)

            # Prefer the new action system, reusing the session page.
            from core.action_adapter import execute_action_as_job_result

            result = await execute_action_as_job_result(
                job_data, browser_pool=self.pool, page=page
            )
            
            artifacts = await self._collect_artifacts(page, job_data)
            result.artifacts.update(artifacts)
            
            return result
            
        finally:
            if session_key not in self.active_sessions:
                await self.pool.release_page(page)
    
    async def _get_session_key(self, job_data: Dict[str, Any]) -> str:
        target = job_data.get('target', {})
        domain = target.get('domain', '')
        credentials = await self._get_credentials(job_data)
        
        if credentials:
            cred_hash = hashlib.md5(json.dumps(credentials).encode()).hexdigest()
            return f"{domain}:{cred_hash}"
        
        return domain
    
    async def _get_credentials(self, job_data: Dict[str, Any]) -> Dict[str, str]:
        credentials = job_data.get('credentials', {})
        
        if not credentials:
            target = job_data.get('target', {})
            domain = target.get('domain', '')
            
            env_key = f"CRED_{domain.upper().replace('.', '_')}"
            username = credentials.get('username') or os.getenv(f"{env_key}_USERNAME")
            password = credentials.get('password') or os.getenv(f"{env_key}_PASSWORD")
            
            if username and password:
                return {'username': username, 'password': password}
        
        return credentials
    
    async def _setup_artifact_capture(self, page: Page, job_id: str):
        console_messages = []
        network_requests = []
        
        def console_handler(msg):
            console_messages.append({
                'type': msg.type,
                'text': msg.text,
                'timestamp': datetime.utcnow().isoformat()
            })
        
        def request_handler(request: Request):
            network_requests.append({
                'url': request.url,
                'method': request.method,
                'headers': dict(request.headers),
                'timestamp': datetime.utcnow().isoformat()
            })
        
        page.on('console', console_handler)
        page.on('request', request_handler)
        
        self.artifact_queue.put_nowait({
            'job_id': job_id,
            'console': console_messages,
            'network': network_requests
        })
    
    async def _collect_artifacts(self, page: Page, job_data: Dict[str, Any]) -> Dict[str, Any]:
        artifacts = {}
        config = job_data.get('artifact_config', {})
        
        if config.get('capture_screenshots', True):
            if config.get('full_page', True):
                screenshot = await page.screenshot(full_page=True)
                artifacts['screenshot_full'] = screenshot
            
            screenshot = await page.screenshot()
            artifacts['screenshot'] = screenshot
        
        if config.get('capture_dom', True):
            dom_content = await page.content()
            artifacts['dom'] = dom_content
        
        if config.get('capture_console', True):
            console_logs = await page.evaluate("""() => {
                return window.console.messages || [];
            }""")
            artifacts['console_logs'] = console_logs
        
        return artifacts
    
    async def _persist_session(self, cookies: List[Dict], job_data: Dict[str, Any]):
        session_key = await self._get_session_key(job_data)
        
        await self.redis.setex(
            f"session:{session_key}",
            timedelta(hours=24),
            json.dumps(cookies)
        )
    
    async def cleanup(self):
        for page in self.active_sessions.values():
            await self.pool.release_page(page)
        self.active_sessions.clear()
        await super().cleanup()
