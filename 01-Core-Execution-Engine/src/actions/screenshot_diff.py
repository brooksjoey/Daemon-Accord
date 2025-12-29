# backend/app/execution/actions/screenshot_diff.py
import asyncio
import os
import base64
from typing import Dict, Any, Optional, Tuple
from playwright.async_api import Page
from PIL import Image, ImageDraw, ImageChops, ImageFilter
import numpy as np
from .legacy_base_action import BaseAction, ActionContext, ActionResult

class ScreenshotDiffAction(BaseAction):
    action_type = "screenshot_diff"
    
    async def execute(self, context: ActionContext) -> ActionResult:
        try:
            page = context.page
            job = context.job
            
            if not job.payload or 'screenshot_config' not in job.payload:
                return self._create_result(
                    success=False,
                    context=context,
                    error="No screenshot configuration provided"
                )
            
            screenshot_config = job.payload['screenshot_config']
            
            # Create artifacts directory
            artifacts_dir = f"/app/artifacts/{job.id}/screenshots"
            os.makedirs(artifacts_dir, exist_ok=True)
            
            # Take before screenshot
            before_path = os.path.join(artifacts_dir, "before.png")
            before_screenshot = await self._take_screenshot(page, before_path, screenshot_config)
            
            # Perform action if specified
            if screenshot_config.get('action'):
                action_result = await self._perform_action(page, screenshot_config['action'])
                if not action_result.get('success', True):
                    return self._create_result(
                        success=False,
                        context=context,
                        error=f"Action failed: {action_result.get('error')}"
                    )
            
            # Wait for changes
            if screenshot_config.get('wait_for_change'):
                await self._wait_for_change(page, screenshot_config['wait_for_change'])
            else:
                await asyncio.sleep(screenshot_config.get('delay', 1))
            
            # Take after screenshot
            after_path = os.path.join(artifacts_dir, "after.png")
            after_screenshot = await self._take_screenshot(page, after_path, screenshot_config)
            
            # Generate diff
            diff_result = await self._generate_diff(before_path, after_path, artifacts_dir, screenshot_config)
            
            artifacts = {
                'before': before_path,
                'after': after_path,
                'diff': diff_result.get('diff_path'),
                'highlight': diff_result.get('highlight_path'),
                'heatmap': diff_result.get('heatmap_path')
            }
            
            return self._create_result(
                success=True,
                context=context,
                data={
                    'screenshots': {
                        'before': before_screenshot,
                        'after': after_screenshot
                    },
                    'diff': diff_result.get('metrics', {}),
                    'changes': diff_result.get('changes', [])
                },
                artifacts=artifacts
            )
            
        except Exception as e:
            return self._create_result(
                success=False,
                context=context,
                error=f"Screenshot diff failed: {str(e)}"
            )
    
    async def _take_screenshot(self, page: Page, path: str, config: Dict[str, Any]) -> Dict[str, Any]:
        screenshot_config = {
            'path': path,
            'full_page': config.get('full_page', False),
            'type': 'png'
        }
        
        if not screenshot_config['full_page']:
            viewport = await page.evaluate('''() => ({
                width: window.innerWidth,
                height: window.innerHeight
            })''')
            screenshot_config['clip'] = {
                'x': 0,
                'y': 0,
                'width': viewport['width'],
                'height': viewport['height']
            }
        
        await page.screenshot(**screenshot_config)
        
        # Get metadata
        metadata = {
            'path': path,
            'timestamp': asyncio.get_event_loop().time(),
            'full_page': config.get('full_page', False)
        }
        
        return metadata