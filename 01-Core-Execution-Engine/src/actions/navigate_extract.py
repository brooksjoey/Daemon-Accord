from typing import Dict, Any, Optional
from playwright.async_api import Page
import asyncio
import time
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ExtractionResult:
    data: Dict[str, Any]
    duration: float
    success: bool
    error: Optional[str] = None

async def execute_navigation(job_data: Dict[str, Any], browser_pool) -> ExtractionResult:
    page = await browser_pool.acquire_page()
    start_time = time.time()
    
    try:
        target = job_data.get('target', {})
        url = target.get('url', '')
        selectors = job_data.get('selectors', [])
        wait_strategy = job_data.get('wait_strategy', 'networkidle')
        timeout = job_data.get('timeout', 30000)
        
        if not url:
            raise ValueError("No URL provided for navigation")
        
        logger.info(f"Navigating to {url}")
        
        await page.goto(url, wait_until=wait_strategy, timeout=timeout)
        
        extracted_data = {}
        for selector_config in selectors:
            selector = selector_config.get('selector', '')
            attribute = selector_config.get('attribute', 'text')
            multiple = selector_config.get('multiple', False)
            
            if not selector:
                continue
            
            try:
                if multiple:
                    elements = await page.query_selector_all(selector)
                    values = []
                    for element in elements:
                        if attribute == 'text':
                            value = await element.text_content()
                        else:
                            value = await element.get_attribute(attribute)
                        
                        if value:
                            values.append(value.strip())
                    extracted_data[selector] = values
                else:
                    element = await page.query_selector(selector)
                    if element:
                        if attribute == 'text':
                            value = await element.text_content()
                        else:
                            value = await element.get_attribute(attribute)
                        
                        if value:
                            extracted_data[selector] = value.strip()
            except Exception as e:
                logger.warning(f"Failed to extract from selector {selector}: {e}")
        
        duration = time.time() - start_time
        logger.info(f"Extraction completed in {duration:.2f}s")
        
        return ExtractionResult(
            data={'extracted': extracted_data},
            duration=duration,
            success=True
        )
        
    except Exception as e:
        duration = time.time() - start_time
        error_msg = f"Navigation/extraction failed: {str(e)}"
        logger.error(error_msg)
        
        return ExtractionResult(
            data={},
            duration=duration,
            success=False,
            error=error_msg
        )
        
    finally:
        await browser_pool.release_page(page)
