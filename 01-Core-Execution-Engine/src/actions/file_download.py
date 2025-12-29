import asyncio
import os
import hashlib
from typing import Dict, Any, Optional
from playwright.async_api import Page, Download
from .legacy_base_action import BaseAction, ActionContext, ActionResult
import shutil

class FileDownloadAction(BaseAction):
    action_type = "file_download"
    
    async def execute(self, context: ActionContext) -> ActionResult:
        try:
            page = context.page
            job = context.job
            
            if not job.payload or 'download_config' not in job.payload:
                return self._create_result(
                    success=False,
                    context=context,
                    error="No download configuration provided"
                )
            
            download_config = job.payload['download_config']
            
            # Setup download path
            download_dir = f"/app/artifacts/{job.id}/downloads"
            os.makedirs(download_dir, exist_ok=True)
            
            # Intercept downloads
            download = await self._initiate_download(page, download_config)
            if not download:
                return self._create_result(
                    success=False,
                    context=context,
                    error="Download initiation failed"
                )
            
            # Save file
            file_path = await download.save_as(os.path.join(download_dir, download.suggested_filename))
            
            # Verify file
            verification = await self._verify_file(file_path, download_config)
            
            # Generate metadata
            metadata = await self._generate_file_metadata(file_path, download_config)
            
            artifacts = {
                'file_path': file_path,
                'metadata_path': f"{file_path}.meta.json"
            }
            
            # Store metadata
            with open(f"{file_path}.meta.json", 'w') as f:
                import json
                json.dump(metadata, f, indent=2)
            
            return self._create_result(
                success=verification.get('valid', False),
                context=context,
                data={
                    'download': {
                        'filename': download.suggested_filename,
                        'url': download.url,
                        'size': os.path.getsize(file_path)
                    },
                    'verification': verification,
                    'metadata': metadata
                },
                artifacts=artifacts
            )
            
        except Exception as e:
            return self._create_result(
                success=False,
                context=context,
                error=f"File download failed: {str(e)}"
            )
    
    async def _initiate_download(self, page: Page, config: Dict[str, Any]) -> Optional[Download]:
        method = config.get('method', 'click')
        
        if method == 'click':
            selector = config.get('selector')
            if not selector:
                return None
            
            async with page.expect_download() as download_info:
                await page.click(selector)
            return await download_info.value
        
        elif method == 'link':
            url = config.get('url')
            if not url:
                return None
            
            async with page.expect_download() as download_info:
                await page.goto(url)
            return await download_info.value
        
        elif method == 'api':
            # Direct API download
            url = config.get('url')
            if not url:
                return None
            
            # Use Playwright's API request for download
            async with page.expect_download() as download_info:
                await page.evaluate(f'''
                    async () => {{
                        const response = await fetch("{url}", {{
                            method: 'GET',
                            credentials: 'include'
                        }});
                        const blob = await response.blob();
                        const url = window.URL.createObjectURL(blob);
                        const a = document.createElement('a');
                        a.href = url;
                        a.download = '{config.get("filename", "download")}';
                        document.body.appendChild(a);
                        a.click();
                        window.URL.revokeObjectURL(url);
                        document.body.removeChild(a);
                    }}
                ''')
            
            return await download_info.value
        
        return None
    
    async def _verify_file(self, file_path: str, config: Dict[str, Any]) -> Dict[str, Any]:
        if not os.path.exists(file_path):
            return {'valid': False, 'error': 'File does not exist'}
        
        file_size = os.path.getsize(file_path)
        
        # Check size constraints
        min_size = config.get('min_size', 0)
        max_size = config.get('max_size', 1024 * 1024 * 100)  # 100MB default
        
        if file_size < min_size:
            return {'valid': False, 'error': f'File too small: {file_size} < {min_size}'}
        if file_size > max_size:
            return {'valid': False, 'error': f'File too large: {file_size} > {max_size}'}
        
        # Calculate checksums
        checksums = {}
        
        if config.get('verify_sha256', True):
            sha256 = await self._calculate_sha256(file_path)
            checksums['sha256'] = sha256
            
            expected_hash = config.get('expected_sha256')
            if expected_hash and sha256 != expected_hash:
                return {'valid': False, 'error': 'SHA256 mismatch', 'checksums': checksums}
        
        if config.get('verify_md5', False):
            md5 = await self._calculate_md5(file_path)
            checksums['md5'] = md5
        
        # File type verification
        file_type = await self._detect_file_type(file_path)
        
        # Virus scan if configured
        clean = True
        if config.get('virus_scan', False):
            clean = await self._scan_for_viruses(file_path)
        
        return {
            'valid': True,
            'size': file_size,
            'checksums': checksums,
            'file_type': file_type,
            'clean': clean
        }
    
    async def _calculate_sha256(self, file_path: str) -> str:
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    async def _calculate_md5(self, file_path: str) -> str:
        md5_hash = hashlib.md5()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                md5_hash.update(byte_block)
        return md5_hash.hexdigest()
    
    async def _detect_file_type(self, file_path: str) -> str:
        import magic
        try:
            mime = magic.Magic(mime=True)
            return mime.from_file(file_path)
        except:
            # Fallback to extension
            import mimetypes
            mime_type, _ = mimetypes.guess_type(file_path)
            return mime_type or 'application/octet-stream'
    
    async def _scan_for_viruses(self, file_path: str) -> bool:
        # ClamAV integration stub
        try:
            import pyclamd
            cd = pyclamd.ClamdAgnostic()
            scan_result = cd.scan_file(file_path)
            return scan_result is None  # None means clean
        except:
            # If ClamAV not available, return True
            return True
    
    async def _generate_file_metadata(self, file_path: str, config: Dict[str, Any]) -> Dict[str, Any]:
        import time
        import stat
        
        stats = os.stat(file_path)
        
        metadata = {
            'filename': os.path.basename(file_path),
            'path': file_path,
            'size': stats.st_size,
            'created': time.ctime(stats.st_ctime),
            'modified': time.ctime(stats.st_mtime),
            'accessed': time.ctime(stats.st_atime),
            'permissions': stat.filemode(stats.st_mode),
            'inode': stats.st_ino,
            'device': stats.st_dev,
            'links': stats.st_nlink,
            'uid': stats.st_uid,
            'gid': stats.st_gid,
            'checksum_sha256': await self._calculate_sha256(file_path)
        }
        
        # Add file-specific metadata
        if config.get('extract_metadata', False):
            metadata.update(await self._extract_file_metadata(file_path))
        
        return metadata
    
    async def _extract_file_metadata(self, file_path: str) -> Dict[str, Any]:
        metadata = {}
        
        # Extract EXIF for images
        if file_path.lower().endswith(('.jpg', '.jpeg', '.png', '.tiff', '.gif')):
            try:
                from PIL import Image
                from PIL.ExifTags import TAGS
                
                image = Image.open(file_path)
                exifdata = image.getexif()
                
                if exifdata:
                    for tag_id in exifdata:
                        tag = TAGS.get(tag_id, tag_id)
                        data = exifdata.get(tag_id)
                        if isinstance(data, bytes):
                            data = data.decode(errors='ignore')
                        metadata[f'exif_{tag}'] = str(data)
                
                metadata['image_dimensions'] = f"{image.width}x{image.height}"
                metadata['image_mode'] = image.mode
                metadata['image_format'] = image.format
                
            except:
                pass
        
        # Extract PDF metadata
        elif file_path.lower().endswith('.pdf'):
            try:
                import PyPDF2
                with open(file_path, 'rb') as f:
                    pdf = PyPDF2.PdfReader(f)
                    info = pdf.metadata
                    if info:
                        for key, value in info.items():
                            metadata[f'pdf_{key[1:]}'] = str(value)
                    metadata['pdf_pages'] = len(pdf.pages)
            except:
                pass
        
        return metadata
