import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from typing import Optional, Dict, Any, Union
import io
import json
import hashlib
from datetime import datetime, timedelta

class S3StorageClient:
    def __init__(self, endpoint_url: str, 
                 access_key: str, 
                 secret_key: str,
                 bucket: str,
                 region: str = "us-east-1"):
        self.endpoint_url = endpoint_url
        self.bucket = bucket
        self.region = region
        
        self.client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(
                signature_version='s3v4',
                region_name=region
            )
        )
        
        # Ensure bucket exists
        self._ensure_bucket()
    
    def _ensure_bucket(self):
        """Ensure bucket exists"""
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Bucket doesn't exist, create it
                if self.endpoint_url:
                    # MinIO/S3-compatible
                    self.client.create_bucket(Bucket=self.bucket)
                else:
                    # AWS S3
                    self.client.create_bucket(
                        Bucket=self.bucket,
                        CreateBucketConfiguration={
                            'LocationConstraint': self.region
                        }
                    )
    
    def upload_file(self, file_path: str, key: str, 
                   metadata: Optional[Dict] = None) -> str:
        """Upload file from filesystem"""
        extra_args = {}
        if metadata:
            extra_args['Metadata'] = metadata
        
        self.client.upload_file(
            file_path,
            self.bucket,
            key,
            ExtraArgs=extra_args
        )
        
        return self._generate_presigned_url(key)
    
    def upload_bytes(self, data: bytes, key: str,
                    content_type: Optional[str] = None,
                    metadata: Optional[Dict] = None) -> str:
        """Upload bytes data"""
        extra_args = {}
        if content_type:
            extra_args['ContentType'] = content_type
        if metadata:
            extra_args['Metadata'] = metadata
        
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            **extra_args
        )
        
        return self._generate_presigned_url(key)
    
    def upload_json(self, data: Union[Dict, List], key: str,
                   metadata: Optional[Dict] = None) -> str:
        """Upload JSON data"""
        json_bytes = json.dumps(data, indent=2).encode('utf-8')
        
        return self.upload_bytes(
            json_bytes,
            key,
            content_type='application/json',
            metadata=metadata
        )
    
    def download_file(self, key: str, file_path: str):
        """Download file to filesystem"""
        self.client.download_file(
            self.bucket,
            key,
            file_path
        )
    
    def download_bytes(self, key: str) -> bytes:
        """Download as bytes"""
        response = self.client.get_object(
            Bucket=self.bucket,
            Key=key
        )
        
        return response['Body'].read()
    
    def download_json(self, key: str) -> Union[Dict, List]:
        """Download and parse JSON"""
        data = self.download_bytes(key)
        return json.loads(data.decode('utf-8'))
    
    def delete_file(self, key: str) -> bool:
        """Delete file from storage"""
        try:
            self.client.delete_object(
                Bucket=self.bucket,
                Key=key
            )
            return True
        except ClientError:
            return False
    
    def list_files(self, prefix: str = "",
                   limit: int = 1000) -> List[Dict]:
        """List files with prefix"""
        response = self.client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=prefix,
            MaxKeys=limit
        )
        
        files = []
        for obj in response.get('Contents', []):
            files.append({
                'key': obj['Key'],
                'size': obj['Size'],
                'last_modified': obj['LastModified'].isoformat(),
                'etag': obj['ETag']
            })
        
        return files
    
    def get_file_metadata(self, key: str) -> Optional[Dict]:
        """Get file metadata"""
        try:
            response = self.client.head_object(
                Bucket=self.bucket,
                Key=key
            )
            
            return {
                'content_type': response.get('ContentType'),
                'content_length': response.get('ContentLength'),
                'last_modified': response.get('LastModified').isoformat(),
                'etag': response.get('ETag'),
                'metadata': response.get('Metadata', {})
            }
        except ClientError:
            return None
    
    def _generate_presigned_url(self, key: str,
                               expiration: int = 3600) -> str:
        """Generate presigned URL for temporary access"""
        try:
            url = self.client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket,
                    'Key': key
                },
                ExpiresIn=expiration
            )
            return url
        except ClientError:
            # Fallback to public URL if presigning fails
            return f"{self.endpoint_url}/{self.bucket}/{key}"
    
    def copy_file(self, source_key: str, 
                 dest_key: str) -> bool:
        """Copy file within bucket"""
        try:
            copy_source = {
                'Bucket': self.bucket,
                'Key': source_key
            }
            
            self.client.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket,
                Key=dest_key
            )
            return True
        except ClientError:
            return False
    
    def get_usage_stats(self) -> Dict[str, Any]:
        """Get storage usage statistics"""
        total_size = 0
        total_files = 0
        
        paginator = self.client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=self.bucket):
            for obj in page.get('Contents', []):
                total_size += obj['Size']
                total_files += 1
        
        return {
            'total_size_bytes': total_size,
            'total_size_gb': total_size / (1024 ** 3),
            'total_files': total_files,
            'bucket': self.bucket,
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def cleanup_old_files(self, prefix: str,
                         days_old: int = 30) -> int:
        """Cleanup files older than specified days"""
        cutoff_date = datetime.utcnow() - timedelta(days=days_old)
        deleted_count = 0
        
        files = self.list_files(prefix=prefix)
        
        for file_info in files:
            file_date = datetime.fromisoformat(
                file_info['last_modified'].replace('Z', '+00:00')
            )
            
            if file_date < cutoff_date:
                if self.delete_file(file_info['key']):
                    deleted_count += 1
        
        return deleted_count
    
    def calculate_checksum(self, key: str,
                          algorithm: str = 'md5') -> str:
        """Calculate file checksum"""
        data = self.download_bytes(key)
        
        if algorithm == 'md5':
            return hashlib.md5(data).hexdigest()
        elif algorithm == 'sha256':
            return hashlib.sha256(data).hexdigest()
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
