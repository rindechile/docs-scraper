"""
Cloudflare R2 client for file uploads.
"""

import boto3
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


class R2Client:
    """Client for Cloudflare R2 storage (S3-compatible)"""

    def __init__(
        self,
        account_id: str,
        access_key: str,
        secret_key: str,
        bucket: str
    ):
        self.bucket = bucket
        self.s3 = boto3.client(
            's3',
            endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='auto'
        )

    async def upload_bytes(
        self,
        key: str,
        data: bytes,
        content_type: str = 'application/octet-stream'
    ):
        """Upload binary data to R2"""
        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data,
                ContentType=content_type
            )
            logger.debug(f"Uploaded: {key} ({len(data)} bytes)")
        except Exception as e:
            logger.error(f"Failed to upload {key}: {e}")
            raise

    async def upload_json(self, key: str, data: Any):
        """Upload JSON data to R2"""
        json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
        await self.upload_bytes(key, json_bytes, 'application/json')

    async def file_exists(self, key: str) -> bool:
        """Check if a file exists in R2"""
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except self.s3.exceptions.ClientError:
            return False

    async def delete_file(self, key: str):
        """Delete a file from R2"""
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=key)
            logger.debug(f"Deleted: {key}")
        except Exception as e:
            logger.error(f"Failed to delete {key}: {e}")
            raise
