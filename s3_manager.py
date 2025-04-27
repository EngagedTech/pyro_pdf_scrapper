"""
Module for handling S3 operations, especially uploading Parquet files to S3
"""

import os
import logging
import boto3
from typing import List, Optional, Dict, Any
from botocore.exceptions import ClientError
from config import (
    S3_URI,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    S3_ENDPOINT_URL
)

logger = logging.getLogger(__name__)

class S3Manager:
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = None,
        endpoint_url: Optional[str] = None
    ):
        """
        Initialize S3 manager with credentials
        
        Args:
            aws_access_key_id: AWS access key ID. If None, will use value from config
            aws_secret_access_key: AWS secret access key. If None, will use value from config
            region_name: AWS region. If None, will use value from config
            endpoint_url: S3-compatible endpoint URL. If None, will use value from config
        """
        self.aws_access_key_id = aws_access_key_id or AWS_ACCESS_KEY_ID
        self.aws_secret_access_key = aws_secret_access_key or AWS_SECRET_ACCESS_KEY
        self.region_name = region_name or AWS_REGION
        self.endpoint_url = endpoint_url or S3_ENDPOINT_URL
        
        # Log sanitized configuration
        masked_key = self.aws_access_key_id[:4] + "****" if self.aws_access_key_id else None
        masked_secret = "****" if self.aws_secret_access_key else None
        
        logger.info(
            f"Initializing S3 client with: region={self.region_name}, "
            f"endpoint={self.endpoint_url}, access_key={masked_key}"
        )
        
        # Create S3 client
        self.s3_client = self._create_s3_client()
        
        # Extract bucket and prefix from S3 URI
        self.bucket_name, self.prefix = self._parse_s3_uri(S3_URI)
        logger.info(f"Using S3 bucket: {self.bucket_name}, prefix: {self.prefix}")
    
    def _create_s3_client(self):
        """Create boto3 S3 client with provided credentials"""
        try:
            session = boto3.session.Session()
            
            # Initialize client with provided credentials
            client_kwargs = {
                'aws_access_key_id': self.aws_access_key_id,
                'aws_secret_access_key': self.aws_secret_access_key,
            }
            
            # Add optional parameters only if they are provided
            if self.region_name:
                client_kwargs['region_name'] = self.region_name
                
            if self.endpoint_url:
                client_kwargs['endpoint_url'] = self.endpoint_url
            
            s3_client = session.client('s3', **client_kwargs)
            return s3_client
            
        except Exception as e:
            logger.error(f"Failed to create S3 client: {e}")
            raise
    
    def _parse_s3_uri(self, s3_uri: str) -> tuple:
        """
        Parse S3 URI to extract bucket and prefix
        
        Args:
            s3_uri: S3 URI in the format s3://bucket/prefix
            
        Returns:
            tuple: (bucket_name, prefix)
        """
        if not s3_uri.startswith('s3://'):
            raise ValueError(f"Invalid S3 URI format: {s3_uri}. Must start with 's3://'")
        
        # Remove s3:// prefix
        s3_path = s3_uri[5:]
        
        # Split by first /
        parts = s3_path.split('/', 1)
        bucket_name = parts[0]
        
        # Get prefix (folder path)
        prefix = parts[1] if len(parts) > 1 else ''
        
        # Ensure prefix ends with / if not empty
        if prefix and not prefix.endswith('/'):
            prefix += '/'
            
        return bucket_name, prefix
    
    def check_bucket_exists(self) -> bool:
        """
        Check if the configured bucket exists and is accessible
        
        Returns:
            bool: True if bucket exists and is accessible, False otherwise
        """
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"Bucket {self.bucket_name} does not exist")
            elif error_code == '403':
                logger.error(f"Forbidden access to bucket {self.bucket_name}. Check credentials and permissions")
            else:
                logger.error(f"Error accessing bucket {self.bucket_name}: {e}")
            return False
    
    def upload_file(self, local_file_path: str, s3_key: Optional[str] = None, max_retries: int = 3) -> bool:
        """
        Upload a file to S3
        
        Args:
            local_file_path: Path to local file
            s3_key: S3 key to use (if None, will use filename with prefix)
            max_retries: Número máximo de intentos de subida
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not os.path.exists(local_file_path):
            logger.error(f"File not found: {local_file_path}")
            return False
        
        # Get file size for logging
        try:
            file_size_mb = os.path.getsize(local_file_path) / (1024 * 1024)
        except:
            file_size_mb = 0
        
        # If no S3 key provided, use the filename with prefix
        if s3_key is None:
            filename = os.path.basename(local_file_path)
            s3_key = f"{self.prefix}{filename}"
        
        # Intenta subir con reintentos
        for attempt in range(max_retries):
            try:
                logger.info(f"Uploading {local_file_path} ({file_size_mb:.2f} MB) to s3://{self.bucket_name}/{s3_key} (attempt {attempt+1}/{max_retries})")
                
                # Para archivos grandes, usar transferencia con progreso
                if file_size_mb > 50:  # Más de 50MB
                    import boto3.s3.transfer as s3transfer
                    
                    # Creación de callback para seguir progreso
                    class ProgressPercentage:
                        def __init__(self, filename):
                            self._filename = filename
                            self._size = float(os.path.getsize(filename))
                            self._seen_so_far = 0
                            self._last_percent = 0
                            
                        def __call__(self, bytes_amount):
                            self._seen_so_far += bytes_amount
                            percentage = (self._seen_so_far / self._size) * 100
                            
                            # Solo reportar si ha cambiado al menos 10%
                            if int(percentage / 10) > int(self._last_percent / 10):
                                logger.info(f"Uploaded {percentage:.1f}% of {self._filename}")
                                self._last_percent = percentage
                    
                    # Configurar transferencia con progreso
                    transfer_config = s3transfer.TransferConfig(
                        multipart_threshold=8 * 1024 * 1024,  # 8MB
                        max_concurrency=10,
                        multipart_chunksize=8 * 1024 * 1024  # 8MB
                    )
                    
                    # Subir con seguimiento de progreso
                    self.s3_client.upload_file(
                        local_file_path, 
                        self.bucket_name, 
                        s3_key,
                        Callback=ProgressPercentage(local_file_path),
                        Config=transfer_config
                    )
                else:
                    # Para archivos pequeños, subida simple
                    self.s3_client.upload_file(local_file_path, self.bucket_name, s3_key)
                
                logger.info(f"Successfully uploaded {local_file_path} to S3")
                return True
                
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                # Si es un error irrecuperable, no reintentar
                if error_code in ['AccessDenied', 'NoSuchBucket']:
                    logger.error(f"Irrecoverable error uploading to S3 (error code {error_code}): {e}")
                    return False
                
                logger.warning(f"Error uploading to S3 (attempt {attempt+1}/{max_retries}): {e}")
                
                if attempt == max_retries - 1:
                    logger.error(f"Failed to upload {local_file_path} after {max_retries} attempts")
                    return False
                    
                # Esperar antes del siguiente intento
                import time
                time.sleep(2 * (attempt + 1))  # Espera incremental
                
            except Exception as e:
                logger.error(f"Unexpected error uploading {local_file_path} to S3: {e}")
                return False
        
        return False
    
    def upload_directory(self, local_dir: str, include_pattern: str = "*.parquet") -> Dict[str, bool]:
        """
        Upload all files in a directory to S3
        
        Args:
            local_dir: Path to local directory
            include_pattern: Glob pattern to filter files
            
        Returns:
            dict: Dictionary of {filename: success_status}
        """
        import glob
        
        if not os.path.isdir(local_dir):
            logger.error(f"Directory not found: {local_dir}")
            return {}
        
        # Find all matching files
        pattern = os.path.join(local_dir, include_pattern)
        files = glob.glob(pattern)
        
        if not files:
            logger.warning(f"No files matching '{include_pattern}' found in {local_dir}")
            return {}
        
        logger.info(f"Found {len(files)} files to upload in {local_dir}")
        
        # Upload each file
        results = {}
        for file_path in files:
            filename = os.path.basename(file_path)
            success = self.upload_file(file_path)
            results[filename] = success
        
        # Log summary
        success_count = sum(1 for status in results.values() if status)
        logger.info(f"Uploaded {success_count} of {len(results)} files to S3")
        
        return results
    
    def get_s3_url(self, key: str) -> str:
        """
        Get the full S3 URL for a key
        
        Args:
            key: S3 key
            
        Returns:
            str: Full S3 URL
        """
        return f"s3://{self.bucket_name}/{key}" 