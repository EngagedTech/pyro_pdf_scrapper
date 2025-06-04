"""
Service for downloading and processing XBRL files
"""

import os
import asyncio
import aiohttp
import aiofiles
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import logging
from prometheus_client import Counter, Gauge, Histogram
import redis
import zipfile
from dotenv import load_dotenv
import json
from scraper import XBRLScraper
import time
from mongo_manager import MongoManager, ValidationError
import pandas as pd
from pathlib import Path
from xbrl_parser import XBRLParser
from celery_config import celery_app

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('download_service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Prometheus metrics
DOWNLOAD_COUNTER = Counter('zip_downloads_total', 'Total number of ZIP files downloaded')
EXTRACT_COUNTER = Counter('files_extracted_total', 'Total number of files extracted from ZIPs')
ERROR_COUNTER = Counter('download_errors_total', 'Total number of download errors')
ACTIVE_DOWNLOADS = Gauge('active_downloads', 'Number of active downloads')
PARQUET_COUNTER = Counter('parquet_files_total', 'Total number of Parquet files created')

# Performance metrics
PROCESSING_TIME = Histogram(
    'xbrl_processing_duration_seconds',
    'Time spent processing XBRL files',
    ['stage']  # download, extract, convert, upload, import
)

class ProcessingError(Exception):
    """Custom exception for processing errors"""
    def __init__(self, stage: str, message: str, original_error: Exception = None):
        self.stage = stage
        self.message = message
        self.original_error = original_error
        super().__init__(f"Error in {stage}: {message}")

class DownloadService:
    def __init__(self):
        self.download_dir = os.getenv('DOWNLOAD_DIR', 'downloads')
        self.extract_dir = os.getenv('EXTRACT_DIR', 'extracted')
        self.parquet_dir = os.getenv('PARQUET_DIR', 'parquet_files')
        self.max_files_to_download = int(os.getenv('MAX_FILES_TO_DOWNLOAD', '2'))
        self.max_files_to_parse = int(os.getenv('MAX_FILES_TO_PARSE', '100'))
        
        # Redis configuration
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = int(os.getenv('REDIS_PORT', 6379))
        redis_db = int(os.getenv('REDIS_DB', 0))
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        
        self.session = None
        self.semaphore = asyncio.Semaphore(5)  # Limit concurrent downloads
        self.scraper = XBRLScraper()
        self.mongo_manager = MongoManager()
        self.xbrl_parser = XBRLParser(output_dir=self.parquet_dir)
        
        # Create directories if they don't exist
        for directory in [self.download_dir, self.extract_dir, self.parquet_dir]:
            os.makedirs(directory, exist_ok=True)

    async def initialize(self):
        """Initialize aiohttp session"""
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close(self):
        """Close aiohttp session and MongoDB connection"""
        if self.session:
            await self.session.close()
            self.session = None
        self.mongo_manager.close()

    def get_file_date_from_name(self, filename: str) -> Optional[str]:
        """
        Extract date from ZIP filename
        
        Args:
            filename (str): ZIP filename (e.g. Accounts_Monthly_Data-2025-05.zip)
            
        Returns:
            str|None: Date in YYYY-MM format or None if invalid
        """
        try:
            parts = filename.split('-')
            if len(parts) >= 2:
                date_part = parts[-1].split('.')[0]  # 2025-05
                if len(date_part.split('-')) == 2:
                    return date_part
        except:
            pass
        return None

    def get_zip_urls(self) -> List[str]:
        """
        Get ZIP file URLs using XBRLScraper
        """
        try:
            zip_urls = self.scraper.find_zip_links()
            logger.info(f"Found {len(zip_urls)} ZIP files to download")
            
            # Validate URLs
            valid_urls = []
            for url in zip_urls:
                filename = os.path.basename(url)
                file_date = self.get_file_date_from_name(filename)
                if file_date:
                    valid_urls.append(url)
                else:
                    logger.warning(f"Skipping URL with invalid filename format: {url}")
            
            return valid_urls
        except Exception as e:
            logger.error(f"Error getting ZIP URLs: {str(e)}")
            return []

    async def download_zip(self, zip_url: str) -> Optional[str]:
        """Download a ZIP file asynchronously and register in MongoDB"""
        async with self.semaphore:
            try:
                ACTIVE_DOWNLOADS.inc()
                with PROCESSING_TIME.labels(stage='download').time():
                    filename = os.path.basename(zip_url)
                    filepath = os.path.join(self.download_dir, filename)
                    # Extract date from filename 
                    file_date = self.get_file_date_from_name(filename)
                    if not file_date:
                        raise ProcessingError("download", f"Invalid filename format: {filename}")
                    
                    # Create ZIP download record in MongoDB
                    zip_id = self.mongo_manager.create_zip_download(
                        url=zip_url,
                        file_name=filename,
                        file_date=file_date
                    )
                    
                    if not zip_id:
                        raise ProcessingError("download", f"Failed to create MongoDB record for {filename}")
                    
                    # Check if file already exists
                    if os.path.exists(filepath):
                        file_size = os.path.getsize(filepath)
                        if file_size > 100 * 1024:  # More than 100KB
                            logger.info(f"File {filename} already exists ({file_size/1024/1024:.2f} MB). Skipping download.")
                            self.mongo_manager.update_zip_download_status(zip_id, downloaded=True)
                            return filepath
                    
                    logger.info(f"Starting download of {filename}")
                    start_time = time.time()
                    last_log_time = start_time
                    
                    async with self.session.get(zip_url) as response:
                        if response.status == 200:
                            total_size = int(response.headers.get('content-length', 0))
                            total_mb = total_size / (1024 * 1024)
                            
                            async with aiofiles.open(filepath, 'wb') as f:
                                downloaded = 0
                                async for chunk in response.content.iter_chunked(8192):
                                    await f.write(chunk)
                                    downloaded += len(chunk)
                                    
                                    current_time = time.time()
                                    if (current_time - last_log_time >= 2) or (downloaded % (5 * 1024 * 1024) == 0):
                                        downloaded_mb = downloaded / (1024 * 1024)
                                        elapsed_time = current_time - start_time
                                        speed_mbps = downloaded_mb / elapsed_time if elapsed_time > 0 else 0
                                        
                                        if total_size:
                                            progress = (downloaded / total_size) * 100
                                            eta_seconds = (total_size - downloaded) / (downloaded / elapsed_time) if elapsed_time > 0 else 0
                                            logger.info(
                                                f"Downloading {filename}: "
                                                f"{downloaded_mb:.1f}MB/{total_mb:.1f}MB "
                                                f"({progress:.1f}%) - {speed_mbps:.1f}MB/s - "
                                                f"ETA: {eta_seconds/60:.1f}min"
                                            )
                                        else:
                                            logger.info(
                                                f"Downloading {filename}: "
                                                f"{downloaded_mb:.1f}MB - {speed_mbps:.1f}MB/s"
                                            )
                                        last_log_time = current_time
                            
                            # Verify file integrity
                            final_size = os.path.getsize(filepath)
                            if total_size and final_size != total_size:
                                raise ProcessingError(
                                    "download",
                                    f"File size mismatch for {filename}. Expected {total_size}, got {final_size}"
                                )
                            
                            download_time = time.time() - start_time
                            final_size_mb = final_size / (1024 * 1024)
                            avg_speed = final_size_mb / download_time if download_time > 0 else 0
                            
                            logger.info(
                                f"Successfully downloaded {filename}: "
                                f"{final_size_mb:.1f}MB in {download_time:.1f}s "
                                f"(avg: {avg_speed:.1f}MB/s)"
                            )
                            
                            # Update MongoDB status
                            self.mongo_manager.update_zip_download_status(zip_id, downloaded=True)
                            DOWNLOAD_COUNTER.inc()
                            return filepath
                        else:
                            raise ProcessingError(
                                "download",
                                f"HTTP error downloading {filename}: Status {response.status}"
                            )
                            
            except ProcessingError as e:
                ERROR_COUNTER.inc()
                logger.error(str(e))
                if e.original_error:
                    logger.error(f"Original error: {e.original_error}")
                return None
            except Exception as e:
                ERROR_COUNTER.inc()
                logger.error(f"Error downloading ZIP from {zip_url}: {str(e)}")
                return None
            finally:
                ACTIVE_DOWNLOADS.dec()

    def extract_zip(self, zip_path: str) -> List[str]:
        """Extract files from ZIP and register in MongoDB"""
        try:
            with PROCESSING_TIME.labels(stage='extract').time():
                extracted_files = []
                filename = os.path.basename(zip_path)
                zip_name = os.path.splitext(filename)[0]
                
                # Get ZIP download record ID
                zip_info = next(self.mongo_manager.zip_download_collection.find({"file_name": filename}), None)
                if not zip_info:
                    raise ProcessingError("extract", f"No ZIP download record found for {filename}")
                
                zip_id = zip_info["_id"]
                
                # Verify ZIP file integrity
                try:
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.testzip()
                except zipfile.BadZipFile as e:
                    raise ProcessingError("extract", f"Corrupt ZIP file: {filename}", e)
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                files_to_extract = [f for f in zip_ref.namelist() if f.endswith(('.html', '.xml', '.xbrl'))]
                files_to_extract = files_to_extract[:self.max_files_to_parse]
                
                total_files = len(files_to_extract)
                logger.info(f"Extracting {total_files} files from {filename}")
                
                for i, file in enumerate(files_to_extract, 1):
                        # Verify file path safety
                        if os.path.isabs(file) or '..' in file:
                            logger.warning(f"Skipping potentially unsafe path: {file}")
                            continue

                        zip_ref.extract(file, self.extract_dir)
                        extracted_path = os.path.join(self.extract_dir, file)
                        # Register in MongoDB 
                        try:
                            # Extract company number from filename
                            company_number = Path(file).stem
                            
                            # Create conversion record
                            conv_id = self.mongo_manager.create_conversion(
                                zip_download_id=zip_id,
                                file_name=file,
                                file_date=datetime.now(),
                            company_number=company_number,
                                s3_bucket=os.getenv('S3_BUCKET', 'default-bucket')
                        )
                            
                            if conv_id:
                                    extracted_files.append((extracted_path, str(conv_id)))
                            EXTRACT_COUNTER.inc()
                                    # Send to processing queue
                            celery_app.send_task('process_xbrl', args=[extracted_path, str(conv_id)])
                        except Exception as e:
                                logger.error(f"Error registering file {file} in MongoDB: {e}")
                            
                        # Log progress
                        if i % 10 == 0 or i == total_files:
                            logger.info(f"Extracted {i}/{total_files} files from {filename}")
                    
                # Update ZIP unzipped status
                self.mongo_manager.update_zip_download_status(zip_id, unzipped=True)
            
                return [path for path, _ in extracted_files]
                
        except ProcessingError as e:
            ERROR_COUNTER.inc()
            logger.error(str(e))
            if e.original_error:
                logger.error(f"Original error: {e.original_error}")
            return []
        except Exception as e:
            ERROR_COUNTER.inc()
            logger.error(f"Error extracting ZIP {zip_path}: {str(e)}")
            return []

    def cleanup_temp_files(self, file_paths: List[str]):
        """Clean up temporary files after processing"""
        for path in file_paths:
            try:
                if os.path.exists(path):
                    os.remove(path)
                    logger.info(f"Cleaned up temporary file: {path}")
            except Exception as e:
                logger.warning(f"Failed to clean up {path}: {e}")

    async def start_processing(self) -> Dict:
        """
        Start the complete download and processing workflow
        """
        await self.initialize()
        
        try:
            with PROCESSING_TIME.labels(stage='total').time():
                # Get ZIP URLs using existing scraper
                zip_urls = self.get_zip_urls()
                
                if not zip_urls:
                    return {
                        "status": "error",
                        "message": "No ZIP URLs found to process"
                    }
                
                # Limit number of ZIPs according to MAX_FILES_TO_DOWNLOAD
                zip_urls = zip_urls[:self.max_files_to_download]
                logger.info(f"Starting download of {len(zip_urls)} ZIP files")
                
                # Download ZIPs in parallel
                download_tasks = [self.download_zip(url) for url in zip_urls]
                downloaded_zips = await asyncio.gather(*download_tasks)
                
                # Filter failed downloads
                downloaded_zips = [zip_path for zip_path in downloaded_zips if zip_path]
                logger.info(f"Successfully downloaded {len(downloaded_zips)} ZIP files")
                
                # Extract and convert files
                all_extracted_files = []
                
                for zip_path in downloaded_zips:
                        # Extract files from ZIP
                    extracted_files = self.extract_zip(zip_path)
                    all_extracted_files.extend(extracted_files)
                    
                    # Clean up downloaded ZIP files
                    self.cleanup_temp_files(downloaded_zips)
                
                return {
                    "status": "success",
                    "zips_processed": len(downloaded_zips),
                    "files_extracted": len(all_extracted_files),
                    "extracted_files": all_extracted_files
                }
                
        except Exception as e:
            logger.error(f"Error in processing: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
            
        finally:
            await self.close() 