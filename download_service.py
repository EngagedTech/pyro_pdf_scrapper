import os
import asyncio
import aiohttp
import aiofiles
from typing import List, Dict, Optional
from datetime import datetime
import logging
from prometheus_client import Counter, Gauge
import redis
import zipfile
from dotenv import load_dotenv
import json
from scraper import XBRLScraper
import time
from mongo_manager import MongoManager
import pandas as pd
from pathlib import Path
from xbrl_parser import XBRLParser
from celery_config import celery_app

# Cargar variables de entorno
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Métricas Prometheus
DOWNLOAD_COUNTER = Counter('zip_downloads_total', 'Total number of ZIP files downloaded')
EXTRACT_COUNTER = Counter('files_extracted_total', 'Total number of files extracted from ZIPs')
ERROR_COUNTER = Counter('download_errors_total', 'Total number of download errors')
ACTIVE_DOWNLOADS = Gauge('active_downloads', 'Number of active downloads')
PARQUET_COUNTER = Counter('parquet_files_total', 'Total number of Parquet files created')

class DownloadService:
    def __init__(self):
        self.download_dir = os.getenv('DOWNLOAD_DIR', 'downloads')
        self.extract_dir = os.getenv('EXTRACT_DIR', 'extracted')
        self.parquet_dir = os.getenv('PARQUET_DIR', 'parquet_files')
        self.max_files_to_download = int(os.getenv('MAX_FILES_TO_DOWNLOAD', '2'))
        self.max_files_to_parse = int(os.getenv('MAX_FILES_TO_PARSE', '100'))
        
        # Configuración de Redis
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = int(os.getenv('REDIS_PORT', 6379))
        redis_db = int(os.getenv('REDIS_DB', 0))
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        
        self.session = None
        self.semaphore = asyncio.Semaphore(5)  # Límite de descargas concurrentes
        self.scraper = XBRLScraper()
        self.mongo_manager = MongoManager()
        self.xbrl_parser = XBRLParser(output_dir=self.parquet_dir)
        
        # Crear directorios si no existen
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

    def get_zip_urls(self) -> List[str]:
        """
        Obtiene las URLs de los archivos ZIP usando XBRLScraper
        """
        try:
            # Usar el scraper existente para obtener las URLs
            zip_urls = self.scraper.find_zip_links()
            logger.info(f"Found {len(zip_urls)} ZIP files to download")
            return zip_urls
        except Exception as e:
            logger.error(f"Error getting ZIP URLs: {str(e)}")
            return []

    async def download_zip(self, zip_url: str) -> Optional[str]:
        """Descarga un archivo ZIP de manera asíncrona y registra en MongoDB"""
        async with self.semaphore:
            try:
                ACTIVE_DOWNLOADS.inc()
                filename = os.path.basename(zip_url)
                filepath = os.path.join(self.download_dir, filename)
                
                # Registrar la descarga en MongoDB
                self.mongo_manager.register_zip_download(filename)
                
                # Verificar si el archivo ya existe
                if os.path.exists(filepath):
                    file_size = os.path.getsize(filepath)
                    if file_size > 100 * 1024:  # Más de 100KB
                        logger.info(f"File {filename} already exists ({file_size/1024/1024:.2f} MB). Skipping download.")
                        self.mongo_manager.update_zip_status(filename, "downloaded")
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
                        
                        download_time = time.time() - start_time
                        final_size_mb = os.path.getsize(filepath) / (1024 * 1024)
                        avg_speed = final_size_mb / download_time if download_time > 0 else 0
                        
                        logger.info(
                            f"Successfully downloaded {filename}: "
                            f"{final_size_mb:.1f}MB in {download_time:.1f}s "
                            f"(avg: {avg_speed:.1f}MB/s)"
                        )
                        
                        # Actualizar estado en MongoDB
                        self.mongo_manager.update_zip_status(filename, "downloaded")
                        DOWNLOAD_COUNTER.inc()
                        return filepath
                    else:
                        ERROR_COUNTER.inc()
                        self.mongo_manager.update_zip_status(filename, "error")
                        logger.error(f"Error downloading ZIP from {zip_url}: Status {response.status}")
                        return None
                        
            except Exception as e:
                ERROR_COUNTER.inc()
                if 'filename' in locals():
                    self.mongo_manager.update_zip_status(filename, "error")
                logger.error(f"Error downloading ZIP from {zip_url}: {str(e)}")
                return None
            finally:
                ACTIVE_DOWNLOADS.dec()

    def extract_zip(self, zip_path: str) -> List[str]:
        """Extrae archivos del ZIP y registra en MongoDB"""
        try:
            extracted_files = []
            filename = os.path.basename(zip_path)
            zip_name = os.path.splitext(filename)[0]  # Obtener nombre del ZIP sin extensión
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                files_to_extract = [f for f in zip_ref.namelist() if f.endswith(('.html', '.xml', '.xbrl'))]
                files_to_extract = files_to_extract[:self.max_files_to_parse]
                
                total_files = len(files_to_extract)
                logger.info(f"Extracting {total_files} files from {filename}")
                
                for i, file in enumerate(files_to_extract, 1):
                    zip_ref.extract(file, self.extract_dir)
                    extracted_path = os.path.join(self.extract_dir, file)
                    
                    # Registrar en MongoDB
                    try:
                        # Extraer fecha de cuenta y número de compañía del nombre del archivo
                        account_date = datetime.now().strftime("%Y-%m-%d")  # Placeholder
                        company_number = Path(file).stem  # Usar el nombre del archivo sin extensión
                        
                        self.mongo_manager.register_file_conversion(
                            filename=file,
                            account_date=account_date,
                            company_number=company_number,
                            zip_name=zip_name  # Añadir el nombre del ZIP
                        )
                        extracted_files.append(extracted_path)
                        EXTRACT_COUNTER.inc()
                    except Exception as e:
                        logger.error(f"Error registering file {file} in MongoDB: {e}")
                    
                    # Log progress
                    if i % 10 == 0 or i == total_files:
                        logger.info(f"Extracted {i}/{total_files} files from {filename}")
                    
                    # Enviar a cola de procesamiento
                    celery_app.send_task('process_xbrl', args=[extracted_path])
            
            return extracted_files
            
        except Exception as e:
            ERROR_COUNTER.inc()
            logger.error(f"Error extracting ZIP {zip_path}: {str(e)}")
            return []

    def convert_to_parquet(self, file_path: str) -> Optional[str]:
        """Convierte un archivo XBRL/HTML a parquet usando XBRLParser"""
        try:
            filename = os.path.basename(file_path)
            logger.info(f"Starting parquet conversion for {filename}")
            
            # Convertir el archivo usando XBRLParser
            parquet_path = self.xbrl_parser.xbrl_to_parquet(
                file_path=file_path,
                mongo_manager=self.mongo_manager
            )
            
            if parquet_path and os.path.exists(parquet_path):
                # Actualizar estado en MongoDB
                self.mongo_manager.update_conversion_status(filename, "completed")  # Cambiar a "completed"
                PARQUET_COUNTER.inc()
                logger.info(f"Successfully converted {filename} to parquet: {parquet_path}")
                return parquet_path
            else:
                raise Exception("Parquet file was not created")
            
        except Exception as e:
            logger.error(f"Error converting {file_path} to parquet: {e}")
            self.mongo_manager.update_conversion_status(filename, "error")
            return None

    async def start_processing(self) -> Dict:
        """
        Inicia el proceso completo de descarga y procesamiento
        """
        await self.initialize()
        
        try:
            # Obtener URLs de los ZIPs usando el scraper existente
            zip_urls = self.get_zip_urls()
            
            if not zip_urls:
                return {
                    "status": "error",
                    "message": "No ZIP URLs found to process"
                }
            
            # Limitar la cantidad de ZIPs según MAX_FILES_TO_DOWNLOAD
            zip_urls = zip_urls[:self.max_files_to_download]
            logger.info(f"Starting download of {len(zip_urls)} ZIP files")
            
            # Descargar ZIPs en paralelo
            download_tasks = [self.download_zip(url) for url in zip_urls]
            downloaded_zips = await asyncio.gather(*download_tasks)
            
            # Filtrar descargas fallidas
            downloaded_zips = [zip_path for zip_path in downloaded_zips if zip_path]
            logger.info(f"Successfully downloaded {len(downloaded_zips)} ZIP files")
            
            # Extraer y convertir archivos
            all_extracted_files = []
            all_parquet_files = []
            
            for zip_path in downloaded_zips:
                # Extraer archivos del ZIP
                extracted_files = self.extract_zip(zip_path)
                all_extracted_files.extend(extracted_files)
                
                # Convertir cada archivo extraído a parquet
                for file in extracted_files:
                    logger.info(f"Processing file for parquet conversion: {file}")
                    parquet_file = self.convert_to_parquet(file)
                    if parquet_file:
                        all_parquet_files.append(parquet_file)
                        logger.info(f"Successfully created parquet file: {parquet_file}")
            
            return {
                "status": "success",
                "zips_processed": len(downloaded_zips),
                "files_extracted": len(all_extracted_files),
                "parquet_files": len(all_parquet_files),
                "extracted_files": all_extracted_files,
                "parquet_files_list": all_parquet_files
            }
            
        except Exception as e:
            logger.error(f"Error in processing: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
            
        finally:
            await self.close() 