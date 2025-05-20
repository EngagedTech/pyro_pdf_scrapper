import os
import logging
from bs4 import BeautifulSoup
import dask.dataframe as dd
from typing import Dict, List, Optional
import json
from datetime import datetime
from prometheus_client import Counter, Gauge, start_http_server
import redis
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from dotenv import load_dotenv
from celery_config import celery_app

# Cargar variables de entorno
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Métricas Prometheus
PROCESS_COUNTER = Counter('xbrl_processed_total', 'Total number of XBRL files processed')
PROCESS_TIME = Gauge('xbrl_process_time_seconds', 'Time taken to process XBRL files')
ERROR_COUNTER = Counter('xbrl_process_errors_total', 'Total number of processing errors')

# Configuración de MongoDB usando variables de entorno
mongo_uri = os.getenv('MONGO_URI', 'mongodb://127.0.0.1:27017/')
db_name = os.getenv('MONGO_DB_NAME', 'xbrl_db')
mongo_client = MongoClient(mongo_uri)
db = mongo_client[db_name]
collection = db['conversions']

# Crear o actualizar índices
try:
    collection.drop_index('filename_1')  # Eliminar el índice anterior si existe
    collection.create_index([
        ('file_path', ASCENDING),
        ('company_number', ASCENDING),
        ('account_date', ASCENDING)
    ], unique=True, name='unique_file_composite')
    logger.info("MongoDB indexes updated successfully")
except Exception as e:
    logger.warning(f"Error updating indexes: {e}")

logger.info(f"Connecting to MongoDB at {mongo_uri}")

class XBRLProcessor:
    def __init__(self):
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = int(os.getenv('REDIS_PORT', 6379))
        redis_db = int(os.getenv('REDIS_DB', 1))
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True
        )
        
        # Configuración de caché optimizada
        self.cache_ttl = int(os.getenv('CACHE_TTL', 3600))  # 1 hora por defecto
        self.chunk_size = int(os.getenv('CHUNK_SIZE', 8192))
    
    def clear_cache(self, pattern: str = None):
        """
        Limpia la caché de Redis
        
        Args:
            pattern (str, optional): Patrón para limpiar caché específica (e.g., 'process:*')
                                   Si es None, limpia toda la caché
        """
        try:
            if pattern:
                # Obtener todas las claves que coinciden con el patrón
                keys = self.redis_client.keys(pattern)
                if keys:
                    self.redis_client.delete(*keys)
                    logger.info(f"Cleared {len(keys)} cache entries matching pattern: {pattern}")
            else:
                # Limpiar toda la base de datos
                self.redis_client.flushdb()
                logger.info("Cleared all cache entries")
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
    
    def clear_file_cache(self, file_path: str):
        """
        Limpia la caché para un archivo específico
        
        Args:
            file_path (str): Ruta del archivo para limpiar su caché
        """
        try:
            cache_key = f"process:{file_path}"
            if self.redis_client.exists(cache_key):
                self.redis_client.delete(cache_key)
                logger.info(f"Cleared cache for file: {file_path}")
            else:
                logger.info(f"No cache found for file: {file_path}")
        except Exception as e:
            logger.error(f"Error clearing file cache: {e}")
    
    def _extract_data_from_filename(self, filename: str) -> Dict[str, str]:
        """
        Extrae información del nombre del archivo usando el patrón correcto
        """
        try:
            # Eliminar extensión y dividir por guiones bajos
            base_name = os.path.splitext(filename)[0]
            parts = base_name.split('_')
            
            # Verificar si tiene el formato esperado
            if len(parts) >= 4:
                # Company number está en la penúltima posición
                company_number = parts[-2]
                
                # Fecha en la última posición
                date_str = parts[-1]
                if len(date_str) == 8 and date_str.isdigit():
                    year = date_str[0:4]
                    month = date_str[4:6]
                    day = date_str[6:8]
                    account_date = f"{year}-{month}-{day}"
                else:
                    account_date = date_str
                    
                return {
                    'company_number': company_number,
                    'account_date': account_date
                }
            else:
                logger.warning(f"Formato de nombre de archivo inesperado: {filename}")
                return {}
                
        except Exception as e:
            logger.error(f"Error al extraer datos del nombre del archivo {filename}: {e}")
            return {}

    def process_file(self, file_path: str) -> Optional[Dict]:
        try:
            filename = os.path.basename(file_path)
            
            # Verificar caché con timeout
            cache_key = f"process:{file_path}"
            try:
                if self.redis_client.exists(cache_key):
                    logger.info(f"Cache hit for file {filename}")
                    return None
            except redis.RedisError as e:
                logger.warning(f"Redis cache check failed: {e}")

            start_time = datetime.now()
            
            # Procesar archivo en chunks para optimizar memoria
            with open(file_path, 'r', encoding='utf-8') as f:
                content = []
                while chunk := f.read(self.chunk_size):
                    content.append(chunk)
                content = ''.join(content)
                
                soup = BeautifulSoup(content, 'lxml')
            
            # Extraer datos con manejo de memoria optimizado
            data = {
                'filename': filename,
                'file_path': file_path,
                'company_number': self._extract_data_from_filename(filename)['company_number'],
                'account_date': self._extract_data_from_filename(filename)['account_date'],
                'processed_at': datetime.now().isoformat(),
                'status': 'success',
                'content_length': len(content),
                'tags_count': len(soup.find_all()),
                'processing_time': None
            }
            
            # Liberar memoria
            del content
            del soup
            
            try:
                # Intentar insertar en MongoDB con timeout
                collection.update_one(
                    {
                        'file_path': file_path,
                        'company_number': data['company_number'],
                        'account_date': data['account_date']
                    },
                    {'$set': data},
                    upsert=True,
                    timeout=5000  # 5 segundos timeout
                )
                
                # Actualizar métricas y caché con manejo de errores
                process_time = (datetime.now() - start_time).total_seconds()
                data['processing_time'] = process_time
                
                try:
                    PROCESS_TIME.observe(process_time)
                    PROCESS_COUNTER.inc()
                    self.redis_client.setex(
                        cache_key,
                        self.cache_ttl,
                        "1",
                        nx=True  # Solo si no existe
                    )
                except (redis.RedisError, Exception) as e:
                    logger.warning(f"Metrics/cache update failed: {e}")
                
                logger.info(f"Successfully processed file {filename} in {process_time:.2f}s")
                return data
                
            except DuplicateKeyError:
                logger.warning(f"File {filename} already processed, skipping")
                return {
                    'file_path': file_path,
                    'status': 'skipped',
                    'message': 'File already processed'
                }
            
        except Exception as e:
            ERROR_COUNTER.inc()
            error_msg = f"Error processing file {file_path}: {str(e)}"
            logger.error(error_msg)
            return {
                'file_path': file_path,
                'filename': os.path.basename(file_path) if file_path else None,
                'status': 'error',
                'error': error_msg
            }

@celery_app.task(
    name='process_xbrl',
    bind=True,
    rate_limit='100/m',
    retry_backoff=True,
    retry_backoff_max=600,
    max_retries=3,
    queue='high_priority'
)
def process_xbrl_task(self, file_path: str):
    """
    Tarea Celery optimizada para procesar archivos XBRL
    """
    try:
        processor = XBRLProcessor()
        return processor.process_file(file_path)
    except Exception as exc:
        logger.error(f"Task failed: {exc}")
        self.retry(exc=exc)

if __name__ == '__main__':
    # Iniciar servidor de métricas Prometheus
    start_http_server(8000)
    
    # Iniciar worker Celery con configuración optimizada
    argv = [
        'worker',
        '--loglevel=info',
        f'--concurrency={OPTIMAL_WORKERS}',
        '--pool=prefork',
        '--max-tasks-per-child=1000',
        '--max-memory-per-child=150000'
    ]
    celery_app.worker_main(argv) 