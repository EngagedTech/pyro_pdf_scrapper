import os
import logging
from celery import Celery
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

# Cargar variables de entorno
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Métricas Prometheus
PROCESS_COUNTER = Counter('xbrl_processed_total', 'Total number of XBRL files processed')
PROCESS_TIME = Gauge('xbrl_process_time_seconds', 'Time taken to process XBRL files')
ERROR_COUNTER = Counter('xbrl_process_errors_total', 'Total number of processing errors')

# Configuración de Celery
celery_app = Celery('xbrl_tasks', broker='redis://localhost:6379/0')

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
        self.redis_client = redis.Redis(host='localhost', port=6379, db=1)
    
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
        """
        Procesa un archivo XBRL individual
        """
        try:
            filename = os.path.basename(file_path)
            
            # Verificar caché
            cache_key = f"process:{file_path}"
            if self.redis_client.exists(cache_key):
                logger.info(f"Cache hit for file {filename}")
                return None

            start_time = datetime.now()
            
            # Extraer información del nombre del archivo
            file_info = self._extract_data_from_filename(filename)
            
            if not file_info.get('company_number'):
                raise ValueError(f"No se pudo extraer el número de compañía del archivo {filename}")
            
            # Leer y parsear el archivo
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                soup = BeautifulSoup(content, 'lxml')
            
            # Extraer datos relevantes
            data = {
                'filename': filename,
                'file_path': file_path,
                'company_number': file_info['company_number'],
                'account_date': file_info['account_date'],
                'processed_at': datetime.now().isoformat(),
                'status': 'success',
                'content_length': len(content),
                'tags_count': len(soup.find_all()),
                'processing_time': None
            }
            
            try:
                # Intentar insertar en MongoDB
                collection.update_one(
                    {
                        'file_path': file_path,
                        'company_number': file_info['company_number'],
                        'account_date': file_info['account_date']
                    },
                    {'$set': data},
                    upsert=True
                )
                
                # Actualizar métricas y caché
                process_time = (datetime.now() - start_time).total_seconds()
                data['processing_time'] = process_time
                PROCESS_TIME.observe(process_time)
                PROCESS_COUNTER.inc()
                self.redis_client.setex(cache_key, 3600, "1")  # Cache por 1 hora
                
                logger.info(f"Successfully processed file {filename}")
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

@celery_app.task(name='process_xbrl')
def process_xbrl_task(file_path: str):
    """
    Tarea Celery para procesar archivos XBRL
    """
    processor = XBRLProcessor()
    return processor.process_file(file_path)

if __name__ == '__main__':
    # Iniciar servidor de métricas Prometheus
    start_http_server(8000)
    
    # Iniciar worker Celery
    celery_app.worker_main(['worker', '--loglevel=info']) 