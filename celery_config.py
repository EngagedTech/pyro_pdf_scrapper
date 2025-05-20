from celery import Celery
from dotenv import load_dotenv
import os
import multiprocessing

# Cargar variables de entorno
load_dotenv()

# Configuración de Redis desde variables de entorno
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')
REDIS_DB = os.getenv('REDIS_DB', '0')

# Calcular número óptimo de workers basado en CPU
OPTIMAL_WORKERS = multiprocessing.cpu_count() * 2

# Crear aplicación Celery
celery_app = Celery(
    'xbrl_tasks',
    broker=f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}',
    backend=f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}'
)

# Configuración optimizada de Celery
celery_app.conf.update(
    # Configuración básica
    broker_connection_retry_on_startup=True,
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    
    # Optimizaciones de rendimiento
    worker_prefetch_multiplier=1,  # Previene acaparamiento de tareas
    task_acks_late=True,  # Mejor distribución de carga
    task_reject_on_worker_lost=True,  # Manejo robusto de fallos
    worker_max_tasks_per_child=1000,  # Previene fugas de memoria
    task_track_started=True,  # Mejor monitoreo
    
    # Configuración de colas
    task_default_queue='default',
    task_queues={
        'high_priority': {'routing_key': 'high'},
        'default': {'routing_key': 'normal'},
        'low_priority': {'routing_key': 'low'},
    },
    
    # Configuración de rate limiting
    task_annotations={
        'process_xbrl': {
            'rate_limit': '100/m'  # Límite de 100 tareas por minuto
        }
    },
    
    # Configuración de reintentos
    task_default_retry_delay=60,  # 1 minuto entre reintentos
    task_max_retries=3,  # Máximo 3 reintentos
    
    # Optimización de memoria
    worker_max_memory_per_child=150000,  # 150MB por worker
) 