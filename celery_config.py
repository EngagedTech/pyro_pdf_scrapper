"""
Celery configuration for XBRL processing tasks
"""

from celery import Celery
from dotenv import load_dotenv
import os
import multiprocessing
import logging
from bson import ObjectId
from prometheus_client import Histogram

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('celery.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Redis configuration from environment variables
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')
REDIS_DB = os.getenv('REDIS_DB', '0')

# Calculate optimal number of workers based on CPU
OPTIMAL_WORKERS = multiprocessing.cpu_count() * 2

# Performance metrics
TASK_PROCESSING_TIME = Histogram(
    'celery_task_duration_seconds',
    'Time spent processing Celery tasks',
    ['task_name']
)

# Create Celery application
celery_app = Celery(
    'xbrl_tasks',
    broker=f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}',
    backend=f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}'
)

# Optimized Celery configuration
celery_app.conf.update(
    # Basic configuration
    broker_connection_retry_on_startup=True,
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    
    # Performance optimizations
    worker_prefetch_multiplier=1,  # Prevents task hoarding
    task_acks_late=True,  # Better load distribution
    task_reject_on_worker_lost=True,  # Robust failure handling
    worker_max_tasks_per_child=1000,  # Prevents memory leaks
    task_track_started=True,  # Better monitoring
    
    # Queue configuration
    task_default_queue='default',
    task_queues={
        'high_priority': {'routing_key': 'high'},
        'default': {'routing_key': 'normal'},
        'low_priority': {'routing_key': 'low'},
    },
    
    # Task routing
    task_routes={
        'process_xbrl': {'queue': 'high_priority'},
        'convert_to_parquet': {'queue': 'high_priority'},
        'upload_to_s3': {'queue': 'default'},
        'import_to_pinecone': {'queue': 'default'},
        'process_company_data': {'queue': 'low_priority'}
    },
    
    # Rate limiting configuration
    task_annotations={
        'process_xbrl': {
            'rate_limit': '100/m',  # 100 tasks per minute
            'max_retries': 3,
            'default_retry_delay': 60,
            'autoretry_for': (Exception,),
            'retry_backoff': True,
            'retry_backoff_max': 600,
            'retry_jitter': True
        },
        'convert_to_parquet': {
            'rate_limit': '100/m',
            'max_retries': 3,
            'default_retry_delay': 60,
            'autoretry_for': (Exception,),
            'retry_backoff': True
        },
        'upload_to_s3': {
            'rate_limit': '50/m',  # 50 uploads per minute
            'max_retries': 5,
            'default_retry_delay': 120,
            'autoretry_for': (Exception,),
            'retry_backoff': True
        },
        'import_to_pinecone': {
            'rate_limit': '30/m',  # 30 imports per minute
            'max_retries': 3,
            'default_retry_delay': 300,
            'autoretry_for': (Exception,),
            'retry_backoff': True
        }
    },
    
    # Retry configuration
    task_default_retry_delay=60,  # 1 minute between retries
    task_max_retries=3,  # Maximum 3 retries
    
    # Memory optimization
    worker_max_memory_per_child=150000,  # 150MB per worker
)

class TaskError(Exception):
    """Custom exception for task errors"""
    def __init__(self, task_name: str, message: str, original_error: Exception = None):
        self.task_name = task_name
        self.message = message
        self.original_error = original_error
        super().__init__(f"Error in task {task_name}: {message}")

def handle_task_error(task_name: str, error: Exception, mongo_manager=None, conversion_id: str = None):
    """
    Handle task errors consistently
    
    Args:
        task_name (str): Name of the failed task
        error (Exception): The error that occurred
        mongo_manager: Optional MongoDB manager instance
        conversion_id (str): Optional conversion record ID
    """
    logger.error(f"Error in task {task_name}: {error}")
    
    if mongo_manager and conversion_id:
        try:
            # Update MongoDB status based on task
            if task_name == "process_xbrl":
                mongo_manager.update_conversion_status(conversion_id, converted=False)
            elif task_name == "upload_to_s3":
                mongo_manager.update_conversion_status(conversion_id, uploaded=False)
            elif task_name == "import_to_pinecone":
                mongo_manager.update_conversion_status(conversion_id, pineconeImported=False)
        except Exception as e:
            logger.error(f"Error updating MongoDB status: {e}")
        finally:
            mongo_manager.close()

# Task definitions
@celery_app.task(
    name='process_xbrl',
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True
)
def process_xbrl(self, file_path: str, conversion_id: str):
    """
    Process XBRL file and convert to Parquet
    
    Args:
        file_path (str): Path to XBRL file
        conversion_id (str): MongoDB conversion record ID
    """
    from xbrl_parser import XBRLParser
    from mongo_manager import MongoManager
    
    parser = XBRLParser()
    mongo = MongoManager()
    
    try:
        with TASK_PROCESSING_TIME.labels(task_name='process_xbrl').time():
            # Convert to Parquet
            parquet_file = parser.xbrl_to_parquet(file_path, conversion_id, mongo)
            
            if parquet_file:
                # Queue upload to S3
                upload_to_s3.delay(parquet_file, conversion_id)
            else:
                raise TaskError("process_xbrl", f"Failed to convert {file_path} to Parquet")
            
    except Exception as e:
        handle_task_error("process_xbrl", e, mongo, conversion_id)
        raise self.retry(exc=e)

@celery_app.task(
    name='upload_to_s3',
    bind=True,
    max_retries=5,
    default_retry_delay=120,
    autoretry_for=(Exception,),
    retry_backoff=True
)
def upload_to_s3(self, parquet_file: str, conversion_id: str):
    """
    Upload Parquet file to S3
    
    Args:
        parquet_file (str): Path to Parquet file
        conversion_id (str): MongoDB conversion record ID
    """
    from s3_manager import S3Manager
    from mongo_manager import MongoManager
    
    s3 = S3Manager()
    mongo = MongoManager()
    
    try:
        with TASK_PROCESSING_TIME.labels(task_name='upload_to_s3').time():
            # Get conversion record to get S3 bucket and folder
            conversion = mongo.conversions_collection.find_one({"_id": ObjectId(conversion_id)})
            if not conversion:
                raise TaskError("upload_to_s3", f"No conversion record found for ID: {conversion_id}")
            
            # Upload to S3
            s3_path = s3.upload_file(
                file_path=parquet_file,
                bucket=conversion["s3Bucket"],
                folder=conversion["s3Folder"]
            )
            
            if s3_path:
                # Update MongoDB status
                mongo.update_conversion_status(conversion_id, uploaded=True)
                
                # Queue Pinecone import
                import_to_pinecone.delay(s3_path, conversion_id)
            else:
                raise TaskError("upload_to_s3", f"Failed to upload {parquet_file} to S3")
            
    except Exception as e:
        handle_task_error("upload_to_s3", e, mongo, conversion_id)
        raise self.retry(exc=e)

@celery_app.task(
    name='import_to_pinecone',
    bind=True,
    max_retries=3,
    default_retry_delay=300,
    autoretry_for=(Exception,),
    retry_backoff=True
)
def import_to_pinecone(self, s3_path: str, conversion_id: str):
    """
    Import Parquet file from S3 to Pinecone
    
    Args:
        s3_path (str): S3 path to Parquet file
        conversion_id (str): MongoDB conversion record ID
    """
    from pinecone_client import PineconeClient
    from mongo_manager import MongoManager
    
    pinecone = PineconeClient()
    mongo = MongoManager()
    
    try:
        with TASK_PROCESSING_TIME.labels(task_name='import_to_pinecone').time():
            # Get conversion record to get namespace
            conversion = mongo.conversions_collection.find_one({"_id": ObjectId(conversion_id)})
            if not conversion:
                raise TaskError("import_to_pinecone", f"No conversion record found for ID: {conversion_id}")
            
            # Get ZIP record to get namespace
            zip_info = mongo.zip_download_collection.find_one({"_id": conversion["zipDownloadId"]})
            if not zip_info:
                raise TaskError("import_to_pinecone", f"No ZIP record found for conversion: {conversion_id}")
            
            # Import to Pinecone
            result = pinecone.import_from_s3(
                s3_path=s3_path,
                namespace=zip_info["namespace"]
            )
            
            if result:
                # Update MongoDB status
                mongo.update_conversion_status(
                    conversion_id,
                    pineconeImported=True,
                    recordCount=result.get("recordCount", 0)
                )
                
                # Queue company data processing
                process_company_data.delay(conversion_id)
            else:
                raise TaskError("import_to_pinecone", f"Failed to import {s3_path} to Pinecone")
            
    except Exception as e:
        handle_task_error("import_to_pinecone", e, mongo, conversion_id)
        raise self.retry(exc=e)

@celery_app.task(name='process_company_data')
def process_company_data(conversion_id: str):
    """
    Process company data after successful Pinecone import
    
    Args:
        conversion_id (str): MongoDB conversion record ID
    """
    from mongo_manager import MongoManager
    from query_processor import QueryProcessor
    
    mongo = MongoManager()
    processor = QueryProcessor()
    
    try:
        with TASK_PROCESSING_TIME.labels(task_name='process_company_data').time():
            # Get conversion record
            conversion = mongo.conversions_collection.find_one({"_id": ObjectId(conversion_id)})
            if not conversion:
                raise TaskError("process_company_data", f"No conversion record found for ID: {conversion_id}")
            
            # Get ZIP record for namespace
            zip_info = mongo.zip_download_collection.find_one({"_id": conversion["zipDownloadId"]})
            if not zip_info:
                raise TaskError("process_company_data", f"No ZIP record found for conversion: {conversion_id}")
            
            # Process company data using semantic search
            company_data = processor.extract_company_data(
                company_number=conversion["company_number"],
                namespace=zip_info["namespace"]
            )
            
            if company_data:
                # Update result collection
                mongo.upsert_company_result(
                    company_number=company_data["company_number"],
                    company_name=company_data["company_name"],
                    new_entry={
                        "zip_name": zip_info["file_name"],
                        "date": zip_info["file_date"],
                        "company_legal_type": company_data.get("company_legal_type", ""),
                        "currency": company_data.get("currency", "GBP"),
                        "total_director_remuneration": company_data.get("total_director_remuneration", ""),
                        "highest_paid_director": company_data.get("highest_paid_director", {})
                    }
                )
            else:
                raise TaskError("process_company_data", f"Failed to extract company data for {conversion['company_number']}")
            
    except Exception as e:
        handle_task_error("process_company_data", e, mongo, conversion_id)
        raise 