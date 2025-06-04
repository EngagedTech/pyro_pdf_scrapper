# pinecone_client.py

import os
import logging
import time
from typing import Optional, Dict, Any, List
from pinecone import Pinecone
from dotenv import load_dotenv
import numpy as np
import hashlib
from prometheus_client import Histogram

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pinecone.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Performance metrics
PINECONE_OPERATION_TIME = Histogram(
    'pinecone_operation_duration_seconds',
    'Time spent on Pinecone operations',
    ['operation']  # import, search
)

class PineconeError(Exception):
    """Custom exception for Pinecone operations"""
    def __init__(self, operation: str, message: str, original_error: Exception = None):
        self.operation = operation
        self.message = message
        self.original_error = original_error
        super().__init__(f"Pinecone {operation} error: {message}")

class PineconeClient:
    """
    Client for interacting with Pinecone vector database
    """
    def __init__(self):
        """Initialize Pinecone client"""
        self.api_key = os.getenv('PINECONE_API_KEY')
        self.environment = os.getenv('PINECONE_ENVIRONMENT')
        self.index_name = os.getenv('PINECONE_INDEX')
        
        if not all([self.api_key, self.environment, self.index_name]):
            raise ValueError("Missing required Pinecone configuration")
        
        # Initialize client
        self.pc = Pinecone(api_key=self.api_key)
        self.index = self.pc.Index(
            host=f"{self.index_name}-{self.environment}.svc.pinecone.io"
        )
        
        logger.info(f"Initialized Pinecone client for index: {self.index_name}")
    
    def import_from_s3(self, s3_path: str, namespace: str, max_retries: int = 3, retry_delay: int = 60, timeout: int = 3600) -> Optional[Dict[str, Any]]:
        """
        Import vectors from S3 Parquet file into Pinecone with retry logic
        
        Args:
            s3_path (str): S3 path to Parquet file
            namespace (str): Pinecone namespace to import into
            max_retries (int): Maximum number of retry attempts
            retry_delay (int): Delay between retries in seconds
            timeout (int): Maximum time to wait for import in seconds
            
        Returns:
            dict|None: Import result with recordCount if successful
            
        Raises:
            PineconeError: If import fails after retries
        """
        with PINECONE_OPERATION_TIME.labels(operation='import').time():
            start_time = time.time()
            retries = 0
            
            while retries < max_retries:
                try:
                    # Start import operation
                    operation = self.index.start_import(
                        source=s3_path,
                        namespace=namespace,
                        error_mode="CONTINUE"  # Continue on individual record errors
                    )
                    
                    if not operation:
                        raise PineconeError("import", f"Failed to start import from {s3_path}")
                    
                    # Get operation ID
                    operation_id = operation.get("id")
                    if not operation_id:
                        raise PineconeError("import", "No operation ID returned from import start")
                    
                    # Wait for import to complete
                    while True:
                        if time.time() - start_time > timeout:
                            raise PineconeError("import", "Import operation timed out")
                        
                        status = self.index.describe_import(operation_id)
                        state = status.get("state", "")
                        
                        if state == "Completed":
                            logger.info(f"Import completed successfully: {status}")
                            return {
                                "recordCount": status.get("recordsProcessed", 0),
                                "operation_id": operation_id,
                                "duration": time.time() - start_time
                            }
                        elif state in ["Failed", "Cancelled"]:
                            error_msg = status.get("error", "Unknown error")
                            raise PineconeError("import", f"Import failed: {error_msg}")
                        elif not state:
                            raise PineconeError("import", "Invalid status response")
                        
                        # Wait before checking again
                        time.sleep(10)
                    
                except PineconeError as e:
                    retries += 1
                    if retries >= max_retries:
                        logger.error(f"Final error importing to Pinecone: {e}")
                        if e.original_error:
                            logger.error(f"Original error: {e.original_error}")
                        raise
                    
                    logger.warning(f"Retry {retries}/{max_retries} after error: {e}")
                    time.sleep(retry_delay * (2 ** (retries - 1)))  # Exponential backoff
                    
                except Exception as e:
                    raise PineconeError("import", "Unexpected error", e)
    
    def _generate_vector(self, text: str, dimensions: int = 1536) -> List[float]:
        """
        Generate a vector embedding for text (placeholder implementation)
        
        Args:
            text (str): Text to generate vector for
            dimensions (int): Vector dimensions (default: 1536 for OpenAI)
            
        Returns:
            list: Vector embedding
        """
        # Create deterministic vector for testing
        hash_object = hashlib.sha256(text.encode())
        hash_hex = hash_object.hexdigest()
        seed = int(hash_hex, 16) % (10**8)
        np.random.seed(seed)
        vector = np.random.normal(0, 1, dimensions)
        vector = vector / np.linalg.norm(vector)
        return vector.tolist()
    
    def search(self, text: str, namespace: str, top_k: int = 5, filter: Dict = None, include_metadata: bool = True) -> Optional[Dict[str, Any]]:
        """
        Perform semantic search in Pinecone with retry logic
        
        Args:
            text (str): Query text
            namespace (str): Namespace to search in
            top_k (int): Number of results to return
            filter (dict): Optional metadata filter
            include_metadata (bool): Whether to include metadata in results
            
        Returns:
            dict|None: Search results if successful
            
        Raises:
            PineconeError: If search fails
        """
        with PINECONE_OPERATION_TIME.labels(operation='search').time():
            try:
                # Generate query vector
                vector = self._generate_vector(text)
                
                # Perform search
                results = self.index.query(
                    namespace=namespace,
                    vector=vector,
                    top_k=min(top_k, 10000),  # Enforce Pinecone limit
                    include_metadata=include_metadata,
                    filter=filter
                )
                
                if not results:
                    logger.warning(f"No results found for query in namespace {namespace}")
                    return None
                
                return results
                
            except Exception as e:
                raise PineconeError("search", "Search failed", e)
    
    def delete_namespace(self, namespace: str) -> bool:
        """
        Delete a namespace and all its vectors
        
        Args:
            namespace (str): Namespace to delete
            
        Returns:
            bool: True if successful
            
        Raises:
            PineconeError: If deletion fails
        """
        try:
            self.index.delete(delete_all=True, namespace=namespace)
            logger.info(f"Successfully deleted namespace: {namespace}")
            return True
        except Exception as e:
            raise PineconeError("delete", f"Failed to delete namespace {namespace}", e)
    
    def get_namespace_stats(self, namespace: str) -> Optional[Dict[str, Any]]:
        """
        Get statistics for a namespace
        
        Args:
            namespace (str): Namespace to get stats for
            
        Returns:
            dict|None: Namespace statistics if successful
            
        Raises:
            PineconeError: If stats retrieval fails
        """
        try:
            stats = self.index.describe_index_stats(filter={"namespace": namespace})
            if not stats:
                logger.warning(f"No stats found for namespace {namespace}")
                return None
            return stats
        except Exception as e:
            raise PineconeError("stats", f"Failed to get stats for namespace {namespace}", e)
