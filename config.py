# config.py
import os
from dotenv import load_dotenv

# Reemplazo de distutils.util.strtobool (obsoleto en Python 3.12+)
def strtobool(val):
    """
    Convert a string representation of truth to True or False.
    
    True values are 'y', 'yes', 't', 'true', 'on', and '1';
    False values are 'n', 'no', 'f', 'false', 'off', and '0'.
    Raises ValueError if 'val' is anything else.
    """
    val = val.lower()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return True
    elif val in ('n', 'no', 'f', 'false', 'off', '0'):
        return False
    else:
        raise ValueError(f"Invalid truth value: {val}")

# Load environment variables from .env file
load_dotenv()

# Pinecone configuration
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY", "your_pinecone_api_key")
INDEX_HOST = os.getenv("PINECONE_INDEX_HOST", "your_index_host")
S3_URI = os.getenv("S3_URI", "s3://pyrocodefi/MONTH/TO/NAMESPACES")
INTEGRATION_ID = os.getenv("INTEGRATION_ID", "7dd9da09-2cc0-4418-a2c4-f69f7ef13f20")

# S3 configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", None)  # Usar None para AWS S3 estándar
S3_UPLOAD_ENABLED = bool(strtobool(os.getenv("S3_UPLOAD_ENABLED", "false")))

# Web scraper configuration
BASE_URL = os.getenv("BASE_URL", "https://download.companieshouse.gov.uk/")
LIST_URL = os.getenv("LIST_URL", "https://download.companieshouse.gov.uk/historicmonthlyaccountsdata.html")
# Alias para mantener compatibilidad con el código existente
SCRAPER_URL = LIST_URL

# Processing limits
MAX_FILES_TO_DOWNLOAD = int(os.getenv("MAX_FILES_TO_DOWNLOAD", "3"))
MAX_FILES_TO_PARSE = int(os.getenv("MAX_FILES_TO_PARSE", "100") or "100")
USE_LIMIT = bool(strtobool(os.getenv("USE_LIMIT", "true")))
FILTER_YEAR = os.getenv("FILTER_YEAR", "")

# Processing options
SEGMENT_FILES = bool(strtobool(os.getenv("SEGMENT_FILES", "true")))
SCRAPE_ONLY = bool(strtobool(os.getenv("SCRAPE_ONLY", "false")))
IMPORT_ONLY = bool(strtobool(os.getenv("IMPORT_ONLY", "false")))

# Execution control - Controla hasta dónde llega el flujo
# Valores posibles: 'download', 'extract', 'parse', 'upload', 'import'
EXECUTION_STAGE = os.getenv("EXECUTION_STAGE", "import")

# File paths configuration
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "downloads")
EXTRACT_DIR = os.getenv("EXTRACT_DIR", "extracted")
PARQUET_DIR = os.getenv("PARQUET_DIR", "parquet_files")

# Vector dimensions (for placeholder vectors)
VECTOR_DIMENSIONS = int(os.getenv("VECTOR_DIMENSIONS", "1536"))