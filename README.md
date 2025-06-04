# XBRL to Pinecone Pipeline

This project automates the process of extracting XBRL data from the Commercial Registry, processing it, and importing it into a Pinecone vector database.

## Features

- Crawls [Companies House historic monthly accounts data](https://download.companieshouse.gov.uk/historicmonthlyaccountsdata.html) for ZIP files containing XBRL
- Extracts XBRL files from ZIP archives
- Parses XBRL formats (.html) and XBRL (.xml) inline
- Converts XBRL data to Parquet format for Pinecone
- Uploads Parquet files to S3 (optional)
- Imports data into Pinecone using the bulk import API

## Requirements

- Python 3.7+
- Pinecone account with Standard plan
- S3 bucket to store Parquet files (optional if using local files)

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/xbrl-to-pinecone.git
cd xbrl-to-pinecone

# Install dependencies
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the root directory with the following:

```
# Pinecone configuration
PINECONE_API_KEY=your_pinecone_api_key
INDEX_HOST=your_index_host
S3_URI=s3://your_bucket/path/
INTEGRATION_ID=your_integration_id

# S3 configuration (optional - only if you want to upload files to S3)
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
AWS_REGION=your_region
S3_ENDPOINT_URL=https://s3.amazonaws.com  # Optional: for S3-compatible endpoints
S3_UPLOAD_ENABLED=true

# Scraper configuration
SCRAPER_URL=https://download.companieshouse.gov.uk/historicmonthlyaccountsdata.html
BASE_URL=https://download.companieshouse.gov.uk/
MAX_FILES_TO_DOWNLOAD=3  # Limit to 3 ZIP files to avoid downloading large amounts of data
MAX_FILES_TO_PARSE=100  # Limit of 100 XBRL files per ZIP
FILTER_YEAR=  # Leave empty for all years, or specify a year (e.g., "2023")

# File paths
DOWNLOAD_DIR=downloads
EXTRACT_DIR=extracted
PARQUET_DIR=parquet_files

# Vector dimensions
VECTOR_DIMENSIONS=1536  # Dimension for OpenAI embeddings

# Variables for query_processor.py
PINECONE_FIELDS=company_number,company_name,company_legal_type,accounts_date,highest_paid_director.name,highest_paid_director.remuneration,total_director_remuneration,currency
BATCH_SIZE=50
OPENAI_MAX_TOKENS_PER_BATCH=4000
OPENAI_REQUEST_TIMEOUT=60
```

## Usage

Run the main script:

```bash
python main.py
```

Available options:

```
python main.py --help
```

Common examples:

```bash
# Run the full pipeline
python3 main.py 
# Only download and process XBRL without importing to Pinecone
python main.py --scrape-only

# Only import to Pinecone (use existing Parquet files)
python main.py --import-only

# Filter by year
python main.py --year 2023

# Limit number of files
python main.py --max-zips 5 --max-files 100

# Enable S3 upload
python main.py --upload-s3

# No limit on files to process
python main.py --no-limit
```

## Directory Structure

- `/downloads`: Contains downloaded ZIP files
- `/extracted`: Contains extracted XBRL files
- `/parquet_files`: Contains generated Parquet files
- `/logs`: Contains process logs (created automatically)

## Processing Flow

1. Checks requirements (dependencies, environment variables)
2. Downloads ZIP files with XBRL data
3. Extracts XBRL files from ZIPs
4. Converts XBRL files to Parquet format
5. (Optional) Uploads Parquet files to S3
6. Imports data to Pinecone using the bulk import API

## New Flow with Pinecone and OpenAI

The project now includes a new module `query_processor.py` that allows querying documents in Pinecone and extracting structured information using OpenAI.

### How to use the new flow

The API processor (`api_processor.py`) now internally uses this functionality to query company information in Pinecone, replacing queries to external APIs:

```python
from query_processor import query_pinecone_and_summarize

# Generate a query
query = f"company number {company_number} account date {account_date}"

# Execute the query and get structured information
company_info = query_pinecone_and_summarize(query)
```

This production-optimized flow includes:
- Automatic retry with exponential backoff
- Precise token control with tiktoken
- Batch processing
- Configuration validation with Pydantic
- Detailed error and event logging