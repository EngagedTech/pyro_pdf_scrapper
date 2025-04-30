"""
query_processor.py - Optimized Pinecone query and OpenAI summarization module.

This module provides production-ready functions to query Pinecone vector database
and summarize/extract structured information via OpenAI.
"""

import os
import json
import time
import logging
import tiktoken
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass
from enum import Enum

import openai
# Actualizar importación de excepciones para OpenAI API v1.0+
try:
    # Para OpenAI SDK v1.0.0 y posteriores
    from openai import RateLimitError, APIError, APITimeoutError as Timeout
except ImportError:
    # Fallback para versiones anteriores
    from openai.error import RateLimitError, APIError, Timeout

# Importación para Pinecone SDK v6.x
import pinecone
# Importación para Pydantic v2+
try:
    # Para Pydantic v2.0+ 
    from pydantic_settings import BaseSettings
    from pydantic import Field, validator, root_validator
except ImportError:
    # Fallback para versiones anteriores
    from pydantic import BaseSettings, Field, validator, root_validator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('query_processor.log')
    ]
)
logger = logging.getLogger('query_processor')


class EnvConfig(BaseSettings):
    """Pydantic model for environment variables validation."""
    
    # Pinecone settings
    pinecone_api_key: str = Field(..., env='PINECONE_API_KEY')
    pinecone_index_host: str = Field(..., env='PINECONE_INDEX_HOST')
    pinecone_index_name: str = Field(..., env='PINECONE_INDEX_NAME')
    pinecone_namespace: str = Field(..., env='PINECONE_NAMESPACE')
    pinecone_fields: str = Field(..., env='PINECONE_FIELDS')
    
    # OpenAI settings
    openai_api_key: str = Field(..., env='OPENAI_API_KEY')
    openai_model: str = Field(..., env='OPENAI_MODEL')
    openai_max_tokens_per_batch: int = Field(4000, env='OPENAI_MAX_TOKENS_PER_BATCH')
    openai_request_timeout: int = Field(60, env='OPENAI_REQUEST_TIMEOUT')
    openai_extra_headers: Optional[Dict[str, str]] = None
    
    # Processing settings
    batch_size: int = Field(100, env='BATCH_SIZE')
    
    @validator('pinecone_fields')
    def validate_fields(cls, v):
        """Validate that fields is a comma-separated string."""
        if not v or not isinstance(v, str):
            raise ValueError("PINECONE_FIELDS must be a non-empty comma-separated string")
        return v
    
    @validator('openai_extra_headers', pre=True)
    def parse_extra_headers(cls, v):
        """Parse JSON string to dict if provided."""
        if not v:
            return None
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                raise ValueError("OPENAI_EXTRA_HEADERS must be a valid JSON string")
        return v
    
    @root_validator(skip_on_failure=True)
    def check_token_limits(cls, values):
        """Ensure batch_size and token limits are compatible."""
        batch_size = values.get('batch_size', 100)
        max_tokens = values.get('openai_max_tokens_per_batch', 4000)
        
        # Each record might take ~200 tokens on average (very rough estimate)
        # This ensures we don't exceed token limits by default
        estimated_tokens_per_record = 200
        max_records_per_batch = max_tokens // estimated_tokens_per_record
        
        if batch_size > max_records_per_batch:
            logger.warning(
                f"BATCH_SIZE ({batch_size}) might be too large for "
                f"OPENAI_MAX_TOKENS_PER_BATCH ({max_tokens}). "
                f"Recommended maximum: {max_records_per_batch}"
            )
        
        return values
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"  # Permitir variables extras en el .env


def get_token_count(text: str, model: str = "gpt-3.5-turbo") -> int:
    """
    Calculate the number of tokens in a text string for a specific model.
    
    Args:
        text: The input text to count tokens for
        model: The model name to use for encoding
        
    Returns:
        int: Number of tokens
    """
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        # Fall back to cl100k_base for unknown models
        encoding = tiktoken.get_encoding("cl100k_base")
    
    return len(encoding.encode(text))


def retry_with_exponential_backoff(
    func,
    initial_delay: float = 1,
    exponential_base: float = 2,
    jitter: bool = True,
    max_retries: int = 5,
    errors: tuple = (RateLimitError, APIError, Timeout),
):
    """
    Retry a function with exponential backoff.
    
    Args:
        func: The function to execute with retries
        initial_delay: Initial delay between retries in seconds
        exponential_base: Base of the exponential backoff
        jitter: Whether to add random jitter to the delay
        max_retries: Maximum number of retries
        errors: Tuple of exceptions to catch and retry
        
    Returns:
        The result of the function call
    """
    import random
    
    def wrapper(*args, **kwargs):
        num_retries = 0
        delay = initial_delay
        
        while True:
            try:
                return func(*args, **kwargs)
            
            except errors as e:
                num_retries += 1
                if num_retries > max_retries:
                    logger.error(f"Maximum retries ({max_retries}) exceeded.")
                    raise
                
                delay *= exponential_base * (1 + jitter * random.random() * 0.1)
                logger.warning(
                    f"Rate limit or API error: {str(e)}. Retrying in {delay:.2f} seconds. "
                    f"(Attempt {num_retries}/{max_retries})"
                )
                time.sleep(delay)
    
    return wrapper


def query_pinecone(query_vector: List[float], config: EnvConfig) -> Dict:
    """
    Query Pinecone for similar vectors.
    
    Args:
        query_vector: The query embedding vector
        config: Environment configuration
        
    Returns:
        Dict: Pinecone query results
    """
    try:
        # Initialize Pinecone client
        pc = pinecone.Pinecone(api_key=config.pinecone_api_key)
        index = pc.Index(host=config.pinecone_index_host)
        
        # Parse fields from config
        fields = [f.strip() for f in config.pinecone_fields.split(',') if f.strip()]
        
        # Execute query
        response = index.query(
            vector=query_vector,
            top_k=config.batch_size,
            namespace=config.pinecone_namespace,
            include_metadata=True,
            fields=fields
        )
        
        logger.info(f"Pinecone query successful, retrieved {len(response.get('matches', []))} matches")
        return response
    
    except Exception as e:
        logger.error(f"Error querying Pinecone: {str(e)}")
        raise


def prepare_batches(
    matches: List[Dict], 
    batch_size: int, 
    max_tokens: int,
    model: str
) -> List[List[Dict]]:
    """
    Split matches into batches, ensuring each batch's prompt stays within token limits.
    
    Args:
        matches: List of Pinecone match results
        batch_size: Maximum number of records per batch
        max_tokens: Maximum tokens allowed per batch
        model: OpenAI model identifier
        
    Returns:
        List of batches, where each batch is a list of records
    """
    batches = []
    current_batch = []
    current_token_count = 0
    
    # Token count for the prompt template (estimated)
    prompt_template = """
    Extract as JSON the following fields from these records:
    - company_number
    - company_name
    - company_legal_type
    - accounts_date (dd/mm/yyyy)
    - highest_paid_director.name
    - highest_paid_director.remuneration
    - total_director_remuneration
    - currency
    Records: []
    """
    base_token_count = get_token_count(prompt_template, model)
    
    for match in matches:
        # Get metadata from match
        metadata = match.get('metadata', {})
        
        # Calculate token count for this record
        record_text = json.dumps(metadata)
        record_token_count = get_token_count(record_text, model)
        
        # Check if adding this record would exceed token limit
        if (current_token_count + record_token_count + base_token_count > max_tokens or 
            len(current_batch) >= batch_size) and current_batch:
            # Current batch is full, start a new one
            batches.append(current_batch)
            current_batch = [metadata]
            current_token_count = record_token_count
        else:
            # Add to current batch
            current_batch.append(metadata)
            current_token_count += record_token_count
    
    # Add the last batch if it's not empty
    if current_batch:
        batches.append(current_batch)
    
    logger.info(f"Split {len(matches)} records into {len(batches)} batches")
    return batches


@retry_with_exponential_backoff
def query_openai(
    batch: List[Dict],
    model: str,
    max_tokens: int,
    timeout: int,
    extra_headers: Optional[Dict[str, str]] = None
) -> Dict:
    """
    Query OpenAI to extract structured information from a batch of records.
    """
    # Format the prompt
    prompt = f"""
    Carefully analyze the following company information and extract the requested data in JSON format.
    These are search results from a database containing company financial data.
    If any value is not explicitly reported in the records, you must use exactly "N/R".
    
    Records to analyze:
    {json.dumps(batch)}
    
    Please return the data in the specified JSON format. If any value is not explicitly reported, return "N/R".
    
    1. Company Number (Core Ref)
    2. Company Name
    3. Company Legal Type
    4. Accounts Date (dd/mm/yyyy)
    5. Highest Paid Director (name and remuneration)
    6. Total Director Remuneration
    7. Currency (ISO code e.g. GBP, EUR, USD)
    
    Instructions:
    - If the records contain values in a column format by year, use the most recent year available.
    - For "Total Director Remuneration":
      - Look for values near or below labels like "Remuneration paid to directors"
      - If more than one year is shown, use the most recent year's figure
    - For "Highest Paid Director":
      - VERY IMPORTANT: Search for any text containing "Remuneration" related to the highest paid director
      - Use values under sections such as "Remuneration disclosed above include the following amounts paid to the highest paid director"
      - If there is a line like "Remuneration for qualifying services" within a section mentioning the highest paid director, use that number
      - If no name is mentioned, return "N/R" for name
      - Make sure to thoroughly scan all records for any mention of highest paid director remuneration
    - For "Currency":
      - Look for currency symbols (£, €, $, etc.) or explicit currency mentions and return the corresponding ISO code:
        * £ -> "GBP"
        * € -> "EUR" 
        * $ -> "USD"
      - If no explicit symbol is found but amounts are shown with decimals (e.g. 1,234.56), infer "GBP" for UK companies
      - If still unclear, return "N/R"
    
    Expected format:
    {{
      "company_number": "...",
      "company_name": "...",
      "company_legal_type": "...",
      "accounts_date": "dd/mm/yyyy",
      "highest_paid_director": {{
        "name": "...",
        "remuneration": "..."
      }},
      "total_director_remuneration": "...",
      "currency": "..."
    }}
    
    Respond ONLY with the JSON object, no explanation.
    """
    
    # Prepare common request arguments
    common_args = {
        "model": model,
        "messages": [
            {"role": "system", "content": "Extract company info from the provided records."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": max_tokens,
        "temperature": 0.2,  # Lower temperature for more deterministic results
    }
    
    # Add extra headers if provided
    if extra_headers:
        common_args["headers"] = extra_headers
    
    # Call OpenAI API
    try:
        # Initialize OpenAI client with proper configuration
        client = openai.OpenAI(
            api_key=openai.api_key,
            timeout=timeout,
            max_retries=3
        )
        
        # Make the API call with proper error handling
        response = client.chat.completions.create(
            **common_args,
            timeout=timeout
        )
        
        # Extract and parse response content
        content = response.choices[0].message.content.strip()
        
    except openai.APITimeoutError as e:
        logger.error(f"OpenAI API timeout: {str(e)}")
        raise
    except openai.APIError as e:
        logger.error(f"OpenAI API error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in OpenAI API call: {str(e)}")
        raise
    
    try:
        # Clean up the response if needed (remove markdown code blocks)
        if content.startswith("```json"):
            content = content[7:]
        if content.endswith("```"):
            content = content[:-3]
        
        return json.loads(content.strip())
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse OpenAI response as JSON: {e}")
        logger.debug(f"Raw response: {content}")
        raise ValueError(f"OpenAI response is not valid JSON: {e}")


def process_batches(batches: List[List[Dict]], config: EnvConfig) -> Dict:
    """
    Process batches through OpenAI and aggregate results.
    
    Args:
        batches: List of record batches
        config: Environment configuration
        
    Returns:
        Dict: Aggregated results from all batches
    """
    results = {}
    
    for i, batch in enumerate(batches):
        logger.info(f"Processing batch {i+1}/{len(batches)} with {len(batch)} records")
        
        try:
            batch_results = query_openai(
                batch=batch,
                model=config.openai_model,
                max_tokens=config.openai_max_tokens_per_batch,
                timeout=config.openai_request_timeout,
                extra_headers=config.openai_extra_headers
            )
            
            # Merge batch results into final results
            for key, value in batch_results.items():
                if key == "highest_paid_director" and isinstance(value, dict):
                    # Handle nested dictionary for director
                    results.setdefault(key, {}).update(value)
                else:
                    # For simple fields, use the latest value
                    results[key] = value
            
        except Exception as e:
            logger.error(f"Error processing batch {i+1}: {str(e)}")
            # Continue with next batch
    
    return results


def embed_text(query: str) -> List[float]:
    """
    Generate an embedding for a text query using OpenAI.
    
    Args:
        query: Text to embed
        
    Returns:
        List[float]: Embedding vector
    """
    try:
        try:
            # Para OpenAI SDK v1.0.0 y posteriores
            client = openai.OpenAI(api_key=openai.api_key)
            response = client.embeddings.create(
                model="text-embedding-ada-002",
                input=query
            )
            return response.data[0].embedding
        except AttributeError:
            # Fallback para versiones anteriores
            response = openai.Embedding.create(
                model="text-embedding-ada-002",
                input=query
            )
            return response["data"][0]["embedding"]
    except Exception as e:
        logger.error(f"Error generating embedding for query: {str(e)}")
        raise


def query_pinecone_and_summarize(query: str) -> Dict:
    """
    Query Pinecone with a text query, extract information via OpenAI.
    
    This function:
    1. Loads configuration from environment variables
    2. Converts the text query to an embedding vector
    3. Searches Pinecone for similar vectors
    4. Batches results within token limits
    5. Uses OpenAI to extract structured information
    6. Returns a structured dict with company information
    
    Args:
        query: Text query string
        
    Returns:
        Dict: Structured company information
    """
    # Load and validate configuration
    try:
        config = EnvConfig()
    except Exception as e:
        logger.error(f"Environment configuration error: {str(e)}")
        raise
    
    # Configure OpenAI
    openai.api_key = config.openai_api_key
    
    try:
        # Generate embedding for query
        query_vector = embed_text(query)
        
        # Query Pinecone
        results = query_pinecone(query_vector, config)
        logger.info(f"Pinecone query results: {results}")
        matches = results.get("matches", [])
        
        if not matches:
            logger.warning("No matches found in Pinecone")
            return {}
        
        # Prepare batches within token limits
        batches = prepare_batches(
            matches=matches,
            batch_size=config.batch_size,
            max_tokens=config.openai_max_tokens_per_batch,
            model=config.openai_model
        )
        
        # Process batches and aggregate results
        final_results = process_batches(batches, config)
        
        # Ensure the output has the expected structure
        expected_keys = [
            "company_number", "company_name", "company_legal_type", 
            "accounts_date", "highest_paid_director", 
            "total_director_remuneration", "currency"
        ]
        
        for key in expected_keys:
            if key not in final_results:
                if key == "highest_paid_director":
                    final_results[key] = {"name": "", "remuneration": ""}
                else:
                    final_results[key] = ""
        
        return final_results
        
    except Exception as e:
        logger.error(f"Error in query_pinecone_and_summarize: {str(e)}")
        # Return empty dict on failure
        return {} 