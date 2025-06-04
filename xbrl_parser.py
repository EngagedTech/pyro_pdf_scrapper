"""
Module for parsing XBRL data and converting it to Parquet format compatible with Pinecone
"""

import os
import pandas as pd
import logging
import uuid
import json
from bs4 import BeautifulSoup
from typing import Dict, List, Any, Union, Tuple, Optional
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import hashlib
from config import VECTOR_DIMENSIONS
import xbrl
from xbrl import XBRLParser as LibXBRLParser
from bson import ObjectId

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class XBRLParser:
    """
    Class for parsing XBRL documents and converting them to Pinecone-compatible Parquet format
    using the python-xbrl library.
    """
    def __init__(self, output_dir="parquet_files", vector_dimensions=None):
        """
        Initialize the XBRL parser
        
        Args:
            output_dir (str): Directory to save Parquet files
            vector_dimensions (int, optional): Dimensions for vector embeddings
        """
        self.output_dir = output_dir
        self.vector_dimensions = vector_dimensions or VECTOR_DIMENSIONS
        self.lib_parser = LibXBRLParser()
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        logger.info(f"Initialized XBRL parser with vector dimensions: {self.vector_dimensions}")
    
    def _generate_vector(self, text: str) -> List[float]:
        """
        Generate a vector embedding for text
        In a real-world scenario, this would use an embedding model like OpenAI
        Here we're using a deterministic hash-based approach for demonstration
        
        Args:
            text (str): Text to generate vector for
            
        Returns:
            list: Vector embedding
        """
        # This is a placeholder for a real embedding model
        # In production, you would use a proper embedding service
        
        # Create a deterministic but varied vector based on text hash
        hash_object = hashlib.sha256(text.encode())
        hash_hex = hash_object.hexdigest()
        
        # Convert hash to a list of floats
        seed = int(hash_hex, 16) % (10**8)
        np.random.seed(seed)
        
        # Generate a normalized vector of the correct dimension
        vector = np.random.normal(0, 1, self.vector_dimensions)
        # Normalize to unit length (important for vector search)
        vector = vector / np.linalg.norm(vector)
        
        return vector.tolist()
    
    def _truncate_text(self, text, max_length=10000):
        """
        Truncate text to maximum length while preserving complete sentences
        
        Args:
            text (str): Text to truncate
            max_length (int): Maximum allowed length
            
        Returns:
            str: Truncated text
        """
        if not text or len(text) <= max_length:
            return text
            
        # Try to truncate at a period
        truncated = text[:max_length]
        last_period = truncated.rfind('. ')
        
        if last_period > max_length * 0.7:  # If we find a period in the last 30% of text
            return truncated[:last_period+1]
        return truncated

    def _check_metadata_size(self, metadata, max_size=38000):
        """
        Check metadata size and reduce if necessary
        
        Args:
            metadata (dict): Metadata to check
            max_size (int): Maximum allowed size in bytes
            
        Returns:
            dict: Adjusted metadata within size limit
        """
        # Convert to JSON to measure size
        metadata_json = json.dumps(metadata)
        current_size = len(metadata_json.encode('utf-8'))
        
        # If within limit, return unchanged
        if current_size <= max_size:
            return metadata
            
        logger.warning(f"Metadata size is {current_size} bytes, exceeding limit of {max_size} bytes")
        
        # If size exceeds limit, reduce full_text
        if 'full_text' in metadata and metadata['full_text']:
            # Calculate how much text we can keep
            excess_bytes = current_size - max_size
            # Add 20% safety margin
            text_reduction = excess_bytes + int(excess_bytes * 0.2)
            
            current_text_len = len(metadata['full_text'])
            new_text_len = max(1000, current_text_len - text_reduction)
            
            # Truncate text
            metadata['full_text'] = self._truncate_text(metadata['full_text'], new_text_len)
            
            # Check again
            metadata_json = json.dumps(metadata)
            new_size = len(metadata_json.encode('utf-8'))
            
            logger.info(f"Reduced metadata size from {current_size} to {new_size} bytes")
            
            # If still exceeds, try more aggressive reduction
            if new_size > max_size:
                # Extract only beginning and end of text
                if len(metadata['full_text']) > 5000:
                    start = metadata['full_text'][:2500]
                    end = metadata['full_text'][-2500:]
                    metadata['full_text'] = f"{start}... [TRUNCATED] ...{end}"
                    
                    # Last resort: if still too large, drastically reduce
                    metadata_json = json.dumps(metadata)
                    if len(metadata_json.encode('utf-8')) > max_size:
                        metadata['full_text'] = metadata['full_text'][:max_size // 10]
                        metadata['full_text'] += " [SEVERELY TRUNCATED DUE TO SIZE LIMITS]"
        
        return metadata

    def _extract_data_from_filename(self, filename):
        """
        Extract information from filename using pattern Prod224_2476_00002687_20240405.html
        
        Args:
            filename (str): Filename
            
        Returns:
            dict: Data extracted from filename
        """
        try:
            # Remove extension and split by underscores
            base_name = os.path.splitext(filename)[0]
            parts = base_name.split('_')
            
            # Check if has expected format (at least 4 parts)
            if len(parts) >= 4:
                # Company number is in second-to-last position
                company_number = parts[-2]
                
                # Date in last position
                date_str = parts[-1]
                if len(date_str) == 8 and date_str.isdigit():
                    # Format YYYYMMDD
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
                logger.warning(f"Unexpected filename format: {filename}")
                return {}
            
        except Exception as e:
            logger.error(f"Error extracting data from filename {filename}: {e}")
            return {}

    def _flatten_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create simplified metadata structure with only required fields
        
        Args:
            data (dict): Data extracted from document
            
        Returns:
            dict: Structured metadata with simplified key fields
        """
        # Base metadata structure (simplified to requested fields)
        metadata = {
            "company_number": "",
            "company_name": "",
            "company_legal_type": "",
            "total_director_remuneration": "",
            "highest_paid_director": {
                "name": "",
                "remuneration": ""
            },
            "currency": "GBP",
            "full_text": ""
        }
        
        # First, extract data from filename
        if "doc_file_name" in data:
            filename_data = self._extract_data_from_filename(data["doc_file_name"])
            metadata.update(filename_data)
        
        # Second, extract company_name from document title
        if "doc_title" in data and data["doc_title"]:
            # Usually title follows pattern "Company Name - rest of title"
            title_parts = data["doc_title"].split(' - ', 1)
            if len(title_parts) > 0:
                metadata["company_name"] = title_parts[0].strip()
        
        # Third, look in metadata if not found elsewhere
        if "meta_tags" in data:
            for meta in data["meta_tags"]:
                name = meta.get("name", "").lower()
                content = meta.get("content", "")
                
                # Capture specific metadata information
                if name == "companynumber" and not metadata["company_number"]:
                    metadata["company_number"] = content
                elif name == "companyname" and not metadata["company_name"]:
                    metadata["company_name"] = content
                elif name == "companylegaltype":
                    metadata["company_legal_type"] = content
        
        # Extract director information from full text
        if "full_text" in data:
            metadata["full_text"] = data["full_text"]
            
            # Look for director remuneration information
            import re
            
            # Find highest paid director info
            hpd_match = re.search(r"highest\s+paid\s+director.*?[£$€]([0-9,.]+)", data["full_text"], re.IGNORECASE)
            if hpd_match:
                metadata["highest_paid_director"]["remuneration"] = hpd_match.group(1)
            
            # Find total director remuneration
            total_match = re.search(r"total\s+directors[']?\s+remuneration.*?[£$€]([0-9,.]+)", data["full_text"], re.IGNORECASE)
            if total_match:
                metadata["total_director_remuneration"] = total_match.group(1)
            
        return metadata
    
    def _partition_document(self, doc_id, metadata, full_text, vector, max_metadata_size=38000):
        """
        Partition a large document into multiple records to avoid exceeding
        Pinecone metadata size limit.
        
        Args:
            doc_id (str): Document ID
            metadata (dict): Structured metadata
            full_text (str): Full document text
            vector (list): Embeddings vector
            max_metadata_size (int): Maximum metadata size in bytes
            
        Returns:
            list: List of partitioned records
        """
        # Calculate how many characters we can include in each partition
        # First check size of metadata without full text
        base_metadata = metadata.copy()
        base_metadata["full_text"] = ""
        base_metadata_json = json.dumps(base_metadata)
        base_size = len(base_metadata_json.encode('utf-8'))
        
        # Available space for text in each partition
        available_size = max_metadata_size - base_size - 100  # 100 bytes margin
        
        # Split text into segments that fit in metadata
        text_segments = []
        remaining_text = full_text
        
        while remaining_text:
            # Estimate how many characters we can include (approximate)
            # On average, each UTF-8 character can take ~1-4 bytes
            estimated_chars = int(available_size / 2)  # Conservative estimate
            
            # Adjust if remaining text is shorter
            if len(remaining_text) <= estimated_chars:
                text_segments.append(remaining_text)
                break
                
            # Look for a good cut point (end of sentence)
            segment = remaining_text[:estimated_chars]
            last_period = segment.rfind('. ')
            
            if last_period > estimated_chars * 0.7:  # If there's a period in last 30%
                cut_point = last_period + 1
            else:
                # If no good cut point, look for space
                last_space = segment.rfind(' ')
                cut_point = last_space if last_space > 0 else estimated_chars
            
            text_segments.append(remaining_text[:cut_point])
            remaining_text = remaining_text[cut_point:].lstrip()
        
        # Create partitioned records
        records = []
        total_parts = len(text_segments)
        
        for i, text_segment in enumerate(text_segments):
            part_num = i + 1
            part_metadata = base_metadata.copy()
            part_metadata["full_text"] = text_segment
            part_metadata["part_number"] = part_num
            part_metadata["total_parts"] = total_parts
            
            # Create unique ID for this partition
            part_id = f"{doc_id}_part{part_num}" if total_parts > 1 else doc_id
            
            # Create record
            record = {
                "id": part_id,
                "values": vector,
                "metadata": json.dumps(part_metadata)
            }
            
            records.append(record)
            
        logger.info(f"Document partitioned into {len(records)} parts")
        return records

    def xbrl_to_parquet(self, file_path: str, conversion_id: str, mongo_manager=None) -> Optional[str]:
        """
        Convert an XBRL file to Parquet format compliant with Pinecone requirements
        
        Args:
            file_path (str): Path to XBRL file (.html or .xml)
            conversion_id (str): MongoDB conversion record ID
            mongo_manager (MongoManager, optional): MongoDB manager for storing data
            
        Returns:
            str: Path to the generated Parquet file or None if failed
        """
        try:
            # Skip files that are too large to avoid memory issues
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            if file_size_mb > 10:
                logger.warning(f"Skipping large file ({file_size_mb:.2f} MB): {file_path}")
                return None
                
            # Get conversion record
            if mongo_manager and conversion_id:
                conversion = mongo_manager.conversions_collection.find_one({"_id": ObjectId(conversion_id)})
                if not conversion:
                    logger.error(f"No conversion record found for ID: {conversion_id}")
                    return None
                
                # Get ZIP info for output directory structure
                zip_info = mongo_manager.zip_download_collection.find_one({"_id": conversion["zipDownloadId"]})
                if not zip_info:
                    logger.error(f"No ZIP record found for conversion: {conversion_id}")
                    return None
                
                # Create output directory structure
                output_subdir = os.path.join(self.output_dir, zip_info["file_name"])
            os.makedirs(output_subdir, exist_ok=True)
            parquet_file = os.path.join(output_subdir, f"{os.path.basename(file_path).split('.')[0]}.parquet")
            # else:
            #     # Fallback to simple path if no MongoDB info
            #     parquet_file = os.path.join(self.output_dir, f"{os.path.basename(file_path).split('.')[0]}.parquet")
            
            # Skip if output file already exists
            if os.path.exists(parquet_file):
                logger.info(f"Parquet file already exists: {parquet_file}")
                return parquet_file
            
            logger.info(f"Parsing XBRL file: {file_path}")
            
            # General document data
            document_data = {
                'doc_file_name': os.path.basename(file_path),
                'doc_file_path': file_path,
                'meta_tags': [],
                'full_text': "",
                'doc_title': ""
            }
            
            # Extract metadata from filename
            filename_data = self._extract_data_from_filename(document_data['doc_file_name'])
            
            # Parse with BeautifulSoup to extract text and metadata
            try:
                with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                    content = file.read()
                
                # Use BeautifulSoup to extract content
                soup = BeautifulSoup(content, 'xml' if file_path.lower().endswith('.xml') else 'lxml')
                
                # Extract all metadata
                meta_tags = soup.find_all('meta')
                for tag in meta_tags:
                    document_data['meta_tags'].append({
                        'name': tag.get('name', ''),
                        'content': tag.get('content', '')
                    })
                
                # Extract title
                title_tag = soup.find('title')
                if title_tag:
                    document_data['doc_title'] = title_tag.text.strip()
                
                # Extract all readable text
                document_data['full_text'] = soup.get_text(" ", strip=True)
                
                # Generate unique ID for document
                doc_id = hashlib.md5(os.path.basename(file_path).encode()).hexdigest()
                
                # Generate vector for complete document
                vector = self._generate_vector(document_data['full_text'][:32000])
                
                # Generate structured metadata
                structured_metadata = self._flatten_metadata(document_data)
                
                # Check metadata size and partition if necessary
                metadata_json = json.dumps(structured_metadata)
                metadata_size = len(metadata_json.encode('utf-8'))
                
                # Detailed logging of metadata size
                logger.info(f"Metadata size for {os.path.basename(file_path)}: {metadata_size} bytes")
                logger.info(f"Full text length: {len(structured_metadata.get('full_text', ''))}")
                
                records = []
                
                if metadata_size > 38000:
                    logger.warning(f"Metadata size is {metadata_size} bytes, exceeding limit. Partitioning document.")
                    records = self._partition_document(
                        doc_id=doc_id,
                        metadata=structured_metadata,
                        full_text=document_data['full_text'],
                        vector=vector
                    )
                else:
                    # If size is within limit, use single record
                    records = [{
                        "id": doc_id,
                        "values": vector,
                        "metadata": metadata_json
                    }]
                
                # Create DataFrame with all records
                df = pd.DataFrame(records)
                
                # Ensure correct columns
                if set(df.columns) != set(["id", "values", "metadata"]):
                    logger.warning(f"Incorrect DataFrame columns: {df.columns}, adjusting...")
                    # Ensure required columns exist
                    required_columns = ["id", "values", "metadata"]
                    for col in required_columns:
                        if col not in df.columns:
                            df[col] = ["" for _ in range(len(df))]
                    # Remove additional columns if they exist
                    df = df[required_columns]
                
                # Export to Parquet
                table = pa.Table.from_pandas(df)
                pq.write_table(
                    table, 
                    parquet_file,
                    compression='snappy',
                    use_dictionary=False,
                    version='2.6'
                )
                
                if len(records) > 1:
                    logger.info(f"Created Parquet file with {len(records)} partitions: {parquet_file}")
                else:
                    logger.info(f"Created Parquet file for document: {parquet_file}")
                
                # Update conversion status in MongoDB if manager is provided
                if mongo_manager and conversion_id:
                    mongo_manager.update_conversion_status(
                        _id=ObjectId(conversion_id),
                        converted=True,
                        recordCount=len(records)
                    )
                
                return parquet_file
                
            except Exception as e:
                logger.error(f"Error parsing file {file_path}: {e}")
                
                # Update conversion status in MongoDB if manager is provided
                if mongo_manager and conversion_id:
                    mongo_manager.update_conversion_status(
                        _id=ObjectId(conversion_id),
                        converted=False
                    )
                    
                return None
                
        except Exception as e:
            logger.error(f"Error converting {file_path} to Parquet: {e}")
            
            # Update conversion status in MongoDB if manager is provided
            if mongo_manager and conversion_id:
                mongo_manager.update_conversion_status(
                    _id=ObjectId(conversion_id),
                    converted=False
                )
                
            return None
    
    def process_all_files(self, file_paths: List[str]) -> List[str]:
        """
        Process multiple XBRL files to Parquet
        
        Args:
            file_paths (list): List of paths to XBRL files
            
        Returns:
            list: List of paths to generated Parquet files
        """
        parquet_files = []
        total_files = len(file_paths)
        
        for i, file_path in enumerate(file_paths):
            if i % 10 == 0 or i == total_files - 1:
                logger.info(f"Processing file {i+1}/{total_files}")
                
            if file_path.lower().endswith(('.html', '.xml')):
                parquet_file = self.xbrl_to_parquet(file_path, None)
                if parquet_file:
                    parquet_files.append(parquet_file)
        
        return parquet_files 