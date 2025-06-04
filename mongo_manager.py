"""
MongoDB manager for storing and retrieving XBRL data
"""

import os
import logging
import pymongo
from pymongo import MongoClient
from pymongo.errors import PyMongoError, DuplicateKeyError
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from bson import ObjectId
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mongo.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ValidationError(Exception):
    """Custom exception for data validation errors"""
    pass

class MongoManager:
    """
    Class for managing MongoDB connections and operations
    """
    def __init__(self, connection_string=None, db_name=None):
        """
        Initialize MongoDB connection
        
        Args:
            connection_string (str): MongoDB connection string, defaults to env var MONGO_URI
            db_name (str): Database name, defaults to env var MONGO_DB_NAME
        """
        # Get MongoDB connection string from environment
        self.connection_string = connection_string or os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
        self.db_name = db_name or os.environ.get("MONGO_DB_NAME", "xbrl_db")
        self.client = None
        self.db = None
        
        # Collections
        self.results_collection = None
        self.conversions_collection = None
        self.zip_download_collection = None
        
        # Connect to database
        self._connect()
        
        logger.info(f"MongoDB manager initialized - Database: {self.db_name}")
    
    def _connect(self):
        """
        Connect to MongoDB database
        """
        try:
            # Get authentication credentials from environment if needed
            username = os.environ.get("MONGO_USERNAME")
            password = os.environ.get("MONGO_PASSWORD")
            
            # Create a MongoDB client with authentication if credentials are provided
            if username and password and 'mongodb+srv' in self.connection_string:
                # For MongoDB Atlas or other services that use srv format
                self.client = MongoClient(self.connection_string)
            elif username and password:
                # For standalone MongoDB with authentication
                auth_source = os.environ.get("MONGO_AUTH_SOURCE", "admin")
                self.client = MongoClient(
                    self.connection_string,
                    username=username,
                    password=password,
                    authSource=auth_source
                )
            else:
                # No authentication
                self.client = MongoClient(self.connection_string)
            
            # Ping the database to check connection
            self.client.admin.command('ping')
            
            # Get database
            self.db = self.client[self.db_name]
            
            # Initialize collections
            self.results_collection = self.db["result"]
            self.conversions_collection = self.db["conversions"]
            self.zip_download_collection = self.db["zip_download"]
            
            # Create indexes for efficient querying
            self._create_indexes()
            
            logger.info("Successfully connected to MongoDB")
            
        except PyMongoError as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def _create_indexes(self):
        """
        Create indexes for collections
        """
        try:
            # result collection
            self.results_collection.create_index(
                [("company_number", pymongo.ASCENDING)],
                unique=True
            )
            
            # conversions collection - Composite index
            self.conversions_collection.create_index([
                ("zipDownloadId", pymongo.ASCENDING),
                ("file_name", pymongo.ASCENDING)
            ], unique=True)
            
            # Additional indexes for conversions
            self.conversions_collection.create_index([("company_number", pymongo.ASCENDING)])
            self.conversions_collection.create_index([("converted", pymongo.ASCENDING)])
            self.conversions_collection.create_index([("uploaded", pymongo.ASCENDING)])
            self.conversions_collection.create_index([("pineconeImported", pymongo.ASCENDING)])
            
            # zip_download collection - Composite index
            self.zip_download_collection.create_index([
                ("file_name", pymongo.ASCENDING),
                ("file_date", pymongo.ASCENDING)
            ], unique=True)
            
            # Additional indexes for zip_download
            self.zip_download_collection.create_index([("namespace", pymongo.ASCENDING)])
            self.zip_download_collection.create_index([("downloaded", pymongo.ASCENDING)])
            self.zip_download_collection.create_index([("unzipped", pymongo.ASCENDING)])
            
            logger.info("MongoDB indexes created successfully")
            
        except PyMongoError as e:
            logger.error(f"Failed to create indexes: {e}")
            raise
    
    def validate_zip_data(self, url: str, file_name: str, file_date: str) -> bool:
        """
        Validate ZIP download data
        
        Args:
            url (str): ZIP file URL
            file_name (str): ZIP file name
            file_date (str): Date identifier (YYYY-MM)
            
        Returns:
            bool: True if valid
            
        Raises:
            ValidationError: If validation fails
        """
        if not url or not url.startswith(('http://', 'https://')):
            raise ValidationError("Invalid URL format")
            
        if not file_name or not file_name.endswith('.zip'):
            raise ValidationError("Invalid file name format")
            
        # Validate file_date format (YYYY-MM)
        import re
        if not re.match(r'^\d{4}-\d{2}$', file_date):
            raise ValidationError("Invalid file date format (should be YYYY-MM)")
            
        return True
    
    def validate_conversion_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate conversion data
        
        Args:
            data (dict): Conversion data to validate
            
        Returns:
            bool: True if valid
            
        Raises:
            ValidationError: If validation fails
        """
        required_fields = {
            "zipDownloadId": ObjectId,
            "file_name": str,
            "company_number": str,
            "s3Bucket": str
        }
        
        for field, field_type in required_fields.items():
            if field not in data:
                raise ValidationError(f"Missing required field: {field}")
            if not isinstance(data[field], field_type):
                raise ValidationError(f"Invalid type for {field}")
                
        return True
    
    def validate_company_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate company data
        
        Args:
            data (dict): Company data to validate
            
        Returns:
            bool: True if valid
            
        Raises:
            ValidationError: If validation fails
        """
        required_fields = [
            "company_number",
            "company_name",
            "company_legal_type",
            "total_director_remuneration",
            "highest_paid_director"
        ]
        
        for field in required_fields:
            if field not in data:
                raise ValidationError(f"Missing required field: {field}")
            
        if not isinstance(data["highest_paid_director"], dict):
            raise ValidationError("highest_paid_director must be a dictionary")
            
        hpd_fields = ["name", "remuneration"]
        for field in hpd_fields:
            if field not in data["highest_paid_director"]:
                raise ValidationError(f"Missing field in highest_paid_director: {field}")
                
        return True
    
    def close(self):
        """
        Close MongoDB connection
        """
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
    
    # Methods for zip_download collection
    def create_zip_download(self, url: str, file_name: str, file_date: str) -> Optional[ObjectId]:
        """
        Create a new ZIP download record
        
        Args:
            url (str): URL of the ZIP file
            file_name (str): Name of the ZIP file
            file_date (str): Date identifier for the batch (YYYY-MM)
        
        Returns:
            ObjectId|None: ID of created document or None if failed
        """
        try:
            # Validate input data
            self.validate_zip_data(url, file_name, file_date)
            
            doc = {
                "url": url,
                "file_name": file_name,
                "file_date": file_date,
                "namespace": file_name,
                "downloaded": False,
                "unzipped": False,
                "createdAt": datetime.now(),
                "updatedAt": datetime.now()
            }
            
            # Try to insert, handle duplicate case
            try:
                result = self.zip_download_collection.insert_one(doc)
                logger.info(f"Created ZIP download record: {file_name}")
                return result.inserted_id
            except DuplicateKeyError:
                # If duplicate, return existing record ID
                existing = self.zip_download_collection.find_one({
                    "file_name": file_name,
                    "file_date": file_date
                })
            if existing:
                    logger.info(f"Found existing ZIP download record: {file_name}")
                    return existing["_id"]
            return None
                
        except ValidationError as e:
            logger.error(f"Validation error creating ZIP download record: {e}")
            return None
        except PyMongoError as e:
            logger.error(f"Failed to create ZIP download record: {e}")
            return None
    
    def update_zip_download_status(self, _id: ObjectId, downloaded: bool = None, unzipped: bool = None) -> bool:
        """
        Update ZIP download status
        
        Args:
            _id (ObjectId): Document ID
            downloaded (bool): Set downloaded status and timestamp
            unzipped (bool): Set unzipped status and timestamp
            
        Returns:
            bool: True if successful
        """
        try:
            update = {"updatedAt": datetime.now()}
            
            if downloaded is not None:
                update["downloaded"] = downloaded
                if downloaded:
                    update["downloadedAt"] = datetime.now()
                    
            if unzipped is not None:
                update["unzipped"] = unzipped
                if unzipped:
                    update["unzippedAt"] = datetime.now()
            
            result = self.zip_download_collection.update_one(
                {"_id": _id},
                {"$set": update}
            )
            
            success = result.modified_count > 0
            if success:
                logger.info(f"Updated ZIP download status for {_id}")
            else:
                logger.warning(f"No ZIP download record found for {_id}")
            
            return success
            
        except PyMongoError as e:
            logger.error(f"Failed to update ZIP download status: {e}")
            return False
    
    # Methods for conversions collection
    def create_conversion(self, zip_download_id: ObjectId, file_name: str, file_date: datetime, 
                         company_number: str, s3_bucket: str) -> Optional[ObjectId]:
        """
        Create a new conversion record
        
        Args:
            zip_download_id (ObjectId): Reference to zip_download document
            file_name (str): Name of the HTML/XML file
            file_date (datetime): Date of the file
            company_number (str): Company identifier
            s3_bucket (str): S3 bucket name for Parquet files
            
        Returns:
            ObjectId|None: ID of created document or None if failed
        """
        try:
            # Validate input data
            data = {
                "zipDownloadId": zip_download_id,
                "file_name": file_name,
                "company_number": company_number,
                "s3Bucket": s3_bucket
            }
            self.validate_conversion_data(data)
            
            doc = {
                **data,
                "file_date": file_date,
                "retrieved": False,
                "converted": False,
                "uploaded": False,
                "pineconeImported": False,
                "s3Folder": "",  # Will be set after getting zip_download info
                "recordCount": 0,
                "createdAt": datetime.now(),
                "updatedAt": datetime.now()
            }
            
            # Get zip_download info to set s3Folder
            zip_info = self.zip_download_collection.find_one({"_id": zip_download_id})
            if zip_info:
                doc["s3Folder"] = zip_info["file_name"]
            else:
                raise ValidationError(f"No ZIP record found for ID: {zip_download_id}")
            
            # Try to insert, handle duplicate case
            try:
                result = self.conversions_collection.insert_one(doc)
                logger.info(f"Created conversion record for file: {file_name}")
                return result.inserted_id
            except DuplicateKeyError:
                # If duplicate, return existing record ID
                existing = self.conversions_collection.find_one({
                    "zipDownloadId": zip_download_id,
                    "file_name": file_name
                })
                if existing:
                    logger.info(f"Found existing conversion record: {file_name}")
                    return existing["_id"]
                return None
                
        except ValidationError as e:
            logger.error(f"Validation error creating conversion record: {e}")
            return None
        except PyMongoError as e:
            logger.error(f"Failed to create conversion record: {e}")
            return None
    
    def update_conversion_status(self, _id: ObjectId, **kwargs) -> bool:
        """
        Update conversion status fields
        
        Args:
            _id (ObjectId): Document ID
            **kwargs: Status fields to update (retrieved, converted, uploaded, pineconeImported)
            
        Returns:
            bool: True if successful
        """
        try:
            valid_fields = {
                "retrieved": "retrievedAt",
                "converted": "convertedAt",
                "uploaded": "uploadedAt",
                "pineconeImported": "importedAt"
            }
            
            update = {"updatedAt": datetime.now()}
            
            for field, value in kwargs.items():
                if field in valid_fields and isinstance(value, bool):
                    update[field] = value
                    if value and valid_fields[field]:
                        update[valid_fields[field]] = datetime.now()
            
            if "recordCount" in kwargs:
                if not isinstance(kwargs["recordCount"], int) or kwargs["recordCount"] < 0:
                    raise ValidationError("recordCount must be a non-negative integer")
                update["recordCount"] = kwargs["recordCount"]
            
            result = self.conversions_collection.update_one(
                {"_id": _id if isinstance(_id, ObjectId) else ObjectId(_id)},
                {"$set": update}
            )
            
            success = result.modified_count > 0
            if success:
                logger.info(f"Updated conversion status for {_id}")
            else:
                logger.warning(f"No conversion record found for {_id}")
            
            return success
            
        except ValidationError as e:
            logger.error(f"Validation error updating conversion status: {e}")
            return False
        except PyMongoError as e:
            logger.error(f"Failed to update conversion status: {e}")
            return False
    
    # Methods for result collection
    def upsert_company_result(self, company_number: str, company_name: str, new_entry: Dict[str, Any]) -> bool:
        """
        Upsert company result with new accounts data
        
        Args:
            company_number (str): Company identifier
            company_name (str): Company name
            new_entry (dict): New accounts data entry
            
        Returns:
            bool: True if successful
        """
        try:
            # Validate company data
            self.validate_company_data(new_entry)
            
            # Prepare the accounts entry
            accounts_entry = {
                "zip_name": new_entry.get("zip_name", ""),
                "date": new_entry.get("date", ""),
                "company_legal_type": new_entry.get("company_legal_type", ""),
                "currency": new_entry.get("currency", "GBP"),
                "total_director_remuneration": new_entry.get("total_director_remuneration", ""),
                "highest_paid_director": new_entry.get("highest_paid_director", {}),
                "inserted_at": datetime.now()
            }
            
            # First, check if entry with same zip_name and date exists
            existing = self.results_collection.find_one({
                "company_number": company_number,
                "accounts_date": {
                    "$elemMatch": {
                        "zip_name": accounts_entry["zip_name"],
                        "date": accounts_entry["date"]
                    }
                }
            })
            
            if existing:
                # Update existing entry
                result = self.results_collection.update_one(
                    {
                        "company_number": company_number,
                        "accounts_date": {
                            "$elemMatch": {
                                "zip_name": accounts_entry["zip_name"],
                                "date": accounts_entry["date"]
                            }
                        }
                    },
                    {
                        "$set": {
                            "company_name": company_name,
                            "updatedAt": datetime.now(),
                            "accounts_date.$": accounts_entry
                        }
                    }
                )
            else:
                # Add new entry
                result = self.results_collection.update_one(
                    {"company_number": company_number},
                {
                        "$setOnInsert": {
                            "company_name": company_name,
                            "createdAt": datetime.now()
                        },
                        "$set": {"updatedAt": datetime.now()},
                        "$push": {"accounts_date": accounts_entry}
                    },
                    upsert=True
            )
            
            success = result.modified_count > 0 or result.upserted_id is not None
            if success:
                logger.info(f"Updated company result for {company_number}")
            else:
                logger.warning(f"No changes made to company result for {company_number}")
            
            return success
            
        except ValidationError as e:
            logger.error(f"Validation error upserting company result: {e}")
            return False
        except PyMongoError as e:
            logger.error(f"Failed to upsert company result: {e}")
            return False
    
    def get_pending_conversions(self) -> List[Dict[str, Any]]:
        """
        Get all pending conversions
            
        Returns:
            list: List of conversion documents that need processing
        """
        try:
            return list(self.conversions_collection.find({
                "$or": [
                    {"converted": False},
                    {"uploaded": False},
                    {"pineconeImported": False}
                ]
            }))
        except PyMongoError as e:
            logger.error(f"Failed to get pending conversions: {e}")
            return []
    
    def get_company_data(self, company_number: str) -> Optional[Dict[str, Any]]:
        """
        Get company data by company number
        
        Args:
            company_number (str): Company registration number
            
        Returns:
            dict|None: Company data or None if not found
        """
        try:
            return self.results_collection.find_one({"company_number": company_number})
        except PyMongoError as e:
            logger.error(f"Failed to get company data: {e}")
            return None

# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Initialize MongoDB manager
    mongo = MongoManager()
    
    # Example: Create a ZIP download record
    zip_id = mongo.create_zip_download(
        url="https://example.com/data.zip",
        file_name="Accounts_Monthly_Data-2025-04.zip",
        file_date="2025-04"
    )
    
    if zip_id:
        # Update its status
        mongo.update_zip_download_status(zip_id, downloaded=True)
        
        # Create a conversion record
        conv_id = mongo.create_conversion(
            zip_download_id=zip_id,
            file_name="12345678.html",
            file_date=datetime.now(),
            company_number="12345678",
            s3_bucket="my-bucket"
        )
        
        if conv_id:
            # Update conversion status
            mongo.update_conversion_status(
                conv_id,
                retrieved=True,
                converted=True,
                recordCount=100
            )
    
    # Close connection
    mongo.close() 