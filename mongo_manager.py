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
from dotenv import load_dotenv

# Cargar variables de entorno del archivo .env
load_dotenv()

# Setup logging
logger = logging.getLogger(__name__)

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
        self.downloads_collection = None
        
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
            self.results_collection = self.db["results"]
            self.conversions_collection = self.db["conversions"]
            self.downloads_collection = self.db["downloads"]
            
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
            # results collection
            self.results_collection.create_index([("company_number", pymongo.ASCENDING)], unique=True)
            
            # conversions collection
            self.conversions_collection.create_index([("filename", pymongo.ASCENDING)], unique=True)
            self.conversions_collection.create_index([("status", pymongo.ASCENDING)])
            
            # downloads collection
            self.downloads_collection.create_index([("filename", pymongo.ASCENDING)], unique=True)
            self.downloads_collection.create_index([("status", pymongo.ASCENDING)])
            
            logger.info("MongoDB indexes created successfully")
            
        except PyMongoError as e:
            logger.error(f"Failed to create indexes: {e}")
    
    def close(self):
        """
        Close MongoDB connection
        """
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
    
    # Methods for results collection
    def insert_result(self, result_data: Dict[str, Any]) -> bool:
        """
        Insert company financial data into results collection
        
        Args:
            result_data (dict): Financial data from XBRL documents
                Required fields:
                - company_number
                - company_name
                Optional fields:
                - company_legal_type
                - accounts_date
                - highest_paid_director (dict with name and remuneration)
                - total_director_remuneration
                - currency
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not result_data.get("company_number"):
            logger.error("Cannot insert result without company_number")
            return False
        
        try:
            # Format for MongoDB storage
            document = {
                "company_number": result_data.get("company_number"),
                "company_name": result_data.get("company_name", ""),
                "company_legal_type": result_data.get("company_legal_type", ""),
                "accounts_date": result_data.get("accounts_date", ""),
                "highest_paid_director": result_data.get("highest_paid_director", {"name": "", "remuneration": ""}),
                "total_director_remuneration": result_data.get("total_director_remuneration", ""),
                "currency": result_data.get("currency", "GBP"),
                "inserted_at": datetime.now(),
                "updated_at": datetime.now()
            }
            
            # Use upsert to update if exists, insert if not
            result = self.results_collection.update_one(
                {"company_number": document["company_number"]},
                {"$set": document},
                upsert=True
            )
            
            if result.upserted_id:
                logger.info(f"Inserted new company data: {document['company_number']}")
            else:
                logger.info(f"Updated existing company data: {document['company_number']}")
            
            return True
            
        except PyMongoError as e:
            logger.error(f"Failed to insert result data: {e}")
            return False
    
    def get_company_data(self, company_number: str) -> Optional[Dict[str, Any]]:
        """
        Get company data by company number
        
        Args:
            company_number (str): Company registration number
            
        Returns:
            dict|None: Company data or None if not found
        """
        try:
            result = self.results_collection.find_one({"company_number": company_number})
            return result
        except PyMongoError as e:
            logger.error(f"Failed to retrieve company data: {e}")
            return None
    
    # Methods for conversions collection
    def register_file_conversion(self, filename: str, account_date: str, company_number: str) -> bool:
        """
        Register a file for conversion tracking
        
        Args:
            filename (str): HTML filename
            account_date (str): Account date (e.g., "2023-12-31")
            company_number (str): Company number
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            document = {
                "filename": filename,
                "account_date": account_date,
                "company_number": company_number,
                "status": "pending",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            
            # Use upsert in case the file was already registered
            result = self.conversions_collection.update_one(
                {"filename": filename},
                {"$set": document},
                upsert=True
            )
            
            logger.info(f"Registered file conversion: {filename}")
            return True
            
        except PyMongoError as e:
            logger.error(f"Failed to register file conversion: {e}")
            return False
    
    def update_conversion_status(self, filename: str, status: str) -> bool:
        """
        Update conversion status
        
        Args:
            filename (str): HTML filename
            status (str): New status (e.g., "completed", "failed")
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.conversions_collection.update_one(
                {"filename": filename},
                {
                    "$set": {
                        "status": status,
                        "updated_at": datetime.now()
                    }
                }
            )
            
            if result.modified_count == 0:
                logger.warning(f"No conversion record found for file: {filename}")
                
            logger.info(f"Updated conversion status for {filename}: {status}")
            return result.modified_count > 0
            
        except PyMongoError as e:
            logger.error(f"Failed to update conversion status: {e}")
            return False
    
    def get_pending_conversions(self) -> List[Dict[str, Any]]:
        """
        Get all pending file conversions
        
        Returns:
            list: List of pending conversion documents
        """
        try:
            return list(self.conversions_collection.find({"status": "pending"}))
        except PyMongoError as e:
            logger.error(f"Failed to retrieve pending conversions: {e}")
            return []
    
    # Methods for downloads collection
    def register_zip_download(self, filename: str) -> bool:
        """
        Register a ZIP file download
        
        Args:
            filename (str): ZIP filename
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            document = {
                "filename": filename,
                "date": datetime.now(),
                "status": "downloading",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            
            # Try to insert a new document, handle case where it already exists
            try:
                result = self.downloads_collection.insert_one(document)
                logger.info(f"Registered new ZIP download: {filename}")
                return True
            except DuplicateKeyError:
                # Update existing document if insert fails due to duplicate key
                result = self.downloads_collection.update_one(
                    {"filename": filename},
                    {
                        "$set": {
                            "status": "downloading",
                            "updated_at": datetime.now()
                        }
                    }
                )
                logger.info(f"Updated existing ZIP download record: {filename}")
                return result.modified_count > 0
            
        except PyMongoError as e:
            logger.error(f"Failed to register ZIP download: {e}")
            return False
    
    def update_zip_status(self, filename: str, status: str) -> bool:
        """
        Update ZIP download status
        
        Args:
            filename (str): ZIP filename
            status (str): New status ("downloading", "downloaded", "error")
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.downloads_collection.update_one(
                {"filename": filename},
                {
                    "$set": {
                        "status": status,
                        "updated_at": datetime.now()
                    }
                }
            )
            
            if result.modified_count == 0:
                logger.warning(f"No download record found for ZIP: {filename}")
                
            logger.info(f"Updated ZIP status for {filename}: {status}")
            return result.modified_count > 0
            
        except PyMongoError as e:
            logger.error(f"Failed to update ZIP status: {e}")
            return False
    
    def get_downloads_by_status(self, status: str) -> List[Dict[str, Any]]:
        """
        Get all downloads with specified status
        
        Args:
            status (str): Status to filter by
            
        Returns:
            list: List of download documents
        """
        try:
            return list(self.downloads_collection.find({"status": status}))
        except PyMongoError as e:
            logger.error(f"Failed to retrieve downloads by status: {e}")
            return []
    
    # Utility methods
    def extract_company_data(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract relevant company data from metadata
        
        Args:
            metadata (dict): Metadata from XBRL file
            
        Returns:
            dict: Structured company data
        """
        # Extract highest paid director info from text if available
        highest_paid_director = {"name": "N/R", "remuneration": ""}
        total_director_remuneration = ""
        
        if "full_text" in metadata:
            full_text = metadata["full_text"]
            
            # Find highest paid director info
            import re
            hpd_match = re.search(r"highest\s+paid\s+director.*?[£$€]([0-9,.]+)", full_text, re.IGNORECASE)
            if hpd_match:
                highest_paid_director["remuneration"] = hpd_match.group(1)
            
            # Find total director remuneration
            total_match = re.search(r"total\s+directors[']?\s+remuneration.*?[£$€]([0-9,.]+)", full_text, re.IGNORECASE)
            if total_match:
                total_director_remuneration = total_match.group(1)
        
        # Format accounts date if needed
        accounts_date = metadata.get("account_date", "")
        if accounts_date and "-" in accounts_date:
            # Convert from YYYY-MM-DD to DD/MM/YYYY
            try:
                parts = accounts_date.split("-")
                if len(parts) == 3:
                    accounts_date = f"{parts[2]}/{parts[1]}/{parts[0]}"
            except:
                pass
        
        return {
            "company_number": metadata.get("company_number", ""),
            "company_name": metadata.get("company_name", ""),
            "company_legal_type": metadata.get("company_legal_type", ""),
            "accounts_date": accounts_date,
            "highest_paid_director": highest_paid_director,
            "total_director_remuneration": total_director_remuneration,
            "currency": "GBP"  # Default currency
        }


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Initialize MongoDB manager
    mongo = MongoManager()
    
    # Insert sample data
    result = mongo.insert_result({
        "company_number": "62473",
        "company_name": "George Bence & Sons Limited",
        "company_legal_type": "Private Limited Company",
        "accounts_date": "31/12/2023",
        "highest_paid_director": {
            "name": "N/R",
            "remuneration": "332,567"
        },
        "total_director_remuneration": "547,415",
        "currency": "GBP"
    })
    
    print(f"Insert result: {result}")
    
    # Close connection
    mongo.close() 