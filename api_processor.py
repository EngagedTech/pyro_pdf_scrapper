#!/usr/bin/env python
"""
API Processor for XBRL data extraction
This module fetches processed files from MongoDB, extracts company information via API,
and stores the results in MongoDB.
"""

import os
import sys
import time
import requests
import logging
from tqdm import tqdm
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from typing import Dict, List, Any, Optional
from query_processor import query_pinecone_and_summarize

# Cargar variables de entorno
load_dotenv()

# Configurar logging según el contexto de ejecución
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('api_processor.log')
    ]
)
logger = logging.getLogger('api_processor')

class APIProcessor:
    """
    Class for processing company data via API based on MongoDB records
    """
    def __init__(
            self, 
            api_base_url: str = None,
            mongo_uri: str = None,
            mongo_db_name: str = None,
            batch_size: int = 50,
            delay_seconds: float = 0.5
        ):
        """
        Initialize API Processor
        
        Args:
            api_base_url (str): Base URL for the API endpoint
            mongo_uri (str): MongoDB connection string
            mongo_db_name (str): MongoDB database name
            batch_size (int): Number of records to process in a batch
            delay_seconds (float): Delay between API requests to avoid rate limiting
        """
        # Obtener configuración de variables de entorno o valores por defecto
        self.api_base_url = api_base_url or os.environ.get("API_BASE_URL", "http://localhost:3002/api")
        self.mongo_uri = mongo_uri or os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
        self.mongo_db_name = mongo_db_name or os.environ.get("MONGO_DB_NAME", "xbrl_db")
        self.batch_size = batch_size
        self.delay_seconds = delay_seconds
        
        # MongoDB client y colecciones
        self.client = None
        self.db = None
        self.conversions_collection = None
        self.results_collection = None
        
        # Inicializar conexión a MongoDB
        self._connect_mongodb()
        
        logger.info(f"APIProcessor initialized with API URL: {self.api_base_url}")

    def _connect_mongodb(self):
        """Conectar a MongoDB y obtener colecciones"""
        try:
            # Get authentication credentials from environment
            username = os.environ.get("MONGO_USERNAME")
            password = os.environ.get("MONGO_PASSWORD")
            
            # Create a MongoDB client with authentication if credentials are provided
            if username and password and 'mongodb+srv' in self.mongo_uri:
                self.client = MongoClient(self.mongo_uri)
            elif username and password:
                auth_source = os.environ.get("MONGO_AUTH_SOURCE", "admin")
                self.client = MongoClient(
                    self.mongo_uri,
                    username=username,
                    password=password,
                    authSource=auth_source
                )
            else:
                self.client = MongoClient(self.mongo_uri)
            
            # Ping the database to check connection
            self.client.admin.command('ping')
            
            # Get database
            self.db = self.client[self.mongo_db_name]
            
            # Initialize collections
            self.conversions_collection = self.db["conversions"]
            self.results_collection = self.db["results"]
            
            # Crear índices únicos para evitar duplicados
            self.results_collection.create_index(
                [("company_number", 1)],
                unique=True,
                background=True
            )
            self.results_collection.create_index(
                [
                    ("company_number", 1),
                    ("accounts_date.zip_name", 1),
                    ("accounts_date.date", 1)
                ],
                unique=True,
                background=True,
                partialFilterExpression={
                    "accounts_date": {"$exists": True}
                }
            )
            
            logger.info(f"Successfully connected to MongoDB: {self.mongo_db_name}")
            
        except PyMongoError as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def close(self):
        """Cerrar conexión a MongoDB"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
    
    def _normalize_company_number(self, company_number: str) -> str:
        """
        Mantiene el formato original del número de compañía
        
        Args:
            company_number (str): Número de compañía a procesar
            
        Returns:
            str: Número de compañía en su formato original
        """
        if not company_number:
            return company_number
        
        # Mantener el formato original, incluyendo ceros a la izquierda
        return company_number.strip()

    def get_processed_files(self, limit: int = None) -> List[Dict[str, Any]]:
        """
        Obtener lista de archivos procesados de MongoDB
        
        Args:
            limit (int, optional): Límite de registros a obtener
            
        Returns:
            list: Lista de documentos procesados
        """
        try:
            # Buscar todos los archivos con estado "completed"
            query = {"status": "completed"}
            
            # Limitar resultados si se especifica
            cursor = self.conversions_collection.find(query)
            
            if limit:
                cursor = cursor.limit(limit)
            
            # Convertir cursor a lista
            files = list(cursor)
            logger.info(f"Found {len(files)} processed files")
            
            # Normalizar company_number para cada archivo
            for file in files:
                if 'company_number' in file:
                    file['company_number'] = self._normalize_company_number(file['company_number'])
                # Asegura que zip_name esté presente (puede estar vacío si no se propagó antes)
                file['zip_name'] = file.get('zip_name', '')
            
            return files
            
        except PyMongoError as e:
            logger.error(f"Error retrieving processed files: {e}")
            return []

    def fetch_company_info(self, company_number: str, account_date: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene información de la compañía desde la API externa.
        """
        def normalize_api_response(data):
            """
            Normaliza la respuesta de la API para extraer la información de la empresa,
            soportando tanto el formato antiguo (company_info) como el nuevo (estructura directa).
            Siempre retorna accounts_date como lista de objetos planos, con 'date' como string.
            """
            # Caso 1: Formato antiguo
            if data.get("success") and "company_info" in data:
                return data["company_info"]
            # Caso 2: Formato directo (sin company_info)
            if "company_name" in data and "company_number" in data:
                accounts_date = data.get("accounts_date")
                flat_accounts = []
                if isinstance(accounts_date, list) and len(accounts_date) > 0:
                    for acc in accounts_date:
                        # Si acc['date'] es lista, aplanar
                        if isinstance(acc.get("date"), list):
                            for d in acc["date"]:
                                merged = {**acc, **d}
                                merged.pop("date", None)  # Evita recursión
                                # Fuerza date a string plano, usando d['date'] si existe
                                date_value = d.get("date", "") if isinstance(d, dict) else str(d)
                                if isinstance(date_value, (list, dict)):
                                    date_value = str(date_value)
                                flat_accounts.append({
                                    "zip_name": merged.get("zip_name", ""),
                                    "date": date_value,
                                    "company_legal_type": merged.get("company_legal_type", ""),
                                    "currency": merged.get("currency", ""),
                                    "total_director_remuneration": merged.get("total_director_remuneration", ""),
                                    "highest_paid_director": merged.get("highest_paid_director", ""),
                                    "inserted_at": merged.get("inserted_at", datetime.now())
                                })
                        else:
                            date_value = acc.get("date", "")
                            if isinstance(date_value, (list, dict)):
                                date_value = str(date_value)
                            flat_accounts.append({
                                "zip_name": acc.get("zip_name", ""),
                                "date": date_value,
                                "company_legal_type": acc.get("company_legal_type", ""),
                                "currency": acc.get("currency", ""),
                                "total_director_remuneration": acc.get("total_director_remuneration", ""),
                                "highest_paid_director": acc.get("highest_paid_director", ""),
                                "inserted_at": acc.get("inserted_at", datetime.now())
                            })
                    data["accounts_date"] = flat_accounts
                return {
                    "company_name": data.get("company_name", ""),
                    "company_number": data.get("company_number", ""),
                    "accounts_date": data.get("accounts_date", [])
                }
            # Si no cumple, retorna None
            return None
        try:
            # Mantener el company_number en su formato original
            company_number = company_number.strip()
            # Construir la URL de la API
            url = f"{self.api_base_url}/company/extract/"
            # Hacer la petición a la API
            response = requests.get(
                url,
                params={"company_number": company_number, "account_date": account_date},
                timeout=30
            )
            # Esperar explícitamente a que se descargue todo el contenido
            content = response.content
            # Verificar respuesta exitosa
            if response.status_code == 200:
                # Verificar que la respuesta sea un JSON válido
                try:
                    data = response.json()
                    print(f"[DEBUG] API raw response for {company_number}: {data}")
                    logger.info(f"[DEBUG] API raw response for {company_number}: {data}")
                except ValueError as e:
                    logger.error(f"Invalid JSON response from API for company {company_number}: {e}")
                    logger.debug(f"Response content: {content[:500]}...")  # Log primeros 500 caracteres
                    return None
                # Normaliza la respuesta
                company_info = normalize_api_response(data)
                if company_info:
                    # Normalizar el company_number en la respuesta
                    if "company_number" in company_info:
                        company_info["company_number"] = self._normalize_company_number(company_info["company_number"])
                    logger.info(f"Successfully fetched info for company {company_number}")
                    return company_info
                else:
                    logger.warning(f"API response for company {company_number} does not contain recognizable company info")
                    logger.debug(f"Full API response: {data}")
                    return None
            else:
                logger.warning(f"API request failed with status {response.status_code}: {response.text}")
                return None
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout while fetching company info for {company_number}: {e}")
            return None
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error while fetching company info for {company_number}: {e}")
            return None
        except requests.RequestException as e:
            logger.error(f"Error fetching company info from API: {e}")
            return None
        except ValueError as e:
            logger.error(f"Error parsing API response: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during API request for company {company_number}: {e}", exc_info=True)
            return None

    def store_company_info(self, company_info: Dict[str, Any]) -> bool:
        """
        Store company information in MongoDB results collection
        """
        try:
            company_number = company_info.get("company_number", "").strip()
            if not company_number:
                logger.error("Cannot store company info without company_number")
                return False

            # Normalizar estructura una sola vez
            normalized_doc = {
                "company_number": company_number,
                "company_name": company_info.get("company_name", ""),
                "accounts_date": []
            }

            # Procesar accounts_date
            accounts = company_info.get("accounts_date", [])
            if not isinstance(accounts, list):
                accounts = [accounts]

            current_time = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

            for acc in accounts:
                if not isinstance(acc, dict):
                    continue
                
                # Normalizar fecha a formato ISO
                date = acc.get("date", "")
                if date and "/" in date:
                    try:
                        day, month, year = date.split("/")
                        date = f"{year}-{month}-{day}"
                    except ValueError:
                        logger.warning(f"Invalid date format: {date}")
                
                entry = {
                    "zip_name": acc.get("zip_name", ""),
                    "date": date,
                    "company_legal_type": acc.get("company_legal_type", ""),
                    "currency": acc.get("currency", "GBP"),
                    "total_director_remuneration": acc.get("total_director_remuneration", ""),
                    "highest_paid_director": str(acc.get("highest_paid_director", "")),
                    "inserted_at": current_time
                }
                normalized_doc["accounts_date"].append(entry)

            # Actualizar o insertar usando upsert
            self.results_collection.update_one(
                {"company_number": company_number},
                {
                    "$set": {
                        "company_name": normalized_doc["company_name"],
                        "updated_at": current_time
                    },
                    "$addToSet": {
                        "accounts_date": {
                            "$each": normalized_doc["accounts_date"]
                        }
                    }
                },
                upsert=True
            )

            return True
            
        except PyMongoError as e:
            logger.error(f"Failed to store company info: {e}")
            return False

    def process_files(self, limit: int = None, max_retries: int = 3, source: str = "pinecone") -> Dict[str, int]:
        """Process files and update MongoDB"""
        files = self.get_processed_files(limit)
        if not files:
            logger.warning("No processed files found to process")
            return {"total": 0, "success": 0, "failed": 0}

        stats = {"total": len(files), "success": 0, "failed": 0}
        
        for file in tqdm(files, desc="Processing files"):
            company_number = file.get("company_number", "")
            account_date = file.get("account_date", "")
            zip_name = file.get("zip_name", "")  # Obtener el zip_name del archivo
            print(f"[DEBUG] Processing zip name: {zip_name}")
            
            if not company_number or not account_date:
                logger.warning(f"Skipping file: Missing company_number or account_date")
                continue

            # Obtener información (con reintentos)
            company_info = None
            for attempt in range(max_retries):
                try:
                    if source == "pinecone":
                        query = f"company number {company_number} account date {account_date}"
                        company_info = query_pinecone_and_summarize(query)
                        
                        # Si no hay zip_name en la respuesta, forzarlo
                        if company_info:
                            if not isinstance(company_info.get("accounts_date"), list):
                                company_info["accounts_date"] = [company_info.get("accounts_date", {})]
                            
                            for acc in company_info["accounts_date"]:
                                if isinstance(acc, dict):
                                    # Siempre usar el zip_name del archivo original
                                    acc["zip_name"] = zip_name
                    else:
                        company_info = self.fetch_company_info(company_number, account_date)
                        if company_info:
                            if not isinstance(company_info.get("accounts_date"), list):
                                company_info["accounts_date"] = [company_info.get("accounts_date", {})]
                            
                            for acc in company_info["accounts_date"]:
                                if isinstance(acc, dict):
                                    acc["zip_name"] = zip_name
                    
                    if company_info:
                        break
                except Exception as e:
                    logger.error(f"Error in attempt {attempt + 1}: {str(e)}")
                    if attempt < max_retries - 1:
                        time.sleep(self.delay_seconds * (attempt + 1))

            if company_info and self.store_company_info(company_info):
                stats["success"] += 1
            else:
                stats["failed"] += 1
                logger.warning(f"Failed to process company {company_number}")

            time.sleep(self.delay_seconds)

        logger.info(f"Processing completed: {stats['success']} successful, {stats['failed']} failed")
        return stats

def main():
    """Main function to run as standalone script"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Process XBRL files via API and update MongoDB')
    parser.add_argument('--limit', type=int, default=None, help='Maximum number of files to process')
    parser.add_argument('--batch-size', type=int, default=50, help='Number of records to process in a batch')
    parser.add_argument('--delay', type=float, default=0.5, help='Delay between API requests (seconds)')
    parser.add_argument('--api-url', type=str, help='Base URL for the API endpoint')
    parser.add_argument('--mongo-uri', type=str, help='MongoDB connection string')
    parser.add_argument('--mongo-db', type=str, help='MongoDB database name')
    parser.add_argument('--retries', type=int, default=3, help='Maximum number of retries for API requests')
    parser.add_argument(
        '--source',
        type=str,
        choices=['pinecone', 'api'],
        default='pinecone',
        help='Fuente de datos: "pinecone" (default) o "api" para usar la API externa'
    )
    
    args = parser.parse_args()
    
    try:
        processor = APIProcessor(
            api_base_url=args.api_url,
            mongo_uri=args.mongo_uri,
            mongo_db_name=args.mongo_db,
            batch_size=args.batch_size,
            delay_seconds=args.delay
        )
        
        stats = processor.process_files(
            limit=args.limit,
            max_retries=args.retries,
            source=args.source
        )
        
        processor.close()
        
        print(f"\nProcessing Summary:")
        print(f"  Total files: {stats['total']}")
        print(f"  Successfully processed: {stats['success']}")
        print(f"  Failed: {stats['failed']}")
        
        return 1 if stats['failed'] > 0 else 0
        
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main()) 