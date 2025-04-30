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

# Configurar logging
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
                # For MongoDB Atlas or other services that use srv format
                self.client = MongoClient(self.mongo_uri)
            elif username and password:
                # For standalone MongoDB with authentication
                auth_source = os.environ.get("MONGO_AUTH_SOURCE", "admin")
                self.client = MongoClient(
                    self.mongo_uri,
                    username=username,
                    password=password,
                    authSource=auth_source
                )
            else:
                # No authentication
                self.client = MongoClient(self.mongo_uri)
            
            # Ping the database to check connection
            self.client.admin.command('ping')
            
            # Get database
            self.db = self.client[self.mongo_db_name]
            
            # Initialize collections
            self.conversions_collection = self.db["conversions"]
            self.results_collection = self.db["results"]
            
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
            
            return files
            
        except PyMongoError as e:
            logger.error(f"Error retrieving processed files: {e}")
            return []
    
    def fetch_company_info(self, company_number: str, account_date: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene información de la compañía desde la API externa.
        """
        try:
            # Mantener el company_number en su formato original
            company_number = company_number.strip()
            
            # Construir la URL de la API
            url = f"{self.api_base_url}/company/{company_number}"
            
            # Hacer la petición a la API
            response = requests.get(
                url,
                params={"account_date": account_date},
                timeout=30
            )
            
            # Esperar explícitamente a que se descargue todo el contenido
            content = response.content
            
            # Verificar respuesta exitosa
            if response.status_code == 200:
                # Verificar que la respuesta sea un JSON válido
                try:
                    data = response.json()
                except ValueError as e:
                    logger.error(f"Invalid JSON response from API for company {company_number}: {e}")
                    logger.debug(f"Response content: {content[:500]}...")  # Log primeros 500 caracteres
                    return None
                
                # Verificar si la respuesta contiene company_info
                if data.get("success") and "company_info" in data:
                    company_info = data["company_info"]
                    
                    # Normalizar el company_number en la respuesta
                    if "company_number" in company_info:
                        company_info["company_number"] = self._normalize_company_number(company_info["company_number"])
                    
                    logger.info(f"Successfully fetched info for company {company_number}")
                    return company_info
                else:
                    logger.warning(f"API response for company {company_number} does not contain company_info")
                    if "meta" in data:
                        logger.info(f"API response metadata: {data['meta']}")
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
        
        Args:
            company_info (dict): Company information from API
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Verificar que tenemos company_number
            company_number = company_info.get("company_number")
            if not company_number:
                logger.error("Cannot store company info without company_number")
                return False
            
            # Mantener el formato original del company_number
            company_number = company_number.strip()
            company_info["company_number"] = company_number
            
            # Añadir timestamps
            document = {
                **company_info,
                "updated_at": datetime.now()
            }
            
            # Verificar si ya existe un documento con este company_number normalizado
            existing = self.results_collection.find_one({"company_number": company_number})
            
            # Si es un nuevo documento, añadir inserted_at
            if not existing:
                document["inserted_at"] = datetime.now()
            
            # Upsert para actualizar si existe, insertar si no
            result = self.results_collection.update_one(
                {"company_number": company_number},
                {"$set": document},
                upsert=True
            )
            
            if result.upserted_id:
                logger.info(f"Inserted new company data: {company_number}")
            else:
                logger.info(f"Updated existing company data: {company_number}")
            
            return True
            
        except PyMongoError as e:
            logger.error(f"Failed to store company info: {e}")
            return False
    
    def deduplicate_results(self) -> Dict[str, int]:
        """
        Elimina duplicados en la colección results por company_number
        
        Returns:
            dict: Estadísticas de la operación de deduplicación
        """
        stats = {
            "examined": 0,
            "normalized": 0,
            "deleted": 0
        }
        
        try:
            # Obtener todos los documentos de results
            all_results = list(self.results_collection.find({}))
            stats["examined"] = len(all_results)
            logger.info(f"Examining {len(all_results)} documents for duplicates")
            
            # Diccionario para seguir los IDs a mantener por company_number normalizado
            keep_ids = {}
            delete_ids = []
            
            # Primera pasada: identificar duplicados
            for doc in all_results:
                company_number = doc.get("company_number", "")
                if not company_number:
                    continue
                    
                normalized = self._normalize_company_number(company_number)
                
                # Si el número de compañía necesita normalización
                if company_number != normalized:
                    stats["normalized"] += 1
                
                # Si ya tenemos un documento con este company_number normalizado
                if normalized in keep_ids:
                    # Si el documento actual es más reciente, mantenerlo en lugar del anterior
                    existing_doc = next((d for d in all_results if d["_id"] == keep_ids[normalized]), None)
                    
                    if existing_doc and "updated_at" in doc and "updated_at" in existing_doc:
                        if doc["updated_at"] > existing_doc["updated_at"]:
                            delete_ids.append(keep_ids[normalized])
                            keep_ids[normalized] = doc["_id"]
                        else:
                            delete_ids.append(doc["_id"])
                    else:
                        delete_ids.append(doc["_id"])
                else:
                    # Este es el primer documento con este company_number normalizado
                    keep_ids[normalized] = doc["_id"]
            
            # Segunda pasada: eliminar duplicados
            for doc_id in delete_ids:
                self.results_collection.delete_one({"_id": doc_id})
            
            stats["deleted"] = len(delete_ids)
            logger.info(f"Deduplication complete: {stats['deleted']} duplicates removed, {stats['normalized']} numbers normalized")
            
            return stats
            
        except PyMongoError as e:
            logger.error(f"Error during deduplication: {e}")
            return stats
    
    def process_files(self, limit: int = None, max_retries: int = 3) -> Dict[str, int]:
        """
        Process files and update MongoDB
        
        Args:
            limit (int, optional): Maximum number of files to process
            max_retries (int): Maximum number of retries for API requests
            
        Returns:
            dict: Statistics about processing
        """
        # Obtener archivos a procesar
        files = self.get_processed_files(limit)
        
        if not files:
            logger.warning("No processed files found to process")
            return {"total": 0, "success": 0, "failed": 0, "skipped": 0}
        
        # Inicializar estadísticas
        stats = {
            "total": len(files),
            "success": 0,
            "failed": 0,
            "skipped": 0
        }
        
        # Procesar archivos con barra de progreso
        logger.info(f"Starting to process {len(files)} files")
        for file in tqdm(files, desc="Processing files"):
            # Extraer company_number y account_date
            company_number = file.get("company_number", "")
            account_date = file.get("account_date", "")
            
            # Verificar si tenemos datos necesarios
            if not company_number or not account_date:
                logger.warning(f"Skipping file {file.get('filename')}: Missing company_number or account_date")
                stats["skipped"] += 1
                continue
            
            # Formatear account_date si es necesario (de DD/MM/YYYY a YYYY-MM-DD)
            if "/" in account_date:
                try:
                    day, month, year = account_date.split("/")
                    account_date = f"{year}-{month}-{day}"
                except ValueError:
                    logger.warning(f"Invalid account_date format: {account_date}")
            
            # Intentar obtener información de la compañía con reintentos
            company_info = None
            for attempt in range(max_retries):
                # Reemplazar fetch_company_info por query_pinecone_and_summarize
                # Crear query con company_number y account_date
                query = f"company number {company_number} account date {account_date}"
                company_info = query_pinecone_and_summarize(query)
                
                if company_info:
                    break
                
                if attempt < max_retries - 1:
                    logger.info(f"Retrying Pinecone query for {company_number} (attempt {attempt+1}/{max_retries})")
                    time.sleep(self.delay_seconds * (attempt + 1))  # Espera incremental
            
            # Si tenemos información, guardarla en MongoDB
            if company_info:
                success = self.store_company_info(company_info)
                if success:
                    stats["success"] += 1
                else:
                    stats["failed"] += 1
            else:
                logger.warning(f"Could not fetch company info for {company_number} after {max_retries} attempts")
                stats["failed"] += 1
            
            # Pausa entre solicitudes para evitar límites de tasa
            time.sleep(self.delay_seconds)
        
        # Resumen de procesamiento
        logger.info(f"Processing completed: {stats['success']} successful, {stats['failed']} failed, {stats['skipped']} skipped")
        return stats

def main():
    """
    Main function to run as standalone script
    """
    import argparse
    
    # Configurar parser de argumentos
    parser = argparse.ArgumentParser(description='Process XBRL files via API and update MongoDB')
    parser.add_argument('--limit', type=int, default=None, help='Maximum number of files to process')
    parser.add_argument('--batch-size', type=int, default=50, help='Number of records to process in a batch')
    parser.add_argument('--delay', type=float, default=0.5, help='Delay between API requests (seconds)')
    parser.add_argument('--api-url', type=str, help='Base URL for the API endpoint')
    parser.add_argument('--mongo-uri', type=str, help='MongoDB connection string')
    parser.add_argument('--mongo-db', type=str, help='MongoDB database name')
    parser.add_argument('--retries', type=int, default=3, help='Maximum number of retries for API requests')
    parser.add_argument('--deduplicate', action='store_true', help='Run deduplication on results collection before processing')
    
    args = parser.parse_args()
    
    try:
        # Inicializar procesador de API
        processor = APIProcessor(
            api_base_url=args.api_url,
            mongo_uri=args.mongo_uri,
            mongo_db_name=args.mongo_db,
            batch_size=args.batch_size,
            delay_seconds=args.delay
        )
        
        # Ejecutar deduplicación si se solicita
        if args.deduplicate:
            print("Running deduplication on results collection...")
            dedup_stats = processor.deduplicate_results()
            print(f"Deduplication complete: {dedup_stats['deleted']} duplicates removed")
        
        # Procesar archivos
        stats = processor.process_files(limit=args.limit, max_retries=args.retries)
        
        # Cerrar conexión a MongoDB
        processor.close()
        
        # Resumen final
        print(f"\nProcessing Summary:")
        print(f"  Total files: {stats['total']}")
        print(f"  Successfully processed: {stats['success']}")
        print(f"  Failed: {stats['failed']}")
        print(f"  Skipped: {stats['skipped']}")
        if stats['total'] > 0:
            print(f"  Success rate: {stats['success']/stats['total']*100:.2f}%")
        
        # Código de salida basado en éxito/fallo
        if stats['failed'] > 0:
            return 1
        return 0
    
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        return 1
    
if __name__ == "__main__":
    sys.exit(main()) 