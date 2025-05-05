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
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError, BulkWriteError
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from query_processor import query_pinecone_and_summarize
import aiohttp

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
            delay_seconds: float = 0.5,
            max_workers: int = 10
        ):
        """
        Initialize API Processor
        
        Args:
            api_base_url (str): Base URL for the API endpoint
            mongo_uri (str): MongoDB connection string
            mongo_db_name (str): MongoDB database name
            batch_size (int): Number of records to process in a batch
            delay_seconds (float): Delay between API requests to avoid rate limiting
            max_workers (int): Maximum number of concurrent workers
        """
        self.api_base_url = api_base_url or os.environ.get("API_BASE_URL", "http://localhost:3002/api")
        self.mongo_uri = mongo_uri or os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
        self.mongo_db_name = mongo_db_name or os.environ.get("MONGO_DB_NAME", "xbrl_db")
        self.batch_size = batch_size
        self.delay_seconds = delay_seconds
        self.max_workers = max_workers
        
        # MongoDB clients
        self.client = None
        self.async_client = None
        self.db = None
        self.conversions_collection = None
        self.results_collection = None
        
        # Cache para resultados
        self._results_cache = {}
        
        # Inicializar conexiones
        self._connect_mongodb()
        self._connect_async_mongodb()
        
        logger.info(f"APIProcessor initialized with {max_workers} workers")

    def _connect_mongodb(self):
        """Conectar a MongoDB y obtener colecciones"""
        try:
            username = os.environ.get("MONGO_USERNAME")
            password = os.environ.get("MONGO_PASSWORD")
            
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
            
            self.client.admin.command('ping')
            self.db = self.client[self.mongo_db_name]
            self.conversions_collection = self.db["conversions"]
            self.results_collection = self.db["results"]
            
            # Crear índices optimizados
            self._create_optimized_indexes()
            
            logger.info(f"Successfully connected to MongoDB: {self.mongo_db_name}")
            
        except PyMongoError as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def _connect_async_mongodb(self):
        """Conectar a MongoDB de forma asíncrona"""
        try:
            self.async_client = AsyncIOMotorClient(self.mongo_uri)
            logger.info("Async MongoDB connection established")
        except Exception as e:
            logger.error(f"Failed to connect to async MongoDB: {e}")
            raise

    def _create_optimized_indexes(self):
        """Crear índices optimizados para mejor rendimiento"""
        try:
            # Índices para la colección results
            self.results_collection.create_index([
                ("company_number", 1),
                ("account_date", -1)
            ], unique=True)
            
            # Índices para la colección conversions
            self.conversions_collection.create_index([
                ("status", 1),
                ("company_number", 1)
            ])
            
            # Índice para búsqueda por fecha
            self.results_collection.create_index([
                ("account_date", -1)
            ])
            
            logger.info("Optimized indexes created successfully")
        except PyMongoError as e:
            logger.error(f"Error creating optimized indexes: {e}")

    async def process_single_file_async(self, session, company_number: str, account_date: str) -> Optional[Dict]:
        """Procesar un archivo individual de forma asíncrona"""
        try:
            # Verificar cache
            cache_key = f"{company_number}:{account_date}"
            if cache_key in self._results_cache:
                return self._results_cache[cache_key]

            # Crear query para Pinecone
            query = f"company number {company_number} account date {account_date}"
            company_info = await self.query_pinecone_async(query)
            
            if company_info and isinstance(company_info, dict):
                # Añade los campos clave si faltan
                company_info['company_number'] = company_number
                company_info['account_date'] = account_date
                # Validación extra: asegurar que no es un dict anidado ni lista
                for k, v in company_info.items():
                    if isinstance(v, (list, dict)) and k not in ["highest_paid_director"]:
                        logger.warning(f"Campo '{k}' es un {type(v)} inesperado en resultado para {company_number}")
                self._results_cache[cache_key] = company_info
                logger.info(f"[process_single_file_async] Resultado para {company_number}: {company_info}")
                
                # INSERTAR INMEDIATAMENTE EN MONGODB
                success = await self._store_result_immediately(company_info)
                if success:
                    logger.info(f"✅ Documento guardado en MongoDB para company_number: {company_number}")
                else:
                    logger.error(f"❌ Error al guardar documento en MongoDB para company_number: {company_number}")
                
                return company_info
            else:
                logger.warning(f"Resultado inesperado/no dict para {company_number}: {company_info}")
            return None
            
        except Exception as e:
            logger.error(f"Error processing file async {company_number}: {e}")
            return None
            
    async def _store_result_immediately(self, result: Dict) -> bool:
        """Almacena un resultado individual en MongoDB inmediatamente"""
        try:
            # ENFOQUE SIMPLE Y DIRECTO: Usar el cliente pymongo síncrono que sí funciona
            # basado en el script de diagnóstico que ya probamos exitosamente
            
            company_number = result.get('company_number')
            account_date = result.get('account_date')
            
            if not company_number or not account_date:
                logger.error(f"Falta company_number o account_date en el resultado: {result}")
                return False
                
            # Asegurar que account_date está en formato YYYY-MM-DD
            if not isinstance(account_date, str) or not account_date.strip() or len(account_date.strip()) < 8:
                logger.error(f"account_date inválido: {account_date}")
                return False
                
            # Normalizar campos
            result['company_number'] = str(company_number).strip()
            result['account_date'] = str(account_date).strip()
            
            # Verificar si ya existe el documento
            existing = self.db.results.find_one({
                'company_number': result['company_number'],
                'account_date': result['account_date']
            })
            
            if existing:
                # Actualizar documento existente
                logger.info(f"🔄 Actualizando documento para company_number={company_number}")
                update_result = self.db.results.update_one(
                    {'_id': existing['_id']},
                    {'$set': result}
                )
                logger.info(f"✅ Documento actualizado con éxito para company_number={company_number}")
                return True
            else:
                # Insertar nuevo documento
                logger.info(f"➕ Insertando nuevo documento para company_number={company_number}")
                insert_result = self.db.results.insert_one(result)
                logger.info(f"✅ Documento insertado con ID={insert_result.inserted_id} para company_number={company_number}")
                return True
                
        except Exception as e:
            import traceback
            logger.error(f"❌ Error al almacenar resultado para company_number={result.get('company_number')}: {e}")
            logger.error(traceback.format_exc())
            return False

    async def process_batch_async(self, batch: List[Dict]) -> List[Dict]:
        """Procesar un lote de archivos de forma asíncrona"""
        async with aiohttp.ClientSession() as session:
            tasks = []
            for file in batch:
                company_number = file.get("company_number")
                account_date = file.get("account_date")
                if company_number and account_date:
                    task = self.process_single_file_async(session, company_number, account_date)
                    tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            processed_results = []
            for r in results:
                if r is not None and not isinstance(r, Exception):
                    processed_results.append(r)
                elif isinstance(r, Exception):
                    logger.error(f"Error en procesamiento: {r}")
            
            return processed_results

    async def process_files_async(self, files: List[Dict], max_retries: int = 3) -> Dict[str, int]:
        """Procesar archivos de forma asíncrona con procesamiento en lotes"""
        stats = {"processed": 0, "failed": 0, "skipped": 0}
        
        try:
            # Procesar en lotes para mejor rendimiento
            batches = [files[i:i + self.batch_size] for i in range(0, len(files), self.batch_size)]
            
            for batch in tqdm(batches, desc="Processing batches"):
                # Procesar batch
                results = await self.process_batch_async(batch)
                
                # Ya no es necesario almacenar en bulk porque cada resultado ya se guardó individualmente
                if results:
                    stats["processed"] += len(results)
                
                # Pequeña pausa para evitar sobrecarga
                await asyncio.sleep(self.delay_seconds)
            
            return stats
            
        except Exception as e:
            logger.error(f"Error in async processing: {e}")
            return stats

    def process_files(self, files: List[Dict], max_retries: int = 3) -> Dict[str, int]:
        """Método principal para procesar archivos"""
        return asyncio.run(self.process_files_async(files, max_retries))

    def close(self):
        """Cerrar conexiones"""
        if self.client:
            self.client.close()
        if self.async_client:
            self.async_client.close()
        logger.info("All MongoDB connections closed")

    def get_processed_files(self, limit: int = None) -> List[Dict[str, Any]]:
        """
        Obtener lista de archivos procesados de MongoDB
        
        Args:
            limit (int, optional): Límite de registros a obtener
            
        Returns:
            list: Lista de documentos procesados
        """
        try:
            # Buscar archivos con estado "converted"
            query = {"status": "converted"}
            
            # Limitar resultados si se especifica
            cursor = self.conversions_collection.find(query)
            if limit:
                cursor = cursor.limit(limit)
            
            # Convertir cursor a lista
            files = list(cursor)
            logger.info(f"Found {len(files)} files with status 'converted'")
            
            # Mostrar algunos detalles de los archivos encontrados
            if files:
                logger.info(f"First file example: {files[0]}")
            else:
                # Si no hay archivos, veamos qué estados existen en la colección
                states = self.conversions_collection.distinct("status")
                total_docs = self.conversions_collection.count_documents({})
                logger.info(f"No files found with status 'converted'. Current states in collection: {states}")
                logger.info(f"Total documents in collection: {total_docs}")
            
            return files
            
        except PyMongoError as e:
            logger.error(f"Error retrieving processed files: {e}")
            return []

    async def query_pinecone_async(self, query: str) -> Optional[Dict]:
        """Consultar Pinecone de forma asíncrona"""
        try:
            # Aquí usamos query_pinecone_and_summarize que ya existe
            # En una futura optimización, podríamos hacer esta función completamente asíncrona
            return query_pinecone_and_summarize(query)
        except Exception as e:
            logger.error(f"Error querying Pinecone async: {e}")
            return None

    def _prepare_upsert_operation(self, result: Dict) -> Optional[UpdateOne]:
        """Prepara una operación de upsert, normalizando campos y validando requisitos"""
        try:
            # Extraer y normalizar campos críticos
            company_number = result.get('company_number')
            
            # Verificar que company_number exista y no sea vacío
            if not company_number or not isinstance(company_number, str) or company_number.strip() == "":
                logger.error(f"Invalid company_number: {company_number}")
                return None
                
            # Normalizar company_number (eliminar espacios, etc.)
            company_number = company_number.strip()
            
            # Priorizar account_date sobre accounts_date (el primero es del archivo original)
            account_date = result.get('account_date')
            
            # Si no hay account_date, intentar usar accounts_date (formato DD/MM/YYYY de OpenAI)
            if not account_date and result.get('accounts_date'):
                accounts_date = result.get('accounts_date')
                logger.info(f"Using accounts_date '{accounts_date}' instead of account_date")
                
                # Convertir de DD/MM/YYYY a YYYY-MM-DD
                try:
                    if isinstance(accounts_date, str) and '/' in accounts_date:
                        day, month, year = accounts_date.split('/')
                        account_date = f"{year}-{month}-{day}"
                        logger.info(f"Converted accounts_date '{accounts_date}' to account_date '{account_date}'")
                except Exception as e:
                    logger.error(f"Failed to convert accounts_date '{accounts_date}': {e}")
            
            # Verificar que account_date exista y tenga formato YYYY-MM-DD
            if not account_date:
                logger.error(f"Missing account_date for company {company_number}")
                return None
            
            # Asegurar que tenga formato YYYY-MM-DD
            import re
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", str(account_date)):
                logger.error(f"account_date '{account_date}' is not in YYYY-MM-DD format for company {company_number}")
                return None
                
            # Crear copia del resultado para no modificar el original
            normalized_result = result.copy()
            
            # Normalizar campos en el resultado
            normalized_result['company_number'] = company_number
            normalized_result['account_date'] = account_date
            
            # Asegurar que ambos dates estén presentes
            if 'accounts_date' not in normalized_result and account_date:
                # Convertir YYYY-MM-DD a DD/MM/YYYY para accounts_date
                try:
                    year, month, day = account_date.split('-')
                    normalized_result['accounts_date'] = f"{day}/{month}/{year}"
                except Exception as e:
                    logger.warning(f"Failed to create accounts_date from account_date '{account_date}': {e}")
                    
            # Log completo para depuración
            logger.info(f"[_prepare_upsert_operation] Prepared upsert for company {company_number}, account_date {account_date}")
            
            # Crear operación de upsert con los campos normalizados
            return UpdateOne(
                {
                    'company_number': company_number,
                    'account_date': account_date
                },
                {'$set': normalized_result},
                upsert=True
            )
            
        except Exception as e:
            logger.error(f"Error preparing upsert operation: {e}")
            return None

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
        
        # Obtener archivos a procesar
        files = processor.get_processed_files(limit=args.limit)
        
        if not files:
            logger.warning("No processed files found to process")
            return 0
            
        # Procesar archivos
        stats = processor.process_files(files=files, max_retries=args.retries)
        
        # Cerrar conexión a MongoDB
        processor.close()
        
        # Resumen final
        print(f"\nProcessing Summary:")
        print(f"  Total files processed: {stats['processed']}")
        print(f"  Failed: {stats['failed']}")
        print(f"  Skipped: {stats['skipped']}")
        if stats['processed'] + stats['failed'] + stats['skipped'] > 0:
            success_rate = stats['processed'] / (stats['processed'] + stats['failed'] + stats['skipped']) * 100
            print(f"  Success rate: {success_rate:.2f}%")
        
        # Código de salida basado en éxito/fallo
        if stats['failed'] > 0:
            return 1
        return 0
        
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main()) 