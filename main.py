#!/usr/bin/env python
# main.py

import time
import logging
import argparse
import os
import sys

# Intenta importar dependencias mínimas
try:
    from setup import setup_logging, check_dependencies, validate_environment
except ImportError as e:
    print("❌ Error al importar módulos básicos:", e)
    print("   Instala las dependencias mínimas con: pip install python-dotenv")
    sys.exit(1)

# Variables para controlar la disponibilidad de módulos
scraper_available = False
extractor_available = False 
parser_available = False
s3_available = False
pinecone_available = False
mongo_available = False

# Intenta importar módulos que podrían fallar si no están instalados
try:
    from scraper import XBRLScraper
    scraper_available = True
except ImportError:
    pass

try:
    from extractor import ZipExtractor
    extractor_available = True
except ImportError:
    pass
    
try:
    from xbrl_parser import XBRLParser
    parser_available = True
except ImportError:
    pass

# Los scraper_modules son los módulos de scraping, extracción y parseo
scraper_modules_available = scraper_available and extractor_available and parser_available
    
try:
    from import_manager import start_import, describe_import
    pinecone_available = True
except ImportError:
    # Manejo de error para permitir ejecución sin Pinecone configurado
    def start_import():
        return None
    def describe_import(_):
        return {"state": "NOT_CONFIGURED"}

try:
    from s3_manager import S3Manager
    s3_available = True
except ImportError:
    pass

try:
    from mongo_manager import MongoManager
    mongo_available = True
except ImportError:
    pass

try:
    from config import (
        SCRAPER_URL, 
        MAX_FILES_TO_DOWNLOAD, 
        MAX_FILES_TO_PARSE, 
        DOWNLOAD_DIR, 
        EXTRACT_DIR, 
        PARQUET_DIR,
        VECTOR_DIMENSIONS,
        SCRAPE_ONLY,
        IMPORT_ONLY,
        FILTER_YEAR,
        USE_LIMIT,
        SEGMENT_FILES,
        BASE_URL,
        LIST_URL,
        S3_UPLOAD_ENABLED,
        EXECUTION_STAGE
    )
    config_loaded = True
except ImportError as e:
    print(f"⚠️ Error al cargar configuración: {e}")
    config_loaded = False

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='XBRL Scraper and Pinecone Importer')
    
    # Si config está cargada, usamos valores por defecto de allí
    if config_loaded:
        # Cargar valores predeterminados desde config
        parser.add_argument('--scrape-only', action='store_true', default=SCRAPE_ONLY,
                            help='Only scrape and extract XBRL files, without importing to Pinecone')
        parser.add_argument('--import-only', action='store_true', default=IMPORT_ONLY,
                            help='Skip scraping and only import existing Parquet files to Pinecone')
        parser.add_argument('--url', type=str, default=SCRAPER_URL,
                            help='URL to scrape XBRL ZIP files from')
        parser.add_argument('--max-zips', type=int, default=MAX_FILES_TO_DOWNLOAD,
                            help='Maximum number of ZIP files to download')
        parser.add_argument('--max-files', type=int, default=MAX_FILES_TO_PARSE,
                            help='Maximum number of XBRL files to parse from each ZIP')
        parser.add_argument('--year', type=str, default=FILTER_YEAR,
                            help='Only download files from specified year (e.g., 2023)')
        parser.add_argument('--month', type=str, default="",
                            help='Only download files from specified month (e.g., December, Dec, 12)')
        parser.add_argument('--no-limit', action='store_true', default=not USE_LIMIT,
                            help='Do not limit the number of files to parse')
        parser.add_argument('--segment-files', action='store_true', default=SEGMENT_FILES,
                            help='Segment large files into smaller chunks')
        parser.add_argument('--upload-s3', action='store_true', default=S3_UPLOAD_ENABLED,
                            help='Upload Parquet files to S3 before importing to Pinecone')
        parser.add_argument('--use-mongodb', action='store_true', default=True,
                            help='Use MongoDB to track conversions and store results')
        parser.add_argument('--stage', type=str, default=EXECUTION_STAGE, 
                            choices=['download', 'extract', 'parse', 'upload', 'import'],
                            help='Execution stage to reach before stopping')
    else:
        # Sin config, valores por defecto hardcoded
        parser.add_argument('--scrape-only', action='store_true', default=False,
                            help='Only scrape and extract XBRL files, without importing to Pinecone')
        parser.add_argument('--import-only', action='store_true', default=False,
                            help='Skip scraping and only import existing Parquet files to Pinecone')
        parser.add_argument('--url', type=str, 
                            default="https://download.companieshouse.gov.uk/historicmonthlyaccountsdata.html",
                            help='URL to scrape XBRL ZIP files from')
        parser.add_argument('--max-zips', type=int, default=3,
                            help='Maximum number of ZIP files to download')
        parser.add_argument('--max-files', type=int, default=100,
                            help='Maximum number of XBRL files to parse from each ZIP')
        parser.add_argument('--year', type=str, default="",
                            help='Only download files from specified year (e.g., 2023)')
        parser.add_argument('--month', type=str, default="",
                            help='Only download files from specified month (e.g., December, Dec, 12)')
        parser.add_argument('--no-limit', action='store_true', default=False,
                            help='Do not limit the number of files to parse')
        parser.add_argument('--segment-files', action='store_true', default=True,
                            help='Segment large files into smaller chunks')
        parser.add_argument('--upload-s3', action='store_true', default=False,
                            help='Upload Parquet files to S3 before importing to Pinecone')
        parser.add_argument('--use-mongodb', action='store_true', default=True,
                            help='Use MongoDB to track conversions and store results')
        parser.add_argument('--stage', type=str, default="parse", 
                            choices=['download', 'extract', 'parse', 'upload', 'import'],
                            help='Execution stage to reach before stopping')
    
    return parser.parse_args()

def ensure_directories_exist():
    """Asegura que los directorios necesarios existan"""
    directories = []
    
    # Si config está cargado, usamos esos valores
    if config_loaded:
        directories = [DOWNLOAD_DIR, EXTRACT_DIR, PARQUET_DIR]
    else:
        # Valores por defecto
        directories = ["downloads", "extracted", "parquet_files"]
    
    for directory in directories:
        if not os.path.exists(directory):
            try:
                os.makedirs(directory)
                print(f"✅ Directorio creado: {directory}")
            except Exception as e:
                print(f"❌ Error al crear directorio {directory}: {e}")
                return False
    
    return True

def main():
    """Main function to orchestrate the entire process"""
    # Parse arguments primero para saber qué etapa vamos a ejecutar
    args = parse_args()
    stage = args.stage
    
    # Pre-requisitos: verificar dependencias críticas para la etapa solicitada
    if not check_dependencies():
        print("❌ Verifique las dependencias críticas antes de continuar.")
        print("   Instale las dependencias faltantes con: pip install -r requirements.txt")
        return
    
    # Crear directorios necesarios
    if not ensure_directories_exist():
        print("❌ No se pudieron crear los directorios necesarios. Verifique permisos.")
        return
    
    setup_logging()
    logger = logging.getLogger('main')
    
    # Parse arguments
    scrape_url = args.url
    max_zips = args.max_zips
    max_files = None if args.no_limit else args.max_files
    use_segmentation = args.segment_files
    upload_to_s3 = args.upload_s3
    use_mongodb = args.use_mongodb
    
    # Inicializar MongoDB si está disponible y habilitado
    mongo_manager = None
    if mongo_available and use_mongodb:
        try:
            mongo_manager = MongoManager()
            logger.info("MongoDB habilitado para seguimiento y almacenamiento de datos")
        except Exception as e:
            logger.error(f"Error al inicializar MongoDB: {e}")
            logger.warning("Continuando sin MongoDB")
            mongo_manager = None
    elif not mongo_available and use_mongodb:
        logger.warning("MongoDB solicitado pero el módulo no está disponible")
    
    # Verificar si tenemos que validar todas las variables o solo algunas según el flujo
    required_stage = 'all'
    if stage == 'download' or stage == 'extract' or stage == 'parse':
        required_stage = 'scrape_only'
    elif stage == 'upload':
        required_stage = 's3_only'
    
    # Validar solo las variables necesarias para las etapas que vamos a ejecutar
    is_valid, missing_vars, invalid_vars = validate_environment(required_stage=required_stage)
    
    # Para las etapas que no necesitan todas las variables, mostrar advertencias en lugar de errores fatales
    if not is_valid:
        if stage == 'import' and required_stage == 'all':
            print("❌ Configuración inválida para importación a Pinecone:")
            if missing_vars:
                print(f"  Variables obligatorias no configuradas: {', '.join(missing_vars)}")
            if invalid_vars:
                print(f"  Variables con valores incorrectos: {', '.join(invalid_vars)}")
            print("Por favor, revise su archivo .env y vuelva a intentarlo.")
            return
        else:
            # Solo mostrar advertencias para etapas anteriores
            logger.warning("⚠️ Algunas variables no están configuradas correctamente:")
            if missing_vars:
                logger.warning(f"Variables faltantes: {', '.join(missing_vars)}")
            if invalid_vars:
                logger.warning(f"Variables inválidas: {', '.join(invalid_vars)}")
            logger.warning(f"Continuando hasta la etapa '{stage}' con la configuración actual")
    else:
        logger.info("✅ Configuración validada correctamente")
    
    logger.info("Iniciando procesamiento XBRL")
    
    # Log de configuración
    logger.info(f"Configuración: URL={scrape_url}, max_zips={max_zips}, " +
                f"max_files={'Sin límite' if max_files is None else max_files}, " +
                f"year={args.year or 'Todos'}, month={args.month or 'Todos'}, segmentación={'Activada' if use_segmentation else 'Desactivada'}, " +
                f"S3 upload={'Activado' if upload_to_s3 else 'Desactivado'}, " +
                f"MongoDB={'Activado' if mongo_manager else 'Desactivado'}, " +
                f"Etapa final='{stage}'")
    
    # Paso 1-3: Descargar ZIP, extraer XBRL y convertir a Parquet
    parquet_files = []
    
    if not args.import_only:
        logger.info("Iniciando extracción y procesamiento XBRL")
        
        # Step 1: Scrape ZIP files
        if stage in ['download', 'extract', 'parse', 'upload', 'import']:
            logger.info(f"Scraping XBRL ZIP files from {scrape_url}")
            
            # Verificar individualmente los módulos necesarios
            missing_modules = []
            if not scraper_available:
                missing_modules.append("scraper")
            
            if missing_modules:
                logger.error(f"No se puede realizar scraping: faltan módulos {', '.join(missing_modules)}")
                logger.error("Verifica que los archivos del proyecto estén completos")
                return
                
            # Comprobar si beautifulsoup4 está instalado
            try:
                from bs4 import BeautifulSoup
            except ImportError:
                logger.warning("No se puede realizar scraping: la biblioteca beautifulsoup4 no está instalada")
                logger.warning("Instale beautifulsoup4 con: pip install beautifulsoup4")
                return
                
            try:
                scraper = XBRLScraper(
                    base_url=BASE_URL if config_loaded else "https://download.companieshouse.gov.uk/",
                    list_url=scrape_url,
                    download_dir=DOWNLOAD_DIR if config_loaded else "downloads"
                )
                
                # Find all ZIP links
                all_zip_links = scraper.find_zip_links()
                
                # Filter by year if specified
                if args.year:
                    logger.info(f"Filtering for ZIP files from year {args.year}")
                    filtered_links = [link for link in all_zip_links if link['year'] == args.year]
                    logger.info(f"Found {len(filtered_links)} ZIP files for year {args.year}")
                    all_zip_links = filtered_links
                
                # Filter by month if specified
                if args.month:
                    logger.info(f"Filtering for ZIP files from month {args.month}")
                    # Normalizar entrada de mes: convertir a nombre, número o abreviatura
                    month_input = args.month.lower()
                    
                    # Buscar coincidencias tanto en el nombre del mes como en el número
                    filtered_links = []
                    for link in all_zip_links:
                        # Verificar si coincide con el nombre, la abreviatura o el número del mes
                        if (month_input in link['month_name'].lower() or 
                            month_input == link['month'] or 
                            (len(month_input) >= 3 and link['month_name'].lower().startswith(month_input))):
                            filtered_links.append(link)
                    
                    logger.info(f"Found {len(filtered_links)} ZIP files for month {args.month}")
                    all_zip_links = filtered_links
                
                # Download ZIP files
                zip_files = scraper.download_all_zips(urls=all_zip_links, max_files=max_zips, mongo_manager=mongo_manager)
                
                if not zip_files:
                    logger.error("No ZIP files found or downloaded. Exiting.")
                    return
                
                logger.info(f"Downloaded {len(zip_files)} ZIP files")
                
                # Si solo necesitábamos descargar, terminamos aquí
                if stage == 'download':
                    logger.info("Etapa de descarga completada. Finalizando según configuración.")
                    return
            except Exception as e:
                logger.error(f"Error en la etapa de descarga: {e}")
                return
        
        # Step 2: Extract XBRL files from ZIPs
        if stage in ['extract', 'parse', 'upload', 'import']:
            logger.info("Extracting XBRL files from ZIP archives")
            
            # Verificar individualmente los módulos necesarios
            missing_modules = []
            if not extractor_available:
                missing_modules.append("extractor")
            
            if missing_modules:
                logger.error(f"No se puede realizar extracción: faltan módulos {', '.join(missing_modules)}")
                logger.error("Verifica que los archivos del proyecto estén completos")
                return
                
            try:
                extractor = ZipExtractor(extract_dir=EXTRACT_DIR if config_loaded else "extracted")
                
                # Si estamos en modo extract o superior pero no tenemos zip_files de la etapa anterior
                if 'zip_files' not in locals():
                    logger.info("Buscando archivos ZIP existentes...")
                    zip_files = []
                    if os.path.exists(DOWNLOAD_DIR):
                        for file in os.listdir(DOWNLOAD_DIR):
                            if file.endswith('.zip'):
                                zip_files.append(os.path.join(DOWNLOAD_DIR, file))
                    
                    if not zip_files:
                        logger.error("No ZIP files found in downloads directory. Exiting.")
                        return
                    
                    logger.info(f"Found {len(zip_files)} existing ZIP files")
                
                # Extract files with progress reporting
                extracted_files = []
                for i, zip_file in enumerate(zip_files):
                    logger.info(f"Extracting ZIP file {i+1}/{len(zip_files)}: {zip_file}")
                    files_from_zip = extractor.extract_zip(
                        zip_file, 
                        max_files=max_files,
                        use_segmentation=use_segmentation
                    )
                    
                    extracted_files.extend(files_from_zip)
                    logger.info(f"Extracted {len(files_from_zip)} files from {zip_file}")
                
                if not extracted_files:
                    logger.error("No files extracted from ZIP archives. Exiting.")
                    return
                
                logger.info(f"Extracted {len(extracted_files)} XBRL files in total")
                
                # Si solo necesitábamos extraer, terminamos aquí
                if stage == 'extract':
                    logger.info("Etapa de extracción completada. Finalizando según configuración.")
                    return
            except Exception as e:
                logger.error(f"Error en la etapa de extracción: {e}")
                return
        
        # Step 3: Convert XBRL files to Parquet
        if stage in ['parse', 'upload', 'import']:
            logger.info("Converting XBRL files to Parquet format")
            
            # Verificar individualmente los módulos necesarios
            missing_modules = []
            if not parser_available:
                missing_modules.append("xbrl_parser")
            
            if missing_modules:
                logger.error(f"No se puede realizar parseo: faltan módulos {', '.join(missing_modules)}")
                logger.error("Verifica que los archivos del proyecto estén completos")
                return
                
            try:
                parser = XBRLParser(output_dir=PARQUET_DIR if config_loaded else "parquet_files")
                
                # Si estamos en modo parse o superior pero no tenemos extracted_files de la etapa anterior
                if 'extracted_files' not in locals():
                    logger.info("Buscando archivos XBRL existentes...")
                    extracted_files = []
                    if os.path.exists(EXTRACT_DIR):
                        for file in os.listdir(EXTRACT_DIR):
                            if file.lower().endswith(('.html', '.xml')):
                                extracted_files.append(os.path.join(EXTRACT_DIR, file))
                    
                    if not extracted_files:
                        logger.error("No XBRL files found in extracted directory. Exiting.")
                        return
                    
                    logger.info(f"Found {len(extracted_files)} existing XBRL files")
                
                # Process files with progress reporting
                for i, file_path in enumerate(extracted_files):
                    if i % 10 == 0 or i == len(extracted_files) - 1:
                        logger.info(f"Processing file {i+1}/{len(extracted_files)}")
                        
                    if file_path.lower().endswith(('.html', '.xml')):
                        parquet_file = parser.xbrl_to_parquet(file_path, mongo_manager=mongo_manager)
                        if parquet_file:
                            parquet_files.append(parquet_file)
                
                if not parquet_files:
                    logger.error("No Parquet files generated. Exiting.")
                    return
                
                logger.info(f"Generated {len(parquet_files)} Parquet files")
                
                # Si solo necesitábamos convertir a Parquet, terminamos aquí
                if stage == 'parse':
                    logger.info("Etapa de parseo completada. Finalizando según configuración.")
                    return
            except Exception as e:
                logger.error(f"Error en la etapa de parseo: {e}")
                return
    
    # Si estamos en modo import-only, encontrar todos los archivos Parquet existentes
    if (args.import_only or stage in ['upload', 'import']) and not parquet_files:
        logger.info("Buscando archivos Parquet existentes")
        if os.path.exists(PARQUET_DIR):
            for file in os.listdir(PARQUET_DIR):
                if file.endswith('.parquet'):
                    parquet_files.append(os.path.join(PARQUET_DIR, file))
            logger.info(f"Found {len(parquet_files)} existing Parquet files")
        else:
            logger.error(f"Parquet directory {PARQUET_DIR} does not exist")
            return
    
    # Step 4: Upload to S3 if enabled
    if stage in ['upload', 'import'] and upload_to_s3:
        logger.info("Uploading Parquet files to S3")
        
        # Verificar que el módulo de S3 esté disponible
        if not s3_available:
            logger.error("No se puede subir a S3: módulo boto3 no disponible")
            logger.error("Instala las dependencias con: pip install boto3")
            if stage == 'upload':
                logger.info("Finalizando en la etapa de upload a S3 (no completada).")
                return
            else:
                logger.warning("Continuando al siguiente paso sin subir a S3.")
        else:
            try:
                s3_manager = S3Manager()
                
                # Verificar que el bucket sea accesible
                if not s3_manager.check_bucket_exists():
                    logger.error("S3 bucket is not accessible. Check S3 credentials and permissions.")
                    return
                
                # Subir todos los archivos Parquet al S3
                upload_results = s3_manager.upload_directory(PARQUET_DIR)
                
                success_count = sum(1 for status in upload_results.values() if status)
                if success_count == 0:
                    logger.error("Failed to upload any Parquet files to S3. Aborting import.")
                    return
                    
                logger.info(f"Successfully uploaded {success_count} of {len(upload_results)} Parquet files to S3")
                
                # Si solo necesitábamos subir a S3, terminamos aquí
                if stage == 'upload':
                    logger.info("Etapa de subida a S3 completada. Finalizando según configuración.")
                    return
                
            except Exception as e:
                logger.error(f"Error uploading to S3: {e}")
                return
    
    # Step 5: Import Parquet files to Pinecone
    if stage == 'import' and not args.scrape_only:
        logger.info("Starting Pinecone import")
        
        # Verificar que Pinecone esté disponible
        if not pinecone_available:
            logger.error("No se puede importar a Pinecone: módulo pinecone no disponible")
            logger.error("Instala las dependencias con: pip install pinecone>=6.0.0")
            return
            
        try:
            operation_id = start_import()
            
            if operation_id is None:
                logger.error("No se pudo iniciar la importación a Pinecone. Verifica la configuración de Pinecone.")
                return
                
            logger.info(f"Pinecone import started with operation ID: {operation_id}")
            
            # Esperar y reportar estado de importación
            waiting_time = 0
            while True:
                time.sleep(10)
                waiting_time += 10                
                status = describe_import(operation_id)
                state = status.get('state', 'UNKNOWN')
                
                if state in ['RUNNING', 'PENDING']:
                    logger.info(f"Import in progress (waiting {waiting_time}s)... State: {state}")
                elif state == 'COMPLETED':
                    logger.info(f"Import completed successfully! Vectors imported: {status.get('vectors_imported', 'unknown')}")
                    break
                elif state in ['FAILED', 'CANCELLED']:
                    logger.error(f"Import failed or was cancelled. State: {state}, Error: {status.get('error', 'unknown')}")
                    break
                elif state == 'NOT_CONFIGURED':
                    logger.error("Pinecone no está configurado correctamente. Verifica las variables de entorno.")
                    break
                else:
                    logger.warning(f"Unknown import state: {state}")
                    if waiting_time > 300:  # 5 minutos máximo de espera
                        logger.error("Import taking too long, please check Pinecone console for status")
                        break
                    
        except Exception as e:
            logger.error(f"Error starting Pinecone import: {e}")
    
    # Cerrar conexión a MongoDB si está activa
    if mongo_manager:
        mongo_manager.close()
        logger.info("Conexión a MongoDB cerrada")
    
    logger.info("XBRL to Pinecone process completed")

if __name__ == "__main__":
    main()
