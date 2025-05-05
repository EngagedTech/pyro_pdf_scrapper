#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para subir archivos Parquet a S3
"""

import os
import sys
import logging
from dotenv import load_dotenv
from s3_manager import S3Manager
import glob

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('upload_parquet')

def main():
    # Cargar variables de entorno
    load_dotenv()
    
    # Obtener la ruta del directorio de archivos Parquet
    parquet_dir = os.getenv('PARQUET_DIR', 'parquet_files')
    
    # Verificar que el directorio existe
    if not os.path.exists(parquet_dir):
        logger.error(f"El directorio {parquet_dir} no existe. Verifica la configuración.")
        sys.exit(1)
    
    # Verificar que hay archivos Parquet
    parquet_files = glob.glob(os.path.join(parquet_dir, "*.parquet"))
    if not parquet_files:
        logger.error(f"No se encontraron archivos .parquet en {parquet_dir}")
        sys.exit(1)
    
    logger.info(f"Se encontraron {len(parquet_files)} archivos Parquet para subir")
    
    try:
        # Inicializar S3Manager
        logger.info("Iniciando conexión con S3...")
        s3_manager = S3Manager()
        
        # Verificar que el bucket existe y es accesible
        if not s3_manager.check_bucket_exists():
            logger.error("El bucket no existe o no es accesible. Verifica la configuración de S3.")
            sys.exit(1)
        
        # Subir archivos
        logger.info(f"Iniciando subida de archivos a S3 desde {parquet_dir}...")
        results = s3_manager.upload_directory(parquet_dir, include_pattern="*.parquet")
        
        # Analizar resultados
        successful = sum(1 for status in results.values() if status)
        failed = len(results) - successful
        
        if successful == 0:
            logger.error("No se pudo subir ningún archivo a S3.")
            sys.exit(1)
        
        logger.info(f"Subida completada: {successful} archivos subidos exitosamente, {failed} fallidos.")
        
        # Mostrar URLs de los archivos subidos (si es posible)
        if successful > 0:
            logger.info("Archivos subidos:")
            for file_name, status in results.items():
                if status:
                    file_key = os.path.basename(file_name)
                    s3_path = f"{s3_manager.prefix}{file_key}"
                    logger.info(f"  - {file_name} → s3://{s3_manager.bucket_name}/{s3_path}")
    
    except Exception as e:
        logger.error(f"Error al subir archivos: {str(e)}")
        sys.exit(1)
    
    logger.info("Proceso de subida finalizado.")

if __name__ == "__main__":
    main() 