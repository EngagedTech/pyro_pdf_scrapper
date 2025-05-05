#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para llamar específicamente a la etapa de subida a S3
"""

import os
import requests
import json
import logging
from dotenv import load_dotenv

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('upload_api_client')

def main():
    # Cargar variables de entorno
    load_dotenv()
    
    # Obtener URL base de la API desde .env o usar el valor predeterminado
    api_base_url = os.getenv('API_BASE_URL', 'http://localhost:3002/api')
    
    # Construir la URL completa del endpoint
    endpoint_url = f"{api_base_url.rstrip('/')}/start-processing"
    
    # Parámetros para forzar SOLO la etapa de subida
    data = {
        "upload_to_s3": True,
        "stage": "upload",
        "import_only": False,
        "scrape_only": False,
        "execution_stage": "upload",
        "parquet_dir": os.getenv('PARQUET_DIR', 'parquet_files'),
        "skip_download": True,
        "skip_extract": True,
        "skip_parse": True
    }
    
    logger.info(f"Llamando al endpoint para SOLO subir a S3: {endpoint_url}")
    logger.info(f"Con los parámetros: {json.dumps(data, indent=2)}")
    
    try:
        # Enviar solicitud POST al endpoint
        response = requests.post(
            endpoint_url,
            json=data,
            headers={"Content-Type": "application/json"}
        )
        
        # Verificar respuesta
        if response.status_code in (200, 201, 202):
            logger.info("Solicitud exitosa!")
            try:
                logger.info(f"Respuesta: {json.dumps(response.json(), indent=2)}")
            except:
                logger.info(f"Respuesta: {response.text}")
        else:
            logger.error(f"Error al llamar al API: {response.status_code}")
            logger.error(f"Detalle: {response.text}")
    
    except requests.exceptions.ConnectionError:
        logger.error(f"No se pudo conectar al servidor en {api_base_url}")
        logger.info("Verifica que el servidor API esté en ejecución y la URL sea correcta.")
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")

if __name__ == "__main__":
    main() 