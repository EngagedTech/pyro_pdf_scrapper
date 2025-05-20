from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Optional
import uvicorn
import asyncio
from download_service import DownloadService
from prometheus_client import start_http_server
import logging
from dotenv import load_dotenv
import os
import subprocess
from contextlib import asynccontextmanager
import socket
from xbrl_worker import XBRLProcessor

# Cargar variables de entorno
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicializar servicios
download_service = DownloadService()
xbrl_processor = XBRLProcessor()

class CachePattern(BaseModel):
    pattern: Optional[str] = None

def is_port_in_use(port: int) -> bool:
    """Check if a port is in use"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

def find_free_port(start_port: int) -> int:
    """Find a free port starting from start_port"""
    port = start_port
    while is_port_in_use(port):
        port += 1
    return port

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manejador del ciclo de vida de la aplicación
    """
    try:
        # Encontrar un puerto libre para Prometheus
        prometheus_port = find_free_port(9090)
        try:
            start_http_server(prometheus_port)
            logger.info(f"Started Prometheus metrics server on port {prometheus_port}")
        except Exception as e:
            logger.warning(f"Could not start Prometheus server: {e}")
            logger.warning("Continuing without metrics collection")
        
        # Inicializar servicios
        await download_service.initialize()
        logger.info("Services initialized successfully")
        
        yield  # La aplicación se ejecuta aquí
        
    finally:
        # Limpieza al cerrar
        await download_service.close()
        logger.info("Services shutdown completed")

app = FastAPI(
    title="XBRL Processing API",
    lifespan=lifespan
)

@app.post("/start-processing")
async def start_processing() -> Dict:
    """
    Endpoint para iniciar el proceso completo de descarga y procesamiento
    """
    try:
        # Iniciar el proceso de descarga y extracción asíncrona
        results = await download_service.start_processing()
        
        if results["status"] == "success" and results["files_extracted"] > 0:
            # Solo intentar ejecutar process_results.py si existe
            process_results_path = os.path.join(os.path.dirname(__file__), "process_results.py")
            if os.path.exists(process_results_path):
                try:
                    logger.info("Iniciando procesamiento posterior...")
                    # Usar python3 explícitamente y la ruta completa de Python
                    python_path = "/usr/bin/python3"  # Ruta estándar en macOS
                    if not os.path.exists(python_path):
                        python_path = "python3"  # Fallback al PATH del sistema
                    
                    subprocess.Popen([python_path, process_results_path])
                    logger.info("Proceso posterior iniciado correctamente")
                except Exception as e:
                    logger.warning(f"No se pudo iniciar el procesamiento posterior: {e}")
            else:
                logger.info("process_results.py no encontrado - continuando sin procesamiento posterior")
            
            return {
                "status": "success",
                "message": "Procesamiento iniciado correctamente",
                "zips_processed": results["zips_processed"],
                "files_extracted": results["files_extracted"],
                "files": results["extracted_files"],
                "details": "Archivos procesados y guardados correctamente"
            }
        else:
            return {
                "status": "error",
                "message": "No se pudieron procesar los archivos",
                "details": results
            }
        
    except Exception as e:
        logger.error(f"Error en el procesamiento: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/clear-cache")
async def clear_cache(cache_pattern: CachePattern = None) -> Dict:
    """
    Endpoint para limpiar la caché de Redis
    
    Args:
        cache_pattern: Patrón opcional para limpiar caché específica
    """
    try:
        pattern = cache_pattern.pattern if cache_pattern else None
        xbrl_processor.clear_cache(pattern)
        return {
            "status": "success",
            "message": f"Cache cleared successfully{f' for pattern: {pattern}' if pattern else ''}"
        }
    except Exception as e:
        logger.error(f"Error clearing cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/clear-file-cache/{file_path:path}")
async def clear_file_cache(file_path: str) -> Dict:
    """
    Endpoint para limpiar la caché de un archivo específico
    
    Args:
        file_path: Ruta del archivo para limpiar su caché
    """
    try:
        xbrl_processor.clear_file_cache(file_path)
        return {
            "status": "success",
            "message": f"Cache cleared for file: {file_path}"
        }
    except Exception as e:
        logger.error(f"Error clearing file cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    # Encontrar un puerto libre para la API
    api_port = find_free_port(8080)
    logger.info(f"Starting API server on port {api_port}")
    uvicorn.run(app, host="0.0.0.0", port=api_port) 