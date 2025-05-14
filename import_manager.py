# import_manager.py

from pinecone import Pinecone, ImportErrorMode
from config import INDEX_HOST, S3_URI, INTEGRATION_ID, PINECONE_API_KEY

def get_pinecone_client():
    """Inicializa el cliente Pinecone con manejo de errores"""
    try:
        return Pinecone(api_key=PINECONE_API_KEY)
    except Exception as e:
        print(f"Error al inicializar el cliente Pinecone: {e}")
        return None

def start_import():
    """Inicia una operación de importación masiva de Pinecone usando el nuevo SDK"""
    pc = get_pinecone_client()
    if pc is None:
        return None
        
    try:
        index = pc.Index(host=INDEX_HOST)
        # Limpiar espacios accidentales
        s3_uri_clean = S3_URI.strip()
        integration_id_clean = INTEGRATION_ID.strip() if INTEGRATION_ID else None
        response = index.start_import(
            uri=s3_uri_clean,
            error_mode=ImportErrorMode.CONTINUE,  # O .ABORT si prefieres abortar en error
            integration_id=integration_id_clean
        )
        return response["id"]  # El ID de la operación
    except Exception as e:
        print(f"Error al iniciar importación desde S3: {e}")
        return None

def describe_import(operation_id):
    """Obtiene el estado de una operación de importación en curso"""
    pc = get_pinecone_client()
    if pc is None:
        return {"state": "FAILED", "error": "Cliente Pinecone no disponible"}
    try:
        index = pc.Index(host=INDEX_HOST)
        return index.describe_import(id=operation_id)
    except Exception as e:
        print(f"Error al consultar estado de importación: {e}")
        return {"state": "FAILED", "error": str(e)}

def list_imports():
    """Lista todas las importaciones existentes"""
    pc = get_pinecone_client()
    if pc is None:
        return []
        
    try:
        index = pc.Index(host=INDEX_HOST)
        return list(index.list_imports())
    except Exception as e:
        print(f"Error al listar importaciones: {e}")
        return []

def cancel_import(operation_id):
    """Cancela una operación de importación en curso"""
    if operation_id is None:
        return False
        
    pc = get_pinecone_client()
    if pc is None:
        return False
        
    try:
        index = pc.Index(host=INDEX_HOST)
        index.cancel_import(id=operation_id)
        return True
    except Exception as e:
        print(f"Error al cancelar importación: {e}")
        return False
