# import_manager.py

import pinecone
from config import INDEX_HOST, S3_URI, INTEGRATION_ID, PINECONE_API_KEY

def get_pinecone_client():
    """Inicializa el cliente Pinecone con manejo de errores"""
    try:
        return pinecone.Pinecone(api_key=PINECONE_API_KEY)
    except Exception as e:
        print(f"Error al inicializar el cliente Pinecone: {e}")
        return None

def start_import():
    """Inicia una operación de importación masiva de Pinecone"""
    pc = get_pinecone_client()
    if pc is None:
        return None
        
    try:
        index = pc.Index(host=INDEX_HOST)

        response = index.import_from_s3(
            source_uri=S3_URI,
            integration_id=INTEGRATION_ID if INTEGRATION_ID else None,
            error_mode="CONTINUE"
        )
        
        return response["id"]  # El ID de la operación
    except Exception as e:
        print(f"Error al iniciar importación desde S3: {e}")
        return None

def describe_import(operation_id):
    """Obtiene el estado de una operación de importación en curso"""
    if operation_id is None:
        return {"state": "FAILED", "error": "ID de operación no válido"}
        
    pc = get_pinecone_client()
    if pc is None:
        return {"state": "FAILED", "error": "Cliente Pinecone no disponible"}
    
    try:
        # Obtener estado de la operación
        response = pc.import_operations.get(operation_id)
        return response
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
