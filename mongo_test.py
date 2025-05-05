#!/usr/bin/env python
"""
Script de diagnóstico para verificar la conexión a MongoDB y los permisos de escritura.
"""

import os
import sys
import uuid
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

def test_mongodb_connection():
    """Probar la conexión a MongoDB y los permisos de escritura"""
    print("\n🔍 DIAGNÓSTICO DE MONGODB\n" + "="*50)
    
    # Obtener configuración de .env o usar valores por defecto
    mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
    mongo_db_name = os.getenv('MONGO_DB_NAME', 'xbrl_db')
    
    print(f"📌 URI de MongoDB: {mongo_uri}")
    print(f"📌 Base de datos: {mongo_db_name}")
    
    # Intentar conexión
    try:
        client = MongoClient(mongo_uri)
        print("✅ Conexión a MongoDB establecida")
        
        # Verificar que se puede hacer ping
        client.admin.command('ping')
        print("✅ Ping a MongoDB exitoso")
        
        # Acceder a la base de datos
        db = client[mongo_db_name]
        print(f"✅ Base de datos '{mongo_db_name}' accesible")
        
        # Listar colecciones
        collections = db.list_collection_names()
        print(f"📋 Colecciones existentes: {collections}")
        
        # Verificar si la colección results existe
        if 'results' in collections:
            count = db.results.count_documents({})
            print(f"📊 La colección 'results' tiene {count} documentos")
            
            # Comprobar si hay índices que puedan causar problemas
            indexes = list(db.results.list_indexes())
            print(f"🔑 Índices en 'results':")
            for idx in indexes:
                print(f"   - {idx['name']} en {idx['key']}")
        else:
            print("⚠️ La colección 'results' no existe, se creará automáticamente")
        
        # Intentar insertar un documento de prueba
        test_id = str(uuid.uuid4())
        test_doc = {
            "test_id": test_id,
            "company_number": f"TEST-{test_id[:8]}",
            "account_date": datetime.now().strftime("%Y-%m-%d"),
            "company_name": "TEST COMPANY",
            "timestamp": datetime.now().isoformat()
        }
        
        print("\n🧪 Probando inserción de documento...")
        result = db.results.insert_one(test_doc)
        
        if result.inserted_id:
            print(f"✅ Documento insertado exitosamente con ID: {result.inserted_id}")
            
            # Verificar que realmente se insertó
            verification = db.results.find_one({"test_id": test_id})
            if verification:
                print("✅ Verificación exitosa: El documento existe en la base de datos")
                
                # Eliminar el documento de prueba para no dejar basura
                db.results.delete_one({"test_id": test_id})
                print("🧹 Documento de prueba eliminado")
            else:
                print("❌ ERROR: El documento no se encontró después de la inserción")
        else:
            print("❌ ERROR: No se pudo insertar el documento de prueba")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        print(f"   Detalles: {traceback.format_exc()}")
        return False
    
    return True

if __name__ == "__main__":
    success = test_mongodb_connection()
    print("\n" + "="*50)
    if success:
        print("✅ DIAGNÓSTICO COMPLETADO: La conexión a MongoDB funciona correctamente")
        sys.exit(0)
    else:
        print("❌ DIAGNÓSTICO FALLIDO: Hay problemas con la conexión a MongoDB")
        sys.exit(1) 