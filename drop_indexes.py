#!/usr/bin/env python
"""
Script para eliminar índices de la colección results de MongoDB que podrían estar causando problemas.
ADVERTENCIA: Usar con precaución, esto elimina índices que podrían ser necesarios para el rendimiento.
"""

import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

def drop_indexes():
    """Eliminar índices de la colección results"""
    print("\n🔄 ELIMINANDO ÍNDICES DE MONGODB\n" + "="*50)
    
    # Obtener configuración
    mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
    mongo_db_name = os.getenv('MONGO_DB_NAME', 'xbrl_db')
    
    print(f"📌 URI de MongoDB: {mongo_uri}")
    print(f"📌 Base de datos: {mongo_db_name}")
    
    try:
        # Conectar a MongoDB
        client = MongoClient(mongo_uri)
        db = client[mongo_db_name]
        
        # Verificar si la colección exists
        if 'results' not in db.list_collection_names():
            print("❌ La colección 'results' no existe")
            return False
        
        # Listar índices actuales
        print("\n📋 Índices actuales:")
        indexes = list(db.results.list_indexes())
        for idx in indexes:
            print(f"   - {idx['name']} en {idx.get('key')}")
        
        # Eliminar todos los índices excepto _id_
        for idx in indexes:
            if idx['name'] != '_id_':
                print(f"\n🗑️ Eliminando índice: {idx['name']}")
                db.results.drop_index(idx['name'])
                print(f"✅ Índice {idx['name']} eliminado")
        
        # Verificar después de eliminar
        print("\n📋 Índices después de eliminar:")
        remaining_indexes = list(db.results.list_indexes())
        for idx in remaining_indexes:
            print(f"   - {idx['name']} en {idx.get('key')}")
        
        print("\n✅ Operación completada. Solo debe quedar el índice '_id_'")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        print(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = drop_indexes()
    print("\n" + "="*50)
    if success:
        print("✅ ÍNDICES ELIMINADOS: Ahora puede ejecutar nuevamente el procesador")
    else:
        print("❌ ERROR: No se pudieron eliminar todos los índices") 