#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de diagnóstico para verificar la conexión a S3 y listar contenido del bucket
"""

import os
import sys
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError
from dotenv import load_dotenv
import re

# Cargar variables de entorno
load_dotenv()

# Extraer configuración de S3 desde .env
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')
s3_endpoint_url = os.getenv('S3_ENDPOINT_URL')
s3_uri = os.getenv('S3_URI')

# Validar que tenemos la información necesaria
if not aws_access_key_id or not aws_secret_access_key:
    print("❌ ERROR: No se encontraron credenciales AWS. Verifica que AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY estén definidos en .env")
    sys.exit(1)

if not s3_uri:
    print("❌ ERROR: No se encontró S3_URI en .env")
    sys.exit(1)

# Extraer bucket y prefijo de S3_URI
s3_uri_pattern = r"s3://([^/]+)(?:/(.+))?"
match = re.match(s3_uri_pattern, s3_uri)
if not match:
    print(f"❌ ERROR: Formato de S3_URI inválido: {s3_uri}. Debe tener el formato s3://bucket/prefijo")
    sys.exit(1)

bucket_name = match.group(1)
prefix = match.group(2) if match.group(2) else ""

print("\n===== DIAGNÓSTICO DE CONEXIÓN A S3 =====")
print(f"AWS Region: {aws_region}")
print(f"S3 Endpoint URL: {'(predeterminado de AWS)' if not s3_endpoint_url else s3_endpoint_url}")
print(f"Bucket: {bucket_name}")
print(f"Prefijo: {prefix}")
print("========================================\n")

# Intentar crear cliente S3
try:
    print("🔄 Creando cliente S3...")
    
    # Parámetros para el cliente S3
    s3_params = {
        'aws_access_key_id': aws_access_key_id,
        'aws_secret_access_key': aws_secret_access_key,
        'region_name': aws_region
    }
    
    # Añadir endpoint URL solo si está definido
    if s3_endpoint_url:
        s3_params['endpoint_url'] = s3_endpoint_url
    
    s3_client = boto3.client('s3', **s3_params)
    
    # Probar conexión listando buckets
    print("🔄 Probando conexión listando buckets...")
    response = s3_client.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    
    if buckets:
        print(f"✅ Conexión exitosa. Buckets disponibles: {', '.join(buckets)}")
    else:
        print("✅ Conexión exitosa pero no se encontraron buckets.")
    
    # Verificar si el bucket especificado existe
    if bucket_name not in buckets:
        print(f"❌ ADVERTENCIA: El bucket '{bucket_name}' no existe o no es accesible con estas credenciales.")
        print("   Verifica que el nombre del bucket sea correcto y que tengas los permisos adecuados.")
        sys.exit(1)
    
    # Intentar listar objetos en el bucket con el prefijo especificado
    print(f"🔄 Listando objetos en '{bucket_name}/{prefix}'...")
    
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )
        
        # Verificar si hay contenido
        if 'Contents' in response:
            objects = response['Contents']
            print(f"✅ Se encontraron {len(objects)} objetos en el bucket:")
            
            # Mostrar los primeros 10 objetos
            for i, obj in enumerate(objects[:10]):
                print(f"   - {obj['Key']} ({obj['Size']} bytes)")
            
            if len(objects) > 10:
                print(f"   ... y {len(objects) - 10} objetos más.")
        else:
            print(f"ℹ️ No se encontraron objetos con el prefijo '{prefix}' en el bucket '{bucket_name}'.")
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            print(f"❌ ERROR: El bucket '{bucket_name}' no existe.")
        elif error_code == 'AccessDenied':
            print(f"❌ ERROR: Acceso denegado al bucket '{bucket_name}'. Verifica permisos.")
        else:
            print(f"❌ ERROR al listar objetos: {str(e)}")
        sys.exit(1)
        
except NoCredentialsError:
    print("❌ ERROR: Credenciales de AWS inválidas o no disponibles.")
    print("   Verifica que AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY sean correctos.")
    sys.exit(1)
    
except EndpointConnectionError:
    print("❌ ERROR: No se pudo conectar al endpoint S3.")
    print("   Si estás usando un endpoint personalizado, verifica que S3_ENDPOINT_URL sea correcto.")
    print("   Si estás intentando usar AWS S3 estándar, deja S3_ENDPOINT_URL vacío o elimínalo.")
    sys.exit(1)
    
except ClientError as e:
    print(f"❌ ERROR de cliente AWS: {str(e)}")
    error_code = e.response['Error']['Code']
    error_msg = e.response['Error']['Message']
    
    if error_code == 'InvalidAccessKeyId':
        print("   El Access Key ID proporcionado no existe en AWS.")
    elif error_code == 'SignatureDoesNotMatch':
        print("   La Secret Access Key proporcionada no es correcta.")
    
    print(f"   Código de error: {error_code}")
    print(f"   Mensaje: {error_msg}")
    sys.exit(1)
    
except Exception as e:
    print(f"❌ ERROR inesperado: {str(e)}")
    sys.exit(1)

print("\n✅ Diagnóstico completado exitosamente.") 