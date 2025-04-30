# XBRL a Pinecone Pipeline

Este proyecto automatiza el proceso de extraer datos XBRL del Registro Mercantil, procesarlos e importarlos a una base de datos vectorial Pinecone.

## Características

- Rastrea [Companies House historic monthly accounts data](https://download.companieshouse.gov.uk/historicmonthlyaccountsdata.html) en busca de archivos ZIP que contengan XBRL
- Extrae archivos XBRL de archivos ZIP
- Analiza formatos XBRL (.html) y XBRL (.xml) en línea.
- Convierte los datos XBRL a formato Parquet para Pinecone.
- Sube archivos Parquet a S3 (opcional).
- Importa los datos a Pinecone usando la API de importación masiva.

## Requisitos

- Python 3.7+
- Cuenta Pinecone con plan Standard
- Bucket S3 para almacenar archivos Parquet (opcional si se utilizan archivos locales)

## Instalación

```bash
# Clonar el repositorio
git clone https://github.com/yourusername/xbrl-to-pinecone.git
cd xbrl-to-pinecone

# Instalar dependencias
pip install -r requirements.txt
```

## Configuración

Cree un archivo `.env` en el directorio raíz con lo siguiente:

```
# Configuración de Pinecone
PINECONE_API_KEY=tu_clave_api_pinecone
INDEX_HOST=tu_index_host
S3_URI=s3://tu_bucket/ruta/
INTEGRATION_ID=tu_id_integración

# Configuración de S3 (opcional - solo si deseas subir archivos a S3)
AWS_ACCESS_KEY_ID=tu_access_key_id
AWS_SECRET_ACCESS_KEY=tu_secret_access_key
AWS_REGION=tu_region
S3_ENDPOINT_URL=https://s3.amazonaws.com  # Opcional: para S3-compatible endpoints
S3_UPLOAD_ENABLED=true

# Configuración de Scraper
SCRAPER_URL=https://download.companieshouse.gov.uk/historicmonthlyaccountsdata.html
BASE_URL=https://download.companieshouse.gov.uk/
MAX_FILES_TO_DOWNLOAD=3  # Limitar a 3 archivos ZIP para evitar descargar grandes cantidades de datos
MAX_FILES_TO_PARSE=100  # Límite de 100 archivos XBRL por ZIP
FILTER_YEAR=  # Dejar vacío para todos los años, o especificar un año (p.ej. "2023")

# Rutas de archivos
DOWNLOAD_DIR=downloads
EXTRACT_DIR=extracted
PARQUET_DIR=parquet_files

# Vector dimensions
VECTOR_DIMENSIONS=1536  # Dimensión para OpenAI embeddings

# Variables para query_processor.py
PINECONE_FIELDS=company_number,company_name,company_legal_type,accounts_date,highest_paid_director.name,highest_paid_director.remuneration,total_director_remuneration,currency
BATCH_SIZE=50
OPENAI_MAX_TOKENS_PER_BATCH=4000
OPENAI_REQUEST_TIMEOUT=60
```

## Uso

Ejecutar el script principal:

```bash
python main.py
```

Opciones disponibles:

```
python main.py --help
```

Ejemplos comunes:

```bash
# Solo descargar y procesar XBRL sin importar a Pinecone
python main.py --scrape-only

# Solo importar a Pinecone (usar archivos Parquet existentes)
python main.py --import-only

# Filtrar por año
python main.py --year 2023

# Limitar cantidad de archivos
python main.py --max-zips 5 --max-files 100

# Habilitar subida a S3 
python main.py --upload-s3

# Sin límite de archivos a procesar
python main.py --no-limit
```

## Estructura de directorios

- `/downloads`: Contiene los archivos ZIP descargados
- `/extracted`: Contiene los archivos XBRL extraídos
- `/parquet_files`: Contiene los archivos Parquet generados
- `/logs`: Contiene logs del proceso (creado automáticamente)

## Flujo de procesamiento

1. Verifica los requisitos (dependencias, variables de entorno)
2. Descarga archivos ZIP con datos XBRL
3. Extrae archivos XBRL de los ZIP
4. Convierte archivos XBRL a formato Parquet
5. (Opcional) Sube archivos Parquet a S3
6. Importa datos a Pinecone usando la API de importación masiva

## Nuevo flujo con Pinecone y OpenAI

El proyecto ahora incluye un nuevo módulo `query_processor.py` que permite consultar documentos en Pinecone y extraer información estructurada mediante OpenAI.

### Cómo utilizar el nuevo flujo

El procesador API (`api_processor.py`) ahora utiliza internamente esta funcionalidad para consultar información de empresas, reemplazando las consultas a APIs externas:

```python
from query_processor import query_pinecone_and_summarize

# Generar una consulta
query = f"company number {company_number} account date {account_date}"

# Ejecutar la consulta y obtener la información estructurada
company_info = query_pinecone_and_summarize(query)
```

Este flujo optimizado para producción incluye:
- Reintento automático con backoff exponencial
- Control preciso de tokens con tiktoken
- Procesamiento por lotes
- Validación de configuración con Pydantic
- Registro detallado de errores y eventos