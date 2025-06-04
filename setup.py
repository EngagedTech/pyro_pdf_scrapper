import importlib
import logging
import logging.config
import os
import sys

def setup_logging():
    """Configures an advanced logging system with different levels and tags"""
    log_config = {
        'version': 1,
        'formatters': {
            'detailed': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - [%(custom_tag)s] - %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'INFO',
                'formatter': 'detailed',
                'stream': 'ext://sys.stdout'
            },
            'file': {
                'class': 'logging.FileHandler',
                'level': 'DEBUG',
                'formatter': 'detailed',
                'filename': 'xbrl_process.log',
                'mode': 'a'
            }
        },
        'loggers': {
            'scraper': {'level': 'DEBUG', 'handlers': ['console', 'file']},
            'extractor': {'level': 'DEBUG', 'handlers': ['console', 'file']},
            'parser': {'level': 'DEBUG', 'handlers': ['console', 'file']},
            'importer': {'level': 'DEBUG', 'handlers': ['console', 'file']},
            'main': {'level': 'DEBUG', 'handlers': ['console', 'file']}
        },
        'root': {
            'level': 'INFO',
            'handlers': ['console', 'file']
        }
    }
    
    logging.config.dictConfig(log_config)
    
    # Agregar un filtro personalizado para tags, pero con nombre diferente (custom_tag)
    old_factory = logging.getLogRecordFactory()
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        # Inicializar el atributo custom_tag con un valor predeterminado
        record.custom_tag = 'GENERAL'
        return record
    logging.setLogRecordFactory(record_factory)

def check_dependencies():
    """Verifies that all dependencies are installed with the correct version"""
    # First check if the minimum dependencies are installed to start
    try:
        import dotenv
        # python-dotenv está instalado, no necesitamos verificar su versión exacta
    except ImportError:
        print("❌ The python-dotenv dependency is not installed.")
        print("   This is necessary to load environment variables.")
        print("   Install the basic dependencies with: pip install python-dotenv")
        return False
    
    # Una vez que python-dotenv está disponible, podemos cargar config
    try:
        # Ahora importamos config para obtener los parámetros de ejecución
        from config import EXECUTION_STAGE, S3_UPLOAD_ENABLED
        
        # Dependencias requeridas para todas las etapas
        core_dependencies = {
            'requests': '2.28.0',
            'lxml': '4.9.0',
            'pyarrow': '8.0.0',  # Acepta versiones más nuevas
            'pandas': '1.5.0'
            # python-dotenv ya fue verificado arriba
        }
        
        # Dependencias específicas para etapas particulares
        optional_dependencies = {
            'pinecone': '6.0.0',       # Solo necesario para importación a Pinecone
            'boto3': '1.26.0',         # Solo necesario para S3
            'beautifulsoup4': '4.11.0'  # Para web scraping
        }
        
        # Determinar qué dependencias son necesarias según la etapa
        required_dependencies = core_dependencies.copy()
        
        # Para download/scraping necesitamos beautifulsoup4
        if EXECUTION_STAGE in ['download', 'extract', 'parse', 'upload', 'import']:
            required_dependencies['beautifulsoup4'] = optional_dependencies['beautifulsoup4']
        
        # Para upload/S3 necesitamos boto3
        if (EXECUTION_STAGE in ['upload', 'import']) and S3_UPLOAD_ENABLED:
            required_dependencies['boto3'] = optional_dependencies['boto3']
        
        # Para import/Pinecone necesitamos pinecone
        if EXECUTION_STAGE == 'import':
            required_dependencies['pinecone'] = optional_dependencies['pinecone']
    except Exception as e:
        # Si hay algún error al cargar config, usamos solo dependencias básicas
        print(f"⚠️ Could not load configuration: {e}")
        print("   Checking only basic dependencies.")
        
        required_dependencies = {
            'requests': '2.28.0',
            'lxml': '4.9.0',
            'pandas': '1.5.0'
            # python-dotenv ya fue verificado arriba
        }
    
    missing = []
    incompatible = []
    
    for package, min_version in required_dependencies.items():
        try:
            # Caso especial para pinecone
            if package == 'pinecone':
                try:
                    import pinecone
                    # Solo verificamos si se puede importar, no la versión exacta
                    continue
                except ImportError:
                    missing.append(package)
                    continue
            
            # Para los demás paquetes
            module = importlib.import_module(package.replace('-', '_'))
            
            version = getattr(module, '__version__', '0.0.0')
            
            # No comparar versiones para pyarrow si es más reciente
            if package == 'pyarrow':
                try:
                    # Convertir strings de versión a tuples para comparación correcta
                    v1 = tuple(map(int, version.split('.')))
                    v2 = tuple(map(int, min_version.split('.')))
                    if v1 >= v2:
                        continue
                except:
                    # Si hay error en la comparación, asumimos que la versión es correcta
                    continue
            
            # Comparar versiones
            if version < min_version:
                incompatible.append(f"{package} (encontrado: {version}, requerido: {min_version})")
        except ImportError:
            missing.append(package)
    
    # Separar dependencias core de las opcionales
    if 'core_dependencies' in locals():
        core_missing = [pkg for pkg in missing if pkg in core_dependencies]
        core_incompatible = [pkg for pkg in incompatible if pkg.split()[0] in core_dependencies]
    else:
        core_missing = missing
        core_incompatible = incompatible
    
    if missing or incompatible:
        print("⚠️ Problems with dependencies:")
        if missing:
            print(f"  Missing: {', '.join(missing)}")
        if incompatible:
            print(f"  Incompatible versions: {', '.join(incompatible)}")
        
        if core_missing or core_incompatible:
            print("❌ Missing critical dependencies needed for execution.")
            print("   Install all dependencies with: pip install -r requirements.txt")
            return False
        else:
            print("⚠️ Some optional dependencies are not available.")
            print("   The program will continue but some functions may not work correctly.")
    
    return True

def validate_environment(required_stage='all'):
    """
    Validates that critical environment variables are configured
    with valid values before executing the process.
    
    Args:
        required_stage (str): Validation mode:
            - 'all': Validates all variables (Pinecone, S3, Scraping)
            - 'scrape_only': Validates scraping variables only
            - 's3_only': Validates scraping and S3 variables
    
    Returns:
        tuple: (is_valid, missing_vars, invalid_vars)
    """
    from config import (
        PINECONE_API_KEY, INDEX_HOST, S3_URI, 
        SCRAPER_URL, IMPORT_ONLY, SCRAPE_ONLY,
        AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_UPLOAD_ENABLED
    )
    
    missing_vars = []
    invalid_vars = []
    
    # Siempre validamos variables básicas de scraping si no es import_only
    if required_stage in ['all', 'scrape_only', 's3_only'] and not IMPORT_ONLY:
        if not SCRAPER_URL.startswith("http"):
            invalid_vars.append(f"SCRAPER_URL (valor: {SCRAPER_URL})")
    
    # Validación de credenciales S3 (si se requiere upload o importación)
    if required_stage in ['all', 's3_only'] and S3_UPLOAD_ENABLED:
        if not AWS_ACCESS_KEY_ID:
            missing_vars.append("AWS_ACCESS_KEY_ID")
        
        if not AWS_SECRET_ACCESS_KEY:
            missing_vars.append("AWS_SECRET_ACCESS_KEY")
    
    # Validación de variables Pinecone (solo si se requiere importación)
    if required_stage == 'all' and not SCRAPE_ONLY:
        if PINECONE_API_KEY == "your_pinecone_api_key":
            missing_vars.append("PINECONE_API_KEY")
        
        if INDEX_HOST == "your_index_host":
            missing_vars.append("INDEX_HOST")
        
        if S3_URI == "s3://your_bucket/path/to/namespaces/":
            missing_vars.append("S3_URI")
    
    # Verificar que no se hayan activado flags contradictorios
    if SCRAPE_ONLY and IMPORT_ONLY:
        invalid_vars.append("SCRAPE_ONLY and IMPORT_ONLY cannot be active simultaneously")
    
    is_valid = len(missing_vars) == 0 and len(invalid_vars) == 0
    
    return (is_valid, missing_vars, invalid_vars)

def main():
    """Main configuration function"""
    print("⚙️ Configuring the environment...")
    
    # Check dependencies
    if not check_dependencies():
        print("❌ Please install the missing dependencies and restart.")
        sys.exit(1)
    
    # Configurar logging
    setup_logging()
    
    print("✅ Environment configured correctly.")
    
if __name__ == "__main__":
    main()