"""
Module for extracting ZIP files containing XBRL data
"""

import os
import zipfile
import logging
import time
import gc
import tempfile
import shutil
import psutil
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ZipExtractor:
    def __init__(self, extract_dir="extracted", memory_threshold=70, batch_size=500, pause_seconds=2):
        """
        Initialize the extractor
        
        Args:
            extract_dir (str): Directory to extract files to
            memory_threshold (int): Memory usage percentage threshold to trigger memory management
            batch_size (int): Number of files to extract in a batch before checking memory
            pause_seconds (int): Seconds to pause between batches when memory usage is high
        """
        self.extract_dir = extract_dir
        self.memory_threshold = memory_threshold
        self.batch_size = batch_size
        self.pause_seconds = pause_seconds
        
        # Create extraction directory if it doesn't exist
        if not os.path.exists(extract_dir):
            os.makedirs(extract_dir)
            
    def _get_memory_usage(self):
        """Get current memory usage as percentage"""
        return psutil.virtual_memory().percent
        
    def _memory_management(self, force=False):
        """
        Perform memory management if usage is above threshold
        
        Args:
            force (bool): Force memory management regardless of threshold
            
        Returns:
            bool: True if memory management was performed
        """
        memory_usage = self._get_memory_usage()
        
        if memory_usage > self.memory_threshold or force:
            logger.info(f"Current memory usage: {memory_usage}% - Performing memory management")
            
            # Force garbage collection
            gc.collect()
            
            # If still high, pause to let system free memory
            if self._get_memory_usage() > self.memory_threshold:
                logger.info(f"Memory still high, pausing for {self.pause_seconds} seconds")
                time.sleep(self.pause_seconds)
                
            return True
        return False
    
    def _check_disk_space(self, zip_path):
        """
        Verificar si hay suficiente espacio en disco antes de extraer
        
        Args:
            zip_path (str): Ruta al archivo ZIP para estimar espacio necesario
            
        Returns:
            bool: True si hay suficiente espacio, False en caso contrario
        """
        try:
            # Estimar espacio necesario (asumimos factor de compresión de 3x)
            zip_size = os.path.getsize(zip_path)
            estimated_extracted_size = zip_size * 3
            
            # Obtener espacio libre en disco
            disk_usage = shutil.disk_usage(self.extract_dir)
            free_space = disk_usage.free
            
            # Verificar si hay suficiente espacio
            if free_space < estimated_extracted_size:
                logger.error(f"Insuficiente espacio en disco. Necesario: {estimated_extracted_size/(1024*1024):.2f} MB, " 
                             f"Disponible: {free_space/(1024*1024):.2f} MB")
                return False
                
            return True
        except Exception as e:
            logger.warning(f"Error al verificar espacio en disco: {e}")
            return True  # En caso de error, continuamos pero con advertencia
    
    def extract_zip(self, zip_path, specific_extensions=None, max_files=None, 
                    use_segmentation=False, max_files_per_dir=1000):
        """
        Extract a ZIP file with memory management
        
        Args:
            zip_path (str): Path to the ZIP file
            specific_extensions (list, optional): Only extract files with these extensions.
                                                 Defaults to ['.html', '.xml'].
            max_files (int, optional): Maximum number of files to extract. None means no limit.
            use_segmentation (bool): Whether to segment files into subdirectories.
            max_files_per_dir (int): Maximum number of files per subdirectory when segmentation is used.
        
        Returns:
            list: List of paths to extracted files
        """
        if specific_extensions is None:
            specific_extensions = ['.html', '.xml']
        
        # Verificar que el archivo ZIP existe
        if not os.path.exists(zip_path):
            logger.error(f"El archivo ZIP no existe: {zip_path}")
            return []
            
        # Verificar que el archivo ZIP tiene un tamaño razonable
        try:
            zip_size_mb = os.path.getsize(zip_path) / (1024 * 1024)
            if zip_size_mb < 0.1:  # Menos de 100KB
                logger.error(f"El archivo ZIP {zip_path} parece estar vacío o corrupto (tamaño: {zip_size_mb:.2f} MB)")
                return []
        except Exception as e:
            logger.error(f"Error al verificar tamaño del ZIP {zip_path}: {e}")
            return []
            
        # Verificar espacio en disco
        if not self._check_disk_space(zip_path):
            return []
            
        # Create a subdirectory for this specific zip file
        zip_name = os.path.basename(zip_path).replace('.zip', '')
        extract_subdir = os.path.join(self.extract_dir, zip_name)
        
        if not os.path.exists(extract_subdir):
            os.makedirs(extract_subdir)
            
        extracted_files = []
        
        try:
            logger.info(f"Extracting {zip_path} to {extract_subdir}")
            logger.info(f"ZIP file size: {zip_size_mb:.2f} MB")
            
            # Verificar que el archivo ZIP no está corrupto
            try:
                with zipfile.ZipFile(zip_path, 'r') as test_zip:
                    if test_zip.testzip() is not None:
                        logger.error(f"El archivo ZIP {zip_path} está corrupto")
                        return []
            except zipfile.BadZipFile:
                logger.error(f"El archivo {zip_path} no es un archivo ZIP válido")
                return []
            except Exception as e:
                logger.error(f"Error al validar archivo ZIP {zip_path}: {e}")
                return []
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Get list of file names in the archive
                try:
                    file_list = zip_ref.namelist()
                except Exception as e:
                    logger.error(f"Error al leer contenido del ZIP {zip_path}: {e}")
                    return []
                
                # Log total files in ZIP
                logger.info(f"ZIP contains {len(file_list)} files in total")
                
                # Filter by extensions if specified
                if specific_extensions:
                    file_list = [f for f in file_list if any(f.lower().endswith(ext) for ext in specific_extensions)]
                    logger.info(f"After filtering by extensions, {len(file_list)} files remain")
                
                # Apply max_files limit if specified
                if max_files is not None:
                    file_list = file_list[:max_files]
                    logger.info(f"Limiting extraction to {max_files} files")
                
                total_files = len(file_list)
                
                # Process in batches
                segment_counter = 0
                current_segment = 0
                
                # Create a temporary directory for extraction
                with tempfile.TemporaryDirectory() as temp_dir:
                    temp_dir_path = Path(temp_dir)
                    logger.info(f"Using temporary directory for initial extraction: {temp_dir}")
                    
                    for i, file in enumerate(file_list):
                        try:
                            # Check if we need to create a new segment directory
                            if use_segmentation and segment_counter == 0:
                                current_segment += 1
                                segment_dir = os.path.join(extract_subdir, f"segment_{current_segment}")
                                if not os.path.exists(segment_dir):
                                    os.makedirs(segment_dir)
                                    logger.info(f"Created segment directory: {segment_dir}")
                            
                            # Extract to temporary directory first
                            temp_file_path = temp_dir_path / file
                            zip_ref.extract(file, temp_dir)
                            
                            # Verificar que el archivo extraído existe y tiene tamaño > 0
                            if not os.path.exists(temp_file_path) or os.path.getsize(temp_file_path) == 0:
                                logger.warning(f"Archivo extraído vacío o no existe: {file}")
                                continue
                            
                            # Move to final destination
                            if use_segmentation:
                                final_path = os.path.join(segment_dir, os.path.basename(file))
                                os.makedirs(os.path.dirname(final_path), exist_ok=True)
                                shutil.move(temp_file_path, final_path)
                                extracted_files.append(final_path)
                                segment_counter = (segment_counter + 1) % max_files_per_dir
                            else:
                                final_path = os.path.join(extract_subdir, file)
                                os.makedirs(os.path.dirname(final_path), exist_ok=True)
                                shutil.move(temp_file_path, final_path)
                                extracted_files.append(final_path)
                        except Exception as e:
                            logger.warning(f"Error al extraer archivo {file}: {e}")
                            continue
                        
                        # Log progress periodically and check memory
                        if (i + 1) % self.batch_size == 0 or i == total_files - 1:
                            progress = (i + 1) / total_files * 100
                            memory_usage = self._get_memory_usage()
                            logger.info(f"Extracted {i+1}/{total_files} files ({progress:.1f}%) - Memory usage: {memory_usage}%")
                            
                            # Memory management
                            self._memory_management()
                
                # Final memory cleanup
                self._memory_management(force=True)
                    
            logger.info(f"Extracted {len(extracted_files)} files from {zip_path}")
            if use_segmentation:
                logger.info(f"Files were segmented into {current_segment} directories")
            
            return extracted_files
            
        except zipfile.BadZipFile:
            logger.error(f"El archivo {zip_path} no es un archivo ZIP válido")
            self._memory_management(force=True)
            return []
        except Exception as e:
            logger.error(f"Error extracting {zip_path}: {e}")
            # If we encounter an error, try to free memory before continuing
            self._memory_management(force=True)
            return []
    
    def extract_all_zips(self, zip_paths, max_parallel_extractions=1):
        """
        Extract multiple ZIP files with memory management
        
        Args:
            zip_paths (list): List of paths to ZIP files
            max_parallel_extractions (int): Maximum number of extractions to run in parallel
                                           (Currently only sequential extraction is supported)
            
        Returns:
            list: List of paths to all extracted files
        """
        all_extracted_files = []
        
        logger.info(f"Starting extraction of {len(zip_paths)} ZIP files")
        
        for i, zip_path in enumerate(zip_paths):
            logger.info(f"Extracting ZIP file {i+1}/{len(zip_paths)}: {zip_path}")
            
            # Full memory management between ZIP files
            self._memory_management(force=True)
            
            # Extract this ZIP
            extracted_files = self.extract_zip(zip_path)
            all_extracted_files.extend(extracted_files)
            
            logger.info(f"Completed extraction of ZIP {i+1}/{len(zip_paths)}")
            
        return all_extracted_files

# Si la función validate_environment está en el archivo, voy a eliminarla
def validate_environment():
    """
    Valida que las variables de entorno críticas estén configuradas 
    con valores válidos antes de ejecutar el proceso.
    
    Returns:
        tuple: (is_valid, missing_vars, invalid_vars)
    """
    from config import (
        PINECONE_API_KEY, INDEX_HOST, S3_URI, 
        SCRAPER_URL, IMPORT_ONLY, SCRAPE_ONLY
    )
    
    missing_vars = []
    invalid_vars = []
    
    # Validación de variables Pinecone (solo si no estamos en modo scrape-only)
    if not SCRAPE_ONLY:
        if PINECONE_API_KEY == "your_pinecone_api_key":
            missing_vars.append("PINECONE_API_KEY")
        
        if INDEX_HOST == "your_index_host":
            missing_vars.append("INDEX_HOST")
        
        if S3_URI == "s3://your_bucket/path/to/namespaces/":
            missing_vars.append("S3_URI")
    
    # Validación de URL de scraping (solo si no estamos en modo import-only)
    if not IMPORT_ONLY:
        if not SCRAPER_URL.startswith("http"):
            invalid_vars.append(f"SCRAPER_URL (valor: {SCRAPER_URL})")
    
    # Verificar que no se hayan activado flags contradictorios
    if SCRAPE_ONLY and IMPORT_ONLY:
        invalid_vars.append("SCRAPE_ONLY y IMPORT_ONLY no pueden estar activos simultáneamente")
    
    is_valid = len(missing_vars) == 0 and len(invalid_vars) == 0
    
    return (is_valid, missing_vars, invalid_vars) 