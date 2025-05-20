"""
Module for scraping and downloading ZIP files containing XBRL data from Companies House
"""

import os
import requests
from bs4 import BeautifulSoup
import logging
from urllib.parse import urljoin
import re
from typing import Dict, Any
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class XBRLScraper:
    def __init__(self, 
                 base_url=None, 
                 list_url=None, 
                 download_dir="downloads"):
        """
        Initialize the scraper
        
        Args:
            base_url (str): Base URL of the API endpoint
            list_url (str): URL containing the list of files
            download_dir (str): Directory to save downloaded files
        """
        # Usar valores del .env por defecto
        self.base_url = base_url or os.getenv('BASE_URL', 'https://download.companieshouse.gov.uk/')
        self.list_url = list_url or os.getenv('LIST_URL', 'https://download.companieshouse.gov.uk/en_accountsdata.html')
        self.download_dir = download_dir
        
        # Log de configuración
        logger.info(f"XBRLScraper initialized with BASE_URL: {self.base_url}")
        logger.info(f"XBRLScraper initialized with LIST_URL: {self.list_url}")
        
        # Create download directory if it doesn't exist
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
    
    def find_zip_links(self):
        """
        Find all ZIP file links on the historical data page
        
        Returns:
            list: List of ZIP file URLs
        """
        try:
            logger.info(f"Scraping ZIP links from {self.list_url}")
            response = requests.get(self.list_url, timeout=30)  # Agregamos timeout
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all 'a' tags that might contain ZIP links
            all_links = soup.find_all('a') 
            
            # Intenta selectores más específicos si el anterior no funciona
            zip_links = []
            found_links = False
            
            # Método 1: buscar enlaces dentro de lis
            li_links = soup.select('li > a')
            if li_links:
                found_links = True
                logger.info(f"Encontrados {len(li_links)} enlaces en elementos li")
                all_links = li_links
            
            # Si no se encuentran enlaces con el primer método, buscar en toda la página
            if not found_links:
                logger.info("No se encontraron enlaces en elementos li, buscando todos los enlaces")
            
            # Filter for ZIP files
            for link in all_links:
                href = link.get('href', '')
                if href and href.lower().endswith('.zip'):
                    # Check if it matches the Companies House accounts data pattern
                    if any(pattern in href.lower() for pattern in ['accounts', 'data', 'xbrl']):
                        # Manejar correctamente la URL de descarga
                        if href.startswith('http'):
                            # Ya es una URL absoluta
                            full_url = href
                        elif href.startswith('/'):
                            # Es una ruta absoluta desde la raíz del dominio
                            full_url = urljoin(self.base_url, href)
                        else:
                            # No anteponer 'archive/' si no corresponde
                                full_url = urljoin(self.base_url, href)
                        
                        # Extract year and month if possible for better logging
                        year_month = self._extract_year_month(href)
                        
                        # Intentar obtener información de tamaño si está disponible
                        size = "unknown"
                        if link.parent and hasattr(link.parent, 'get_text'):
                            parent_text = link.parent.get_text().strip()
                            size_match = re.search(r'\((.*?)\)', parent_text)
                            if size_match:
                                size = size_match.group(1)
                        
                        zip_links.append({
                            'url': full_url,
                            'year_month': year_month,
                            'size': size
                        })
            
            logger.info(f"Found {len(zip_links)} ZIP files")
            
            # Si no se encuentran enlaces, puede ser un problema con la página
            if not zip_links:
                logger.warning("No se encontraron enlaces ZIP. Verifica la estructura de la página y la URL.")
                logger.debug(f"Contenido de la página: {response.text[:500]}...")  # Log primeros 500 caracteres
                
                # Intentar buscar cualquier enlace para depuración
                all_hrefs = [a.get('href', '') for a in soup.find_all('a')]
                logger.debug(f"Todos los hrefs encontrados: {all_hrefs[:10]}...")
            
            # Ordenar por año y mes (del más reciente al más antiguo)
            zip_links.sort(key=lambda x: x['year_month'], reverse=True)
            
            # FILTRO TEMPORAL: solo dejar el enlace 'Accounts_Monthly_Data-od22.zip'
            filtered_zip_links = [link for link in zip_links if 'Accounts_Monthly_Data-od22.zip' in link['url']]
            if filtered_zip_links:
                logger.info("Filtro temporal activo: solo se tomará 'Accounts_Monthly_Data-od22.zip'")
                zip_links = filtered_zip_links
            else:
                logger.warning("No se encontró el enlace 'Accounts_Monthly_Data-od22.zip' entre los resultados.")

            for zip_link in zip_links:
                logger.info(f"Found ZIP: {zip_link['year_month']} - {zip_link['size']} - {zip_link['url']}")
                
            return [link['url'] for link in zip_links]
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Error de red al buscar enlaces ZIP: {e}")
            return []
        except Exception as e:
            logger.error(f"Error inesperado al buscar enlaces ZIP: {e}")
            return []
    
    def _extract_year_month(self, href):
        """Extract year and month from filename if possible"""
        # Try to match patterns like 'January2020' or 'Jan2020'
        match = re.search(r'(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)[-_]?(\d{4})', href, re.IGNORECASE)
        if match:
            return match.group(0)
        return os.path.basename(href)
    
    def download_file(self, url, filename=None, max_retries=3, mongo_manager=None):
        """
        Download a file from a URL
        
        Args:
            url (str): URL to download
            filename (str, optional): Filename to save as. Defaults to the last part of the URL.
            max_retries (int): Número máximo de intentos de descarga
            mongo_manager (MongoManager, optional): MongoDB manager for tracking downloads
            
        Returns:
            str: Path to downloaded file or None if download failed
        """
        if filename is None:
            filename = url.split('/')[-1]
            
        filepath = os.path.join(self.download_dir, filename)
        
        # Register download in MongoDB if manager is provided
        if mongo_manager:
            mongo_manager.register_zip_download(filename)
        
        # Check if file already exists
        if os.path.exists(filepath):
            try:
                file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
                # Verificar si el archivo no está vacío o corrupto
                if file_size_mb > 0.1:  # Más de 100 KB
                    logger.info(f"File {filepath} already exists ({file_size_mb:.2f} MB). Skipping download.")
                    
                    # Update MongoDB status if manager is provided
                    if mongo_manager:
                        mongo_manager.update_zip_status(filename, "downloaded")
                        
                    return filepath
                else:
                    logger.warning(f"File {filepath} exists but is too small ({file_size_mb:.2f} MB). Re-downloading.")
                    os.remove(filepath)
            except Exception as e:
                logger.error(f"Error checking existing file {filepath}: {e}")
                # Intentar eliminar archivo corrupto
                try:
                    os.remove(filepath)
                except:
                    pass
        
        # Intentar descarga con reintentos
        for attempt in range(max_retries):
            try:
                logger.info(f"Downloading {url} to {filepath} (attempt {attempt+1}/{max_retries})")
                
                # Stream download with progress reporting for large files
                response = requests.get(url, stream=True, timeout=60)  # 60 segundos de timeout
                response.raise_for_status()
                
                # Get file size if available
                total_size = int(response.headers.get('content-length', 0))
                total_size_mb = total_size / (1024 * 1024)
                
                logger.info(f"File size: {total_size_mb:.2f} MB")
                
                # Crear una versión temporal para evitar archivos corruptos
                temp_filepath = filepath + ".tmp"
                
                downloaded = 0
                with open(temp_filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            
                            # Log progress for large files
                            if total_size > 0 and downloaded % (100 * 1024 * 1024) < 8192:  # Log every ~100MB
                                percent = (downloaded / total_size) * 100
                                downloaded_mb = downloaded / (1024 * 1024)
                                logger.info(f"Downloaded {downloaded_mb:.2f} MB of {total_size_mb:.2f} MB ({percent:.1f}%)")
                
                # Verificar tamaño del archivo
                if total_size > 0 and total_size > downloaded:
                    raise Exception(f"Download incomplete: expected {total_size} bytes but got {downloaded} bytes")
                
                # Mover archivo temporal a destino final
                if os.path.exists(temp_filepath):
                    os.rename(temp_filepath, filepath)
                    
                logger.info(f"Downloaded {filepath}")
                
                # Update MongoDB status if manager is provided
                if mongo_manager:
                    mongo_manager.update_zip_status(filename, "downloaded")
                    
                return filepath
            
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout downloading {url} (attempt {attempt+1}/{max_retries})")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to download {url} after {max_retries} attempts due to timeout")
                    
                    # Update MongoDB status if manager is provided
                    if mongo_manager:
                        mongo_manager.update_zip_status(filename, "error")
                        
                    return None
            
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error downloading {url}: {e}")
                
                # Update MongoDB status if manager is provided
                if mongo_manager:
                    mongo_manager.update_zip_status(filename, "error")
                    
                return None
                
            except Exception as e:
                logger.error(f"Error downloading {url} (attempt {attempt+1}/{max_retries}): {e}")
                # Limpiar archivos temporales en caso de error
                if os.path.exists(temp_filepath):
                    try:
                        os.remove(temp_filepath)
                    except:
                        pass
                        
                if attempt == max_retries - 1:
                    logger.error(f"Failed to download {url} after {max_retries} attempts")
                    
                    # Update MongoDB status if manager is provided
                    if mongo_manager:
                        mongo_manager.update_zip_status(filename, "error")
                        
                    return None
                    
                # Esperar un poco antes del siguiente intento
                import time
                time.sleep(2 * (attempt + 1))  # Espera incremental: 2s, 4s, 6s, etc.
        
        return None
    
    def download_all_zips(self, urls=None, max_files=None, mongo_manager=None):
        """
        Download all ZIP files from a list of URLs
        
        Args:
            urls (list, optional): List of URLs to download. If None, find_zip_links is called.
            max_files (int, optional): Maximum number of files to download (newest first)
            mongo_manager (MongoManager, optional): MongoDB manager for tracking downloads
            
        Returns:
            list: List of paths to downloaded files
        """
        if urls is None:
            urls = self.find_zip_links()
        
        # Limit number of files to download if specified
        if max_files and max_files > 0:
            urls = urls[:max_files]
            logger.info(f"Limiting download to {max_files} files")
            
        downloaded_files = []
        for i, url in enumerate(urls):
            logger.info(f"Downloading file {i+1}/{len(urls)}")
            filepath = self.download_file(url, mongo_manager=mongo_manager)
            if filepath:
                downloaded_files.append(filepath)
                
        return downloaded_files

    def parse_html_xbrl(self, file_path: str) -> Dict[str, Any]:
        """
        Parse inline XBRL from HTML file
        
        Args:
            file_path (str): Path to HTML file
            
        Returns:
            dict: Extracted XBRL data
        """
        extra_log = {'extra_tag': 'PARSER_HTML'}
        try:
            self.logger.info(f"Parsing HTML XBRL file: {file_path}", extra=extra_log)
            
            with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                content = file.read()
            
            soup = BeautifulSoup(content, 'lxml')
            
            # Extract basic document information
            document_info = {
                'file_path': file_path,
                'file_name': os.path.basename(file_path),
                'doc_type': 'html_xbrl',
            }
            
            # Extract all text content (not just facts)
            all_text = soup.get_text(" ", strip=True)
            document_info['full_text'] = all_text[:32000]  # Limit text length to avoid huge strings
            document_info['text_length'] = len(all_text)
            
            # Continue with normal XBRL fact extraction...
            facts = []
            
            # Try multiple tag patterns for inline XBRL
            xbrl_tag_patterns = [
                ['ix:nonfraction', 'ix:fraction', 'ix:nonnumeric'],  # Standard ix tags
                ['ix:nonFraction', 'ix:fraction', 'ix:nonNumeric'],  # Alternative capitalization
                ['nonfraction', 'fraction', 'nonnumeric'],  # Without namespace
                ['nonFraction', 'fraction', 'nonNumeric']   # Without namespace, alternative capitalization
            ]
            
            for pattern in xbrl_tag_patterns:
                xbrl_tags = soup.find_all(pattern)
                if xbrl_tags:
                    break
            
            # If no tags found with standard patterns, look for data- attributes which often indicate XBRL facts
            if not xbrl_tags:
                xbrl_tags = soup.find_all(lambda tag: any(attr.startswith('data-xbrl') for attr in tag.attrs))
            
            for tag in xbrl_tags:
                fact = {
                    'name': tag.get('name', ''),
                    'value': tag.text.strip(),
                    'context_ref': tag.get('contextref', '') or tag.get('contextRef', ''),
                    'unit_ref': tag.get('unitref', '') or tag.get('unitRef', ''),
                    'decimals': tag.get('decimals', ''),
                    'format': tag.get('format', ''),
                }
                facts.append(fact)
            
            # Extract metadata from HTML headers
            meta_tags = soup.find_all('meta')
            metadata = {}
            for tag in meta_tags:
                name = tag.get('name', '')
                content = tag.get('content', '')
                if name and content:
                    metadata[name] = content
            
            document_info['metadata'] = metadata
            
            # Rest of the function remains the same...
        except Exception as e:
            self.logger.error(f"Error parsing HTML XBRL file: {e}", extra=extra_log)
            return {} 