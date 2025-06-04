"""
Module for scraping and downloading ZIP files containing Companies House data
"""

import os
import requests
from bs4 import BeautifulSoup
import logging
from urllib.parse import urljoin
import re
from typing import Dict, Any, Optional, List, TypedDict
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ZipLinkInfo(TypedDict):
    """Type definition for ZIP link information"""
    url: str
    year_month: str
    size: str

class DownloadError(Exception):
    """Custom exception for download errors"""
    def __init__(self, message: str, original_error: Exception = None):
        self.message = message
        self.original_error = original_error
        super().__init__(message)

class XBRLScraper:
    def __init__(self, 
                 base_url: Optional[str] = None, 
                 list_url: Optional[str] = None, 
                 download_dir: str = "downloads"):
        """
        Initialize the scraper
        
        Args:
            base_url (str): Base URL of the API endpoint
            list_url (str): URL containing the list of files
            download_dir (str): Directory to save downloaded files
        """
        # Use default values from .env
        self.base_url = base_url or os.getenv('BASE_URL', 'https://download.companieshouse.gov.uk/')
        self.list_url = list_url or os.getenv('LIST_URL', 'https://download.companieshouse.gov.uk/en_accountsdata.html')
        self.download_dir = download_dir
        
        # Log configuration
        logger.info(f"XBRLScraper initialized with BASE_URL: {self.base_url}")
        logger.info(f"XBRLScraper initialized with LIST_URL: {self.list_url}")
        
        # Create download directory if it doesn't exist
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
    
    def find_zip_links(self) -> List[str]:
        """
        Find all ZIP file links on the historical data page
        
        Returns:
            list: List of ZIP file URLs
        """
        try:
            logger.info(f"Scraping ZIP links from {self.list_url}")
            response = requests.get(self.list_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all 'a' tags that might contain ZIP links
            all_links = soup.find_all('a') 
            
            # Try more specific selectors if needed
            zip_links: List[ZipLinkInfo] = []
            found_links = False
            
            # Method 1: look for links inside li elements
            li_links = soup.select('li > a')
            if li_links:
                found_links = True
                logger.info(f"Found {len(li_links)} links in li elements")
                all_links = li_links
            
            # If no links found with first method, search whole page
            if not found_links:
                logger.info("No links found in li elements, searching all links")
            
            # Filter for ZIP files
            for link in all_links:
                href = link.get('href', '')
                if href and href.lower().endswith('.zip'):
                    # Check if it matches the Companies House accounts data pattern
                    if any(pattern in href.lower() for pattern in ['accounts', 'data', 'xbrl']):
                        # Handle download URL correctly
                        if href.startswith('http'):
                            # Already absolute URL
                            full_url = href
                        elif href.startswith('/'):
                            # Absolute path from domain root
                            full_url = urljoin(self.base_url, href)
                        else:
                            # Relative path
                                full_url = urljoin(self.base_url, href)
                        
                        # Extract year and month if possible for better logging
                        year_month = self._extract_year_month(href)
                        
                        # Try to get size information if available
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
            
            # If no links found, might be an issue with the page
            if not zip_links:
                logger.warning("No ZIP links found. Verify page structure and URL.")
                logger.debug(f"Page content: {response.text[:500]}...")  # Log first 500 chars
                
                # Try to find any links for debugging
                all_hrefs = [a.get('href', '') for a in soup.find_all('a')]
                logger.debug(f"All hrefs found: {all_hrefs[:10]}...")
            
            # Sort by year and month (newest first)
            zip_links.sort(key=lambda x: x['year_month'], reverse=True)
            
            # TEMPORARY FILTER: only keep 'Accounts_Monthly_Data-od22.zip'
            filtered_zip_links = [link for link in zip_links if 'Accounts_Monthly_Data-od22.zip' in link['url']]
            if filtered_zip_links:
                logger.info("Temporary filter active: only using 'Accounts_Monthly_Data-od22.zip'")
                zip_links = filtered_zip_links
            else:
                logger.warning("Link 'Accounts_Monthly_Data-od22.zip' not found in results.")

            for zip_link in zip_links:
                logger.info(f"Found ZIP: {zip_link['year_month']} - {zip_link['size']} - {zip_link['url']}")
                
            return [link['url'] for link in zip_links]
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error finding ZIP links: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error finding ZIP links: {e}")
            return []
    
    def _extract_year_month(self, href: str) -> str:
        """
        Extract year and month from filename if possible
        
        Args:
            href (str): URL or filename to extract from
            
        Returns:
            str: Extracted year/month or basename of href
        """
        # Try to match patterns like 'January2020' or 'Jan2020'
        match = re.search(r'(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)[-_]?(\d{4})', href, re.IGNORECASE)
        if match:
            return match.group(0)
        return os.path.basename(href)
    
    def _extract_date_from_filename(self, filename: str) -> Optional[str]:
        """
        Extract date from filename in YYYY-MM format
        
        Args:
            filename (str): Filename to extract date from
            
        Returns:
            str|None: Date in YYYY-MM format or None if not found
        """
        # Try to match YYYY-MM pattern in filename
        match = re.search(r'(\d{4})-(\d{2})', filename)
        if match:
            return f"{match.group(1)}-{match.group(2)}"
            
        # Try to match MonthYYYY pattern
        match = re.search(r'(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)[-_]?(\d{4})', filename, re.IGNORECASE)
        if match:
            # Convert month name to number
            month_map = {
                'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
                'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
                'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
            }
            month = match.group(0)[:3].lower()
            year = match.group(1)
            if month in month_map:
                return f"{year}-{month_map[month]}"
        
        return None

    def download_file(self, url: str, filename: Optional[str] = None, max_retries: int = 3, mongo_manager=None) -> Optional[str]:
        """
        Download a file from a URL with retry logic
        
        Args:
            url (str): URL to download
            filename (str, optional): Filename to save as. Defaults to the last part of the URL.
            max_retries (int): Maximum number of download attempts
            mongo_manager (MongoManager, optional): MongoDB manager for tracking downloads
            
        Returns:
            str|None: Path to downloaded file or None if download failed
            
        Raises:
            DownloadError: If download fails after retries
        """
        if filename is None:
            filename = url.split('/')[-1]
            
        filepath = os.path.join(self.download_dir, filename)
        temp_filepath = filepath + ".tmp"
        zip_id = None
        
        try:
            # Extract date from filename
            file_date = self._extract_date_from_filename(filename)
            if not file_date:
                raise DownloadError(f"Could not extract date from filename: {filename}")
        
            # Register download in MongoDB if manager is provided
            if mongo_manager:
                    zip_id = mongo_manager.create_zip_download(
                        url=url,
                        file_name=filename,
                        file_date=file_date
                    )
                    if not zip_id:
                        raise DownloadError(f"Failed to create MongoDB record for {filename}")
            
            # Check if file already exists
            if os.path.exists(filepath):
                try:
                    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
                    if file_size_mb > 0.1:  # More than 100 KB
                            logger.info(f"File {filepath} already exists ({file_size_mb:.2f} MB). Skipping download.")
                            if mongo_manager and zip_id:
                                    mongo_manager.update_zip_download_status(zip_id, downloaded=True)
                    return filepath
                    # else:
                    #     logger.warning(f"File {filepath} exists but is too small ({file_size_mb:.2f} MB). Re-downloading.")
                    #     os.remove(filepath)
                except Exception as e:
                    logger.error(f"Error checking existing file {filepath}: {e}")
                    try:
                        os.remove(filepath)
                    except:
                        pass
            
                # Download with retries
            for attempt in range(max_retries):
                try:
                    logger.info(f"Downloading {url} to {filepath} (attempt {attempt+1}/{max_retries})")
                    
                    response = requests.get(url, stream=True, timeout=60)
                    response.raise_for_status()
                    
                    total_size = int(response.headers.get('content-length', 0))
                    total_size_mb = total_size / (1024 * 1024)
                    logger.info(f"File size: {total_size_mb:.2f} MB")
                    
                    downloaded = 0
                    with open(temp_filepath, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                
                                if total_size > 0 and downloaded % (100 * 1024 * 1024) < 8192:
                                    percent = (downloaded / total_size) * 100
                                    downloaded_mb = downloaded / (1024 * 1024)
                                    logger.info(f"Downloaded {downloaded_mb:.2f} MB of {total_size_mb:.2f} MB ({percent:.1f}%)")
                    
                        # Verify file size
                        if total_size > 0 and total_size != downloaded:
                            raise DownloadError(f"Download incomplete: expected {total_size} bytes but got {downloaded} bytes")
                    
                        # Move temporary file to final destination
                        os.rename(temp_filepath, filepath)
                        logger.info(f"Successfully downloaded {filepath}")
                    
                        # Update MongoDB status
                        if mongo_manager and zip_id:
                            mongo_manager.update_zip_download_status(zip_id, downloaded=True)
                        
                    return filepath
                
                except requests.exceptions.Timeout:
                    logger.warning(f"Timeout downloading {url} (attempt {attempt+1}/{max_retries})")
                    if attempt == max_retries - 1:
                            raise DownloadError(f"Failed to download {url} after {max_retries} attempts due to timeout")
                
                except requests.exceptions.HTTPError as e:
                        raise DownloadError(f"HTTP error downloading {url}", e)
                    
                except Exception as e:
                    logger.error(f"Error downloading {url} (attempt {attempt+1}/{max_retries}): {e}")
                    if os.path.exists(temp_filepath):
                        try:
                            os.remove(temp_filepath)
                        except:
                            pass
                            
                    if attempt == max_retries - 1:
                            raise DownloadError(f"Failed to download {url} after {max_retries} attempts", e)
                        
                            time.sleep(2 * (attempt + 1))
                            
                    return None
                        
        except DownloadError as e:
            logger.error(str(e))
            if e.original_error:
                logger.error(f"Original error: {e.original_error}")
            if mongo_manager and zip_id:
                mongo_manager.update_zip_download_status(zip_id, downloaded=False)
            return None
            
        except Exception as e:
            logger.error(f"Unexpected error downloading {url}: {e}")
            if mongo_manager and zip_id:
                mongo_manager.update_zip_download_status(zip_id, downloaded=False)
        return None
    
        # finally:
        #     if os.path.exists(temp_filepath):
        #         try:
        #             os.remove(temp_filepath)
        #         except:
        #             pass

    def download_all_zips(self, urls: Optional[List[str]] = None, max_files: Optional[int] = None, mongo_manager=None) -> List[str]:
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
        
        if max_files and max_files > 0:
            urls = urls[:max_files]
            logger.info(f"Limiting download to {max_files} files")
            
        downloaded_files = []
        for i, url in enumerate(urls):
            logger.info(f"Downloading file {i+1}/{len(urls)}")
            try:
                filepath = self.download_file(url, mongo_manager=mongo_manager)
                if filepath:
                    downloaded_files.append(filepath)
            except Exception as e:
                logger.error(f"Error downloading {url}: {e}")
                continue
                
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
            logger.info(f"Parsing HTML XBRL file: {file_path}", extra=extra_log)
            
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
            document_info['full_text'] = all_text[:32000]  # Limit text length
            document_info['text_length'] = len(all_text)
            
            # Extract XBRL facts
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
            
            # If no tags found with standard patterns, look for data- attributes
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
            document_info['facts'] = facts
            
            return document_info
            
        except Exception as e:
            logger.error(f"Error parsing HTML XBRL file: {e}", extra=extra_log)
            return {} 