"""
Module for parsing XBRL data and converting it to Parquet format compatible with Pinecone
"""

import os
import pandas as pd
import logging
import uuid
import json
from bs4 import BeautifulSoup
from typing import Dict, List, Any, Union, Tuple
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import hashlib
from config import VECTOR_DIMENSIONS
import xbrl
from xbrl import XBRLParser as LibXBRLParser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class XBRLParser:
    """
    Class for parsing XBRL documents and converting them to Pinecone-compatible Parquet format
    using the python-xbrl library.
    """
    def __init__(self, output_dir="parquet_files", vector_dimensions=None):
        """
        Initialize the XBRL parser
        
        Args:
            output_dir (str): Directory to save Parquet files
            vector_dimensions (int, optional): Dimensions for vector embeddings
        """
        self.output_dir = output_dir
        self.vector_dimensions = vector_dimensions or VECTOR_DIMENSIONS
        self.lib_parser = LibXBRLParser()
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        logger.info(f"Initialized XBRL parser with vector dimensions: {self.vector_dimensions}")
    
    def _generate_vector(self, text: str) -> List[float]:
        """
        Generate a vector embedding for text
        In a real-world scenario, this would use an embedding model like OpenAI
        Here we're using a deterministic hash-based approach for demonstration
        
        Args:
            text (str): Text to generate vector for
            
        Returns:
            list: Vector embedding
        """
        # This is a placeholder for a real embedding model
        # In production, you would use a proper embedding service
        
        # Create a deterministic but varied vector based on text hash
        hash_object = hashlib.sha256(text.encode())
        hash_hex = hash_object.hexdigest()
        
        # Convert hash to a list of floats
        seed = int(hash_hex, 16) % (10**8)
        np.random.seed(seed)
        
        # Generate a normalized vector of the correct dimension
        vector = np.random.normal(0, 1, self.vector_dimensions)
        # Normalize to unit length (important for vector search)
        vector = vector / np.linalg.norm(vector)
        
        return vector.tolist()
    
    def _truncate_text(self, text, max_length=10000):
        """
        Trunca el texto a un tamaño máximo pero intenta preservar frases completas
        
        Args:
            text (str): Texto a truncar
            max_length (int): Longitud máxima permitida
            
        Returns:
            str: Texto truncado
        """
        if not text or len(text) <= max_length:
            return text
            
        # Si tenemos que truncar, intentamos hacerlo en un punto y aparte
        truncated = text[:max_length]
        last_period = truncated.rfind('. ')
        
        if last_period > max_length * 0.7:  # Si encontramos un punto en el último 30% del texto
            return truncated[:last_period+1]
        return truncated

    def _check_metadata_size(self, metadata, max_size=38000):
        """
        Verifica el tamaño de los metadatos y los reduce si es necesario
        
        Args:
            metadata (dict): Metadatos a verificar
            max_size (int): Tamaño máximo permitido en bytes
            
        Returns:
            dict: Metadatos ajustados al tamaño permitido
        """
        # Convertir a JSON para medir el tamaño
        metadata_json = json.dumps(metadata)
        current_size = len(metadata_json.encode('utf-8'))
        
        # Si estamos dentro del límite, devolver sin cambios
        if current_size <= max_size:
            return metadata
            
        logger.warning(f"Metadata size is {current_size} bytes, exceeding limit of {max_size} bytes")
        
        # Si el tamaño es mayor al límite, reducir el full_text
        if 'full_text' in metadata and metadata['full_text']:
            # Calcular cuánto texto podemos mantener
            excess_bytes = current_size - max_size
            # Añadimos un margen de seguridad del 20%
            text_reduction = excess_bytes + int(excess_bytes * 0.2)
            
            current_text_len = len(metadata['full_text'])
            new_text_len = max(1000, current_text_len - text_reduction)
            
            # Truncar el texto
            metadata['full_text'] = self._truncate_text(metadata['full_text'], new_text_len)
            
            # Verificar nuevamente
            metadata_json = json.dumps(metadata)
            new_size = len(metadata_json.encode('utf-8'))
            
            logger.info(f"Reduced metadata size from {current_size} to {new_size} bytes")
            
            # Si aún excede, intentar una reducción más agresiva
            if new_size > max_size:
                # Extraer solo el comienzo y parte final del texto
                if len(metadata['full_text']) > 5000:
                    start = metadata['full_text'][:2500]
                    end = metadata['full_text'][-2500:]
                    metadata['full_text'] = f"{start}... [TRUNCATED] ...{end}"
                    
                    # Último recurso: si sigue siendo demasiado grande, reducir drásticamente
                    metadata_json = json.dumps(metadata)
                    if len(metadata_json.encode('utf-8')) > max_size:
                        metadata['full_text'] = metadata['full_text'][:max_size // 10]
                        metadata['full_text'] += " [SEVERELY TRUNCATED DUE TO SIZE LIMITS]"
        
        return metadata

    def _extract_data_from_filename(self, filename):
        """
        Extrae datos del nombre del archivo usando el patrón Prod224_2476_00002687_20240405.html
        
        Args:
            filename (str): Nombre del archivo
            
        Returns:
            dict: Datos extraídos del nombre del archivo
        """
        data = {
            'company_number': '',
            'account_date': '',
            'year': None,
            'month': None,
            'day': None
        }
        
        # Eliminar extensión y dividir por guiones bajos
        base_name = os.path.splitext(filename)[0]
        parts = base_name.split('_')
        
        # Verificar si tiene el formato esperado (al menos 4 partes)
        if len(parts) >= 4:
            # Company number está en la tercera posición [2]
            data['company_number'] = parts[2].lstrip('0')  # Eliminar ceros a la izquierda
            
            # Fecha en la cuarta posición [3]
            date_str = parts[3]
            if len(date_str) == 8 and date_str.isdigit():
                # Formato YYYYMMDD
                year = date_str[0:4]
                month = date_str[4:6]
                day = date_str[6:8]
                
                data['account_date'] = f"{year}-{month}-{day}"
                try:
                    data['year'] = int(year)
                    data['month'] = int(month)
                    data['day'] = int(day)
                except ValueError:
                    pass
        
        return data

    def _flatten_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Crear estructura de metadatos simplificada con solo los campos necesarios
        
        Args:
            data (dict): Datos extraídos del documento
            
        Returns:
            dict: Metadata estructurada con campos clave simplificados
        """
        # Estructura base de metadatos (simplificada a los campos solicitados)
        metadata = {
            "company_number": "",
            "company_name": "",
            "account_date": "",
            "year": None,
            "month": None,
            "day": None,
            "full_text": ""
        }
        
        # Primero, extraer datos del nombre del archivo
        if "doc_file_name" in data:
            filename_data = self._extract_data_from_filename(data["doc_file_name"])
            metadata.update(filename_data)
        
        # Segundo, extraer company_name del título del documento
        if "doc_title" in data and data["doc_title"]:
            # Normalmente el título sigue el patrón "Nombre de la Compañía - resto del título"
            title_parts = data["doc_title"].split(' - ', 1)
            if len(title_parts) > 0:
                metadata["company_name"] = title_parts[0].strip()
        
        # Tercer, buscar en metadatos si no se encontró en otros lugares
        if "meta_tags" in data:
            for meta in data["meta_tags"]:
                name = meta.get("name", "").lower()
                content = meta.get("content", "")
                
                # Capturar información específica de metadatos
                if name == "companynumber" and not metadata["company_number"]:
                    metadata["company_number"] = content
                elif name == "companyname" and not metadata["company_name"]:
                    metadata["company_name"] = content
                elif name == "accountdate" and not metadata["account_date"]:
                    metadata["account_date"] = content
        
        # Añadir el texto completo extraído
        if "full_text" in data:
            metadata["full_text"] = data["full_text"]
            
        return metadata
    
    def _partition_document(self, doc_id, metadata, full_text, vector, max_metadata_size=38000):
        """
        Particiona un documento grande en múltiples registros para evitar exceder
        el límite de tamaño de metadatos de Pinecone.
        
        Args:
            doc_id (str): ID del documento
            metadata (dict): Metadatos estructurados
            full_text (str): Texto completo del documento
            vector (list): Vector de embeddings
            max_metadata_size (int): Tamaño máximo de metadatos en bytes
            
        Returns:
            list: Lista de registros particionados
        """
        # Calcular cuántos caracteres podemos incluir en cada partición
        # Primero verificamos el tamaño de los metadatos sin el texto completo
        base_metadata = metadata.copy()
        base_metadata["full_text"] = ""
        base_metadata_json = json.dumps(base_metadata)
        base_size = len(base_metadata_json.encode('utf-8'))
        
        # Espacio disponible para texto en cada partición
        available_size = max_metadata_size - base_size - 100  # 100 bytes de margen
        
        # Dividir texto en segmentos que quepan en los metadatos
        text_segments = []
        remaining_text = full_text
        
        while remaining_text:
            # Estimar cuántos caracteres podemos incluir (aproximado)
            # En promedio, cada carácter UTF-8 puede ocupar ~1-4 bytes
            estimated_chars = int(available_size / 2)  # Estimación conservadora
            
            # Ajustar si el texto restante es más corto
            if len(remaining_text) <= estimated_chars:
                text_segments.append(remaining_text)
                break
                
            # Buscar un buen punto de corte (final de oración)
            segment = remaining_text[:estimated_chars]
            last_period = segment.rfind('. ')
            
            if last_period > estimated_chars * 0.7:  # Si hay un punto en el último 30%
                cut_point = last_period + 1
            else:
                # Si no hay buen punto de corte, buscar espacio
                last_space = segment.rfind(' ')
                cut_point = last_space if last_space > 0 else estimated_chars
            
            text_segments.append(remaining_text[:cut_point])
            remaining_text = remaining_text[cut_point:].lstrip()
        
        # Crear registros particionados
        records = []
        total_parts = len(text_segments)
        
        for i, text_segment in enumerate(text_segments):
            part_num = i + 1
            part_metadata = base_metadata.copy()
            part_metadata["full_text"] = text_segment
            part_metadata["part_number"] = part_num
            part_metadata["total_parts"] = total_parts
            
            # Crear ID único para esta partición
            part_id = f"{doc_id}_part{part_num}" if total_parts > 1 else doc_id
            
            # Crear registro
            record = {
                "id": part_id,
                "values": vector,
                "metadata": json.dumps(part_metadata)
            }
            
            records.append(record)
            
        logger.info(f"Document partitioned into {len(records)} parts")
        return records

    def xbrl_to_parquet(self, file_path: str, mongo_manager=None) -> str:
        """
        Convert an XBRL file to Parquet format compliant with Pinecone requirements
        
        Args:
            file_path (str): Path to XBRL file (.html or .xml)
            mongo_manager (MongoManager, optional): MongoDB manager for storing data
            
        Returns:
            str: Path to the generated Parquet file or None if failed
        """
        try:
            # Skip files that are too large to avoid memory issues
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            if file_size_mb > 10:
                logger.warning(f"Skipping large file ({file_size_mb:.2f} MB): {file_path}")
                return None
                
            # Generate a Parquet file path before parsing
            file_name = os.path.basename(file_path).split('.')[0]
            parquet_file = os.path.join(self.output_dir, f"{file_name}.parquet")
            
            # Skip if output file already exists
            if os.path.exists(parquet_file):
                logger.info(f"Parquet file already exists: {parquet_file}")
                return parquet_file
            
            logger.info(f"Parsing XBRL file: {file_path}")
            
            # Datos generales del documento
            document_data = {
                'doc_file_name': os.path.basename(file_path),
                'doc_file_path': file_path,
                'meta_tags': [],
                'director_info': {
                    'directors': [],
                    'highest_paid': {}
                },
                'full_text': "",
                'doc_title': ""
            }
            
            # Extract metadata from filename
            filename_data = self._extract_data_from_filename(document_data['doc_file_name'])
            
            # Register conversion in MongoDB if manager is provided
            if mongo_manager:
                mongo_manager.register_file_conversion(
                    filename=document_data['doc_file_name'],
                    account_date=filename_data.get('account_date', ''),
                    company_number=filename_data.get('company_number', '')
                )
            
            # Approach 1: Parse with BeautifulSoup to extract text and metadata
            try:
                with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                    content = file.read()
                
                # Usar BeautifulSoup para extraer el contenido
                soup = BeautifulSoup(content, 'xml' if file_path.lower().endswith('.xml') else 'lxml')
                
                # Extraer todos los metadatos
                meta_tags = soup.find_all('meta')
                for tag in meta_tags:
                    document_data['meta_tags'].append({
                        'name': tag.get('name', ''),
                        'content': tag.get('content', '')
                    })
                
                # Extraer título
                title_tag = soup.find('title')
                if title_tag:
                    document_data['doc_title'] = title_tag.text.strip()
                
                # Extraer todo el texto legible
                document_data['full_text'] = soup.get_text(" ", strip=True)
                
                # Buscar información sobre directores
                # Esta es una búsqueda simplificada, podría necesitar adaptarse según el formato específico
                director_sections = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'div', 'p'], 
                                                 string=lambda s: s and ('director' in s.lower() or 'board' in s.lower()))
                
                # Extract director remuneration
                total_remuneration = ""
                for section in director_sections:
                    # Buscar textos cercanos que puedan contener información sobre pagos
                    nearby_text = ' '.join([s.text for s in section.find_next_siblings(['p', 'div'], limit=5)])
                    
                    # Look for highest paid director
                    if 'highest paid' in nearby_text.lower() or 'remuneration' in nearby_text.lower():
                        # Extraer información mediante expresiones regulares
                        import re
                        amount_match = re.search(r'£([\d,]+)', nearby_text)
                        if amount_match:
                            document_data['director_info']['highest_paid'] = {
                                'name': 'Not specified',
                                'amount': amount_match.group(0)
                            }
                    
                    # Look for total director remuneration
                    if 'total' in nearby_text.lower() and 'remuneration' in nearby_text.lower():
                        # Extraer información mediante expresiones regulares
                        import re
                        total_match = re.search(r'£([\d,]+)', nearby_text)
                        if total_match:
                            total_remuneration = total_match.group(0)
                
                # Generar un ID único para el documento
                doc_id = hashlib.md5(file_name.encode()).hexdigest()
                
                # Generar vector para el documento completo
                vector = self._generate_vector(document_data['full_text'][:32000])
                
                # Generar metadatos estructurados
                structured_metadata = self._flatten_metadata(document_data)
                
                # En lugar de truncar el texto, vamos a particionar el documento si es necesario
                # Primero verificamos el tamaño total de los metadatos
                metadata_json = json.dumps(structured_metadata)
                metadata_size = len(metadata_json.encode('utf-8'))
                
                # Log detallado del tamaño de metadatos
                logger.info(f"Metadata size for {file_name}: {metadata_size} bytes")
                logger.info(f"Full text length: {len(structured_metadata.get('full_text', ''))}")
                
                records = []
                
                if metadata_size > 38000:
                    logger.warning(f"Metadata size is {metadata_size} bytes, exceeding limit. Partitioning document.")
                    records = self._partition_document(
                        doc_id=doc_id,
                        metadata=structured_metadata,
                        full_text=document_data['full_text'],
                        vector=vector
                    )
                else:
                    # Si el tamaño está dentro del límite, usamos un solo registro
                    records = [{
                        "id": doc_id,
                        "values": vector,
                        "metadata": metadata_json
                    }]
                
                # Crear DataFrame con todos los registros
                df = pd.DataFrame(records)
                
                # Asegurar columnas correctas
                if set(df.columns) != set(["id", "values", "metadata"]):
                    logger.warning(f"Columnas incorrectas en DataFrame: {df.columns}, ajustando...")
                    # Asegurar que solo contiene las columnas requeridas
                    required_columns = ["id", "values", "metadata"]
                    for col in required_columns:
                        if col not in df.columns:
                            df[col] = ["" for _ in range(len(df))]
                    # Eliminar columnas adicionales si existen
                    df = df[required_columns]
                
                # Exportar a Parquet
                table = pa.Table.from_pandas(df)
                pq.write_table(
                    table, 
                    parquet_file,
                    compression='snappy',
                    use_dictionary=False,
                    version='2.6'
                )
                
                if len(records) > 1:
                    logger.info(f"Created Parquet file with {len(records)} partitions: {parquet_file}")
                else:
                    logger.info(f"Created Parquet file for document: {parquet_file}")
                
                # Update conversion status in MongoDB if manager is provided
                if mongo_manager:
                    mongo_manager.update_conversion_status(
                        filename=document_data['doc_file_name'],
                        status="completed"
                    )
                
                return parquet_file
                
            except Exception as e:
                logger.error(f"Error parsing file {file_path}: {e}")
                
                # Update conversion status in MongoDB if manager is provided
                if mongo_manager:
                    mongo_manager.update_conversion_status(
                        filename=document_data['doc_file_name'],
                        status="failed"
                    )
                    
                return None
                
        except Exception as e:
            logger.error(f"Error converting {file_path} to Parquet: {e}")
            
            # Update conversion status in MongoDB if manager is provided
            if mongo_manager:
                mongo_manager.update_conversion_status(
                    filename=os.path.basename(file_path),
                    status="failed"
                )
                
            return None
    
    def process_all_files(self, file_paths: List[str]) -> List[str]:
        """
        Process multiple XBRL files to Parquet
        
        Args:
            file_paths (list): List of paths to XBRL files
            
        Returns:
            list: List of paths to generated Parquet files
        """
        parquet_files = []
        total_files = len(file_paths)
        
        for i, file_path in enumerate(file_paths):
            if i % 10 == 0 or i == total_files - 1:
                logger.info(f"Processing file {i+1}/{total_files}")
                
            if file_path.lower().endswith(('.html', '.xml')):
                parquet_file = self.xbrl_to_parquet(file_path)
                if parquet_file:
                    parquet_files.append(parquet_file)
        
        return parquet_files 