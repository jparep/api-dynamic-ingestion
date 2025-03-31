#!/usr/bin/env python3
"""
COVID-19 Data Pipeline - Enhanced Python Client
Created: March 2025
Last Updated: March 30, 2025

This script runs on a local machine and handles all dynamic processing
that was previously in JavaScript UDFs within Snowflake. It interacts directly
with the disease.sh API to fetch COVID data and handles schema discovery,
flattening, and view creation - all from the local machine.

Requirements:
- Python 3.8+
- snowflake-connector-python
- requests
- python-dotenv
- pandas
"""

import os
import sys
import json
import time
import hashlib
import logging
import argparse
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple, Set

import requests
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/covid_pipeline_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('covid_pipeline')

# Snowflake connection configuration
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USERNAME'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'compute_wh'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'covid'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'raw'),
    'role': os.getenv('SNOWFLAKE_ROLE', 'sysadmin')
}

# External COVID API configuration
COVID_API_ENDPOINT = os.getenv('COVID_API_ENDPOINT', 'https://disease.sh/v3/covid-19')
SOURCE_NAME = 'disease.sh'  # Source identifier


class SnowflakeClient:
    """Client for interacting with Snowflake"""
    
    def __init__(self, config: Dict[str, str]):
        """Initialize Snowflake connection"""
        self.config = config
        self.conn = None
        self.connect()
    
    def connect(self) -> None:
        """Establish connection to Snowflake"""
        try:
            self.conn = snowflake.connector.connect(
                user=self.config['user'],
                password=self.config['password'],
                account=self.config['account'],
                warehouse=self.config['warehouse'],
                database=self.config['database'],
                schema=self.config['schema'],
                role=self.config['role']
            )
            logger.info("Successfully connected to Snowflake!")
        except snowflake.connector.errors.DatabaseError as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            sys.exit(1)
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results"""
        try:
            cursor = self.conn.cursor(snowflake.connector.DictCursor)
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            results = cursor.fetchall()
            cursor.close()
            return results
        except snowflake.connector.errors.DatabaseError as e:
            logger.error(f"Error executing SQL query: {e}")
            logger.debug(f"Query: {query}")
            logger.debug(f"Params: {params}")
            raise
    
    def execute_script(self, script: str) -> None:
        """Execute a multi-statement SQL script"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(script)
            cursor.close()
            logger.info("Successfully executed SQL script")
        except snowflake.connector.errors.DatabaseError as e:
            logger.error(f"Error executing SQL script: {e}")
            raise
    
    def execute_procedure(self, procedure: str, params: List[Any]) -> Dict[str, Any]:
        """Execute a stored procedure and return results"""
        try:
            cursor = self.conn.cursor(snowflake.connector.DictCursor)
            result = cursor.execute(f"CALL {procedure}({', '.join(['%s'] * len(params))})", params)
            row = result.fetchone()
            cursor.close()
            return dict(row) if row else {}
        except snowflake.connector.errors.DatabaseError as e:
            logger.error(f"Error executing procedure {procedure}: {e}")
            raise
    
    def close(self) -> None:
        """Close Snowflake connection"""
        if self.conn:
            try:
                self.conn.close()
                logger.info("Snowflake connection closed.")
            except:
                pass


class CovidApiClient:
    """Client for interacting with COVID-19 API"""
    
    def __init__(self, base_url: str):
        """Initialize API client"""
        self.base_url = base_url
        self.headers = {
            'User-Agent': 'Covid-Data-Pipeline/1.0 (compliance@example.org)',
            'Accept': 'application/json'
        }
    
    def fetch_countries_data(self) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Fetch COVID data for all countries and return with metadata"""
        endpoint = f"{self.base_url}/countries"
        logger.info(f"Fetching data from: {endpoint}")
        
        params = {}
        try:
            response = requests.get(endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Create metadata about this request
            metadata = {
                'endpoint': endpoint,
                'params': params,
                'status_code': response.status_code,
                'timestamp': datetime.now().isoformat(),
                'response_time_ms': response.elapsed.total_seconds() * 1000
            }
            
            logger.info(f"Successfully fetched data for {len(data)} countries")
            return data, metadata
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching COVID data: {e}")
            raise
    
    def fetch_country_data(self, country: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Fetch COVID data for a specific country and return with metadata"""
        endpoint = f"{self.base_url}/countries/{country}"
        logger.info(f"Fetching data for country: {country}")
        
        params = {}
        try:
            response = requests.get(endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Create metadata about this request
            metadata = {
                'endpoint': endpoint,
                'params': params,
                'status_code': response.status_code,
                'timestamp': datetime.now().isoformat(),
                'response_time_ms': response.elapsed.total_seconds() * 1000
            }
            
            logger.info(f"Successfully fetched data for {country}")
            return data, metadata
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching COVID data for {country}: {e}")
            raise


class SchemaDiscoveryHandler:
    """Handler for schema discovery and registry"""
    
    def __init__(self, snowflake_client: SnowflakeClient):
        self.snowflake = snowflake_client
    
    def discover_and_register_schema(self, source_name: str, load_ts: str) -> Dict[str, Any]:
        """Discover schema from raw records and register fields"""
        logger.info(f"Discovering schema for {source_name}, batch {load_ts}")
        start_time = time.time()
        
        # Get sample records
        query = """
            SELECT source_record 
            FROM raw.COVID_RAW
            WHERE source_name = %(source_name)s
            AND load_ts = %(load_ts)s
            LIMIT 100
        """
        
        records = self.snowflake.execute_query(
            query, 
            {'source_name': source_name, 'load_ts': load_ts}
        )
        
        if not records:
            logger.warning(f"No records found for schema discovery. Source: {source_name}, Load TS: {load_ts}")
            return {
                'status': 'ERROR',
                'message': 'No records found for schema discovery'
            }
        
        # Process records to discover schema
        discovered_fields = {}
        
        for record in records:
            source_record = record['SOURCE_RECORD']
            self._discover_schema(source_record, "", discovered_fields)
        
        # Register discovered fields
        fields_registered = 0
        
        for field_path, field_info in discovered_fields.items():
            self._register_field(
                source_name=source_name,
                field_name=field_info['name'],
                field_path=field_path,
                field_example=field_info['example'],
                data_type=field_info['type'],
                load_ts=load_ts
            )
            fields_registered += 1
        
        # Log the operation
        execution_time_ms = int((time.time() - start_time) * 1000)
        
        self._log_operation(
            operation_name='DISCOVER_AND_REGISTER_SCHEMA',
            target_table='meta.SCHEMA_REGISTRY',
            record_count=fields_registered,
            source_name=source_name,
            load_ts=load_ts,
            status='SUCCESS',
            execution_time_ms=execution_time_ms
        )
        
        return {
            'status': 'SUCCESS',
            'message': f'Successfully discovered and registered schema',
            'fields_discovered': fields_registered,
            'execution_time_ms': execution_time_ms
        }
    
    def _discover_schema(self, obj: Dict[str, Any], prefix: str, result: Dict[str, Dict[str, Any]]) -> None:
        """Recursively discover schema from a JSON object"""
        for key, value in obj.items():
            path = f"{prefix}.{key}" if prefix else key
            value_type = self._get_json_type(value)
            
            if value_type == 'object' and value is not None:
                # Recursively process nested objects
                self._discover_schema(value, path, result)
            elif value_type == 'array' and value:
                # For arrays, process each item type
                if isinstance(value[0], dict):
                    # Array of objects - append index to path
                    for i, item in enumerate(value[:5]):  # Process up to 5 items
                        self._discover_schema(item, f"{path}[{i}]", result)
                else:
                    # Array of primitives - just register the array itself
                    result[path] = {
                        'name': key,
                        'type': 'ARRAY',
                        'example': str(value)[:255]
                    }
            else:
                # Register leaf node
                result[path] = {
                    'name': key,
                    'type': value_type.upper(),
                    'example': str(value)[:255] if value is not None else 'NULL'
                }
    
    def _get_json_type(self, value: Any) -> str:
        """Get JSON data type as string"""
        if value is None:
            return 'null'
        
        if isinstance(value, dict):
            return 'object'
        elif isinstance(value, list):
            return 'array'
        elif isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, int):
            return 'number'
        elif isinstance(value, float):
            return 'number'
        else:
            return 'string'
    
    def _register_field(self, source_name: str, field_name: str, field_path: str, 
                       field_example: str, data_type: str, load_ts: str) -> None:
        """Register a field in the schema registry"""
        query = """
            MERGE INTO meta.SCHEMA_REGISTRY target
            USING (SELECT %(source_name)s as source_name, %(field_name)s as field_name, %(field_path)s as field_path) source
            ON target.source_name = source.source_name AND target.field_path = source.field_path
            WHEN MATCHED THEN
                UPDATE SET 
                    last_seen_ts = CURRENT_TIMESTAMP(),
                    field_example = %(field_example)s,
                    data_type = %(data_type)s,
                    is_active = TRUE
            WHEN NOT MATCHED THEN
                INSERT (source_name, field_name, field_path, field_example, data_type, load_ts)
                VALUES (%(source_name)s, %(field_name)s, %(field_path)s, %(field_example)s, %(data_type)s, %(load_ts)s)
        """
        
        self.snowflake.execute_query(
            query,
            {
                'source_name': source_name,
                'field_name': field_name,
                'field_path': field_path,
                'field_example': field_example,
                'data_type': data_type,
                'load_ts': load_ts
            }
        )
    
    def _log_operation(self, operation_name: str, target_table: str, record_count: int,
                     source_name: str, load_ts: str, status: str,
                     error_message: Optional[str] = None, 
                     execution_time_ms: Optional[int] = None) -> None:
        """Log an operation to the audit log"""
        query = """
            CALL meta.LOG_OPERATION(
                %(operation_name)s,
                %(target_table)s,
                %(record_count)s,
                %(source_name)s,
                %(load_ts)s,
                %(status)s,
                %(error_message)s,
                %(execution_time_ms)s
            )
        """
        
        try:
            self.snowflake.execute_query(
                query,
                {
                    'operation_name': operation_name,
                    'target_table': target_table,
                    'record_count': record_count,
                    'source_name': source_name,
                    'load_ts': load_ts,
                    'status': status,
                    'error_message': error_message,
                    'execution_time_ms': execution_time_ms
                }
            )
        except Exception as e:
            logger.error(f"Failed to log operation: {e}")


class DataFlatteningHandler:
    """Handler for flattening JSON data to staging tables"""
    
    def __init__(self, snowflake_client: SnowflakeClient, schema_handler: SchemaDiscoveryHandler):
        self.snowflake = snowflake_client
        self.schema_handler = schema_handler
    
    def flatten_json_to_staging(self, source_name: str, load_ts: str) -> Dict[str, Any]:
        """Process raw records and flatten to staging table"""
        logger.info(f"Flattening JSON data for {source_name}, batch {load_ts}")
        start_time = time.time()
        
        # First ensure schema is discovered and registered
        self.schema_handler.discover_and_register_schema(source_name, load_ts)
        
        # Get raw records to flatten
        query = """
            SELECT 
                raw_id, source_name, entity_id, source_record, record_hash, load_ts
            FROM raw.COVID_RAW
            WHERE source_name = %(source_name)s
            AND load_ts = %(load_ts)s
        """
        
        records = self.snowflake.execute_query(
            query,
            {'source_name': source_name, 'load_ts': load_ts}
        )
        
        if not records:
            logger.warning(f"No records found to flatten. Source: {source_name}, Load TS: {load_ts}")
            return {
                'status': 'WARNING',
                'message': 'No records found to flatten'
            }
        
        # Process each record
        record_count = 0
        field_count = 0
        
        for record in records:
            raw_id = record['RAW_ID']
            entity_id = record['ENTITY_ID']
            source_record = record['SOURCE_RECORD']
            record_hash = record['RECORD_HASH']
            record_load_ts = record['LOAD_TS']
            
            # Flatten the JSON and insert fields
            inserted_fields = self._flatten_and_insert_json(
                raw_id=raw_id,
                source_name=source_name,
                entity_id=entity_id,
                source_record=source_record,
                record_hash=record_hash,
                load_ts=record_load_ts
            )
            
            record_count += 1
            field_count += inserted_fields
        
        # Log the operation
        execution_time_ms = int((time.time() - start_time) * 1000)
        
        self.schema_handler._log_operation(
            operation_name='FLATTEN_JSON_TO_STAGING',
            target_table='stg.COVID_STG',
            record_count=record_count,
            source_name=source_name,
            load_ts=load_ts,
            status='SUCCESS',
            execution_time_ms=execution_time_ms
        )
        
        # Log performance metrics
        self._track_performance(
            process_name='FLATTEN_JSON_TO_STAGING',
            execution_time_ms=execution_time_ms,
            rows_processed=record_count
        )
        
        return {
            'status': 'SUCCESS',
            'message': f'Successfully flattened JSON data to staging',
            'records_processed': record_count,
            'fields_processed': field_count,
            'execution_time_ms': execution_time_ms
        }
    
    def _flatten_and_insert_json(self, raw_id: int, source_name: str, entity_id: str, 
                               source_record: Dict[str, Any], record_hash: str, 
                               load_ts: str) -> int:
        """Flatten JSON and insert resulting fields into staging"""
        fields = []
        
        # Recursively flatten the JSON
        self._flatten_json(
            raw_id=raw_id,
            source_name=source_name,
            entity_id=entity_id,
            obj=source_record,
            prefix="",
            record_hash=record_hash,
            load_ts=load_ts,
            fields=fields
        )
        
        # Insert in batches for better performance
        batch_size = 1000
        for i in range(0, len(fields), batch_size):
            batch = fields[i:i+batch_size]
            
            insert_values = []
            placeholders = []
            
            for field in batch:
                insert_values.extend([
                    raw_id, 
                    entity_id, 
                    field['field_name'], 
                    field['field_path'], 
                    field['field_value'], 
                    field['field_ordinal'],
                    load_ts, 
                    source_name, 
                    record_hash
                ])
                placeholders.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s)")
            
            # Bulk insert
            query = f"""
                INSERT INTO stg.COVID_STG (
                    raw_id, entity_id, field_name, field_path, field_value, field_ordinal,
                    load_ts, source_name, raw_hash
                ) 
                VALUES {', '.join(placeholders)}
            """
            
            cursor = self.snowflake.conn.cursor()
            cursor.execute(query, insert_values)
            cursor.close()
        
        return len(fields)
    
    def _flatten_json(self, raw_id: int, source_name: str, entity_id: str, obj: Dict[str, Any], 
                    prefix: str, record_hash: str, load_ts: str, fields: List[Dict[str, Any]]) -> None:
        """Recursively flatten JSON and collect fields"""
        for key, value in obj.items():
            path = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict):
                # Recursively process nested objects
                self._flatten_json(
                    raw_id=raw_id,
                    source_name=source_name,
                    entity_id=entity_id,
                    obj=value,
                    prefix=path,
                    record_hash=record_hash,
                    load_ts=load_ts,
                    fields=fields
                )
            elif isinstance(value, list):
                # Handle arrays
                if not value:
                    # Empty array
                    fields.append({
                        'field_name': key,
                        'field_path': path,
                        'field_value': "",
                        'field_ordinal': None
                    })
                else:
                    # Handle array items
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            # Object in array
                            self._flatten_json(
                                raw_id=raw_id,
                                source_name=source_name,
                                entity_id=entity_id,
                                obj=item,
                                prefix=f"{path}[{i}]",
                                record_hash=record_hash,
                                load_ts=load_ts,
                                fields=fields
                            )
                        else:
                            # Primitive value in array
                            fields.append({
                                'field_name': key,
                                'field_path': f"{path}[{i}]",
                                'field_value': self._to_string(item),
                                'field_ordinal': i
                            })
                    
                    # Also store the full array as JSON string
                    fields.append({
                        'field_name': key,
                        'field_path': path,
                        'field_value': json.dumps(value),
                        'field_ordinal': None
                    })
            else:
                # Store leaf value as string with no transformations
                fields.append({
                    'field_name': key,
                    'field_path': path,
                    'field_value': self._to_string(value),
                    'field_ordinal': None
                })
    
    def _to_string(self, value: Any) -> Optional[str]:
        """Safely convert any value to string"""
        if value is None:
            return None
        return str(value)
    
    def _track_performance(self, process_name: str, execution_time_ms: int, rows_processed: int) -> None:
        """Track performance metrics"""
        query = """
            INSERT INTO meta.PERFORMANCE_METRICS (
                process_name,
                execution_time_ms,
                rows_processed
            )
            VALUES (%(process_name)s, %(execution_time_ms)s, %(rows_processed)s)
        """
        
        try:
            self.snowflake.execute_query(
                query,
                {
                    'process_name': process_name,
                    'execution_time_ms': execution_time_ms,
                    'rows_processed': rows_processed
                }
            )
        except Exception as e:
            logger.error(f"Failed to track performance: {e}")


class ViewCreationHandler:
    """Handler for creating dynamic views based on schema registry"""
    
    def __init__(self, snowflake_client: SnowflakeClient):
        self.snowflake = snowflake_client
        self.max_fields_per_view = 950  # Snowflake limit is 1000 columns
    
    def create_dynamic_view(self, view_name: str, source_name: str) -> Dict[str, Any]:
        """Create or update a dynamic view based on schema registry"""
        logger.info(f"Creating dynamic view {view_name} for {source_name}")
        start_time = time.time()
        
        # Get fields from schema registry
        query = """
            SELECT field_name, field_path
            FROM meta.SCHEMA_REGISTRY
            WHERE source_name = %(source_name)s
            AND is_active = TRUE
            ORDER BY field_path
        """
        
        fields = self.snowflake.execute_query(
            query,
            {'source_name': source_name}
        )
        
        if not fields:
            logger.warning(f"No fields found in schema registry. Source: {source_name}")
            return {
                'status': 'ERROR',
                'message': 'No fields found in schema registry'
            }
        
        # Calculate how many views we need to create (to avoid column limit)
        view_count = (len(fields) + self.max_fields_per_view - 1) // self.max_fields_per_view
        created_views = []
        
        for view_index in range(view_count):
            start_idx = view_index * self.max_fields_per_view
            end_idx = min(start_idx + self.max_fields_per_view, len(fields))
            current_fields = fields[start_idx:end_idx]
            
            current_view_name = view_name
            if view_count > 1:
                current_view_name = f"{view_name}_PART{view_index+1}"
            
            # Build dynamic SQL for pivot operation
            view_sql = f"""
                CREATE OR REPLACE VIEW {current_view_name} AS
                WITH all_entities AS (
                    SELECT DISTINCT entity_id, load_ts, raw_id
                    FROM stg.COVID_STG
                    WHERE source_name = '{source_name}'
                )
                SELECT 
                    e.entity_id,
                    e.load_ts,
                    e.raw_id
            """
            
            # Add MAX CASE expression for each field
            for field in current_fields:
                field_name = field['FIELD_NAME']
                field_path = field['FIELD_PATH']
                safe_field_name = re.sub(r'[^a-zA-Z0-9_]', '_', field_name)
                
                view_sql += f"""
                    , MAX(CASE WHEN s.field_path = '{field_path}' THEN s.field_value END) AS "{safe_field_name}"
                """
            
            view_sql += """
                FROM all_entities e
                LEFT JOIN stg.COVID_STG s
                    ON e.entity_id = s.entity_id 
                    AND e.load_ts = s.load_ts
                    AND e.raw_id = s.raw_id
                GROUP BY e.entity_id, e.load_ts, e.raw_id
            """
            
            # Create the view
            try:
                self.snowflake.execute_script(view_sql)
                created_views.append({
                    'name': current_view_name,
                    'field_count': len(current_fields)
                })
                
                # Create union view (only needed if multiple parts)
                if view_count > 1 and view_index == 0:
                    union_view_sql = f"""
                        CREATE OR REPLACE VIEW {view_name} AS
                        SELECT * FROM {current_view_name}
                    """
                    self.snowflake.execute_script(union_view_sql)
            except Exception as e:
                logger.error(f"Error creating view {current_view_name}: {e}")
                return {
                    'status': 'ERROR',
                    'message': f'Error creating view: {str(e)}'
                }
        
        # Calculate execution time
        execution_time_ms = int((time.time() - start_time) * 1000)
        
        return {
            'status': 'SUCCESS',
            'message': f'Successfully created {len(created_views)} view(s)',
            'total_fields': len(fields),
            'execution_time_ms': execution_time_ms,
            'views': created_views
        }


class CovidDataPipeline:
    """Main COVID-19 data pipeline with enhanced features"""
    
    def __init__(self):
        """Initialize the pipeline"""
        logger.info("Initializing COVID-19 data pipeline")
        self.snowflake = SnowflakeClient(SNOWFLAKE_CONFIG)
        self.api = CovidApiClient(COVID_API_ENDPOINT)
        self.schema_handler = SchemaDiscoveryHandler(self.snowflake)
        self.flatten_handler = DataFlatteningHandler(self.snowflake, self.schema_handler)
        self.view_handler = ViewCreationHandler(self.snowflake)
        self.compliance_dir = Path("compliance_archive")
        self.compliance_dir.mkdir(exist_ok=True)
    
    def generate_batch_id(self) -> str:
        """Generate a batch ID for the current data load"""
        return datetime.now().strftime('%Y%m%d%H%M%S')
    
    def calculate_hash(self, data: Any) -> str:
        """Calculate SHA-256 hash of data"""
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
    
    def insert_raw_data(self, entity_id: str, data: Dict[str, Any], 
                       batch_id: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Insert raw COVID data into Snowflake without any transformations"""
        try:
            # Calculate hash for data integrity
            record_hash = self.calculate_hash(data)
            
            # Save original data locally for compliance (belt and suspenders)
            self._save_raw_data_locally(entity_id, data, metadata, batch_id, record_hash)
            
            # Insert the raw record
            query = """
            INSERT INTO raw.COVID_RAW (
                source_name,
                entity_id,
                source_record,
                record_hash,
                load_ts,
                api_endpoint,
                api_params
            ) 
            SELECT 
                %(source_name)s,
                %(entity_id)s,
                PARSE_JSON(%(data)s),
                %(hash)s,
                %(batch_id)s,
                %(endpoint)s,
                PARSE_JSON(%(params)s)
            """
            
            self.snowflake.execute_query(
                query,
                {
                    'source_name': SOURCE_NAME,
                    'entity_id': entity_id,
                    'data': json.dumps(data),  # Raw JSON - no modifications
                    'hash': record_hash,
                    'batch_id': batch_id,
                    'endpoint': metadata.get('endpoint', ''),
                    'params': json.dumps(metadata.get('params', {}))
                }
            )
            
            logger.info(f"Successfully inserted raw data for entity: {entity_id}")
            return {
                'status': 'success', 
                'entity_id': entity_id,
                'hash': record_hash
            }
            
        except Exception as e:
            logger.error(f"Error inserting data for entity {entity_id}: {e}")
            return {
                'status': 'error', 
                'entity_id': entity_id, 
                'error': str(e)
            }
    
    def _save_raw_data_locally(self, entity_id: str, data: Dict[str, Any], 
                             metadata: Dict[str, Any], batch_id: str, 
                             hash_value: str) -> None:
        """Save raw data locally as additional compliance measure"""
        try:
            # Create compliance subdirectory for this batch
            batch_dir = self.compliance_dir / batch_id
            batch_dir.mkdir(exist_ok=True)
            
            # Create record with metadata
            record = {
                'entity_id': entity_id,
                'source_name': SOURCE_NAME,
                'batch_id': batch_id,
                'record_hash': hash_value,
                'metadata': metadata,
                'raw_data': data  # Untransformed raw data
            }
            
            # Use hash in filename for easy lookup
            filename = batch_dir / f"{hash_value}_{entity_id}.json"
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(record, f, indent=2, ensure_ascii=False)
                
            logger.debug(f"Saved raw data locally: {filename}")
        except Exception as e:
            # Log but don't fail - this is a supplementary compliance measure
            logger.warning(f"Failed to save raw data locally: {e}")
    
    def fetch_and_load_all_countries(self) -> Dict[str, Any]:
        """Fetch and load COVID data for all countries"""
        start_time = time.time()
        batch_id = self.generate_batch_id()
        logger.info(f"Starting COVID data fetch with batch ID: {batch_id}")
        
        try:
            # Fetch all countries data
            countries_data, metadata = self.api.fetch_countries_data()
            
            # Process each country record
            succeeded = 0
            failed = 0
            results = []
            
            for country_data in countries_data:
                # Extract entity_id (country name) from data
                entity_id = country_data.get('country', 'unknown')
                
                # Insert raw data without any transformations
                result = self.insert_raw_data(entity_id, country_data, batch_id, metadata)
                results.append(result)
                
                if result['status'] == 'success':
                    succeeded += 1
                else:
                    failed += 1
            
            execution_time_ms = int((time.time() - start_time) * 1000)
            
            # Log to audit table
            log_query = """
                CALL meta.LOG_OPERATION(
                    %(operation_name)s,
                    %(target_table)s,
                    %(record_count)s,
                    %(source_name)s,
                    %(load_ts)s,
                    %(status)s,
                    %(error_message)s,
                    %(execution_time_ms)s
                )
            """
            
            self.snowflake.execute_query(
                log_query,
                {
                    'operation_name': 'FETCH_AND_LOAD_ALL_COUNTRIES',
                    'target_table': 'raw.COVID_RAW',
                    'record_count': succeeded,
                    'source_name': SOURCE_NAME,
                    'load_ts': batch_id,
                    'status': 'PARTIAL_SUCCESS' if failed > 0 else 'SUCCESS',
                    'error_message': f"Failed to process {failed} countries" if failed > 0 else None,
                    'execution_time_ms': execution_time_ms
                }
            )
            
            logger.info(f"Completed COVID data fetch. Succeeded: {succeeded}, Failed: {failed}")
            
            return {
                'status': 'SUCCESS' if failed == 0 else 'PARTIAL_SUCCESS',
                'batch_id': batch_id,
                'succeeded': succeeded,
                'failed': failed,
                'execution_time_ms': execution_time_ms,
                'results': results
            }
            
        except Exception as e:
            execution_time_ms = int((time.time() - start_time) * 1000)
            
            # Log error to audit table
            log_query = """
                CALL meta.LOG_OPERATION(
                    %(operation_name)s,
                    %(target_table)s,
                    %(record_count)s,
                    %(source_name)s,
                    %(load_ts)s,
                    %(status)s,
                    %(error_message)s,
                    %(execution_time_ms)s
                )
            """
            
            self.snowflake.execute_query(
                log_query,
                {
                    'operation_name': 'FETCH_AND_LOAD_ALL_COUNTRIES',
                    'target_table': 'raw.COVID_RAW',
                    'record_count': 0,
                    'source_name': SOURCE_NAME,
                    'load_ts': batch_id,
                    'status': 'ERROR',
                    'error_message': str(e),
                    'execution_time_ms': execution_time_ms
                }
            )
            
            logger.error(f"Failed to fetch and load COVID data: {e}")
            
            return {
                'status': 'ERROR',
                'batch_id': batch_id,
                'error': str(e),
                'execution_time_ms': execution_time_ms
            }
    
    def archive_data(self, days_to_keep: int = 90) -> Dict[str, Any]:
        """Archive data older than the specified number of days"""
        try:
            logger.info(f"Archiving data older than {days_to_keep} days")
            
            from datetime import datetime, timedelta
            cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).strftime('%Y-%m-%d')
            
            result = self.snowflake.execute_procedure(
                'meta.ARCHIVE_DATA', 
                [cutoff_date]
            )
            
            logger.info(f"Archive completed: {result}")
            return result
        except Exception as e:
            logger.error(f"Failed to archive data: {e}")
            raise
    
    def export_to_csv(self, query: str, filename: str) -> str:
        """Export data to local CSV file"""
        try:
            logger.info(f"Exporting data to CSV: {filename}")
            
            # Execute query
            results = self.snowflake.execute_query(query)
            
            if not results:
                logger.warning("No data to export")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(results)
            
            # Ensure output directory exists
            output_dir = os.path.dirname(filename)
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            # Write to CSV
            df.to_csv(filename, index=False)
            
            logger.info(f"Successfully exported data to: {filename}")
            return filename
        except Exception as e:
            logger.error(f"Failed to export data to CSV: {e}")
            raise
    
    def run_pipeline(self) -> Dict[str, Any]:
        """Run the full COVID data pipeline"""
        try:
            # 1. Fetch and load raw data (with no transformations)
            fetch_result = self.fetch_and_load_all_countries()
            
            if fetch_result['status'] == 'ERROR':
                raise Exception(f"Failed to fetch data: {fetch_result.get('error')}")
            
            batch_id = fetch_result['batch_id']
            
            # 2. Flatten JSON data to staging (processed in Python, not JavaScript UDFs)
            flatten_result = self.flatten_handler.flatten_json_to_staging(SOURCE_NAME, batch_id)
            
            if flatten_result['status'] == 'ERROR':
                raise Exception(f"Failed to flatten data: {flatten_result.get('message')}")
            
            # 3. Create dynamic views (processed in Python, not JavaScript UDFs)
            view_result = self.view_handler.create_dynamic_view('shared.COVID_FLATTENED_VIEW', SOURCE_NAME)
            
            # 4. Export sample data for verification
            export_path = f"./exports/{batch_id}"
            Path(export_path).mkdir(parents=True, exist_ok=True)
            
            # Export a sample of raw data
            raw_csv = self.export_to_csv(
                f"""
                SELECT 
                    entity_id, 
                    raw_id,
                    record_hash,
                    ingestion_timestamp
                FROM raw.COVID_RAW
                WHERE load_ts = '{batch_id}'
                LIMIT 100
                """,
                f"{export_path}/covid_raw_sample.csv"
            )
            
            # Export a sample of flattened data
            flattened_csv = self.export_to_csv(
                f"""
                SELECT 
                    entity_id,
                    field_name,
                    field_path,
                    field_value,
                    load_ts
                FROM stg.COVID_STG
                WHERE load_ts = '{batch_id}'
                LIMIT 1000
                """,
                f"{export_path}/covid_flattened_sample.csv"
            )
            
            # Export flattened schema
            schema_csv = self.export_to_csv(
                f"""
                SELECT 
                    field_name,
                    field_path,
                    field_example,
                    data_type,
                    first_seen_ts,
                    last_seen_ts
                FROM meta.SCHEMA_REGISTRY
                WHERE source_name = '{SOURCE_NAME}'
                AND is_active = TRUE
                ORDER BY field_path
                """,
                f"{export_path}/covid_schema.csv"
            )
            
            # Generate compliance verification report
            compliance_report = self.export_to_csv(
                f"""
                WITH raw_counts AS (
                    SELECT COUNT(*) as raw_count 
                    FROM raw.COVID_RAW 
                    WHERE load_ts = '{batch_id}'
                ),
                entity_counts AS (
                    SELECT COUNT(DISTINCT entity_id) as entity_count
                    FROM raw.COVID_RAW
                    WHERE load_ts = '{batch_id}'
                ),
                field_counts AS (
                    SELECT COUNT(*) as field_count
                    FROM stg.COVID_STG
                    WHERE load_ts = '{batch_id}'
                )
                SELECT
                    '{batch_id}' as batch_id,
                    r.raw_count as raw_records,
                    e.entity_count as unique_entities,
                    f.field_count as flattened_fields,
                    f.field_count > 0 as flattening_completed,
                    CURRENT_TIMESTAMP() as report_timestamp
                FROM 
                    raw_counts r, 
                    entity_counts e,
                    field_counts f
                """,
                f"{export_path}/compliance_report.csv"
            )
            
            # Export audit log
            audit_csv = self.export_to_csv(
                f"""
                SELECT 
                    load_ts,
                    table_name,
                    record_count,
                    process_name,
                    process_status,
                    error_message,
                    execution_time_ms,
                    load_time,
                    user_name
                FROM meta.INGEST_AUDIT_LOG
                WHERE load_ts = '{batch_id}'
                ORDER BY load_time
                """,
                f"{export_path}/audit_log.csv"
            )
            
            logger.info("COVID data pipeline completed successfully")
            
            return {
                'status': 'SUCCESS',
                'batch_id': batch_id,
                'fetch_result': fetch_result,
                'flatten_result': flatten_result,
                'view_result': view_result,
                'exports': {
                    'raw_sample': raw_csv,
                    'flattened_sample': flattened_csv,
                    'schema': schema_csv,
                    'compliance_report': compliance_report,
                    'audit_log': audit_csv
                }
            }
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise
    
    def close(self) -> None:
        """Close connections"""
        if hasattr(self, 'snowflake'):
            self.snowflake.close()


def main():
    """Main entry point with dynamic schema support"""
    # Create directories if they don't exist
    Path("logs").mkdir(exist_ok=True)
    Path("exports").mkdir(exist_ok=True)
    Path("compliance_archive").mkdir(exist_ok=True)
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='COVID-19 Data Pipeline (Pure Python, March 2025)')
    parser.add_argument('command', choices=['run', 'fetch', 'flatten', 'view', 'archive', 'export'], 
                      help='Command to execute')
    parser.add_argument('--batch-id', help='Batch ID for processing')
    parser.add_argument('--view-name', help='Name for dynamic view creation', default='shared.COVID_VIEW')
    parser.add_argument('--query', help='SQL query for export')
    parser.add_argument('--output', help='Output filename for export')
    parser.add_argument('--days', type=int, default=90, help='Days of data to keep (for archive)')
    
    args = parser.parse_args()
    
    pipeline = CovidDataPipeline()
    
    try:
        if args.command == 'fetch':
            result = pipeline.fetch_and_load_all_countries()
            print(json.dumps(result, indent=2))
            
        elif args.command == 'flatten':
            batch_id = args.batch_id
            if not batch_id:
                print("Error: flatten command requires --batch-id")
                sys.exit(1)
                
            result = pipeline.flatten_handler.flatten_json_to_staging(SOURCE_NAME, batch_id)
            print(json.dumps(result, indent=2))
            
        elif args.command == 'view':
            result = pipeline.view_handler.create_dynamic_view(args.view_name, SOURCE_NAME)
            print(json.dumps(result, indent=2))
            
        elif args.command == 'archive':
            result = pipeline.archive_data(args.days)
            print(json.dumps(result, indent=2))
            
        elif args.command == 'export':
            if not args.query:
                print("Error: Export requires a SQL query (--query)")
                sys.exit(1)
                
            filename = args.output or f"./exports/export_{pipeline.generate_batch_id()}.csv"
            result = pipeline.export_to_csv(args.query, filename)
            print(f"Exported to: {result}")
            
        else:  # 'run'
            result = pipeline.run_pipeline()
            print(json.dumps(result, indent=2))
            
    except Exception as e:
        logger.error(f"Command execution failed: {e}")
        sys.exit(1)
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
COVID-19 Data Pipeline - Enhanced Python Client
Created: March 2025
Last Updated: March 30, 2025

This script runs on a local machine and handles all dynamic processing
that was previously in JavaScript UDFs within Snowflake. It interacts directly
with the disease.sh API to fetch COVID data and handles schema discovery,
flattening, and view creation - all from the local machine.

Requirements:
- Python 3.8+
- snowflake-connector-python
- requests
- python-dotenv
- pandas
"""

import os
import sys
import json
import time
import hashlib
import logging
import argparse
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple, Set

import requests
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/covid_pipeline_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('covid_pipeline')

# Snowflake connection configuration
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USERNAME'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'compute_wh'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'covid'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'raw'),
    'role': os.getenv('SNOWFLAKE_ROLE', 'sysadmin')
}

# External COVID API configuration
COVID_API_ENDPOINT = os.getenv('COVID_API_ENDPOINT', 'https://disease.sh/v3/covid-19')
SOURCE_NAME = 'disease.sh'  # Source identifier


class SnowflakeClient:
    """Client for interacting with Snowflake"""
    
    def __init__(self, config: Dict[str, str]):
        """Initialize Snowflake connection"""
        self.config = config
        self.conn = None
        self.connect()
    
    def connect(self) -> None:
        """Establish connection to Snowflake"""
        try:
            self.conn = snowflake.connector.connect(
                user=self.config['user'],
                password=self.config['password'],
                account=self.config['account'],
                warehouse=self.config['warehouse'],
                database=self.config['database'],
                schema=self.config['schema'],
                role=self.config['role']
            )
            logger.info("Successfully connected to Snowflake!")
        except snowflake.connector.errors.DatabaseError as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            sys.exit(1)
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results"""
        try:
            cursor = self.conn.cursor(snowflake.connector.DictCursor)
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            results = cursor.fetchall()
            cursor.close()
            return results
        except snowflake.connector.errors.DatabaseError as e:
            logger.error(f"Error executing SQL query: {e}")
            logger.debug(f"Query: {query}")
            logger.debug(f"Params: {params}")
            raise
    
    def execute_script(self, script: str) -> None:
        """Execute a multi-statement SQL script"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(script)
            cursor.close()
            logger.info("Successfully executed SQL script")
        except snowflake.connector.errors.DatabaseError as e:
            logger.error(f"Error executing SQL script: {e}")
            raise
    
    def execute_procedure(self, procedure: str, params: List[Any]) -> Dict[str, Any]:
        """Execute a stored procedure and return results"""
        try:
            cursor = self.conn.cursor(snowflake.connector.DictCursor)
            result = cursor.execute(f"CALL {procedure}({', '.join(['%s'] * len(params))})", params)
            row = result.fetchone()
            cursor.close()
            return dict(row) if row else {}
        except snowflake.connector.errors.DatabaseError as e:
            logger.error(f"Error executing procedure {procedure}: {e}")
            raise
    
    def close(self) -> None:
        """Close Snowflake connection"""
        if self.conn:
            try:
                self.conn.close()
                logger.info("Snowflake connection closed.")
            except:
                pass


class CovidApiClient:
    """Client for interacting with COVID-19 API"""
    
    def __init__(self, base_url: str):
        """Initialize API client"""
        self.base_url = base_url
        self.headers = {
            'User-Agent': 'Covid-Data-Pipeline/1.0 (compliance@example.org)',
            'Accept': 'application/json'
        }
    
    def fetch_countries_data(self) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Fetch COVID data for all countries and return with metadata"""
        endpoint = f"{self.base_url}/countries"
        logger.info(f"Fetching data from: {endpoint}")
        
        params = {}
        try:
            response = requests.get(endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Create metadata about this request
            metadata = {
                'endpoint': endpoint,
                'params': params,
                'status_code': response.status_code,
                'timestamp': datetime.now().isoformat(),
                'response_time_ms': response.elapsed.total_seconds() * 1000
            }
            
            logger.info(f"Successfully fetched data for {len(data)} countries")
            return data, metadata
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching COVID data: {e}")
            raise
    
    def fetch_country_data(self, country: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Fetch COVID data for a specific country and return with metadata"""
        endpoint = f"{self.base_url}/countries/{country}"
        logger.info(f"Fetching data for country: {country}")
        
        params = {}
        try:
            response = requests.get(endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Create metadata about this request
            metadata = {
                'endpoint': endpoint,
                'params': params,
                'status_code': response.status_code,
                'timestamp': datetime.now().isoformat(),
                'response_time_ms': response.elapsed.total_seconds() * 1000
            }
            
            logger.info(f"Successfully fetched data for {country}")
            return data, metadata
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching COVID data for {country}: {e}")
            raise


class SchemaDiscoveryHandler:
    """Handler for schema discovery and registry"""
    
    def __init__(self, snowflake_client: SnowflakeClient):
        self.snowflake = snowflake_client
    
    def discover_and_register_schema(self, source_name: str, load_ts: str) -> Dict[str, Any]:
        """Discover schema from raw records and register fields"""
        logger.info(f"Discovering schema for {source_name}, batch {load_ts}")
        start_time = time.time()
        
        # Get sample records
        query = """
            SELECT source_record 
            FROM raw.COVID_RAW
            WHERE source_name = %(source_name)s
            AND load_ts = %(load_ts)s
            LIMIT 100
        """
        
        records = self.snowflake.execute_query(
            query, 
            {'source_name': source_name, 'load_ts': load_ts}
        )
        
        if not records:
            logger.warning(f"No records found for schema discovery. Source: {source_name}, Load TS: {load_ts}")
            return {
                'status': 'ERROR',
                'message': 'No records found for schema discovery'
            }
        
        # Process records to discover schema
        discovered_fields = {}
        
        for record in records:
            source_record = record['SOURCE_RECORD']
            self._discover_schema(source_record, "", discovered_fields)
        
        # Register discovered fields
        fields_registered = 0
        
        for field_path, field_info in discovered_fields.items():
            self._register_field(
                source_name=source_name,
                field_name=field_info['name'],
                field_path=field_path,
                field_example=field_info['example'],
                data_type=field_info['type'],
                load_ts=load_ts
            )
            fields_registered += 1
        
        # Log the operation
        execution_time_ms = int((time.time() - start_time) * 1000)
        
        self._log_operation(
            operation_name='DISCOVER_AND_REGISTER_SCHEMA',
            target_table='meta.SCHEMA_REGISTRY',
            record_count=fields_registered,
            source_name=source_name,
            load_ts=load_ts,
            status='SUCCESS',
            execution_time_ms=execution_time_ms
        )
        
        return {
            'status': 'SUCCESS',
            'message': f'Successfully discovered and registered schema',
            'fields_discovered': fields_registered,
            'execution_time_ms': execution_time_ms
        }
    
    def _discover_schema(self, obj: Dict[str, Any], prefix: str, result: Dict[str, Dict[str, Any]]) -> None:
        """Recursively discover schema from a JSON object"""
        for key, value in obj.items():
            path = f"{prefix}.{key}" if prefix else key
            value_type = self._get_json_type(value)
            
            if value_type == 'object' and value is not None:
                # Recursively process nested objects
                self._discover_schema(value, path, result)
            elif value_type == 'array' and value:
                # For arrays, process each item type
                if isinstance(value[0], dict):
                    # Array of objects - append index to path
                    for i, item in enumerate(value[:5]):  # Process up to 5 items
                        self._discover_schema(item, f"{path}[{i}]", result)
                else:
                    # Array of primitives - just register the array itself
                    result[path] = {
                        'name': key,
                        'type': 'ARRAY',
                        'example': str(value)[:255]
                    }
            else:
                # Register leaf node
                result[path] = {
                    'name': key,
                    'type': value_type.upper(),
                    'example': str(value)[:255] if value is not None else 'NULL'
                }
    
    def _get_json_type(self, value: Any) -> str:
        """Get JSON data type as string"""
        if value is None:
            return 'null'
        
        if isinstance(value, dict):
            return 'object'
        elif isinstance(value, list):
            return 'array'
        elif isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, int):
            return 'number'
        elif isinstance(value, float):
            return 'number'
        else:
            return 'string'
    
    def _register_field(self, source_name: str, field_name: str, field_path: str, 
                       field_example: str, data_type: str, load_ts: str) -> None:
        """Register a field in the schema registry"""
        query = """
            MERGE INTO meta.SCHEMA_REGISTRY target
            USING (SELECT %(source_name)s as source_name, %(field_name)s as field_name, %(field_path)s as field_path) source
            ON target.source_name = source.source_name AND target.field_path = source.field_path
            WHEN MATCHED THEN
                UPDATE SET