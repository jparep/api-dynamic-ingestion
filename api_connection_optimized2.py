#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Snowflake Data Ingestion with Python - Optimized for March 2025 Snowflake Environment
Created: March 2025
Last Updated: March 30, 2025

This script extracts data from a source (API/files) and loads it into Snowflake,
implementing complementary functionality to the existing Snowflake stored procedures.
It's designed to work with the enhanced COVID Data Pipeline infrastructure in Snowflake,
interfacing with the data quality framework, archive processes, and monitoring systems.
"""

import os
import json
import hashlib
import uuid
import datetime
import re
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from typing import Dict, List, Any, Optional, Set, Tuple
from dotenv import load_dotenv
import requests
import logging
import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("snowflake_ingest.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Snowflake connection parameters
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'COVID')

# Configure sensitive data patterns for PHI detection
PHI_PATTERNS = [
    r'\d{3}-\d{2}-\d{4}',                              # SSN
    r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', # Email
    r'\(\d{3}\)\s*\d{3}-\d{4}',                        # Phone (xxx) xxx-xxxx
    r'\d{3}-\d{3}-\d{4}',                              # Phone xxx-xxx-xxxx
    r'\d{4}[\s-]\d{4}[\s-]\d{4}[\s-]\d{4}',            # Credit card
    r'\d{1,2}/\d{1,2}/\d{4}'                           # Date of birth
]

class SnowflakeIngestion:
    """Class to handle data extraction and ingestion into Snowflake"""
    
    def __init__(self):
        """Initialize the Snowflake connection"""
        self.conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            role=SNOWFLAKE_ROLE,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE
        )
        self.cursor = self.conn.cursor()
        logger.info(f"Connected to Snowflake: {SNOWFLAKE_DATABASE}")
        
        # Check tables exist and run validation
        self._validate_snowflake_environment()
        
    def __del__(self):
        """Close connection when object is destroyed"""
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            logger.info("Snowflake connection closed")
            
    def _validate_snowflake_environment(self):
        """Validate that required Snowflake objects exist"""
        try:
            # Check required schemas
            schemas = ['RAW3', 'STG', 'META', 'QUALITY', 'ARCHIVE']
            for schema in schemas:
                result = self.execute_query(f"SELECT COUNT(*) as count FROM information_schema.schemata WHERE schema_name = '{schema}'")
                if result[0]['COUNT'] == 0:
                    logger.warning(f"Schema {schema} does not exist in database {SNOWFLAKE_DATABASE}")
            
            # Check required tables
            tables = {
                'RAW3': ['COVID_COUNTRY_RAW'],
                'STG': ['COVID_COUNTRY_STG'],
                'META': ['INGEST_AUDIT_LOG', 'PERFORMANCE_METRICS', 'SCHEMA_CHANGE_LOG'],
                'QUALITY': ['DATA_QUALITY_RULES', 'DATA_QUALITY_RESULTS'],
                'ARCHIVE': ['COVID_COUNTRY_ARCHIVE', 'COVID_COUNTRY_RAW_ARCHIVE']
            }
            
            for schema, schema_tables in tables.items():
                for table in schema_tables:
                    result = self.execute_query(
                        f"SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table}'"
                    )
                    if result[0]['COUNT'] == 0:
                        logger.warning(f"Table {schema}.{table} does not exist")
            
            # Check data quality rules exist
            result = self.execute_query("SELECT COUNT(*) as count FROM quality.DATA_QUALITY_RULES")
            if result[0]['COUNT'] == 0:
                logger.warning("No data quality rules defined in quality.DATA_QUALITY_RULES")
                
            logger.info("Snowflake environment validation completed")
        except Exception as e:
            logger.error(f"Error validating Snowflake environment: {str(e)}")
            # Non-fatal error - continue execution
            
    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute a query and return results as a list of dictionaries"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
                
            if self.cursor.description:
                columns = [col[0] for col in self.cursor.description]
                return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            return []
        except Exception as e:
            logger.error(f"Query execution error: {str(e)}")
            logger.error(f"Query: {query}")
            if params:
                logger.error(f"Params: {params}")
            raise
            
    def fetch_json_data(self, url: str) -> List[Dict]:
        """Fetch JSON data from an API endpoint"""
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Log the API response
            logger.info(f"API Response: {response.status_code} from {url}")
            
            # Parse JSON response
            data = response.json()
            
            # If the response is a dictionary, convert to list
            if isinstance(data, dict):
                if 'results' in data:
                    return data['results']
                elif 'data' in data:
                    return data['data']
                else:
                    return [data]
            
            return data
        except Exception as e:
            logger.error(f"Error fetching data from {url}: {str(e)}")
            raise
            
    def read_json_file(self, file_path: str) -> List[Dict]:
        """Read data from a local JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # If the data is a dictionary, convert to list
            if isinstance(data, dict):
                if 'results' in data:
                    return data['results']
                elif 'data' in data:
                    return data['data']
                else:
                    return [data]
                    
            return data
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            raise
    
    def generate_record_hash(self, record: Dict) -> str:
        """Generate a hash for deduplication"""
        record_str = json.dumps(record, sort_keys=True)
        return hashlib.md5(record_str.encode('utf-8')).hexdigest()
        
    def detect_phi(self, record: Dict) -> bool:
        """Detect if a record contains PHI (Protected Health Information)"""
        record_str = json.dumps(record)
        
        for pattern in PHI_PATTERNS:
            if re.search(pattern, record_str):
                return True
                
        return False
        
    def discover_schema(self, records: List[Dict], source_name: str, source_table: str) -> Dict:
        """
        Analyze records to discover schema structure and data types.
        Similar to the Snowflake JavaScript UDF but running locally.
        """
        start_time = datetime.datetime.now()
        logger.info(f"Starting schema discovery for {source_name}.{source_table}")
        
        column_map = {}
        
        # Extract fields and data types from records
        for record in records:
            self._extract_fields(record, column_map)
            
        # Convert results to schema definition
        schema_definition = {}
        for field, info in column_map.items():
            # Determine most specific type
            data_type = 'VARIANT'
            types = list(info['types'])
            
            if 'NUMBER' in types:
                data_type = 'NUMBER'
            elif 'STRING' in types:
                data_type = 'STRING'
            elif 'BOOLEAN' in types:
                data_type = 'BOOLEAN'
            elif 'ARRAY' in types:
                data_type = 'ARRAY'
                
            schema_definition[field] = {
                'dataType': data_type,
                'frequency': info['count'] / len(records),
                'example': info['example']
            }
        
        # Check for existing schema
        existing_schema = self._check_existing_schema(source_name, source_table)
        
        if existing_schema:
            # Update existing schema
            schema_id = existing_schema['schema_id']
            existing_schema_def = json.loads(existing_schema['schema_definition']) 
            
            # Compare and log schema changes
            existing_fields = set(existing_schema_def.keys())
            new_fields = set(schema_definition.keys())
            
            # Find added/updated/removed fields
            added_fields = new_fields - existing_fields
            removed_fields = existing_fields - new_fields
            
            # Find updated fields (type changes)
            updated_fields = set()
            for field in new_fields.intersection(existing_fields):
                if existing_schema_def[field]['dataType'] != schema_definition[field]['dataType']:
                    updated_fields.add(field)
            
            if added_fields or updated_fields or removed_fields:
                # Set current schema as inactive
                self.execute_query(
                    "UPDATE meta.SOURCE_SCHEMA_REGISTRY SET is_current = FALSE, last_updated = CURRENT_TIMESTAMP() WHERE schema_id = %s",
                    (schema_id,)
                )
                
                # Insert new schema version
                new_schema_sql = """
                INSERT INTO meta.SOURCE_SCHEMA_REGISTRY 
                (source_name, source_table, schema_definition, is_current)
                VALUES (%s, %s, %s, TRUE)
                """
                new_schema_id = self.execute_query(
                    new_schema_sql,
                    (source_name, source_table, json.dumps(schema_definition))
                )[0]['SCHEMA_ID']
                
                # Log schema changes
                self._log_schema_changes(new_schema_id, source_table, schema_definition, 
                                        added_fields, updated_fields, removed_fields, 
                                        existing_schema_def)
                
                logger.info(f"Schema updated with {len(added_fields)} added, {len(updated_fields)} updated, "
                           f"and {len(removed_fields)} removed fields")
                return {
                    "status": "UPDATED",
                    "message": "Schema updated with changes detected",
                    "added_fields": list(added_fields),
                    "updated_fields": list(updated_fields),
                    "removed_fields": list(removed_fields),
                    "schema_id": new_schema_id
                }
            else:
                logger.info("Schema analyzed and no changes detected")
                return {
                    "status": "UNCHANGED",
                    "message": "Schema analyzed and no changes detected",
                    "schema_id": schema_id
                }
        else:
            # Insert new schema
            new_schema_sql = """
            INSERT INTO meta.SOURCE_SCHEMA_REGISTRY 
            (source_name, source_table, schema_definition, is_current)
            VALUES (%s, %s, %s, TRUE)
            """
            schema_id = self.execute_query(
                new_schema_sql,
                (source_name, source_table, json.dumps(schema_definition))
            )[0]['SCHEMA_ID']
            
            # Create initial column mappings
            for field, info in schema_definition.items():
                target_column = field.replace('.', '_').lower()
                data_type = info['dataType']
                
                mapping_sql = """
                INSERT INTO meta.COLUMN_MAPPING 
                (schema_id, source_column, target_column, data_type, transformation_rule, is_sensitive)
                VALUES (%s, %s, %s, %s, %s, FALSE)
                """
                self.execute_query(
                    mapping_sql,
                    (schema_id, field, target_column, data_type, f"source_record:{field}::{data_type}")
                )
            
            logger.info(f"New schema discovered with {len(schema_definition)} fields")
            return {
                "status": "CREATED",
                "message": "New schema discovered and registered",
                "field_count": len(schema_definition),
                "schema_id": schema_id
            }
    
    def _extract_fields(self, obj: Any, column_map: Dict, prefix: str = '') -> None:
        """Recursively extract fields and their data types from a nested object"""
        if not isinstance(obj, dict):
            return
            
        for key, value in obj.items():
            full_key = f"{prefix}.{key}" if prefix else key
            
            if value is None:
                # Handle null values
                if full_key not in column_map:
                    column_map[full_key] = {
                        'count': 1,
                        'types': {'NULL'},
                        'example': None
                    }
                else:
                    column_map[full_key]['count'] += 1
                    column_map[full_key]['types'].add('NULL')
            elif isinstance(value, dict):
                # Handle nested objects
                if full_key not in column_map:
                    column_map[full_key] = {
                        'count': 1,
                        'types': {'OBJECT'},
                        'example': 'nested-object'
                    }
                else:
                    column_map[full_key]['count'] += 1
                    column_map[full_key]['types'].add('OBJECT')
                self._extract_fields(value, column_map, full_key)
            elif isinstance(value, list):
                # Handle arrays
                if full_key not in column_map:
                    column_map[full_key] = {
                        'count': 1,
                        'types': {'ARRAY'},
                        'example': value[:2] if value else []
                    }
                else:
                    column_map[full_key]['count'] += 1
                    column_map[full_key]['types'].add('ARRAY')
            else:
                # Handle primitive types
                type_name = type(value).__name__.upper()
                if type_name == 'INT' or type_name == 'FLOAT':
                    type_name = 'NUMBER'
                elif type_name == 'STR':
                    type_name = 'STRING'
                elif type_name == 'BOOL':
                    type_name = 'BOOLEAN'
                
                if full_key not in column_map:
                    column_map[full_key] = {
                        'count': 1,
                        'types': {type_name},
                        'example': value
                    }
                else:
                    column_map[full_key]['count'] += 1
                    column_map[full_key]['types'].add(type_name)
    
    def _check_existing_schema(self, source_name: str, source_table: str) -> Optional[Dict]:
        """Check if a schema already exists for the source/table"""
        query = """
        SELECT schema_id, schema_definition
        FROM meta.SOURCE_SCHEMA_REGISTRY
        WHERE source_name = %s
        AND source_table = %s
        AND is_current = TRUE
        """
        results = self.execute_query(query, (source_name, source_table))
        return results[0] if results else None
    
    def _log_schema_changes(self, schema_id: int, table_name: str, 
                           schema_definition: Dict, added_fields: Set[str],
                           updated_fields: Set[str], removed_fields: Set[str],
                           existing_schema: Dict) -> None:
        """Log schema changes to the metadata tracking tables"""
        # Log added fields
        for field in added_fields:
            add_sql = """
            INSERT INTO meta.SCHEMA_CHANGE_LOG 
            (schema_id, column_name, data_type, table_name, change_type)
            VALUES (%s, %s, %s, %s, 'ADDED')
            """
            self.execute_query(
                add_sql,
                (schema_id, field, schema_definition[field]['dataType'], table_name)
            )
            
            # Add to column mapping
            map_sql = """
            INSERT INTO meta.COLUMN_MAPPING 
            (schema_id, source_column, target_column, data_type, transformation_rule, is_sensitive)
            VALUES (%s, %s, %s, %s, %s, FALSE)
            """
            target_column = field.replace('.', '_').lower()
            data_type = schema_definition[field]['dataType']
            self.execute_query(
                map_sql,
                (schema_id, field, target_column, data_type, f"source_record:{field}::{data_type}")
            )
        
        # Log updated fields
        for field in updated_fields:
            mod_sql = """
            INSERT INTO meta.SCHEMA_CHANGE_LOG 
            (schema_id, column_name, data_type, table_name, change_type)
            VALUES (%s, %s, %s, %s, 'MODIFIED')
            """
            self.execute_query(
                mod_sql,
                (schema_id, field, schema_definition[field]['dataType'], table_name)
            )
            
            # Update column mapping
            update_sql = """
            UPDATE meta.COLUMN_MAPPING
            SET data_type = %s,
                transformation_rule = %s,
                last_updated = CURRENT_TIMESTAMP()
            WHERE schema_id = %s
            AND source_column = %s
            """
            data_type = schema_definition[field]['dataType']
            self.execute_query(
                update_sql,
                (data_type, f"source_record:{field}::{data_type}", schema_id, field)
            )
        
        # Log removed fields
        for field in removed_fields:
            rem_sql = """
            INSERT INTO meta.SCHEMA_CHANGE_LOG 
            (schema_id, column_name, data_type, table_name, change_type)
            VALUES (%s, %s, %s, %s, 'REMOVED')
            """
            self.execute_query(
                rem_sql,
                (schema_id, field, existing_schema[field]['dataType'], table_name)
            )
    
    def ingest_to_raw_table(self, records: List[Dict], source_name: str, source_table: str, api_endpoint: str = None) -> Dict:
        """Ingest records into the raw3.COVID_COUNTRY_RAW table"""
        start_time = datetime.datetime.now()
        logger.info(f"Starting ingestion of {len(records)} records to raw3.COVID_COUNTRY_RAW")
        
        load_ts = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        batch_id = str(uuid.uuid4())
        
        # Create dataframe for loading
        raw_records = []
        
        for record in records:
            # Generate record hash for deduplication
            record_hash = self.generate_record_hash(record)
            
            # Extract country (if available)
            country = record.get('country', record.get('Country', record.get('location', record.get('Location', 'unknown'))))
            
            # Prepare record for insertion - match the exact schema in Snowflake
            raw_records.append({
                'country': country,
                'source_record': json.dumps(record),
                'record_hash': record_hash,
                'load_ts': load_ts,
                'source': source_name,
                'api_response_code': '200',
                'api_endpoint': api_endpoint
            })
        
        # Check for duplicates
        existing_hashes = set()
        if raw_records:
            hash_list = [f"'{r['record_hash']}'" for r in raw_records]
            query = f"""
            SELECT record_hash 
            FROM raw3.COVID_COUNTRY_RAW 
            WHERE record_hash IN ({','.join(hash_list)})
            """
            results = self.execute_query(query)
            existing_hashes = {r['RECORD_HASH'] for r in results}
        
        # Filter out duplicates
        unique_records = [r for r in raw_records if r['record_hash'] not in existing_hashes]
        duplicate_count = len(raw_records) - len(unique_records)
        
        if not unique_records:
            logger.info(f"No new records to ingest, {duplicate_count} duplicates skipped")
            return {
                "status": "SUCCESS",
                "message": "No new records to ingest",
                "duplicates_skipped": duplicate_count,
                "records_loaded": 0,
                "batch_id": batch_id,
                "load_ts": load_ts
            }
        
        # Convert to DataFrame for bulk loading
        df = pd.DataFrame(unique_records)
        
        # Use Snowflake's write_pandas for efficient loading with type hints to match schema
        success, num_chunks, num_rows, _ = write_pandas(
            conn=self.conn,
            df=df,
            table_name='COVID_COUNTRY_RAW',
            schema='RAW3',
            database=SNOWFLAKE_DATABASE,
            quote_identifiers=False
        )
        
        # Log ingestion in audit table
        audit_sql = """
        INSERT INTO meta.INGEST_AUDIT_LOG (
            load_ts,
            table_name,
            record_count,
            duplicates_skipped,
            source,
            process_name,
            process_status,
            execution_time_ms
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        self.execute_query(
            audit_sql,
            (
                batch_load_ts,
                source_table,
                processed_count,
                duplicate_count,
                source_name,
                'PYTHON_PROCESS_SCRIPT',
                'COMPLETED_WITH_ERRORS' if error_count > 0 else 'SUCCESS',
                execution_time
            )
        )
        
        # Record performance metrics
        perf_sql = """
        INSERT INTO meta.PERFORMANCE_METRICS (
            process_name,
            execution_time_ms,
            rows_processed
        )
        VALUES (%s, %s, %s)
        """
        
        self.execute_query(
            perf_sql,
            (
                'PYTHON_PROCESS_SCRIPT',
                execution_time,
                processed_count
            )
        )
        
        logger.info(f"Processing complete: {processed_count} records processed, "
                   f"{duplicate_count} duplicates skipped, {error_count} errors")
                   
        return {
            "status": "SUCCESS",
            "records_processed": processed_count,
            "duplicates_skipped": duplicate_count,
            "errors": error_count,
            "execution_time_ms": execution_time,
            "source": source_name,
            "table": source_table,
            "load_ts": batch_load_ts
        }
        
    def _extract_value_from_path(self, obj: Dict, path: str) -> Any:
        """Extract a value from a nested dictionary using a dot-notation path"""
        keys = path.split('.')
        value = obj
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
                
        return value
    
    def ensure_staging_table_exists(self, source_name: str, source_table: str) -> Dict:
        """
        Ensure the staging table exists for the given source/table.
        Similar to meta.PROC_GENERATE_STAGING_TABLE
        """
        logger.info(f"Ensuring staging table exists for {source_name}.{source_table}")
        
        # Get schema ID
        schema_query = """
        SELECT schema_id
        FROM meta.SOURCE_SCHEMA_REGISTRY
        WHERE source_name = %s
        AND source_table = %s
        AND is_current = TRUE
        """
        
        schema_results = self.execute_query(schema_query, (source_name, source_table))
        
        if not schema_results:
            error_msg = f"No schema found for source: {source_name}, table: {source_table}"
            logger.error(error_msg)
            return {"status": "ERROR", "message": error_msg}
            
        schema_id = schema_results[0]['SCHEMA_ID']
        
        # Form staging table name
        staging_table = f"stg.{source_table.replace('-', '_')}_STG"
        
        # Check if table exists
        table_check_query = """
        SELECT COUNT(*) as count
        FROM information_schema.tables
        WHERE table_schema = 'STG'
        AND table_name = %s
        """
        
        table_check_results = self.execute_query(
            table_check_query, 
            (source_table.replace('-', '_') + '_STG',)
        )
        
        table_exists = table_check_results[0]['COUNT'] > 0
        
        # Get column mappings
        mapping_query = """
        SELECT target_column, data_type
        FROM meta.COLUMN_MAPPING
        WHERE schema_id = %s
        AND enabled = TRUE
        """
        
        mapping_results = self.execute_query(mapping_query, (schema_id,))
        
        # Generate column definitions
        column_definitions = []
        for row in mapping_results:
            data_type = row['DATA_TYPE']
            snowflake_type = 'VARIANT'
            
            if data_type == 'NUMBER':
                snowflake_type = 'NUMBER'
            elif data_type == 'STRING':
                snowflake_type = 'STRING'
            elif data_type == 'BOOLEAN':
                snowflake_type = 'BOOLEAN'
            
            column_definitions.append(f"{row['TARGET_COLUMN']} {snowflake_type}")
        
        if not table_exists:
            # Create new table
            create_sql = f"""
            CREATE TABLE {staging_table} (
                stg_id NUMBER IDENTITY START 1 INCREMENT 1,
                {', '.join(column_definitions)},
                load_ts STRING NOT NULL,
                source STRING NOT NULL,
                source_table STRING NOT NULL,
                raw_id NUMBER NOT NULL,
                processed_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
                quality_check_passed BOOLEAN,
                contains_phi BOOLEAN DEFAULT FALSE,
                CONSTRAINT pk_{source_table.replace('-', '_')}_stg PRIMARY KEY (stg_id)
            )
            DATA_RETENTION_TIME_IN_DAYS = 14
            CLUSTER BY (load_ts, country)
            COMMENT = 'Dynamically generated staging table for {source_table}'
            """
            
            self.execute_query(create_sql)
            
            logger.info(f"Created staging table {staging_table} with {len(column_definitions)} columns")
            return {
                "status": "CREATED",
                "message": "Staging table created successfully",
                "table_name": staging_table,
                "column_count": len(column_definitions)
            }
        else:
            # Get existing columns
            columns_query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'STG'
            AND table_name = %s
            """
            
            columns_result = self.execute_query(
                columns_query, 
                (source_table.replace('-', '_') + '_STG',)
            )
            
            existing_columns = {row['COLUMN_NAME'].upper() for row in columns_result}
            
            # Add missing columns
            columns_added = 0
            
            for row in mapping_results:
                target_column = row['TARGET_COLUMN'].upper()
                
                if target_column not in existing_columns:
                    data_type = row['DATA_TYPE']
                    snowflake_type = 'VARIANT'
                    
                    if data_type == 'NUMBER':
                        snowflake_type = 'NUMBER'
                    elif data_type == 'STRING':
                        snowflake_type = 'STRING'
                    elif data_type == 'BOOLEAN':
                        snowflake_type = 'BOOLEAN'
                    
                    alter_sql = f"""
                    ALTER TABLE {staging_table} 
                    ADD COLUMN {row['TARGET_COLUMN']} {snowflake_type}
                    """
                    
                    self.execute_query(alter_sql)
                    columns_added += 1
            
            logger.info(f"Updated staging table {staging_table}, added {columns_added} new columns")
            return {
                "status": "UPDATED",
                "message": "Staging table updated successfully",
                "table_name": staging_table,
                "columns_added": columns_added
            }

def main():
    """Main function to run the ETL process - optimized for March 2025 Snowflake environment"""
    try:
        # Initialize Snowflake ingestion
        ingest = SnowflakeIngestion()
        
        # Define source information
        source_name = "JHU"  # Example source
        source_table = "covid19-daily"  # Example table name
        api_endpoint = "https://api.example.com/covid19/daily"  # Example API endpoint
        
        # Get command line arguments if available
        parser = argparse.ArgumentParser(description='COVID-19 Data Ingestion Tool')
        parser.add_argument('--source', help='Source name identifier')
        parser.add_argument('--table', help='Source table identifier')
        parser.add_argument('--api', help='API endpoint URL')
        parser.add_argument('--file', help='Local file path for data')
        parser.add_argument('--countries', help='Comma-separated list of countries to process')
        parser.add_argument('--mode', choices=['full', 'extract_only', 'process_only'], 
                            default='full', help='Processing mode')
        args = parser.parse_args()
        
        # Override defaults with command line arguments if provided
        if args.source:
            source_name = args.source
        if args.table:
            source_table = args.table
        if args.api:
            api_endpoint = args.api
        
        # Select data source based on arguments and mode
        if args.mode != 'process_only':
            # Option 1: Fetch data from API
            if not args.file:
                try:
                    logger.info(f"Fetching data from API: {api_endpoint}")
                    
                    # If specific countries requested, fetch only those
                    if args.countries:
                        countries = args.countries.split(',')
                        all_records = []
                        for country in countries:
                            country_api = f"{api_endpoint}?country={country.strip()}"
                            logger.info(f"Fetching data for country: {country} from {country_api}")
                            country_records = ingest.fetch_json_data(country_api)
                            all_records.extend(country_records)
                        records = all_records
                    else:
                        records = ingest.fetch_json_data(api_endpoint)
                        
                except Exception as e:
                    logger.error(f"Error fetching data from API: {str(e)}")
                    # Fallback to local file if API fails
                    file_path = args.file or "sample_covid_data.json"
                    logger.info(f"Falling back to local file: {file_path}")
                    records = ingest.read_json_file(file_path)
            
            # Option 2: Read from local JSON file
            else:
                file_path = args.file
                logger.info(f"Reading data from local file: {file_path}")
                records = ingest.read_json_file(file_path)
            
            logger.info(f"Read {len(records)} records from source")
            
            # Ingest data to raw table
            ingest_result = ingest.ingest_to_raw_table(records, source_name, source_table, api_endpoint)
            logger.info(f"Ingestion result: {ingest_result['status']}, {ingest_result['records_loaded']} records loaded")
            
            # If extract only mode, stop here
            if args.mode == 'extract_only':
                logger.info("Extract-only mode, skipping processing steps")
                return
                
            load_ts = ingest_result['load_ts']
        else:
            # For process_only mode, use the most recent load_ts
            latest_batch_query = """
            SELECT MAX(load_ts) as latest_batch
            FROM raw3.COVID_COUNTRY_RAW
            WHERE source = %s
            """
            latest_batch = ingest.execute_query(latest_batch_query, (source_name,))
            
            if not latest_batch or not latest_batch[0]['LATEST_BATCH']:
                logger.error("No data batches found for processing")
                return
                
            load_ts = latest_batch[0]['LATEST_BATCH']
            logger.info(f"Process-only mode, using most recent batch: {load_ts}")
        
        # Trigger Snowflake processing using the optimized procedures
        process_result = ingest.trigger_snowflake_processing(load_ts, source_name)
        logger.info(f"Snowflake processing result: {process_result}")
        
        # Trigger data quality checks
        quality_result = ingest.trigger_quality_checks('stg.COVID_COUNTRY_STG', load_ts)
        logger.info(f"Quality checks result: {quality_result}")
        
        # Check if any critical data quality issues
        if quality_result.get('critical_failures', 0) > 0:
            logger.warning(f"Critical data quality issues detected: {quality_result.get('critical_failures')} rules failed")
            
        logger.info("Data pipeline execution completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

def parallel_fetch_countries(api_base_url: str, countries: List[str], max_workers: int = 5) -> List[Dict]:
    """Fetch data for multiple countries in parallel for improved performance"""
    all_records = []
    
    logger.info(f"Fetching data for {len(countries)} countries in parallel")
    
    def fetch_country(country):
        try:
            country_api = f"{api_base_url}?country={country.strip()}"
            logger.info(f"Fetching data for country: {country}")
            response = requests.get(country_api, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # If the response is a dictionary with a results/data key, extract the records
            if isinstance(data, dict):
                if 'results' in data:
                    return data['results']
                elif 'data' in data:
                    return data['data']
                else:
                    return [data]
            
            return data
        except Exception as e:
            logger.error(f"Error fetching data for country {country}: {str(e)}")
            return []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_country = {executor.submit(fetch_country, country): country for country in countries}
        
        for future in as_completed(future_to_country):
            country = future_to_country[future]
            try:
                country_records = future.result()
                if country_records:
                    all_records.extend(country_records)
                    logger.info(f"Added {len(country_records)} records for {country}")
            except Exception as e:
                logger.error(f"Exception processing country {country}: {str(e)}")
    
    logger.info(f"Parallel fetch completed: {len(all_records)} total records")
    return all_records

if __name__ == "__main__":
    main()
.INGEST_AUDIT_LOG (
            load_ts,
            table_name,
            record_count,
            duplicates_skipped,
            source,
            process_name,
            process_status,
            execution_time_ms
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        execution_time = (datetime.datetime.now() - start_time).total_seconds() * 1000
        self.execute_query(
            audit_sql,
            (
                load_ts,
                source_table,
                len(unique_records),
                duplicate_count,
                source_name,
                'PYTHON_INGEST_SCRIPT',
                'SUCCESS' if success else 'ERROR',
                execution_time
            )
        )
        
        logger.info(f"Ingestion complete: {len(unique_records)} records loaded, {duplicate_count} duplicates skipped")
        return {
            "status": "SUCCESS" if success else "ERROR",
            "message": "Data ingested successfully" if success else "Error during ingestion",
            "records_loaded": len(unique_records),
            "duplicates_skipped": duplicate_count,
            "batch_id": batch_id,
            "load_ts": load_ts
        }
    
    def trigger_snowflake_processing(self, load_ts: str, source: str) -> Dict:
        """Trigger the Snowflake stored procedure to process and flatten data
        
        This leverages the existing optimized Snowflake procedure rather than
        duplicating the logic in Python
        """
        try:
            logger.info(f"Triggering Snowflake stored procedure to process batch {load_ts}")
            
            # Call the Snowflake procedure directly
            query = f"""
            CALL meta.PROC_FLATTEN_AND_ARCHIVE(
                '{load_ts}',
                '{source}',
                12
            )
            """
            
            result = self.execute_query(query)
            
            logger.info(f"Snowflake processing completed: {result}")
            return result[0]
        except Exception as e:
            logger.error(f"Error triggering Snowflake processing: {str(e)}")
            return {
                "status": "ERROR",
                "message": f"Error: {str(e)}"
            }
            
    def trigger_quality_checks(self, table_name: str, load_ts: str) -> Dict:
        """Trigger data quality checks in Snowflake
        
        Uses the existing quality.PROC_RUN_DATA_QUALITY_CHECKS procedure
        """
        try:
            logger.info(f"Triggering data quality checks for {table_name}, batch {load_ts}")
            
            # Call the Snowflake quality check procedure
            query = f"""
            CALL quality.PROC_RUN_DATA_QUALITY_CHECKS(
                '{table_name}',
                '{load_ts}'
            )
            """
            
            result = self.execute_query(query)
            
            # Check for critical failures
            critical_failures_query = """
            SELECT COUNT(*) as count
            FROM quality.DATA_QUALITY_RESULTS r
            JOIN quality.DATA_QUALITY_RULES q ON r.rule_id = q.rule_id
            WHERE r.load_ts = %s
            AND r.status = 'FAILED'
            AND q.severity = 'ERROR'
            """
            
            critical_failures = self.execute_query(critical_failures_query, (load_ts,))
            
            if critical_failures[0]['COUNT'] > 0:
                logger.error(f"Critical data quality issues detected: {critical_failures[0]['COUNT']} rules failed")
                
            return {
                "status": "SUCCESS",
                "quality_results": result[0],
                "critical_failures": critical_failures[0]['COUNT']
            }
        except Exception as e:
            logger.error(f"Error triggering quality checks: {str(e)}")
            return {
                "status": "ERROR",
                "message": f"Error: {str(e)}"
            }
    
    def process_raw_data(self, source_name: str, source_table: str, load_ts: str = None, batch_id: str = None, batch_size: int = 10000) -> Dict:
        """Process raw data into the staging table"""
        start_time = datetime.datetime.now()
        logger.info(f"Starting processing of raw data for {source_name}.{source_table}")
        
        # Get schema and mapping information
        schema_query = """
        SELECT sr.schema_id, sr.schema_definition
        FROM meta.SOURCE_SCHEMA_REGISTRY sr
        WHERE sr.source_name = %s
        AND sr.source_table = %s
        AND sr.is_current = TRUE
        """
        schema_results = self.execute_query(schema_query, (source_name, source_table))
        
        if not schema_results:
            error_msg = f"No schema found for source: {source_name}, table: {source_table}"
            logger.error(error_msg)
            return {"status": "ERROR", "message": error_msg}
        
        schema_id = schema_results[0]['SCHEMA_ID']
        
        # Get column mappings
        mapping_query = """
        SELECT source_column, target_column, data_type, transformation_rule
        FROM meta.COLUMN_MAPPING
        WHERE schema_id = %s
        AND enabled = TRUE
        """
        mapping_results = self.execute_query(mapping_query, (schema_id,))
        
        if not mapping_results:
            error_msg = f"No enabled column mappings found for schema ID: {schema_id}"
            logger.error(error_msg)
            return {"status": "ERROR", "message": error_msg}
        
        # Build column mappings structure
        column_mappings = []
        for row in mapping_results:
            column_mappings.append({
                'source': row['SOURCE_COLUMN'],
                'target': row['TARGET_COLUMN'],
                'dataType': row['DATA_TYPE'],
                'transformation': row['TRANSFORMATION_RULE']
            })
        
        # Determine staging table name
        staging_table = f"stg.{source_table.replace('-', '_')}_STG"
        
        # Build WHERE clause for fetching records
        where_clauses = [
            f"source = '{source_name}'", 
            f"source_table = '{source_table}'", 
            "is_processed = FALSE"
        ]
        
        if load_ts:
            where_clauses.append(f"load_ts = '{load_ts}'")
        
        if batch_id:
            where_clauses.append(f"incremental_batch_id = '{batch_id}'")
        
        where_clause = " AND ".join(where_clauses)
        
        # Get records to process - adjusted fields to match the March 2025 schema
        records_query = f"""
        SELECT raw_id, source_record, record_hash
        FROM raw3.COVID_COUNTRY_RAW
        WHERE {where_clause}
        LIMIT {batch_size}
        """
        
        raw_records = self.execute_query(records_query)
        
        if not raw_records:
            logger.info(f"No unprocessed records found for {source_name}.{source_table}")
            return {
                "status": "SUCCESS",
                "message": "No unprocessed records found",
                "records_processed": 0
            }
        
        # Process each record
        processed_count = 0
        error_count = 0
        duplicate_count = 0
        
        batch_load_ts = load_ts or datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        
        for record in raw_records:
            raw_id = record['RAW_ID']
            
            try:
                # Check for existing record to avoid duplicates
                check_query = f"""
                SELECT COUNT(*) as count
                FROM {staging_table}
                WHERE raw_id = {raw_id}
                """
                check_result = self.execute_query(check_query)
                
                if check_result[0]['COUNT'] > 0:
                    duplicate_count += 1
                    continue
                
                # Parse source record
                source_record = json.loads(record['SOURCE_RECORD'])
                
                # Extract values using the transformation rules
                values = {}
                for mapping in column_mappings:
                    source_path = mapping['source']
                    target_column = mapping['target']
                    
                    # Extract value from nested JSON using path
                    value = self._extract_value_from_path(source_record, source_path)
                    
                    # Apply data type conversion
                    data_type = mapping['dataType']
                    if data_type == 'NUMBER' and value is not None:
                        try:
                            value = float(value)
                        except (ValueError, TypeError):
                            value = None
                    elif data_type == 'BOOLEAN' and value is not None:
                        if isinstance(value, str):
                            value = value.lower() in ('true', 'yes', '1')
                        else:
                            value = bool(value)
                    elif data_type != 'ARRAY' and value is not None:
                        value = str(value)
                    
                    values[target_column] = value
                
                # Add metadata columns - match the March 2025 schema
                values['load_ts'] = batch_load_ts
                values['source'] = source_name
                values['raw_id'] = raw_id
                values['processed_flag'] = True
                values['processed_timestamp'] = 'CURRENT_TIMESTAMP'
                values['quality_check_passed'] = True
                
                # Generate SQL for insertion
                columns = ', '.join(values.keys())
                placeholders = ', '.join(['%s'] * len(values))
                insert_sql = f"""
                INSERT INTO {staging_table} ({columns}) 
                VALUES ({placeholders})
                """
                
                # Execute insert
                self.execute_query(insert_sql, tuple(values.values()))
                
                # Mark as processed
                update_sql = """
                UPDATE raw3.COVID_COUNTRY_RAW
                SET is_processed = TRUE,
                    processed_timestamp = CURRENT_TIMESTAMP()
                WHERE raw_id = %s
                """
                self.execute_query(update_sql, (raw_id,))
                
                processed_count += 1
            except Exception as e:
                error_count += 1
                logger.error(f"Error processing record {raw_id}: {str(e)}")
                
                # Log error
                error_sql = """
                INSERT INTO meta.INGEST_AUDIT_LOG (
                    table_name,
                    record_count,
                    source,
                    process_name,
                    process_status,
                    error_message,
                    execution_time_ms
                )
                VALUES (%s, 1, %s, %s, %s, %s, %s)
                """
                execution_time = (datetime.datetime.now() - start_time).total_seconds() * 1000
                self.execute_query(
                    error_sql,
                    (
                        source_table,
                        source_name,
                        'PYTHON_PROCESS_SCRIPT',
                        'ERROR',
                        f"Error processing record {raw_id}: {str(e)}",
                        execution_time
                    )
                )
        
        # Run data quality checks using the existing Snowflake procedure
        # This matches the March 2025 implementation approach
        quality_check_sql = f"""
        CALL quality.PROC_RUN_DATA_QUALITY_CHECKS('{staging_table}', '{batch_load_ts}')
        """
        try:
            self.execute_query(quality_check_sql)
            logger.info(f"Quality checks completed for batch {batch_load_ts}")
        except Exception as e:
            logger.error(f"Error running quality checks: {str(e)}")
            # Non-fatal error, continue processing
        
        # Record summary in audit log
        end_time = datetime.datetime.now()
        execution_time = (end_time - start_time).total_seconds() * 1000
        
        audit_sql = """
        INSERT INTO meta