#!/usr/bin/env python3
"""
COVID-19 Data Pipeline - Python Client (Test Mode)
Created: March 2025
Last Updated: March 30, 2025

TEST VERSION: Extracts only 10 records for connection testing.

This script runs on a local machine and handles dynamic processing,
connecting to the disease.sh API to fetch COVID data and loading it into Snowflake.
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

# Define global variables at module level
TEST_MODE = True  # Enable test mode to limit records
TEST_RECORD_LIMIT = 10  # Number of records to process in test mode

# Debug information for environment variables
print("Current directory:", os.getcwd())
print("Files in directory:", os.listdir())
print("Checking for .env file:", os.path.exists('.env'))

# Load environment variables
load_dotenv(verbose=True)

# Print loaded variables for debugging
print("SNOWFLAKE_USERNAME:", os.getenv('SNOWFLAKE_USER'))
print("SNOWFLAKE_ACCOUNT:", os.getenv('SNOWFLAKE_ACCOUNT'))

# Ensure logs directory exists before setting up logging
Path("logs").mkdir(parents=True, exist_ok=True)

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

# Snowflake connection configuration - Fixed to use SNOWFLAKE_USER not USERNAME
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),  # Using USER instead of USERNAME
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'compute_wh'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'covid'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'raw'),
    'role': os.getenv('SNOWFLAKE_ROLE', 'accountadmin')  # Default to accountadmin
}

# Print config for debugging
print("SNOWFLAKE_CONFIG:", {k: v if k != 'password' else '******' for k, v in SNOWFLAKE_CONFIG.items()})

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
            logger.info("Connecting to Snowflake with config: %s", 
                        {k: v if k != 'password' else '******' for k, v in self.config.items()})
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
    
    def close(self) -> None:
        """Close connections"""
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
    
    def fetch_countries_data(self, limit: Optional[int] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Fetch COVID data for all countries and return with metadata"""
        endpoint = self.base_url  # Use the base URL directly
        logger.info(f"Fetching data from: {endpoint}")
        
        params = {}
        try:
            response = requests.get(endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Limit data if requested (for testing)
            if limit and limit > 0:
                logger.info(f"TEST MODE: Limiting to {limit} records (from {len(data)} available)")
                data = data[:limit]
            
            # Create metadata about this request
            metadata = {
                'endpoint': endpoint,
                'params': params,
                'status_code': response.status_code,
                'timestamp': datetime.now().isoformat(),
                'response_time_ms': response.elapsed.total_seconds() * 1000,
                'test_mode': bool(limit),
                'record_limit': limit
            }
            
            logger.info(f"Successfully fetched data for {len(data)} countries")
            return data, metadata
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching COVID data: {e}")
            raise


class CovidDataPipeline:
    """Simplified COVID-19 data pipeline for testing"""
    
    def __init__(self):
        """Initialize the pipeline"""
        logger.info("Initializing COVID-19 data pipeline (TEST MODE)")
        self.snowflake = SnowflakeClient(SNOWFLAKE_CONFIG)
        self.api = CovidApiClient(COVID_API_ENDPOINT)
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
            
            # First, check if table exists and create it if it doesn't
            self._ensure_tables_exist()
            
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
    
    def _ensure_tables_exist(self):
        """Ensure necessary tables exist - simplified for testing"""
        try:
            # Check if COVID_RAW table exists
            tables = self.snowflake.execute_query("""
                SHOW TABLES LIKE 'COVID_RAW' IN SCHEMA raw
            """)
            
            if not tables:
                logger.info("Creating COVID_RAW table")
                self.snowflake.execute_script("""
                    CREATE TABLE IF NOT EXISTS raw.COVID_RAW (
                        raw_id NUMBER IDENTITY START 1 INCREMENT 1,
                        source_name STRING NOT NULL,
                        entity_id STRING,
                        source_record VARIANT NOT NULL,
                        record_hash STRING NOT NULL,
                        load_ts STRING NOT NULL,
                        ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
                        api_endpoint STRING,
                        api_params VARIANT,
                        CONSTRAINT pk_covid_raw PRIMARY KEY (raw_id)
                    );
                """)
                
                logger.info("COVID_RAW table created successfully")
        except Exception as e:
            logger.error(f"Error ensuring tables exist: {e}")
            raise
    
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
        """Fetch and load COVID data for all countries (limited in test mode)"""
        start_time = time.time()
        batch_id = self.generate_batch_id()
        logger.info(f"Starting COVID data fetch with batch ID: {batch_id}")
        
        try:
            # Fetch countries data (limited in test mode)
            limit = TEST_RECORD_LIMIT if TEST_MODE else None
            countries_data, metadata = self.api.fetch_countries_data(limit=limit)
            
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
            
            # Simple log to console instead of using Snowflake log procedures (for testing)
            logger.info(f"Completed COVID data fetch. Succeeded: {succeeded}, Failed: {failed}")
            
            return {
                'status': 'SUCCESS' if failed == 0 else 'PARTIAL_SUCCESS',
                'batch_id': batch_id,
                'succeeded': succeeded,
                'failed': failed,
                'execution_time_ms': execution_time_ms,
                'test_mode': TEST_MODE,
                'record_limit': TEST_RECORD_LIMIT,
                'results': results
            }
            
        except Exception as e:
            execution_time_ms = int((time.time() - start_time) * 1000)
            logger.error(f"Failed to fetch and load COVID data: {e}")
            
            return {
                'status': 'ERROR',
                'batch_id': batch_id,
                'error': str(e),
                'execution_time_ms': execution_time_ms
            }
    
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
    
    def run_test(self) -> Dict[str, Any]:
        """Run a simple test of the data pipeline"""
        try:
            # 1. Fetch and load raw data (limited to 10 records)
            fetch_result = self.fetch_and_load_all_countries()
            
            if fetch_result['status'] == 'ERROR':
                raise Exception(f"Failed to fetch data: {fetch_result.get('error')}")
            
            batch_id = fetch_result['batch_id']
            
            # 2. Export sample of raw data
            export_path = f"./exports/{batch_id}"
            Path(export_path).mkdir(parents=True, exist_ok=True)
            
            raw_csv = self.export_to_csv(
                f"""
                SELECT 
                    entity_id, 
                    raw_id,
                    record_hash,
                    ingestion_timestamp
                FROM raw.COVID_RAW
                WHERE load_ts = '{batch_id}'
                """,
                f"{export_path}/covid_raw_sample.csv"
            )
            
            logger.info("COVID data pipeline test completed successfully")
            
            return {
                'status': 'SUCCESS',
                'batch_id': batch_id,
                'fetch_result': fetch_result,
                'exports': {
                    'raw_sample': raw_csv
                },
                'test_mode': TEST_MODE,
                'record_limit': TEST_RECORD_LIMIT
            }
        except Exception as e:
            logger.error(f"Pipeline test failed: {e}")
            raise
    
    def close(self) -> None:
        """Close connections"""
        if hasattr(self, 'snowflake'):
            self.snowflake.close()


def main():
    """Main entry point - simplified for testing"""
    # Create directories if they don't exist
    Path("logs").mkdir(exist_ok=True)
    Path("exports").mkdir(exist_ok=True)
    Path("compliance_archive").mkdir(exist_ok=True)
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='COVID-19 Data Pipeline (Test Mode)')
    parser.add_argument('command', choices=['test', 'fetch', 'export'], 
                      help='Command to execute')
    parser.add_argument('--query', help='SQL query for export')
    parser.add_argument('--output', help='Output filename for export')
    parser.add_argument('--limit', type=int, default=TEST_RECORD_LIMIT, 
                      help=f'Number of records to process (default: {TEST_RECORD_LIMIT})')
    
    args = parser.parse_args()
    
    # Update test record limit if specified
    global TEST_RECORD_LIMIT
    TEST_RECORD_LIMIT = args.limit
    
    pipeline = CovidDataPipeline()
    
    try:
        if args.command == 'fetch':
            result = pipeline.fetch_and_load_all_countries()
            print(json.dumps(result, indent=2))
            
        elif args.command == 'export':
            if not args.query:
                print("Error: Export requires a SQL query (--query)")
                sys.exit(1)
                
            filename = args.output or f"./exports/export_{pipeline.generate_batch_id()}.csv"
            result = pipeline.export_to_csv(args.query, filename)
            print(f"Exported to: {result}")
            
        else:  # 'test'
            result = pipeline.run_test()
            print(json.dumps(result, indent=2))
            
    except Exception as e:
        logger.error(f"Command execution failed: {e}")
        sys.exit(1)
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()