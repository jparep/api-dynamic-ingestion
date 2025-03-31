#!/usr/bin/env python3
"""
COVID-19 Data Pipeline - Python Client (Test Mode)
"""

import os
import sys
import json
import time
import hashlib
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

import requests
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector

# Default settings
DEFAULT_RECORD_LIMIT = 10

# Debug information for environment variables
print("Current directory:", os.getcwd())
print("Checking for .env file:", os.path.exists('.env'))

# Load environment variables
load_dotenv(verbose=True)

# Print loaded variables for debugging
print("SNOWFLAKE_USER:", os.getenv('SNOWFLAKE_USER'))
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
    'user': os.getenv('SNOWFLAKE_USERNAME'),  # Using USER instead of USERNAME
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'compute_wh'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'covid'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'raw3'),
    'role': os.getenv('SNOWFLAKE_ROLE', 'accountadmin')  # Default to accountadmin
}

# Print config for debugging
print("SNOWFLAKE_CONFIG:", {k: v if k != 'password' else '******' for k, v in SNOWFLAKE_CONFIG.items()})

# External COVID API configuration
COVID_API_ENDPOINT = os.getenv('COVID_API_ENDPOINT', 'https://disease.sh/v3/covid-19/countries')
SOURCE_NAME = 'disease.sh'  # Source identifier


def run_pipeline(record_limit=DEFAULT_RECORD_LIMIT):
    """Run the simplified pipeline"""
    
    # Create a connection to Snowflake
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_CONFIG['user'],
            password=SNOWFLAKE_CONFIG['password'],
            account=SNOWFLAKE_CONFIG['account'],
            warehouse=SNOWFLAKE_CONFIG['warehouse'],
            database=SNOWFLAKE_CONFIG['database'],
            schema=SNOWFLAKE_CONFIG['schema'],
            role=SNOWFLAKE_CONFIG['role']
        )
        logger.info("Successfully connected to Snowflake!")
        
        # Make sure the table exists
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS COVID_RAW (
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
            )
        """)
        
        # Generate a batch ID
        batch_id = datetime.now().strftime('%Y%m%d%H%M%S')
        
        # Fetch data from the API
        logger.info(f"Fetching data from {COVID_API_ENDPOINT}")
        response = requests.get(
            COVID_API_ENDPOINT, 
            headers={'Accept': 'application/json'}
        )
        response.raise_for_status()
        data = response.json()
        
        # Limit the records
        if record_limit > 0:
            logger.info(f"Limiting to {record_limit} records (from {len(data)} available)")
            data = data[:record_limit]
        
        # Insert each record into Snowflake
        succeeded = 0
        failed = 0
        
        for item in data:
            entity_id = item.get('country', 'unknown')
            record_hash = hashlib.sha256(json.dumps(item, sort_keys=True).encode()).hexdigest()
            
            try:
                # Clean and escape the JSON data
                json_str = json.dumps(item).replace("'", "''")
                
                cursor.execute("""
                    INSERT INTO COVID_RAW (
                        source_name, entity_id, source_record, record_hash, load_ts, api_endpoint
                    ) VALUES (
                        %s, %s, PARSE_JSON(%s), %s, %s, %s
                    )
                """, (
                    SOURCE_NAME,
                    entity_id,
                    PARSE_JSON('{json_str}'),
                    record_hash,
                    batch_id,
                    COVID_API_ENDPOINT
                ))
                
                succeeded += 1
                logger.info(f"Inserted data for {entity_id}")
            except Exception as e:
                logger.error(f"Failed to insert data for {entity_id}: {e}")
                failed += 1
        
        # Report results
        logger.info(f"Data loaded successfully. Succeeded: {succeeded}, Failed: {failed}")
        
        # Export a sample to CSV
        export_path = f"./exports/{batch_id}"
        Path(export_path).mkdir(parents=True, exist_ok=True)
        
        cursor.execute(f"""
            SELECT 
                entity_id, 
                raw_id,
                record_hash,
                ingestion_timestamp
            FROM COVID_RAW
            WHERE load_ts = '{batch_id}'
        """)
        
        results = cursor.fetchall()
        
        if results:
            # Convert to a DataFrame and export
            df = pd.DataFrame(results, columns=['entity_id', 'raw_id', 'record_hash', 'ingestion_timestamp'])
            output_file = f"{export_path}/covid_raw_sample.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"Exported sample to {output_file}")
        
        # Close the connection
        cursor.close()
        conn.close()
        
        return {
            'status': 'SUCCESS',
            'batch_id': batch_id,
            'records_processed': len(data),
            'succeeded': succeeded,
            'failed': failed
        }
    
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return {
            'status': 'ERROR',
            'error': str(e)
        }


def main():
    # Create directories
    Path("logs").mkdir(exist_ok=True)
    Path("exports").mkdir(exist_ok=True)
    
    # Parse arguments
    parser = argparse.ArgumentParser(description='COVID-19 Data Pipeline (Test Mode)')
    parser.add_argument('command', choices=['run'], help='Command to execute')
    parser.add_argument('--limit', type=int, default=DEFAULT_RECORD_LIMIT, 
                      help=f'Number of records to process (default: {DEFAULT_RECORD_LIMIT})')
    
    args = parser.parse_args()
    
    # Run the pipeline
    result = run_pipeline(record_limit=args.limit)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()