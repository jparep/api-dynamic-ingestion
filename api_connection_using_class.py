#!/usr/bin/env python3
"""
COVID-19 Data ETL Pipeline

This script fetches COVID-19 data from public APIs, loads it into Snowflake,
and orchestrates the data transformation and archiving process.

March 2025
"""

import hashlib
import json
import os
import time
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

import requests
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.errors import ProgrammingError, DatabaseError
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import backoff 

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("covid_etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('covid_etl')

# Load environment variables
load_dotenv()

# Configuration - Consider moving to a config file for production
CONFIG = {
    "database": "covid",
    "raw_schema": "raw3",
    "meta_schema": "meta",
    "stg_schema": "stg",
    "raw_table": "COVID_COUNTRY_RAW",
    "source_name": "api-disease.sh",
    "api_url": "https://disease.sh/v3/covid-19/countries",
    "api_timeout": 30,  # seconds
    "max_retries": 3,
    "retry_delay": 5,  # seconds
    "batch_size": 100,  # number of records to process in each batch
    "thread_count": 4,  # for parallel processing
    "test_mode": False  # set to True to limit data for testing
}


class SnowflakeConnection:
    """Context manager for Snowflake connection handling"""

    def __init__(self):
        self.conn = None
        self.cursor = None

    def __enter__(self):
        try:
            self.conn = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=CONFIG["database"],
                role=os.getenv("SNOWFLAKE_ROLE"),
                application="COVID_ETL_PIPELINE"
            )
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to Snowflake - Account: {os.getenv('SNOWFLAKE_ACCOUNT')}, "
                       f"Database: {CONFIG['database']}")
            return self.cursor
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Snowflake connection closed")


@backoff.on_exception(backoff.expo, 
                     (requests.exceptions.RequestException, requests.exceptions.Timeout),
                     max_tries=CONFIG["max_retries"])
def fetch_covid_data() -> List[Dict]:
    """
    Fetch COVID-19 data from the disease.sh API with retry logic
    
    Returns:
        List[Dict]: List of COVID data records
    """
    logger.info(f"Fetching COVID data from {CONFIG['api_url']}")
    
    headers = {
        'User-Agent': 'COVID-Data-Pipeline/1.0 (data@example.com)'
    }
    
    response = requests.get(
        CONFIG['api_url'], 
        headers=headers,
        timeout=CONFIG['api_timeout']
    )
    
    if response.status_code != 200:
        logger.error(f"API request failed with status code {response.status_code}: {response.text}")
        response.raise_for_status()
    
    data = response.json()
    
    if CONFIG["test_mode"]:
        logger.info("Test mode enabled - limiting to 10 records")
        data = data[:10]
    
    logger.info(f"Successfully fetched {len(data)} records")
    return data


def get_record_fingerprint(record: Dict) -> str:
    """
    Create a unique hash fingerprint for a record to aid in deduplication
    
    Args:
        record: The JSON data record
        
    Returns:
        str: SHA-256 hash of the sorted JSON record
    """
    # Sort the keys to ensure consistent hashing regardless of key order
    return hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()


def preprocess_records(records: List[Dict]) -> List[Dict]:
    """
    Prepare records for insertion by adding metadata and handling edge cases
    
    Args:
        records: Raw data records from API
        
    Returns:
        List[Dict]: Processed records ready for insertion
    """
    processed_records = []
    load_ts = datetime.utcnow().isoformat()
    
    for record in records:
        # Skip records without required fields
        if 'country' not in record:
            logger.warning(f"Skipping record missing country field: {json.dumps(record)[:100]}...")
            continue
            
        # Handle missing or null values
        if record.get('cases') is None:
            record['cases'] = 0
        
        # Clean country names - remove leading/trailing whitespace and normalize
        country = record.get('country', '').strip()
        
        # Generate fingerprint for deduplication
        fingerprint = get_record_fingerprint(record)
        
        processed_records.append({
            'country': country,
            'source_record': record,
            'record_hash': fingerprint,
            'load_ts': load_ts,
            'source': CONFIG['source_name']
        })
    
    return processed_records, load_ts


def check_for_duplicates(cursor, records: List[Dict]) -> Tuple[List[Dict], int]:
    """
    Check for existing records to avoid duplicates
    
    Args:
        cursor: Snowflake cursor
        records: Preprocessed records
        
    Returns:
        Tuple of new records and count of duplicates skipped
    """
    # Extract fingerprints for batch checking
    fingerprints = [r['record_hash'] for r in records]
    
    # Use parameterized query with IN clause for batch checking
    placeholders = ', '.join(['%s'] * len(fingerprints))
    query = f"""
        SELECT record_hash
        FROM {CONFIG['raw_schema']}.{CONFIG['raw_table']}
        WHERE record_hash IN ({placeholders})
    """
    
    cursor.execute(query, fingerprints)
    existing_hashes = {row[0] for row in cursor.fetchall()}
    
    new_records = [r for r in records if r['record_hash'] not in existing_hashes]
    duplicates_count = len(records) - len(new_records)
    
    if duplicates_count > 0:
        logger.info(f"Found {duplicates_count} duplicate records that will be skipped")
    
    return new_records, duplicates_count


def insert_records_batch(cursor, records: List[Dict]) -> int:
    """
    Insert records in batch using Snowflake's bulk insert capabilities
    
    Args:
        cursor: Snowflake cursor
        records: Records to insert
        
    Returns:
        int: Number of records inserted
    """
    if not records:
        return 0
        
    # Prepare batch insert data
    insert_data = []
    for record in records:
        json_str = json.dumps(record['source_record']).replace("'", "''")
        insert_data.append((
            record['country'],
            json_str,
            record['record_hash'],
            record['load_ts'],
            record['source']
        ))
    
    # Use multi-row insert for better performance
    placeholders = "(%s, PARSE_JSON(%s), %s, %s, %s)"
    placeholders_list = ", ".join([placeholders] * len(insert_data))
    
    flat_values = [item for sublist in insert_data for item in sublist]
    
    query = f"""
        INSERT INTO {CONFIG['raw_schema']}.{CONFIG['raw_table']} 
        (country, source_record, record_hash, load_ts, source)
        VALUES {placeholders_list}
    """
    
    try:
        cursor.execute(query, flat_values)
        return len(records)
    except (ProgrammingError, DatabaseError) as e:
        logger.error(f"Error during batch insert: {str(e)}")
        # Fall back to individual inserts if batch fails
        successful_inserts = 0
        for data in insert_data:
            try:
                single_query = f"""
                    INSERT INTO {CONFIG['raw_schema']}.{CONFIG['raw_table']}
                    (country, source_record, record_hash, load_ts, source)
                    VALUES (%s, PARSE_JSON(%s), %s, %s, %s)
                """
                cursor.execute(single_query, data)
                successful_inserts += 1
            except Exception as e2:
                logger.error(f"Error inserting record for {data[0]}: {str(e2)}")
        
        logger.info(f"Completed individual inserts: {successful_inserts}/{len(records)} successful")
        return successful_inserts


def execute_transformation_procedure(cursor, load_ts: str, source: str) -> Dict:
    """
    Call Snowflake stored procedure to transform and archive data
    
    Args:
        cursor: Snowflake cursor
        load_ts: Load timestamp for this batch
        source: Source identifier
        
    Returns:
        Dict: Procedure execution results
    """
    logger.info(f"Calling data transformation procedure for load_ts: {load_ts}")
    
    try:
        cursor.execute(f"""
            CALL {CONFIG['database']}.{CONFIG['meta_schema']}.PROC_FLATTEN_AND_ARCHIVE(
                '{load_ts}', '{source}'
            )
        """)
        
        result = cursor.fetchone()[0]
        logger.info(f"Procedure executed successfully: {result}")
        return result
    except Exception as e:
        logger.error(f"Error executing transformation procedure: {str(e)}")
        raise


def log_audit_record(cursor, load_ts: str, table_name: str, record_count: int, 
                     duplicates_skipped: int, source: str, status: str, 
                     error_msg: Optional[str] = None) -> None:
    """
    Log audit information about the data load process
    
    Args:
        cursor: Snowflake cursor
        load_ts: Load timestamp
        table_name: Target table name
        record_count: Number of records processed
        duplicates_skipped: Number of duplicate records skipped
        source: Source identifier
        status: Process status (SUCCESS/ERROR)
        error_msg: Error message if applicable
    """
    logger.info(f"Logging audit record for load_ts: {load_ts}")
    
    query = f"""
        INSERT INTO {CONFIG['database']}.{CONFIG['meta_schema']}.INGEST_AUDIT_LOG
        (load_ts, table_name, record_count, duplicates_skipped, source, process_name, process_status, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    cursor.execute(
        query, 
        (load_ts, table_name, record_count, duplicates_skipped, source, 'PYTHON_ETL', status, error_msg)
    )


def run_data_quality_checks(cursor, load_ts: str) -> List