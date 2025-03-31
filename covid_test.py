#!/usr/bin/env python3
"""
Minimal COVID API Test - No global variables
"""
import os
import sys
import json
import requests
import logging
from datetime import datetime
from pathlib import Path
import snowflake.connector
from dotenv import load_dotenv

# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('covid_test')

# Load environment variables
load_dotenv()
print("SNOWFLAKE_USER:", os.getenv('SNOWFLAKE_USER'))
print("SNOWFLAKE_ACCOUNT:", os.getenv('SNOWFLAKE_ACCOUNT'))

# Snowflake configuration
config = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USERNAME'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'compute_wh'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'covid'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'raw'),
    'role': os.getenv('SNOWFLAKE_ROLE', 'accountadmin')
}

# API endpoint
COVID_API = 'https://disease.sh/v3/covid-19/countries'

def test_api_connection():
    """Test connection to COVID API"""
    logger.info(f"Testing connection to: {COVID_API}")
    try:
        # Only request 10 records for testing
        response = requests.get(COVID_API, params={'limit': 10})
        response.raise_for_status()
        data = response.json()
        logger.info(f"API connection successful! Retrieved {len(data)} records.")
        # Save first record as sample
        Path("exports").mkdir(exist_ok=True)
        with open("exports/sample_data.json", "w") as f:
            json.dump(data[0], f, indent=2)
        logger.info(f"Saved sample to exports/sample_data.json")
        return data[:10]  # Return up to 10 records
    except Exception as e:
        logger.error(f"API connection failed: {e}")
        return None

def test_snowflake_connection():
    """Test connection to Snowflake"""
    logger.info(f"Testing connection to Snowflake: {config['account']}")
    try:
        conn = snowflake.connector.connect(
            user=config['user'],
            password=config['password'],
            account=config['account'],
            warehouse=config['warehouse'],
            database=config['database'],
            schema=config['schema'],
            role=config['role']
        )
        logger.info("Snowflake connection successful!")
        
        # Test a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        result = cursor.fetchone()
        logger.info(f"Connected as: {result}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Snowflake connection failed: {e}")
        return False

def main():
    """Run connection tests"""
    logger.info("=== COVID API and Snowflake Connection Test ===")
    
    # Test API connection
    api_data = test_api_connection()
    
    # Test Snowflake connection
    sf_success = test_snowflake_connection()
    
    # Print summary
    print("\n=== TEST RESULTS ===")
    print(f"API Connection: {'SUCCESS' if api_data else 'FAILED'}")
    print(f"Snowflake Connection: {'SUCCESS' if sf_success else 'FAILED'}")
    
    if api_data and sf_success:
        print("\nAll tests passed! Your environment is configured correctly.")
    else:
        print("\nSome tests failed. Please check the logs for details.")

if __name__ == "__main__":
    main()