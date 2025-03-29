import requests
import json
import os
from dotenv import load_dotenv
import snowflake.connector

# Load credentials from .env file
load_dotenv()

# Function to infer Snowflake-compatible data types
def infer_type(value):
    if isinstance(value, int): return "NUMBER"
    if isinstance(value, float): return "FLOAT"
    if isinstance(value, bool): return "BOOLEAN"
    if isinstance(value, str): return "STRING"
    if value is None: return "STRING"
    if isinstance(value, dict): return "VARIANT"
    if isinstance(value, list): return "ARRAY"
    return "STRING"

# Recursively flatten nested JSON
def flatten_json(json_obj, prefix=''):
    fields = {}
    for key, value in json_obj.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            fields.update(flatten_json(value, prefix=full_key))
        else:
            fields[full_key] = infer_type(value)
    return fields

# Constants
raw_schema = "raw3"
stg_schema = "stg"
database = "covid"
raw_table = "COVID_COUNTRY_RAW"
stg_table = "COVID_COUNTRY_STG"

# Step 1: Fetch sample COVID data
url = "https://disease.sh/v3/covid-19/countries"
response = requests.get(url)
data = response.json()[:10]  # Limit to 10 records for testing

# Step 2: Flatten and infer schema from sample data
full_schema = {}
for sample in data:
    flat = flatten_json(sample)
    for k, v in flat.items():
        full_schema[k] = v

# Step 3: Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=database,
    role=os.getenv("SNOWFLAKE_ROLE")
)
cursor = conn.cursor()

# Use correct database/schema
cursor.execute(f"USE DATABASE {database}")
cursor.execute(f"USE SCHEMA {raw_schema}")

# Step 4: Create RAW table if not exists
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {raw_table} (
        country STRING,
        source_record VARIANT,
        ingest_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
    );
""")

# Step 5: Insert raw JSON records into RAW table
insert_sql = f"""
    INSERT INTO {raw_table} (country, source_record)
    VALUES (%s, %s);
"""

for rec in data:
    country = rec.get("country", "UNKNOWN")
    cursor.execute(insert_sql, (country, rec))  # rec as dict

print(f"✅ Inserted {len(data)} raw records into {raw_schema}.{raw_table}")

# Step 6: Switch to STAGING and evolve schema
cursor.execute(f"USE SCHEMA {stg_schema}")

try:
    cursor.execute(f"DESCRIBE TABLE {stg_table}")
    existing_cols = set([row[0].lower() for row in cursor.fetchall()])
    print(f"✅ Table {stg_table} exists. Checking for missing columns...")
except snowflake.connector.errors.ProgrammingError:
    print(f"⚠️ Table {stg_table} does not exist. Creating...")
    col_defs = ",\n".join([
        f'"{k.replace(".", "_").lower()}" {v}' for k, v in full_schema.items()
    ])
    cursor.execute(f"CREATE TABLE {stg_table} ({col_defs});")
    existing_cols = set([k.replace(".", "_").lower() for k in full_schema])
    print(f"✅ Created table {stg_table} with initial schema.")

# Step 7: Add any new fields as columns
for k, dtype in full_schema.items():
    col = k.replace(".", "_").lower()
    if col not in existing_cols:
        alter_sql = f'ALTER TABLE {stg_table} ADD COLUMN "{col}" {dtype};'
        print(f"➕ Adding column: {col} ({dtype})")
        cursor.execute(alter_sql)

print("✅ Staging table schema synchronized successfully.")

# Cleanup
cursor.close()
conn.close()
