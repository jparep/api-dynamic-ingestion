import requests
import json
import os
from dotenv import load_dotenv
import snowflake.connector

# Load credentials
load_dotenv()

# Infer Snowflake-compatible types
def infer_type(value):
    if isinstance(value, int): return "NUMBER"
    if isinstance(value, float): return "FLOAT"
    if isinstance(value, bool): return "BOOLEAN"
    if isinstance(value, str): return "STRING"
    if value is None: return "STRING"
    if isinstance(value, dict): return "VARIANT"
    if isinstance(value, list): return "ARRAY"
    return "STRING"

# Flatten nested JSON
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

# Step 1: Fetch 10 sample rows from COVID API
url = "https://disease.sh/v3/covid-19/countries"
response = requests.get(url)
data = response.json()[:10]  # Just 10 rows for test

# Step 2: Build flattened schema
full_schema = {}
for sample in data:
    flat = flatten_json(sample)
    for k, v in flat.items():
        full_schema[k] = v  # preserve last type seen

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

# Step 4: Use RAW schema and insert JSON into raw table
cursor.execute(f"USE DATABASE {database}")
cursor.execute(f"USE SCHEMA {raw_schema}")

# Create raw table if not exists
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {raw_table} (
        country STRING,
        source_record VARIANT,
        ingest_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
    );
""")

# Insert raw records
insert_sql = f"""
    INSERT INTO {raw_table} (country, source_record)
    VALUES (%s, PARSE_JSON(%s));
"""

raw_rows = [
    (rec.get("country", "UNKNOWN"), json.dumps(rec)) for rec in data
]
cursor.executemany(insert_sql, raw_rows)
print(f"✅ Inserted {len(raw_rows)} raw rows into {raw_schema}.{raw_table}")

# Step 5: Switch to STAGING and manage dynamic schema
cursor.execute(f"USE SCHEMA {stg_schema}")

try:
    # Check if STAGING table exists
    cursor.execute(f"DESCRIBE TABLE {stg_table}")
    existing_cols = set([row[0].lower() for row in cursor.fetchall()])
    print(f"✅ STAGING table {stg_table} exists. Checking for new columns...")
except snowflake.connector.errors.ProgrammingError:
    print(f"⚠️ Table {stg_table} does not exist. Creating...")
    col_defs = ",\n".join([
        f'"{k.replace(".", "_").lower()}" {v}' for k, v in full_schema.items()
    ])
    cursor.execute(f"CREATE TABLE {stg_table} ({col_defs});")
    existing_cols = set([k.replace(".", "_").lower() for k in full_schema])
    print(f"✅ Created STAGING table {stg_table}.")

# Step 6: Add new columns (if any)
for k, dtype in full_schema.items():
    col = k.replace(".", "_").lower()
    if col not in existing_cols:
        alter_sql = f'ALTER TABLE {stg_table} ADD COLUMN "{col}" {dtype};'
        print(f"➕ Adding new column: {col} ({dtype})")
        cursor.execute(alter_sql)

print("✅ Flattened staging schema updated successfully.")

cursor.close()
conn.close()
