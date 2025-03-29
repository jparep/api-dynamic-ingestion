import requests
import json
import os
from dotenv import load_dotenv
import snowflake.connector

# Load .env values
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

# Flatten JSON recursively
def flatten_json(json_obj, prefix=''):
    fields = {}
    for key, value in json_obj.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            fields.update(flatten_json(value, prefix=full_key))
        else:
            fields[full_key] = infer_type(value)
    return fields

# Config
database = "covid"
raw_schema = "raw3"
stg_schema = "stg"
raw_table = "COVID_COUNTRY_RAW"
stg_table = "COVID_COUNTRY_STG"

# Fetch COVID data
url = "https://disease.sh/v3/covid-19/countries"
response = requests.get(url)
data = response.json()[:10]  # Limit to 10 records

# Infer schema from flattened sample
full_schema = {}
for record in data:
    flat = flatten_json(record)
    for k, v in flat.items():
        full_schema[k] = v

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=database,
    role=os.getenv("SNOWFLAKE_ROLE")
)
cursor = conn.cursor()

# Step 1: RAW TABLE CREATION
cursor.execute(f"USE DATABASE {database}")
cursor.execute(f"USE SCHEMA {raw_schema}")
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {raw_table} (
        country STRING,
        source_record VARIANT,
        ingest_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
    );
""")

# Step 2: INSERT RAW RECORDS (escaped JSON with PARSE_JSON)
for record in data:
    country = record.get("country", "UNKNOWN")
    json_text = json.dumps(record).replace("'", "''")  # escape for SQL

    insert_sql = f"""
        INSERT INTO {raw_schema}.{raw_table} (country, source_record)
        SELECT '{country}', PARSE_JSON('{json_text}')
    """
    cursor.execute(insert_sql)

print(f"✅ Inserted {len(data)} rows into {raw_schema}.{raw_table}")

# Step 3: STAGING SCHEMA MANAGEMENT
cursor.execute(f"USE SCHEMA {stg_schema}")
try:
    cursor.execute(f"DESCRIBE TABLE {stg_table}")
    existing_cols = set([row[0].lower() for row in cursor.fetchall()])
    print(f"✅ Table {stg_table} exists. Checking for new columns...")
except snowflake.connector.errors.ProgrammingError:
    print(f"⚠️ Table {stg_table} does not exist. Creating...")
    col_defs = ",\n".join([
        f'"{k.replace(".", "_").lower()}" {v}' for k, v in full_schema.items()
    ])
    cursor.execute(f"CREATE TABLE {stg_table} ({col_defs});")
    existing_cols = set([k.replace(".", "_").lower() for k in full_schema])
    print(f"✅ Created table {stg_schema}.{stg_table}")

# Step 4: Add new fields to STAGING table if needed
for key, dtype in full_schema.items():
    col_name = key.replace(".", "_").lower()
    if col_name not in existing_cols:
        alter_sql = f'ALTER TABLE {stg_table} ADD COLUMN "{col_name}" {dtype};'
        print(f"➕ Adding column: {col_name} ({dtype})")
        cursor.execute(alter_sql)

print(f"✅ STAGING table schema is up-to-date: {stg_schema}.{stg_table}")

# Cleanup
cursor.close()
conn.close()
