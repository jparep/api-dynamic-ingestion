import requests
import json
import os
from dotenv import load_dotenv
import snowflake.connector

# Load environment variables from .env
load_dotenv()

# --------------------------
# Type Inference for Snowflake
# --------------------------
def infer_type(value):
    if isinstance(value, int): return "NUMBER"
    if isinstance(value, float): return "FLOAT"
    if isinstance(value, bool): return "BOOLEAN"
    if isinstance(value, str): return "STRING"
    if value is None: return "STRING"
    if isinstance(value, dict): return "VARIANT"
    if isinstance(value, list): return "ARRAY"
    return "STRING"

# --------------------------
# JSON Flattener
# --------------------------
def flatten_json(json_obj, prefix=''):
    fields = {}
    for key, value in json_obj.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            fields.update(flatten_json(value, prefix=full_key))
        else:
            fields[full_key] = infer_type(value)
    return fields

# --------------------------
# Configs
# --------------------------
raw_schema = "raw3"
stg_schema = "stg"
database = "covid"
raw_table = "COVID_COUNTRY_RAW"
stg_table = "COVID_COUNTRY_STG"

# --------------------------
# Step 1: Fetch Sample Data
# --------------------------
url = "https://disease.sh/v3/covid-19/countries"
response = requests.get(url)
data = response.json()[:10]  # Limit to 10 for test

# Flatten and infer schema
full_schema = {}
for record in data:
    flat = flatten_json(record)
    for k, v in flat.items():
        full_schema[k] = v

# --------------------------
# Step 2: Connect to Snowflake
# --------------------------
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=database,
    role=os.getenv("SNOWFLAKE_ROLE")
)
cursor = conn.cursor()

cursor.execute(f"USE DATABASE {database}")
cursor.execute(f"USE SCHEMA {raw_schema}")

# --------------------------
# Step 3: Create RAW Table if Not Exists
# --------------------------
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {raw_table} (
        country STRING,
        source_record VARIANT,
        ingest_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
    );
""")

# --------------------------
# Step 4: Insert Raw Data with PARSE_JSON
# --------------------------
for record in data:
    country = record.get("country", "UNKNOWN")
    json_text = json.dumps(record).replace("'", "''")  # escape single quotes
    insert_sql = f"""
        INSERT INTO {raw_table} (country, source_record)
        VALUES (%s, PARSE_JSON('{json_text}'));
    """
    cursor.execute(insert_sql, (country,))
print(f"✅ Inserted {len(data)} rows into RAW table: {raw_schema}.{raw_table}")

# --------------------------
# Step 5: Create/Update STAGING Table
# --------------------------
cursor.execute(f"USE SCHEMA {stg_schema}")

try:
    cursor.execute(f"DESCRIBE TABLE {stg_table}")
    existing_cols = set([row[0].lower() for row in cursor.fetchall()])
    print(f"✅ Table {stg_table} exists. Checking for new fields...")
except snowflake.connector.errors.ProgrammingError:
    print(f"⚠️ Table {stg_table} does not exist. Creating...")
    col_defs = ",\n".join([
        f'"{k.replace(".", "_").lower()}" {v}' for k, v in full_schema.items()
    ])
    cursor.execute(f"CREATE TABLE {stg_table} ({col_defs});")
    existing_cols = set([k.replace(".", "_").lower() for k in full_schema])
    print(f"✅ Created table: {stg_schema}.{stg_table}")

# --------------------------
# Step 6: Alter Table to Add New Fields Dynamically
# --------------------------
for key, dtype in full_schema.items():
    col_name = key.replace(".", "_").lower()
    if col_name not in existing_cols:
        alter_sql = f'ALTER TABLE {stg_table} ADD COLUMN "{col_name}" {dtype};'
        print(f"➕ Adding column: {col_name} ({dtype})")
        cursor.execute(alter_sql)

print(f"✅ STAGING table schema is up to date: {stg_schema}.{stg_table}")

# --------------------------
# Cleanup
# --------------------------
cursor.close()
conn.close()
