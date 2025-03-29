import requests
import json
import os
from dotenv import load_dotenv
import snowflake.connector

# Load .env
load_dotenv()

# Flatten + schema inference
def flatten_json(json_obj, prefix=''):
    flat = {}
    for key, value in json_obj.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            flat.update(flatten_json(value, prefix=full_key))
        else:
            flat[full_key] = value
    return flat

def infer_type(value):
    if isinstance(value, int): return "NUMBER"
    if isinstance(value, float): return "FLOAT"
    if isinstance(value, bool): return "BOOLEAN"
    if isinstance(value, str): return "STRING"
    if value is None: return "STRING"
    if isinstance(value, dict): return "VARIANT"
    if isinstance(value, list): return "ARRAY"
    return "STRING"

# Config
database = "covid"
raw_schema = "raw3"
stg_schema = "stg"
raw_table = "COVID_COUNTRY_RAW"
stg_table = "COVID_COUNTRY_STG"

# Step 1: Get COVID data
url = "https://disease.sh/v3/covid-19/countries"
data = requests.get(url).json()[:10]  # For test limit

# Step 2: Infer schema
flat_sample = flatten_json(data[0])
schema = {k: infer_type(v) for k, v in flat_sample.items()}

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

# Use correct context
cursor.execute(f"USE DATABASE {database}")
cursor.execute(f"USE SCHEMA {raw_schema}")

# Step 4: Create RAW table
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {raw_table} (
        country STRING,
        source_record VARIANT,
        ingest_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
    );
""")

# Step 5: Insert raw JSON records
for record in data:
    country = record.get("country", "UNKNOWN")
    json_text = json.dumps(record).replace("'", "''")
    cursor.execute(f"""
        INSERT INTO {raw_table} (country, source_record)
        SELECT '{country}', PARSE_JSON('{json_text}');
    """)

print(f"✅ Inserted {len(data)} records into {raw_schema}.{raw_table}")

# Step 6: Create or evolve STAGING table
cursor.execute(f"USE SCHEMA {stg_schema}")
try:
    cursor.execute(f"DESCRIBE TABLE {stg_table}")
    existing_cols = set([row[0].lower() for row in cursor.fetchall()])
    print(f"✅ Table {stg_table} exists.")
except:
    col_defs = ",\n".join([
        f'"{k.replace(".", "_").lower()}" {v}' for k, v in schema.items()
    ])
    cursor.execute(f"CREATE TABLE {stg_table} ({col_defs});")
    existing_cols = set([k.replace(".", "_").lower() for k in schema])
    print(f"✅ Created table: {stg_table}")

# Step 7: Add missing columns
for k, v in schema.items():
    col = k.replace(".", "_").lower()
    if col not in existing_cols:
        cursor.execute(f'ALTER TABLE {stg_table} ADD COLUMN "{col}" {v};')
        print(f"➕ Added column: {col} ({v})")

# Step 8: Dynamically flatten & insert from RAW → STAGING
column_map = {
    "country": "country"  # keep country as-is
}
for k, v in schema.items():
    col = k.replace(".", "_").lower()
    path = k.replace('"', '')  # remove quotes from key
    column_map[col] = f'source_record:{path}::{v}'

# Build dynamic insert SQL
col_names = ', '.join([f'"{c}"' for c in column_map.keys()])
col_exprs = ',\n  '.join([f'{v} AS "{k}"' for k, v in column_map.items()])

flatten_insert_sql = f"""
    INSERT INTO {stg_schema}.{stg_table} (
        {col_names}
    )
    SELECT
      {col_exprs}
    FROM {raw_schema}.{raw_table};
"""

# Step 9: Run dynamic insert
cursor.execute(flatten_insert_sql)
print(f"✅ Flattened records inserted into {stg_schema}.{stg_table}")

# Done
cursor.close()
conn.close()
