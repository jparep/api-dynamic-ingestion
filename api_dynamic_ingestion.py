import requests
import json
import os
import hashlib
from dotenv import load_dotenv
import snowflake.connector
from datetime import datetime

# Load environment variables
load_dotenv()

# Flatten JSON
def flatten_json(json_obj, prefix=''):
    flat = {}
    for key, value in json_obj.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            flat.update(flatten_json(value, prefix=full_key))
        else:
            flat[full_key] = value
    return flat

# Type inference
def infer_type(value):
    if isinstance(value, int): return "INT"
    if isinstance(value, float): return "FLOAT"
    if isinstance(value, bool): return "BOOLEAN"
    if isinstance(value, str): return "STRING"
    if value is None: return "STRING"
    if isinstance(value, dict): return "VARIANT"
    if isinstance(value, list): return "ARRAY"
    return "STRING"

# Generate SHA256 hash of JSON
def get_record_fingerprint(record):
    canonical = json.dumps(record, sort_keys=True)
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()

# Config
database = "covid"
raw_schema = "raw3"
stg_schema = "stg"
raw_table = "COVID_COUNTRY_RAW"
stg_table = "COVID_COUNTRY_STG"
load_ts = datetime.utcnow().isoformat()

# Step 1: Fetch data
url = "https://disease.sh/v3/covid-19/countries"
data = requests.get(url).json()[:10]  # For test limit

# Step 2: Flatten schema
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

# Step 4: Set context
cursor.execute(f"USE DATABASE {database}")
cursor.execute(f"USE SCHEMA {raw_schema}")

# Step 5: Create RAW table (includes record_hash)
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {raw_table} (
        country STRING,
        source_record VARIANT,
        record_hash STRING,
        ingest_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
        load_ts STRING,
        source STRING
    );
""")
cursor.execute(f'ALTER TABLE {raw_table} CLUSTER BY (country, ingest_time)')

# Step 6: Insert records with deduplication
new_rows = 0
for record in data:
    fingerprint = get_record_fingerprint(record)
    country = record.get("country", "UNKNOWN")

    # Check for duplicate in RAW by hash
    cursor.execute(f"""
        SELECT COUNT(*) FROM {raw_table}
        WHERE record_hash = %s
    """, (fingerprint,))
    
    if cursor.fetchone()[0] == 0:
        json_text = json.dumps(record).replace("'", "''")
        cursor.execute(f"""
            INSERT INTO {raw_table} (country, source_record, record_hash, load_ts, source)
            SELECT %s, PARSE_JSON(%s), %s, %s, %s
        """, (country, json_text, fingerprint, load_ts, "api-disease.sh"))
        new_rows += 1
    else:
        print(f"⏭️ Duplicate skipped for {country}")

print(f"✅ Inserted {new_rows} new rows into {raw_schema}.{raw_table}")

# Step 7: Handle STAGING
cursor.execute(f"USE SCHEMA {stg_schema}")
try:
    cursor.execute(f"DESCRIBE TABLE {stg_table}")
    existing_cols = set([row[0].lower() for row in cursor.fetchall()])
    print(f"✅ STAGING table exists")
except:
    col_defs = ",\n".join([
        f'"{k.replace(".", "_").lower()}" {v}' for k, v in schema.items()
    ])
    col_defs += ', "load_ts" STRING, "source" STRING'
    cursor.execute(f"CREATE TABLE {stg_table} ({col_defs});")
    existing_cols = set([k.replace(".", "_").lower() for k in schema] + ["load_ts", "source"])
    print(f"✅ Created STAGING table")

# Step 8: Evolve schema
for k, v in schema.items():
    col = k.replace(".", "_").lower()
    if col not in existing_cols:
        cursor.execute(f'ALTER TABLE {stg_table} ADD COLUMN "{col}" {v};')
        print(f"➕ Added column: {col} ({v})")

for meta_col in [("load_ts", "STRING"), ("source", "STRING")]:
    col, dtype = meta_col
    if col not in existing_cols:
        cursor.execute(f'ALTER TABLE {stg_table} ADD COLUMN "{col}" {dtype};')
        print(f"➕ Added metadata column: {col} ({dtype})")

# Step 9: Apply clustering to STAGING
try:
    cursor.execute(f'ALTER TABLE {stg_table} CLUSTER BY ("country", "updated")')
    print(f"✅ Applied clustering on STAGING table")
except:
    print(f"⚠️ Clustering may already exist or failed")

# Step 10: Insert into STAGING (flattened)
column_map = {
    "country": "country",
    "load_ts": "load_ts",
    "source": "source"
}
for k, v in schema.items():
    col = k.replace(".", "_").lower()
    path = k.replace('"', '')
    column_map[col] = f'source_record:{path}::{v}'

col_names = ', '.join([f'"{col}"' for col in column_map])
col_exprs = ',\n  '.join([f'{expr} AS "{col}"' for col, expr in column_map.items()])

flatten_insert_sql = f"""
    INSERT INTO {stg_schema}.{stg_table} (
        {col_names}
    )
    SELECT
      {col_exprs}
    FROM {raw_schema}.{raw_table}
    WHERE load_ts = %s;
"""

cursor.execute(flatten_insert_sql, (load_ts,))
print(f"✅ Flattened records inserted into {stg_schema}.{stg_table}")

# Cleanup
cursor.close()
conn.close()
