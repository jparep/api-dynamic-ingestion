import requests
import json
import os
from dotenv import load_dotenv
import snowflake.connector
from datetime import datetime

# Load environment variables
load_dotenv()

# Helpers
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
    if isinstance(value, int): return "INT"
    if isinstance(value, float): return "FLOAT"
    if isinstance(value, bool): return "BOOLEAN"
    if isinstance(value, str): return "STRING"
    if value is None: return "STRING"
    if isinstance(value, dict): return "VARIANT"
    if isinstance(value, list): return "ARRAY"
    return "STRING"

# Configs
database = "covid"
raw_schema = "raw3"
stg_schema = "stg"
raw_table = "COVID_COUNTRY_RAW"
stg_table = "COVID_COUNTRY_STG"
load_ts = datetime.utcnow().isoformat()

# Step 1: Fetch data
url = "https://disease.sh/v3/covid-19/countries"
data = requests.get(url).json()[:10]  # limit for test

# Step 2: Infer schema from sample
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

# Step 4: Use context
cursor.execute(f"USE DATABASE {database}")
cursor.execute(f"USE SCHEMA {raw_schema}")

# Step 5: Create RAW table with metadata
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {raw_table} (
        country STRING,
        source_record VARIANT,
        ingest_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
        load_ts STRING,
        source STRING
    );
""")
cursor.execute(f'ALTER TABLE {raw_table} CLUSTER BY (country, ingest_time)')

# Step 6: Insert raw JSON
for record in data:
    country = record.get("country", "UNKNOWN")
    json_text = json.dumps(record).replace("'", "''")
    cursor.execute(f"""
        INSERT INTO {raw_table} (country, source_record, load_ts, source)
        SELECT '{country}', PARSE_JSON('{json_text}'), '{load_ts}', 'api-disease.sh';
    """)

print(f"✅ Inserted {len(data)} rows into {raw_schema}.{raw_table}")

# Step 7: Prepare STAGING table
cursor.execute(f"USE SCHEMA {stg_schema}")
try:
    cursor.execute(f"DESCRIBE TABLE {stg_table}")
    existing_cols = set([row[0].lower() for row in cursor.fetchall()])
    print(f"✅ STAGING table exists: {stg_table}")
except:
    col_defs = ",\n".join([
        f'"{k.replace(".", "_").lower()}" {v}' for k, v in schema.items()
    ])
    col_defs += ', "load_ts" STRING, "source" STRING'
    cursor.execute(f"CREATE TABLE {stg_table} ({col_defs});")
    existing_cols = set([k.replace(".", "_").lower() for k in schema] + ["load_ts", "source"])
    print(f"✅ Created STAGING table: {stg_table}")

# Step 8: Evolve schema if needed
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

# Step 9: Apply clustering to staging table
try:
    cursor.execute(f'ALTER TABLE {stg_table} CLUSTER BY ("country", "updated")')
    print(f"✅ Applied clustering on country and updated")
except:
    print("⚠️ Could not apply clustering. Might already be clustered.")

# Step 10: Build dynamic insert from RAW → STAGING
column_map = {
    "country": "country",
    "load_ts": "load_ts",
    "source": "source"
}
for k, v in schema.items():
    col = k.replace(".", "_").lower()
    json_path = k.replace('"', '')
    column_map[col] = f'source_record:{json_path}::{v}'

col_names = ', '.join([f'"{col}"' for col in column_map.keys()])
col_exprs = ',\n  '.join([f'{expr} AS "{col}"' for col, expr in column_map.items()])

insert_flattened_sql = f"""
    INSERT INTO {stg_schema}.{stg_table} (
        {col_names}
    )
    SELECT
        {col_exprs}
    FROM {raw_schema}.{raw_table}
    WHERE load_ts = '{load_ts}';
"""

cursor.execute(insert_flattened_sql)
print(f"✅ Flattened rows inserted into {stg_schema}.{stg_table}")

# Cleanup
cursor.close()
conn.close()
