import hashlib
import json
import os
import requests
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector

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
archive_schema = "archive"
meta_schema = "meta"
raw_table = "COVID_COUNTRY_RAW"
stg_table = "COVID_COUNTRY_STG"
archive_table = "COVID_COUNTRY_ARCHIVE"
load_ts = datetime.utcnow().isoformat()

# Fetch data
url = "https://disease.sh/v3/covid-19/countries"
data = requests.get(url).json()[:10]  # limit for test
flat_sample = flatten_json(data[0])
schema = {k: infer_type(v) for k, v in flat_sample.items()}

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

# Set context
cursor.execute(f"USE DATABASE {database}")
cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {raw_schema}")
cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {stg_schema}")
cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {archive_schema}")
cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {meta_schema}")
cursor.execute(f"USE SCHEMA {raw_schema}")

# Create meta tables
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {meta_schema}.INGEST_AUDIT_LOG (
        load_ts STRING,
        table_name STRING,
        record_count INT,
        duplicates_skipped INT,
        source STRING,
        load_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
    );
""")

cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {meta_schema}.SCHEMA_CHANGE_LOG (
        column_name STRING,
        data_type STRING,
        table_name STRING,
        added_on TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
        load_ts STRING
    );
""")

# Create RAW table
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {raw_schema}.{raw_table} (
        country STRING,
        source_record VARIANT,
        record_hash STRING,
        ingest_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
        load_ts STRING,
        source STRING
    );
""")

# Insert RAW data with deduplication
new_rows = 0
for record in data:
    fingerprint = get_record_fingerprint(record)
    country = record.get("country", "UNKNOWN")

    cursor.execute(f"""
        SELECT COUNT(*) FROM {raw_schema}.{raw_table}
        WHERE record_hash = %s
    """, (fingerprint,))

    if cursor.fetchone()[0] == 0:
        json_text = json.dumps(record).replace("'", "''")
        cursor.execute(f"""
            INSERT INTO {raw_schema}.{raw_table} (country, source_record, record_hash, load_ts, source)
            SELECT %s, PARSE_JSON(%s), %s, %s, %s
        """, (country, json_text, fingerprint, load_ts, "api-disease.sh"))
        new_rows += 1

# Switch to STAGING
cursor.execute(f"USE SCHEMA {stg_schema}")
try:
    cursor.execute(f"DESCRIBE TABLE {stg_table}")
    existing_cols = set([row[0].lower() for row in cursor.fetchall()])
except:
    col_defs = ",\n".join([
        f'"{k.replace(".", "_").lower()}" {v}' for k, v in schema.items()
    ]) + ', "load_ts" STRING, "source" STRING'
    cursor.execute(f"CREATE TABLE {stg_table} ({col_defs});")
    existing_cols = set([k.replace(".", "_").lower() for k in schema] + ["load_ts", "source"])

# Add new columns and log schema drift
for k, v in schema.items():
    col = k.replace(".", "_").lower()
    if col not in existing_cols:
        cursor.execute(f'ALTER TABLE {stg_table} ADD COLUMN "{col}" {v};')
        cursor.execute(f"""
            INSERT INTO {meta_schema}.SCHEMA_CHANGE_LOG (column_name, data_type, table_name, load_ts)
            VALUES (%s, %s, %s, %s)
        """, (col, v, stg_table, load_ts))

for meta_col in [("load_ts", "STRING"), ("source", "STRING")]:
    col, dtype = meta_col
    if col not in existing_cols:
        cursor.execute(f'ALTER TABLE {stg_table} ADD COLUMN "{col}" {dtype};')

# Flatten and insert
column_map = {
    "country": "country",
    "load_ts": "load_ts",
    "source": "source"
}
for k, v in schema.items():
    col = k.replace(".", "_").lower()
    column_map[col] = f'source_record:{k}::{v}'

col_names = ', '.join([f'"{c}"' for c in column_map])
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

# Archive logic
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {archive_schema}.{archive_table} CLONE {stg_schema}.{stg_table};
""")

cursor.execute(f"""
    INSERT INTO {archive_schema}.{archive_table}
    SELECT * FROM {stg_schema}.{stg_table}
    WHERE TRY_TO_DATE("updated") < DATEADD(month, -12, CURRENT_DATE);
""")

cursor.execute(f"""
    DELETE FROM {stg_schema}.{stg_table}
    WHERE TRY_TO_DATE("updated") < DATEADD(month, -12, CURRENT_DATE);
""")

# Log to audit
cursor.execute(f"""
    INSERT INTO {meta_schema}.INGEST_AUDIT_LOG
    (load_ts, table_name, record_count, duplicates_skipped, source)
    VALUES (%s, %s, %s, %s, %s)
""", (load_ts, stg_table, new_rows, len(data) - new_rows, "api-disease.sh"))

cursor.close()
conn.close()
import ace_tools as tools; tools.display_dataframe_to_user(name="✅ Ingestion Summary", dataframe={"New_Records_Inserted": [new_rows], "Skipped_Duplicates": [len(data) - new_rows], "Load_TS": [load_ts]})
