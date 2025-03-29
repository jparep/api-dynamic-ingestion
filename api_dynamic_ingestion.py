import hashlib
import json
import os
import requests
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector

# Load secrets
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

# Infer type
def infer_type(value):
    if isinstance(value, int): return "INT"
    if isinstance(value, float): return "FLOAT"
    if isinstance(value, bool): return "BOOLEAN"
    if isinstance(value, str): return "STRING"
    if value is None: return "STRING"
    if isinstance(value, dict): return "VARIANT"
    if isinstance(value, list): return "ARRAY"
    return "STRING"

# Fingerprint
def get_record_fingerprint(record):
    canonical = json.dumps(record, sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

# Config
database = "covid"
raw_schema = "raw3"
meta_schema = "meta"
raw_table = "COVID_COUNTRY_RAW"
source_name = "api-disease.sh"
load_ts = datetime.utcnow().isoformat()

# 1. Fetch data
url = "https://disease.sh/v3/covid-19/countries"
data = requests.get(url).json()[:10]  # test limit
flat_sample = flatten_json(data[0])
schema = {k: infer_type(v) for k, v in flat_sample.items()}

# 2. Connect to Snowflake
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

# 3. Insert raw data with deduplication
new_rows = 0
for record in data:
    fingerprint = get_record_fingerprint(record)
    country = record.get("country", "UNKNOWN")

    cursor.execute(f"""
        SELECT COUNT(*) FROM {raw_table}
        WHERE record_hash = %s
    """, (fingerprint,))
    
    if cursor.fetchone()[0] == 0:
        json_text = json.dumps(record).replace("'", "''")
        cursor.execute(f"""
            INSERT INTO {raw_table} (country, source_record, record_hash, load_ts, source)
            SELECT %s, PARSE_JSON(%s), %s, %s, %s
        """, (country, json_text, fingerprint, load_ts, source_name))
        new_rows += 1

# 4. Call flattening and archive logic (in Snowflake SQL stored procedure)
cursor.execute(f"""
    CALL {database}.meta.PROC_FLATTEN_AND_ARCHIVE('{load_ts}', '{source_name}');
""")

# 5. Audit log
cursor.execute(f"""
    INSERT INTO {database}.meta.INGEST_AUDIT_LOG
    (load_ts, table_name, record_count, duplicates_skipped, source)
    VALUES (%s, %s, %s, %s, %s)
""", (load_ts, raw_table, new_rows, len(data) - new_rows, source_name))

print(f"✅ {new_rows} new records ingested.")
print(f"📞 Archival and flattening triggered via stored procedure.")

cursor.close()
conn.close()
