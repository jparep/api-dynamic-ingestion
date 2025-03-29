import hashlib
import json
import os
import requests
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector

# ░░ Load environment secrets
load_dotenv()

# ░░ Flatten JSON utility
def flatten_json(obj, prefix=''):
    flat = {}
    for k, v in obj.items():
        full_key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            flat.update(flatten_json(v, prefix=full_key))
        else:
            flat[full_key] = v
    return flat

# ░░ Hash fingerprint for deduplication
def get_fingerprint(record):
    return hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()

# ░░ Config
database = "covid"
raw_schema = "raw3"
meta_schema = "meta"
raw_table = "COVID_COUNTRY_RAW"
source_name = "api-disease.sh"
load_ts = datetime.utcnow().isoformat()

# ░░ Step 1: Get data from COVID API
print(f"Fetching data from COVID API at {datetime.now()}")
url = "https://disease.sh/v3/covid-19/countries"
response = requests.get(url)
data = response.json()[:10]  # limit for testing
print(f"Fetched {len(data)} records")

# ░░ Step 2: Connect to Snowflake
print("Connecting to Snowflake...")
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
print("Connected to Snowflake")

# ░░ Step 3: Insert only new rows (deduplicated)
print("Beginning data insertion...")
new_rows = 0
for record in data:
    fingerprint = get_fingerprint(record)
    country = record.get("country", "UNKNOWN")
    # Check if the record already exists by fingerprint
    cursor.execute(f"SELECT COUNT(*) FROM {raw_table} WHERE record_hash = %s", (fingerprint,))
    if cursor.fetchone()[0] == 0:
        json_str = json.dumps(record).replace("'", "''")
        cursor.execute(f"""
            INSERT INTO {raw_table} (country, source_record, record_hash, load_ts, source)
            SELECT %s, PARSE_JSON(%s), %s, %s, %s
        """, (country, json_str, fingerprint, load_ts, source_name))
        new_rows += 1

print(f"Inserted {new_rows} new records into {raw_schema}.{raw_table}")

# ░░ Step 4: Call Snowflake Stored Procedure to Flatten + Archive
print("Calling flattening and archiving procedure...")
cursor.execute(f"""
    CALL {database}.{meta_schema}.PROC_FLATTEN_AND_ARCHIVE('{load_ts}', '{source_name}');
""")
result = cursor.fetchone()
print(f"Procedure result: {result[0]}")

# ░░ Step 5: Audit Log
print("Logging audit information...")
cursor.execute(f"""
    INSERT INTO {database}.{meta_schema}.INGEST_AUDIT_LOG
    (load_ts, table_name, record_count, duplicates_skipped, source)
    VALUES (%s, %s, %s, %s, %s)
""", (load_ts, raw_table, new_rows, len(data) - new_rows, source_name))

print("Archival and flattening logic executed.")
print(f"Logged audit for load timestamp: {load_ts}")

# ░░ Close connection
cursor.close()
conn.close()
print("Process completed successfully!")