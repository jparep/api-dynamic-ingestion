import requests
import json
import os
from dotenv import load_dotenv
import snowflake.connector

# Load credentials from .env file
load_dotenv()

# Infer Snowflake-compatible data types
def infer_type(value):
    if isinstance(value, int): return "NUMBER"
    if isinstance(value, float): return "FLOAT"
    if isinstance(value, bool): return "BOOLEAN"
    if isinstance(value, str): return "STRING"
    if value is None: return "STRING"
    if isinstance(value, dict): return "VARIANT"
    if isinstance(value, list): return "ARRAY"
    return "STRING"

# Flatten nested JSON fields
def flatten_json(json_obj, prefix=''):
    fields = {}
    for key, value in json_obj.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            fields.update(flatten_json(value, prefix=full_key))
        else:
            fields[full_key] = infer_type(value)
    return fields

# Configuration
database = "covid"
raw_schema = "raw3"
stg_schema = "stg"
raw_table = "COVID_COUNTRY_RAW"
stg_table = "COVID_COUNTRY_STG"

# Step 1: Fetch live COVID data (limit 10 for testing)
url = "https://disease.sh/v3/covid-19/countries"
response = requests.get(url)
data = response.json()[:10]  # Remove [:10] for full run

# Step 2: Flatten schema
full_schema = {}
for record in data:
    flat = flatten_json(record)
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

# Step 4: RAW Table — Create if not exists
cursor.execute(f"USE DATABASE {database}")
cursor.execute(f"USE SCHEMA {raw_schema}")
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {raw_table} (
        country STRING,
        source_record VARIANT,
        ingest_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
    );
""")

# Step 5: Insert raw JSON
for record in data:
    country = record.get("country", "UNKNOWN")
    json_text = json.dumps(record).replace("'", "''")
    insert_sql = f"""
        INSERT INTO {raw_schema}.{raw_table} (country, source_record)
        SELECT '{country}', PARSE_JSON('{json_text}')
    """
    cursor.execute(insert_sql)
print(f"✅ Inserted {len(data)} rows into {raw_schema}.{raw_table}")

# Step 6: STAGING Table — Create or evolve
cursor.execute(f"USE SCHEMA {stg_schema}")
try:
    cursor.execute(f"DESCRIBE TABLE {stg_table}")
    existing_cols = set([row[0].lower() for row in cursor.fetchall()])
    print(f"✅ Table {stg_table} exists.")
except snowflake.connector.errors.ProgrammingError:
    print(f"⚠️ Table {stg_table} does not exist. Creating...")
    col_defs = ",\n".join([
        f'"{k.replace(".", "_").lower()}" {v}' for k, v in full_schema.items()
    ])
    cursor.execute(f"CREATE TABLE {stg_table} ({col_defs});")
    existing_cols = set([k.replace(".", "_").lower() for k in full_schema])
    print(f"✅ Created table: {stg_schema}.{stg_table}")

# Step 7: Alter missing columns dynamically
for key, dtype in full_schema.items():
    col = key.replace(".", "_").lower()
    if col not in existing_cols:
        cursor.execute(f'ALTER TABLE {stg_table} ADD COLUMN "{col}" {dtype};')
        print(f"➕ Added column: {col} ({dtype})")

# Step 8: Insert flattened rows into STAGING
for record in data:
    flat_record = flatten_json(record)
    col_names = []
    col_values = []

    for k, dtype in full_schema.items():
        col = k.replace(".", "_").lower()
        col_names.append(f'"{col}"')
        value = flat_record.get(k)

        if value is None:
            col_values.append("NULL")
        elif dtype in ["STRING", "VARIANT"]:
            escaped_value = str(value).replace("'", "''")
            col_values.append(f"'{escaped_value}'")
        elif dtype == "BOOLEAN":
            col_values.append(str(value).upper())
        elif dtype in ["NUMBER", "FLOAT"]:
            try:
                col_values.append(str(float(value)))
            except:
                col_values.append("NULL")
        else:
            escaped_value = str(value).replace("'", "''")
            col_values.append(f"'{escaped_value}'")

    insert_stmt = f"""
        INSERT INTO {stg_schema}.{stg_table} ({', '.join(col_names)})
        VALUES ({', '.join(col_values)});
    """
    try:
        cursor.execute(insert_stmt)
    except Exception as e:
        print(f"❌ Error inserting row: {e}")

print(f"✅ Inserted {len(data)} flattened rows into {stg_schema}.{stg_table}")

# Cleanup
cursor.close()
conn.close()
