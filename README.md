# API Dynamic Ingestion to Snowflake

This project demonstrates how to connect to an API, ingest JSON data directly into Snowflake, and dynamically flatten the data into staging tables for further processing.

## Features
- Connects to an API to fetch JSON data.
- Loads the JSON data directly into Snowflake.
- Dynamically flattens nested JSON structures into staging tables.

## Prerequisites
- Python 3.8+
- Snowflake account and credentials
- Required Python libraries:
    - `snowflake-connector-python`
    - `requests`
    - `pandas`

## Installation
1. Clone the repository:
     ```bash
     git clone https://github.com/your-repo/api-dynamic-ingestion.git
     cd api-dynamic-ingestion
     ```
2. Install dependencies:
     ```bash
     pip install -r requirements.txt
     ```

## Configuration
1. Create a `.env` file in the project root with the following variables:
     ```env
     SNOWFLAKE_ACCOUNT=<your_account>
     SNOWFLAKE_USER=<your_username>
     SNOWFLAKE_PASSWORD=<your_password>
     SNOWFLAKE_DATABASE=<your_database>
     SNOWFLAKE_SCHEMA=<your_schema>
     SNOWFLAKE_WAREHOUSE=<your_warehouse>
     API_ENDPOINT=<api_endpoint>
     API_KEY=<api_key>
     ```

## Usage
1. Run the script to fetch data from the API and load it into Snowflake:
     ```bash
     python ingest_to_snowflake.py
     ```
2. The script will:
     - Fetch JSON data from the API.
     - Load the raw JSON into a Snowflake table.
     - Dynamically flatten the JSON into staging tables.

## Project Structure
```
api-dynamic-ingestion/
├── ingest_to_snowflake.py  # Main script for ingestion and flattening
├── requirements.txt        # Python dependencies
├── README.md               # Project documentation
└── .env                    # Configuration file (not included in repo)
```

## Example
1. Sample API response:
     ```json
     {
         "id": 1,
         "name": "John Doe",
         "address": {
             "city": "New York",
             "zip": "10001"
         }
     }
     ```
2. Flattened staging table:
     | id | name     | address_city | address_zip |
     |----|----------|--------------|-------------|
     | 1  | John Doe | New York     | 10001       |

## License
This project is licensed under the MIT License.