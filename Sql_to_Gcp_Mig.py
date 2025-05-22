import os
import gzip
import pandas as pd
import pyodbc
from google.cloud import storage, bigquery
from tqdm import tqdm
import sys

# SQL Server Configuration
SQLSERVER_HOST = "localhost"
SQLSERVER_USER = "sa"
SQLSERVER_PASSWORD = "Admin@1234"
SQLSERVER_DATABASE = "my_db"

# Google Cloud Configuration
GCS_BUCKET_NAME = "test-gcs-bkt"
BIGQUERY_DATASET = "test_mig_dataset"

# Directory for CSV exports
EXPORT_DIR = "sqlserver_exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

# Define SQL Server to BigQuery Data Type Mapping
SQLSERVER_TO_BIGQUERY_TYPES = {
    "int": "INTEGER",
    "tinyint": "BOOL",
    "smallint": "INTEGER",
    "bigint": "INT64",
    "decimal": "NUMERIC",
    "float": "FLOAT64",
    "double": "FLOAT64",
    "varchar": "STRING",
    "text": "STRING",
    "char": "STRING",
    "date": "DATE",
    "datetime": "TIMESTAMP",
    "json": "STRING"
}

def get_table_schema(cursor, table_name):
    """Fetch schema from SQL Server and convert it to BigQuery format."""
    cursor.execute(f"""
        SELECT name, TYPE_NAME(system_type_id)
        FROM sys.columns
        WHERE object_id = OBJECT_ID('{table_name}')
    """)
    schema = []
    for row in cursor.fetchall():
        column_name, column_type = row[0], row[1].lower()
        column_type = next((bq_type for sql_type, bq_type in SQLSERVER_TO_BIGQUERY_TYPES.items() if sql_type in column_type), "STRING")
        schema.append({"name": column_name, "type": column_type})
    return schema

def export_sqlserver_to_csv():
    """Export SQL Server tables to CSV files, handling schema mismatches dynamically."""
    conn = pyodbc.connect(
        driver='{ODBC Driver 17 for SQL Server}',
        server=SQLSERVER_HOST,
        database=SQLSERVER_DATABASE,
        uid=SQLSERVER_USER,
        pwd=SQLSERVER_PASSWORD
    )
    cursor = conn.cursor()

    # Fetch all tables
    cursor.execute("SELECT name FROM sys.tables WHERE type = 'U'")
    tables = [table[0] for table in cursor.fetchall()][:1]

    for table in tqdm(tables, desc="Exporting SQL Server Tables"):
        query = f"SELECT * FROM {table}"
        df = pd.read_sql(query, conn)

        # Debugging: Print table info
        print(f"\nTable: {table}")
        print(f"Expected columns: {len(df.columns)}, Actual columns in data: {len(df.columns)}")

        if df.empty:
            print(f"No data found in table {table}. Skipping.")
            continue

        # Handle Data Type Mismatches
        for col in df.columns:
            cursor.execute(f"SELECT TYPE_NAME(system_type_id) FROM sys.columns WHERE object_id = OBJECT_ID('{table}') AND name = '{col}'")
            col_type = cursor.fetchone()[0].lower()

            print(str(col_type))

            if "tinyint" in str(col_type):
                print(f"Converting {col} to boolean")
                df[col] = df[col].astype(bool)  # ✅ Convert to bool
                print(f"Unique values in {df[col].astype(bool)}")
            print(df)

            # Convert Timestamps to UTC
            if "datetime" in col_type:
                df[col] = pd.to_datetime(df[col])

            # Convert JSON to String
            if "varchar" in col_type and df[col].dtype == 'object':
                df[col] = df[col].astype(str)

        # Save CSV
        file_path = os.path.join(EXPORT_DIR, f"{table}.csv")
        df.to_csv(file_path, index=False)

    cursor.close()
    conn.close()

def compress_csv():
    """Compress CSV files using gzip."""
    for file in tqdm(os.listdir(EXPORT_DIR), desc="Compressing Files"):
        if file.endswith(".csv"):
            file_path = os.path.join(EXPORT_DIR, file)
            with open(file_path, "rb") as f_in, gzip.open(f"{file_path}.gz", "wb") as f_out:
                f_out.writelines(f_in)
            os.remove(file_path)  # Delete the original CSV file after compression

def upload_to_gcs():
    """Upload compressed CSV files to Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    for file in tqdm(os.listdir(EXPORT_DIR), desc="Uploading to GCS"):
        if file.endswith(".gz"):
            blob = bucket.blob(file)
            blob.upload_from_filename(os.path.join(EXPORT_DIR, file))

def load_to_bigquery():
    """Load CSV files from GCS to BigQuery, using predefined schema."""
    bigquery_client = bigquery.Client()

    for file in tqdm(os.listdir(EXPORT_DIR), desc="Loading to BigQuery"):
        if file.endswith(".gz"):
            table_name = file.replace(".csv.gz", "")
            table_id = f"{BIGQUERY_DATASET}.{table_name}"
            uri = f"gs://{GCS_BUCKET_NAME}/{file}"

            # Get schema
            conn = pyodbc.connect(
                driver='{ODBC Driver 17 for SQL Server}',
                server=SQLSERVER_HOST,
                database=SQLSERVER_DATABASE,
                uid=SQLSERVER_USER,
                pwd=SQLSERVER_PASSWORD
            )
            cursor = conn.cursor()
            schema = get_table_schema(cursor, table_name)
            conn.close()

            job_config = bigquery.LoadJobConfig(
                schema=[bigquery.SchemaField(col["name"], col["type"]) for col in schema],
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1
            )

            load_job = bigquery_client.load_table_from_uri(uri, table_id, job_config=job_config)
            load_job.result()  # Wait for the job to complete

def main():
    """Run the complete SQL Server → GCS → BigQuery pipeline."""
    print("\nStarting SQL Server to BigQuery Transfer...")
    export_sqlserver_to_csv()
    compress_csv()
    upload_to_gcs()
    load_to_bigquery()
    print("\nData successfully transferred to BigQuery!")

if __name__ == "__main__":
    main()
