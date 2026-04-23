import pandas as pd
from google.cloud import bigquery
import os

# Configuration
PROJECT_ID = "redsox-analytics-pipeline"
DATASET_ID = "mlb_raw"
DATA_DIR = os.path.expanduser("~/Desktop/mlb-gcp-pipeline/data")

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

# Create dataset if it doesn't exist
def create_dataset():
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {DATASET_ID} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Created dataset {DATASET_ID}")

# Clean a dataframe for BigQuery
def clean_dataframe(df, table_name):
    # Lowercase all column names
    df.columns = [col.lower().strip() for col in df.columns]
    # Replace spaces with underscores in column names
    df.columns = [col.replace(" ", "_") for col in df.columns]
    # Convert all columns to string to avoid type conflicts
    df = df.where(pd.notnull(df), None)
    print(f"  {table_name}: {len(df)} rows, {len(df.columns)} columns")
    return df

# Upload a CSV to BigQuery
def upload_table(filename, table_name):
    filepath = os.path.join(DATA_DIR, filename)
    print(f"Loading {table_name}...")
    df = pd.read_csv(filepath, low_memory=False)
    df = clean_dataframe(df, table_name)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"  Uploaded successfully\n")

# Tables to load
tables = {
    "Batting.csv": "batting",
    "Pitching.csv": "pitching",
    "Teams.csv": "teams",
    "People.csv": "people",
    "Salaries.csv": "salaries"
}

if __name__ == "__main__":
    print("Starting MLB data pipeline...\n")
    create_dataset()
    print()
    for filename, table_name in tables.items():
        upload_table(filename, table_name)
    print("Pipeline complete. All tables loaded to BigQuery.")
import pandas as pd
from google.cloud import bigquery
import os

# Configuration
PROJECT_ID = "redsox-analytics-pipeline"
DATASET_ID = "mlb_raw"
DATA_DIR = os.path.expanduser("~/Desktop/mlb-gcp-pipeline/data")

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

# Create dataset if it doesn't exist
def create_dataset():
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {DATASET_ID} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Created dataset {DATASET_ID}")

# Clean a dataframe for BigQuery
def clean_dataframe(df, table_name):
    df.columns = [col.lower().strip() for col in df.columns]
    df.columns = [col.replace(" ", "_") for col in df.columns]
    df = df.where(pd.notnull(df), None)
    print(f"  {table_name}: {len(df)} rows, {len(df.columns)} columns")
    return df

# Upload a CSV to BigQuery
def upload_table(filename, table_name):
    filepath = os.path.join(DATA_DIR, filename)
    print(f"Loading {table_name}...")
    df = pd.read_csv(filepath, low_memory=False)
    df = clean_dataframe(df, table_name)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"  Uploaded successfully\n")

# Tables to load
tables = {
    "Batting.csv": "batting",
    "Pitching.csv": "pitching",
    "Teams.csv": "teams",
    "People.csv": "people",
    "Salaries.csv": "salaries"
}

if __name__ == "__main__":
    print("Starting MLB data pipeline...\n")
    create_dataset()
    print()
    for filename, table_name in tables.items():
        upload_table(filename, table_name)
    print("Pipeline complete. All tables loaded to BigQuery.")