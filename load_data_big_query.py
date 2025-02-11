from datasets import load_dataset
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("REGION")

dataset = load_dataset("traversaal-ai-hackathon/hotel_datasets")
df=pd.DataFrame(dataset['train'])
df["id"] = df.index + 1


bq = bigquery.Client(project=PROJECT_ID)

TABLE_NAME = "hotel_data"
DATASET_ID = f"{PROJECT_ID}.google_redis_llms"

dataset = bigquery.Dataset(DATASET_ID)
dataset.location = "US"
dataset = bq.create_dataset(dataset, timeout=30, exists_ok=True)

TABLE_ID = f"{DATASET_ID}.{TABLE_NAME}"
job = bq.load_table_from_dataframe(
    df, TABLE_ID
)
job.result()

print(f"Loaded {job.output_rows} rows into {TABLE_ID}:")