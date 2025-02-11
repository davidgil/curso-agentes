import os
import pandas as pd
from google.cloud import bigquery
from typing import Generator, Any
from dotenv import load_dotenv

load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")

TABLE_NAME = "hotel_data"
QUERY_TEMPLATE = f"""
    SELECT id, review_title, review_text, hotel_name
    FROM `{PROJECT_ID}.google_redis_llms.{TABLE_NAME}`
    WHERE review_text IS NOT NULL
    LIMIT {{limit}} OFFSET {{offset}};
"""

def query_bigquery_batches(
    max_rows: int,
    rows_per_batch: int,
    start_batch: int = 0
) -> Generator[pd.DataFrame, Any, None]:
    # Generate batches from a table in big query
    for offset in range(start_batch, max_rows, rows_per_batch):
        bq = bigquery.Client(project=PROJECT_ID)
        query = QUERY_TEMPLATE.format(limit=rows_per_batch, offset=offset)
        query_job = bq.query(query)
        rows = query_job.result()
        df = rows.to_dataframe()
        # Join title and text fields
        df["content"] = df.apply(
            lambda r: f"Hotel Name: {r.hotel_name}. Title: {r.review_title}. Content: {r.review_text if r.review_text is not None else ''}",
            axis=1
        )
        yield df
