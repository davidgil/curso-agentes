import numpy as np
import math
from tqdm.auto import tqdm

from utils.redis_utils import create_redis_client, load_redis_batch
from utils.embeddings_utils import embed_text, convert_embedding
from utils.bigquery_utils import query_bigquery_batches

def create_embeddings_bigquery_redis(redis_client):
    max_rows = 1000
    rows_per_batch = 100
    bq_content_query = query_bigquery_batches(max_rows, rows_per_batch)
    for batch in tqdm(bq_content_query):
        batch_splits = np.array_split(batch, math.ceil(rows_per_batch / 5))

        all_embeddings = []

        for split_df in batch_splits:
            texts = split_df['content'].tolist()

            split_embeddings = embed_text(texts)

            all_embeddings.extend(convert_embedding(e) for e in split_embeddings)

        batch['embedding'] = all_embeddings

        records = batch.to_dict('records')
        for record in records:
          if record["review_text"] == None:
            record["review_text"] = ""
        load_redis_batch(redis_client, records)        

redis_client = create_redis_client()
create_embeddings_bigquery_redis(redis_client)        