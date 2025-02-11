import pandas as pd
import redis
import os
from dotenv import load_dotenv
from tqdm.auto import tqdm
import redis
from redis.commands.search.field import VectorField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query

from utils.embeddings_utils import convert_embedding, embed_text

def create_redis_client():
  load_dotenv()
  REDIS_HOST = os.getenv("REDIS_HOST")
  REDIS_PORT = os.getenv("REDIS_PORT")
  REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

  return redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD
  )

def redis_key(key_prefix: str, id: str) -> str:
  return f"{key_prefix}:{id}"

def process_record(record: dict) -> dict:
  return {
    'id': record['id'],
    'embedding': record['embedding'],
    'text': record['review_text'],
    'title': record['review_title'],
    'hotel_name': record['hotel_name']
  }

def load_redis_batch(
  redis_client: redis.Redis,
  dataset: list,
  key_prefix: str = "doc",
  id_column: str = "id",
):
  pipe = redis_client.pipeline()
  for i, record in enumerate(tqdm(dataset)):
    record = process_record(record)
    key = redis_key(key_prefix, record[id_column])
    pipe.hset(key, mapping=record)
  pipe.execute()

INDEX_NAME = "google:idx"
PREFIX = "doc:"
VECTOR_FIELD_NAME = "embedding"
VECTOR_DIMENSIONS = 768

def create_redis_index(
    redis_client: redis.Redis,
    vector_field_name: str = VECTOR_FIELD_NAME,
    index_name: str = INDEX_NAME,
    prefix: list = [PREFIX],
    dim: int = VECTOR_DIMENSIONS
  ):

    try:
        redis_client.ft(index_name).info()
        print("Existing index found. Dropping and recreating the index", flush=True)
        redis_client.ft(index_name).dropindex(delete_documents=False)
    except:
        print("Creating new index", flush=True)

    # Create new index
    redis_client.ft(index_name).create_index(
        (
            VectorField(
                vector_field_name, "FLAT",
                {
                    "TYPE": "FLOAT32",
                    "DIM": dim,
                    "DISTANCE_METRIC": "COSINE",
                }
            )
        ),
        definition=IndexDefinition(prefix=prefix, index_type=IndexType.HASH)
    )

def similarity_search(redis_client: redis.Redis, query: str, k: int, return_fields: tuple, index_name: str = INDEX_NAME) -> list:
    # create embedding from query text
    query_vector = embed_text([query])[0]
    # create redis query object
    redis_query = (
        Query(f"*=>[KNN {k} @{VECTOR_FIELD_NAME} $embedding AS score]")
            .sort_by("score")
            .return_fields(*return_fields)
            .paging(0, k)
            .dialect(2)
    )
    # execute the search
    results = redis_client.ft(index_name).search(
        redis_query, query_params={"embedding": convert_embedding(query_vector)}
    )
    return pd.DataFrame([t.__dict__ for t in results.docs ]).drop(columns=["payload"])