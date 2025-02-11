import redis
import os
from dotenv import load_dotenv
from tqdm.auto import tqdm

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
    'title': record['review_title']
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

