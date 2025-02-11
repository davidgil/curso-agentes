from utils.redis_utils import similarity_search, create_redis_client

query = "What is the best hotel close to the Louvre?"

redis_client = create_redis_client()
results = similarity_search(redis_client, query, k=100, return_fields=("score", "title", "text", "id"))

print(results)