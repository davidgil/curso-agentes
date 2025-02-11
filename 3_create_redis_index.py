from utils.redis_utils import create_redis_client, create_redis_index

redis_client = create_redis_client()
create_redis_index(redis_client)