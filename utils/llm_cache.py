import hashlib

from utils.redis_utils import create_redis_client

redis_client = create_redis_client()

def hash_input(prefix: str, _input: str):
    return prefix + hashlib.sha256(_input.encode("utf-8")).hexdigest()

def standard_check(key: str):
    res = redis_client.hgetall(key)
    if res:
      return res[b'response'].decode('utf-8')

def cache_response(query: str, response: str):
    key = hash_input("llmcache:", query)
    redis_client.hset(key, mapping={"prompt": query, "response": response})

def standard_llmcache(llm_callable):
    def wrapper(*args, **kwargs):
        # Check LLM Cache first
        key = hash_input("llmcache:", *args, **kwargs)
        response = standard_check(key)
        # Check if we have a cached response we can use
        if response:
            return response
        # Otherwise execute the llm callable here
        response = llm_callable(*args, **kwargs)
        query = args[0]  # Asumiendo que el primer argumento es el query
        cache_response(query, response)
        
        return response

    return wrapper