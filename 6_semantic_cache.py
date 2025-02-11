from utils.llm_cache import standard_llmcache
from utils.rag import rag
import time

@standard_llmcache
def ask_palm(query: str):
  prompt = PROMPT
  response = rag(query, prompt, verbose=False)
  return response


PROMPT = """
You are a helpful virtual technology and IT assistant. Use the hotel reviews below as relevant context and sources to help answer the user question. 
Don't blindly make things up.

SOURCES:
{sources}

QUESTION:
{query}?

ANSWER:
"""

query = "Best hotel near the Louvre in Paris?"

start_time = time.time()
response = ask_palm(query)
end_time = time.time()

execution_time = end_time - start_time
print(f"Tiempo de ejecución: {execution_time:.4f} segundos")

print(response)