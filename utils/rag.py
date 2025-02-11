from vertexai.generative_models import GenerativeModel
from utils.redis_utils import similarity_search, create_redis_client

def create_prompt(prompt_template: str, **kwargs) -> str:
  return prompt_template.format(**kwargs)

def rag(query: str, prompt: str, verbose: bool = True) -> str:
    """
    Simple pipeline for performing retrieval augmented generation with
    Google Vertex PaLM API and Redis Enterprise.
    """
    if verbose:
        print("Pulling relevant data sources from Redis", flush=True)
    redis_client = create_redis_client()
    relevant_sources = similarity_search(redis_client, query, k=3, return_fields=("title", "text", "hotel_name"))
    if verbose:
        print("Relevant sources found!", flush=True)

    sources_text = "- " + "\n- ".join(
        [f"{row['hotel_name']}: {row['title']} {row['text']}" for index, row in relevant_sources.iterrows()]
    )


    full_prompt = create_prompt(
        prompt_template=prompt,
        sources=sources_text,
        query=query
      )
    if verbose:
        print("\nFull prompt:\n\n", full_prompt, flush=True)

    model = GenerativeModel("gemini-1.5-flash-002")
    response = model.generate_content(full_prompt)

    return response.text