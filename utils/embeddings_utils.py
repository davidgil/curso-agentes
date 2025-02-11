from vertexai.preview.language_models import TextEmbeddingModel
from tenacity import retry, stop_after_attempt, wait_random_exponential
import numpy as np
from typing import List

embedding_model = TextEmbeddingModel.from_pretrained("textembedding-gecko-multilingual@001")
VECTOR_DIMENSIONS = 768

@retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(3))
def embed_text(text=[]):
    embeddings = embedding_model.get_embeddings(text)
    return [each.values for each in embeddings]

# Convert embeddings to bytes for Redis storage
def convert_embedding(emb: List[float]):
    return np.array(emb).astype(np.float32).tobytes()
