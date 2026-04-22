import torch
import numpy as np
from transformers import AutoProcessor, AutoModel
from PIL import Image
import io
import httpx
import logging
from typing import Optional, List
from config import EMBEDDING_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EmbeddingGenerator:
    def __init__(self):
        self.model_name = EMBEDDING_CONFIG["model_name"]
        self.embedding_dim = EMBEDDING_CONFIG["embedding_dim"]
        self.device = EMBEDDING_CONFIG["device"] if torch.cuda.is_available() else "cpu"
        
        logger.info(f"Loading model: {self.model_name}")
        self.processor = AutoProcessor.from_pretrained(self.model_name)
        self.model = AutoModel.from_pretrained(self.model_name)
        self.model.to(self.device)
        self.model.eval()
        logger.info(f"Model loaded on {self.device}")

    def generate_image_embedding(self, image_url: str) -> Optional[List[float]]:
        try:
            image = self._load_image(image_url)
            if image is None:
                return None
            
            image = image.resize((384, 384))
            
            # Process image and dummy text
            image_inputs = self.processor(images=image, return_tensors="pt")
            dummy_inputs = self.processor(text=[""], return_tensors="pt", padding=True)
            combined_inputs = {**image_inputs, **dummy_inputs}
            combined_inputs = {k: v.to(self.device) for k, v in combined_inputs.items()}
            
            with torch.no_grad():
                outputs = self.model(**combined_inputs)
            
            # Use vision_model output
            vision_output = outputs.vision_model_output
            embedding = vision_output.pooler_output
            embedding = embedding.cpu().numpy()[0].tolist()
            
            return embedding
        except Exception as e:
            logger.error(f"Error generating image embedding for {image_url}: {e}")
            return None

    def generate_text_embedding(self, text: str) -> Optional[List[float]]:
        try:
            inputs = self.processor(text=[text], return_tensors="pt", padding=True)
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            with torch.no_grad():
                text_outputs = self.model.text_model(**inputs)
            
            embedding = text_outputs.pooler_output
            embedding = embedding.cpu().numpy()[0].tolist()
            
            return embedding
        except Exception as e:
            logger.error(f"Error generating text embedding for '{text[:50]}...': {e}")
            return None

    def _load_image(self, url: str) -> Optional[Image.Image]:
        try:
            client = httpx.Client(timeout=30.0)
            response = client.get(url)
            response.raise_for_status()
            image = Image.open(io.BytesIO(response.content)).convert("RGB")
            client.close()
            return image
        except Exception as e:
            logger.error(f"Error loading image from {url}: {e}")
            return None

    def close(self):
        del self.model
        del self.processor
        if torch.cuda.is_available():
            torch.cuda.empty_cache()