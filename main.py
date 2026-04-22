import json
import logging
from datetime import datetime
from typing import Optional, List
import hashlib

from scraper import ShopifyScraper
from supabase_client import SupabaseClient
from embedding import EmbeddingGenerator
from config import SCRAPER_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SayWayScraper:
    def __init__(self):
        self.scraper = ShopifyScraper()
        self.supabase = SupabaseClient()
        self.embedding = EmbeddingGenerator()
        self.source = SCRAPER_CONFIG["source"]
        self.brand = SCRAPER_CONFIG["brand"]

    def run_full_scrape(self, batch_size: int = 10):
        logger.info("Starting full scrape...")
        
        logger.info("Fetching all product URLs...")
        product_urls = self.scraper.get_all_product_urls()
        logger.info(f"Found {len(product_urls)} products")
        
        all_products = []
        for i, url in enumerate(product_urls):
            logger.info(f"Scraping product {i+1}/{len(product_urls)}: {url}")
            
            product = self.scraper.scrape_product(url)
            if product:
                all_products.append(product)
        
        logger.info(f"Processing {len(all_products)} products with embeddings...")
        
        for i, product in enumerate(all_products):
            logger.info(f"Processing embeddings {i+1}/{len(all_products)}: {product.get('title')}")
            
            image_url = product.get("image_url")
            if image_url:
                logger.info(f"Generating image embedding for: {image_url}")
                image_embedding = self.embedding.generate_image_embedding(image_url)
                product["image_embedding"] = image_embedding
            
            metadata = product.get("metadata")
            if metadata:
                try:
                    metadata_dict = json.loads(metadata)
                    info_text = self._build_info_text(product, metadata_dict)
                except:
                    info_text = self._build_info_text(product, {})
            else:
                info_text = self._build_info_text(product, {})
            
            logger.info(f"Generating info embedding...")
            info_embedding = self.embedding.generate_text_embedding(info_text)
            product["info_embedding"] = info_embedding
            
            product["id"] = self._generate_id(product.get("product_url"))
            product["created_at"] = datetime.utcnow().isoformat()
        
        logger.info(f"Inserting {len(all_products)} products to Supabase...")
        
        for i in range(0, len(all_products), batch_size):
            batch = all_products[i:i+batch_size]
            try:
                self.supabase.insert_products_batch(batch)
                logger.info(f"Inserted batch {i//batch_size + 1}")
            except Exception as e:
                logger.error(f"Error inserting batch: {e}")
        
        logger.info("Full scrape completed!")
        return all_products

    def _build_info_text(self, product: dict, metadata: dict) -> str:
        parts = [
            product.get("title", ""),
            product.get("brand", ""),
            product.get("category", ""),
            product.get("gender", ""),
            product.get("price", ""),
            product.get("description", ""),
        ]
        
        if metadata:
            if metadata.get("sizes"):
                parts.append(" ".join(metadata.get("sizes", [])))
            if metadata.get("colors"):
                parts.append(" ".join(metadata.get("colors", [])))
        
        return " ".join([p for p in parts if p])

    def _generate_id(self, url: str) -> str:
        return hashlib.md5(url.encode()).hexdigest()[:16]

    def close(self):
        self.scraper.close()
        self.embedding.close()


def main():
    scraper = SayWayScraper()
    try:
        products = scraper.run_full_scrape(batch_size=10)
        logger.info(f"Successfully scraped {len(products)} products")
    except Exception as e:
        logger.error(f"Error during scrape: {e}")
        raise
    finally:
        scraper.close()


if __name__ == "__main__":
    main()