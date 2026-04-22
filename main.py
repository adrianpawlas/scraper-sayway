import json
import logging
import hashlib
import time
from datetime import datetime, timezone
from typing import Optional
import os

from scraper import ShopifyScraper
from supabase_client import SupabaseClient
from embedding import EmbeddingGenerator
from config import SCRAPER_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 50
EMBEDDING_DELAY = 0.5
MAX_RETRIES = 3


class SayWayScraper:
    def __init__(self):
        self.scraper = ShopifyScraper()
        self.supabase = SupabaseClient()
        self.embedding = EmbeddingGenerator()
        self.source = SCRAPER_CONFIG["source"]
        self.brand = SCRAPER_CONFIG["brand"]
        
        self.stats = {
            "new": 0,
            "updated": 0,
            "unchanged": 0,
            "deleted": 0,
            "errors": 0,
        }

    def run_full_scrape(self) -> list[dict]:
        logger.info("=" * 50)
        logger.info("Starting SayWay scraper...")
        logger.info("=" * 50)
        
        logger.info("Fetching all product URLs...")
        product_urls = self.scraper.get_all_product_urls()
        logger.info(f"Found {len(product_urls)} products from website")
        
        logger.info("Loading existing products from database...")
        existing_products = self.supabase.get_all_products(self.source)
        existing_by_url = {p["product_url"]: p for p in existing_products}
        logger.info(f"Found {len(existing_by_url)} existing products in database")
        
        all_scraped_products = []
        scraped_urls = set()
        
        for i, url in enumerate(product_urls):
            logger.info(f"Scraping product {i+1}/{len(product_urls)}: {url}")
            product = self.scraper.scrape_product(url)
            if product:
                all_scraped_products.append(product)
                scraped_urls.add(url)
        
        new_batch = []
        update_batch = []
        regenerate_emb_batch = []
        
        for product in all_scraped_products:
            url = product.get("product_url")
            existing = existing_by_url.get(url)
            
            product["id"] = self._generate_id(url)
            
            if existing is None:
                product["first_seen_at"] = datetime.now(timezone.utc).isoformat()
                new_batch.append(product)
                logger.info(f"NEW: {product.get('title')}")
            
            elif self._has_changes(existing, product):
                if existing.get("image_url") != product.get("image_url"):
                    regenerate_emb_batch.append(product)
                    logger.info(f"UPDATE+EMB: {product.get('title')} (image changed)")
                else:
                    update_batch.append(product)
                    logger.info(f"UPDATE: {product.get('title')}")
            else:
                self.stats["unchanged"] += 1
                logger.info(f"UNCHANGED: {product.get('title')}")
        
        logger.info(f"\n--- Processing: {len(new_batch)} new, {len(update_batch)} updated, {len(regenerate_emb_batch)} need embeddings ---")
        
        for product in new_batch:
            image_url = product.get("image_url")
            if image_url:
                product["image_embedding"] = self._generate_image_embedding_with_delay(image_url)
            
            metadata = json.loads(product.get("metadata", "{}"))
            info_text = self._build_info_text(product, metadata)
            product["info_embedding"] = self._generate_text_embedding_with_delay(info_text)
            product["updated_at"] = datetime.now(timezone.utc).isoformat()
        
        for product in regenerate_emb_batch:
            image_url = product.get("image_url")
            if image_url:
                product["image_embedding"] = self._generate_image_embedding_with_delay(image_url)
            
            metadata = json.loads(product.get("metadata", "{}"))
            info_text = self._build_info_text(product, metadata)
            product["info_embedding"] = self._generate_text_embedding_with_delay(info_text)
            product["updated_at"] = datetime.now(timezone.utc).isoformat()
        
        for product in update_batch:
            product["updated_at"] = datetime.now(timezone.utc).isoformat()
        
        logger.info(f"\n--- Inserting batches to database ---")
        
        all_to_insert = new_batch + update_batch + regenerate_emb_batch
        self._insert_batches_with_retry(all_to_insert)
        
        stale_products = self._find_stale_products(existing_by_url, scraped_urls)
        if stale_products:
            logger.info(f"\n--- Found {len(stale_products)} stale products ---")
            self._delete_stale_products(stale_products)
        
        self._print_summary()
        
        return all_scraped_products

    def _has_changes(self, existing: dict, new_product: dict) -> bool:
        fields_to_check = ["title", "price", "sale", "image_url", "additional_images", "description", "category"]
        for field in fields_to_check:
            existing_val = existing.get(field)
            new_val = new_product.get(field)
            if str(existing_val) != str(new_val):
                return True
        return False

    def _generate_image_embedding_with_delay(self, image_url: str) -> Optional[list]:
        time.sleep(EMBEDDING_DELAY)
        return self.embedding.generate_image_embedding(image_url)

    def _generate_text_embedding_with_delay(self, text: str) -> Optional[list]:
        time.sleep(EMBEDDING_DELAY)
        return self.embedding.generate_text_embedding(text)

    def _insert_batches_with_retry(self, products: list[dict]) -> None:
        for i in range(0, len(products), BATCH_SIZE):
            batch = products[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            total_batches = (len(products) + BATCH_SIZE - 1) // BATCH_SIZE
            
            for attempt in range(MAX_RETRIES):
                try:
                    self.supabase.insert_products_batch(batch)
                    logger.info(f"Inserted batch {batch_num}/{total_batches} ({len(batch)} products)")
                    for p in batch:
                        if self._is_new_product(p):
                            self.stats["new"] += 1
                        else:
                            self.stats["updated"] += 1
                    break
                except Exception as e:
                    logger.error(f"Batch {batch_num} failed (attempt {attempt + 1}): {e}")
                    if attempt == MAX_RETRIES - 1:
                        self._log_failed_products(batch)
                        self.stats["errors"] += len(batch)

    def _is_new_product(self, product: dict) -> bool:
        return "first_seen_at" in product

    def _find_stale_products(self, existing_by_url: dict, scraped_urls: set) -> list[dict]:
        stale = []
        for url, product in existing_by_url.items():
            if url not in scraped_urls:
                consecutive_misses = product.get("consecutive_misses", 0) + 1
                if consecutive_misses >= 2:
                    stale.append(product)
                else:
                    product["consecutive_misses"] = consecutive_misses
                    self.supabase.update_product(url, {"consecutive_misses": consecutive_misses})
        return stale

    def _delete_stale_products(self, products: list[dict]) -> None:
        for product in products:
            try:
                self.supabase.delete_product(product["id"])
                self.stats["deleted"] += 1
                logger.info(f"DELETED: {product.get('title')}")
            except Exception as e:
                logger.error(f"Failed to delete {product.get('title')}: {e}")

    def _log_failed_products(self, products: list[dict]) -> None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"failed_products_{timestamp}.log"
        with open(filename, "w") as f:
            for p in products:
                f.write(f"{p.get('product_url')}: {p.get('title')}\n")
        logger.error(f"Logged failed products to {filename}")

    def _print_summary(self) -> None:
        logger.info("=" * 50)
        logger.info("SCRAPER SUMMARY")
        logger.info("=" * 50)
        logger.info(f"  New products added:     {self.stats['new']}")
        logger.info(f"  Products updated:     {self.stats['updated']}")
        logger.info(f"  Products unchanged:   {self.stats['unchanged']}")
        logger.info(f"  Stale products deleted: {self.stats['deleted']}")
        logger.info(f"  Errors:              {self.stats['errors']}")
        logger.info("=" * 50)

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
        products = scraper.run_full_scrape()
        logger.info(f"Scraper completed: {len(products)} products processed")
    except Exception as e:
        logger.error(f"Error during scrape: {e}")
        raise
    finally:
        scraper.close()


if __name__ == "__main__":
    main()