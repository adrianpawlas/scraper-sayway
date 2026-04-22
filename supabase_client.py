import supabase
from typing import Optional, Any
import logging
from config import SUPABASE_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SupabaseClient:
    def __init__(self):
        self.client = supabase.create_client(
            SUPABASE_CONFIG["url"],
            SUPABASE_CONFIG["anon_key"]
        )
        self.table = SUPABASE_CONFIG["table"]

    def insert_product(self, product_data: dict) -> dict:
        try:
            data = {
                "id": product_data.get("id"),
                "source": product_data.get("source"),
                "product_url": product_data.get("product_url"),
                "affiliate_url": product_data.get("affiliate_url"),
                "image_url": product_data.get("image_url"),
                "brand": product_data.get("brand"),
                "title": product_data.get("title"),
                "description": product_data.get("description"),
                "category": product_data.get("category"),
                "gender": product_data.get("gender"),
                "second_hand": product_data.get("second_hand"),
                "price": product_data.get("price"),
                "sale": product_data.get("sale"),
                "metadata": product_data.get("metadata"),
                "additional_images": product_data.get("additional_images"),
                "image_embedding": product_data.get("image_embedding"),
                "info_embedding": product_data.get("info_embedding"),
            }
            
            response = self.client.table(self.table).upsert(
                data,
                on_conflict="source,product_url"
            ).execute()
            
            logger.info(f"Inserted product: {product_data.get('title')}")
            return response.data
        except Exception as e:
            logger.error(f"Error inserting product: {e}")
            raise

    def insert_products_batch(self, products: list[dict]) -> dict:
        try:
            records = []
            for product_data in products:
                records.append({
                    "id": product_data.get("id"),
                    "source": product_data.get("source"),
                    "product_url": product_data.get("product_url"),
                    "affiliate_url": product_data.get("affiliate_url"),
                    "image_url": product_data.get("image_url"),
                    "brand": product_data.get("brand"),
                    "title": product_data.get("title"),
                    "description": product_data.get("description"),
                    "category": product_data.get("category"),
                    "gender": product_data.get("gender"),
                    "second_hand": product_data.get("second_hand"),
                    "price": product_data.get("price"),
                    "sale": product_data.get("sale"),
                    "metadata": product_data.get("metadata"),
                    "additional_images": product_data.get("additional_images"),
                    "image_embedding": product_data.get("image_embedding"),
                    "info_embedding": product_data.get("info_embedding"),
                })
            
            response = self.client.table(self.table).upsert(
                records,
                on_conflict="source,product_url"
            ).execute()
            
            logger.info(f"Batch inserted {len(records)} products")
            return response.data
        except Exception as e:
            logger.error(f"Error batch inserting products: {e}")
            raise

    def get_product_by_url(self, source: str, product_url: str) -> Optional[dict]:
        try:
            response = self.client.table(self.table).select("*").eq("source", source).eq("product_url", product_url).execute()
            return response.data[0] if response.data else None
        except Exception as e:
            logger.error(f"Error getting product: {e}")
            return None

    def get_all_products(self, source: Optional[str] = None) -> list[dict]:
        try:
            query = self.client.table(self.table).select("*")
            if source:
                query = query.eq("source", source)
            response = query.execute()
            return response.data
        except Exception as e:
            logger.error(f"Error getting products: {e}")
            return []

    def delete_product(self, product_id: str) -> bool:
        try:
            self.client.table(self.table).delete().eq("id", product_id).execute()
            logger.info(f"Deleted product: {product_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting product: {e}")
            return False

    def close(self):
        pass