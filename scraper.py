import re
import json
import httpx
from bs4 import BeautifulSoup
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

from config import SCRAPER_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ShopifyScraper:
    def __init__(self):
        self.base_url = SCRAPER_CONFIG["base_url"]
        self.source = SCRAPER_CONFIG["source"]
        self.brand = SCRAPER_CONFIG["brand"]
        self.client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            },
        )

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def fetch_page(self, url: str) -> str:
        logger.info(f"Fetching: {url}")
        response = self.client.get(url)
        response.raise_for_status()
        return response.text

    def parse_collection_page(self, html: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        products = []
        
        product_links = soup.select('a[href*="/products/"]')
        seen_urls = set()
        
        for link in product_links:
            href = link.get("href", "")
            if "/products/" in href and href not in seen_urls:
                seen_urls.add(href)
                full_url = f"{self.base_url}{href}" if href.startswith("/") else href
                products.append({"product_url": full_url})
        
        return products

    def parse_product_page(self, html: str, product_url: str) -> Optional[dict]:
        soup = BeautifulSoup(html, "html.parser")
        
        product_data = {
            "source": self.source,
            "brand": self.brand,
            "product_url": product_url,
            "second_hand": SCRAPER_CONFIG["second_hand"],
        }
        
        title_elem = soup.select_one("h1")
        if not title_elem:
            title_elem = soup.select_one('[id*="ProductTitle"]')
        product_data["title"] = title_elem.get_text(strip=True) if title_elem else "Unknown"
        
        page_text = html
        regular_price = ""
        sale_price = ""
        
        regular_match = re.search(r'Regular price[^<]*?<[^>]*?class="[^"]*compare-at-price[^>]*?>[^$]*?\$?(\d+(?:[.,]\d+)?)', page_text)
        if regular_match:
            regular_price = regular_match.group(1).replace(',', '.')
        
        sale_match = re.search(r'Sale price[^<]*?<[^>]*?class="[^"]*price[^>]*?>[^$]*?\$?(\d+(?:[.,]\d+)?)', page_text)
        if sale_match:
            sale_price = sale_match.group(1).replace(',', '.')
        
        if not sale_price and not regular_price:
            price_elem = soup.select_one('.price')
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                price_match = re.search(r'\$?(\d+(?:[,\.]\d+)?)', price_text)
                if price_match:
                    regular_price = price_match.group(1)
                    sale_price = regular_price
        
        product_data["price"] = f"{regular_price}USD" if regular_price else "€39,99"
        product_data["sale"] = f"{sale_price}USD" if sale_price else product_data["price"]
        
        images = []
        img_elements = soup.select('.product-featured-media img, .product__media img, .product-image-primary img')
        for img in img_elements:
            src = img.get("src") or img.get("data-src")
            if src and "loading" not in src.lower() and "placeholder" not in src.lower():
                if src.startswith("//"):
                    src = "https:" + src
                if src not in images:
                    images.append(src)
        
        if not images:
            img_elements = soup.select('[class*="media"] img')
            for img in img_elements:
                src = img.get("src") or img.get("data-src")
                if src and "loading" not in src.lower():
                    if src.startswith("//"):
                        src = "https:" + src
                    if src not in images:
                        images.append(src)
        
        if not images:
            meta_images = soup.select('meta[property="og:image"]')
            for meta in meta_images:
                content = meta.get("content")
                if content:
                    if content.startswith("//"):
                        content = "https:" + content
                    if content not in images:
                        images.append(content)
        
        product_data["image_url"] = images[0] if images else ""
        product_data["additional_images"] = images[1:] if len(images) > 1 else []
        
        category = self._extract_category(product_url)
        product_data["category"] = category
        
        product_data["gender"] = "unisex"
        
        description = ""
        desc_elem = soup.select_one('[id*="description"], .product-description, .product__description')
        if desc_elem:
            description = desc_elem.get_text(strip=True)
        product_data["description"] = description
        
        sizes = []
        size_options = soup.select('.variant-swatch__item, .product-variant-select option')
        for size in size_options:
            size_text = size.get_text(strip=True)
            if size_text and size_text not in sizes:
                sizes.append(size_text)
        
        colors = []
        
        metadata = {
            "title": product_data["title"],
            "price": product_data["price"],
            "sale": product_data["sale"],
            "description": description,
            "sizes": sizes,
            "colors": colors,
            "category": category,
            "gender": product_data["gender"],
        }
        product_data["metadata"] = json.dumps(metadata)
        
        product_data["id"] = self._generate_id(product_url)
        
        return product_data

    def _extract_category(self, url: str) -> str:
        category_mapping = {
            "/collections/hoodies": "Hoodies",
            "/collections/viral-tees": "T-Shirts",
            "/collections/baby-tees": "Baby Tees",
            "/collections/caps": "Caps",
            "/collections/best-sellers": "Best Sellers",
        }
        
        for pattern, category in category_mapping.items():
            if pattern in url:
                return category
        
        return "T-Shirts"

    def _generate_id(self, url: str) -> str:
        import hashlib
        return hashlib.md5(url.encode()).hexdigest()[:16]

    def get_all_product_urls(self) -> list[str]:
        all_products = []
        
        for collection_url in SCRAPER_CONFIG["collections"]:
            try:
                html = self.fetch_page(collection_url)
                products = self.parse_collection_page(html)
                
                for product in products:
                    if product["product_url"] not in [p["product_url"] for p in all_products]:
                        all_products.append(product)
            except Exception as e:
                logger.error(f"Error fetching collection {collection_url}: {e}")
        
        unique_urls = []
        seen = set()
        for p in all_products:
            url = p["product_url"]
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)
        
        logger.info(f"Found {len(unique_urls)} unique product URLs")
        return unique_urls

    def scrape_product(self, url: str) -> Optional[dict]:
        try:
            html = self.fetch_page(url)
            return self.parse_product_page(html, url)
        except Exception as e:
            logger.error(f"Error scraping product {url}: {e}")
            return None

    def close(self):
        self.client.close()