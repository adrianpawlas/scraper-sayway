import os
from dotenv import load_dotenv

load_dotenv()

SCRAPER_CONFIG = {
    "source": "scraper-sayway",
    "brand": "SayWay",
    "base_url": "https://saywaybrand.com",
    "collections": [
        "https://saywaybrand.com/collections/all",
        "https://saywaybrand.com/collections/best-sellers",
        "https://saywaybrand.com/collections/hoodies",
        "https://saywaybrand.com/collections/viral-tees",
        "https://saywaybrand.com/collections/baby-tees",
        "https://saywaybrand.com/collections/caps",
    ],
    "second_hand": False,
}

SUPABASE_CONFIG = {
    "url": os.getenv("SUPABASE_URL", "https://yqawmzggcgpeyaaynrjk.supabase.co"),
    "anon_key": os.getenv("SUPABASE_ANON_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlxYXdtemdnY2dwZXlhYXlucmprIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NTAxMDkyNiwiZXhwIjoyMDcwNTg2OTI2fQ.XtLpxausFriraFJeX27ZzsdQsFv3uQKXBBggoz6P4D4"),
    "table": "products",
}

EMBEDDING_CONFIG = {
    "model_name": "google/siglip-base-patch16-384",
    "embedding_dim": 768,
    "device": "cuda",
}

CURRENCY_MAP = {
    "EUR": "€",
    "USD": "$",
    "GBP": "£",
    "CZK": "CZK",
    "PLN": "PLN",
}