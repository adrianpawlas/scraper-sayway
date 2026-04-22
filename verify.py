import os
import sys
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_ANON_KEY')
client = create_client(url, key)

result = client.table('products').select('id', count='exact').eq('source', 'scraper-sayway').execute()

print(f'Products in database: {result.count}')

if result.count == 0:
    print('ERROR: No products imported!')
    sys.exit(1)

print('Import verification passed!')
sys.exit(0)