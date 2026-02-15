"""Test Supabase pooler connection"""
import psycopg2, os, re
from dotenv import load_dotenv
load_dotenv()

url = os.getenv('DATABASE_URL')
m = re.search(r'://postgres:(.+)@', url)
password = m.group(1) if m else ''
project_ref = 'pabqrixgzcnqttrkwkil'

for region in ['ap-southeast-2', 'us-east-1']:
    pooler_url = f'postgresql://postgres.{project_ref}:{password}@aws-0-{region}.pooler.supabase.com:6543/postgres'
    print(f'Testing {region} pooler...')
    try:
        conn = psycopg2.connect(pooler_url, connect_timeout=10)
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM race_results LIMIT 1')
        count = cur.fetchone()[0]
        print(f'  WORKS! race_results has {count:,} rows')
        print(f'  USE THIS: DATABASE_POOLER_URL with region {region}')
        conn.close()
        break
    except Exception as e:
        err = str(e).strip()[:150]
        print(f'  FAILED: {err}')
