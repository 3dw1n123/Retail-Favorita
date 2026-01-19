import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

user = os.getenv('POSTGRES_USER', 'favorita_user')
password = os.getenv('POSTGRES_PASSWORD', 'favorita_pass')
host = os.getenv('POSTGRES_HOST', 'localhost')
port = os.getenv('POSTGRES_PORT', '5432')
database = os.getenv('POSTGRES_DB', 'favorita_sales')

try:
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()[0]
    
    print(f"✅ Connected successfully!")
    print(f"PostgreSQL version: {version}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Connection failed: {e}")
    import traceback
    traceback.print_exc()