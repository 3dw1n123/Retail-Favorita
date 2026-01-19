import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

try:
    print("1. Intentando conectar a nivel bajo...")
    conn = psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT')
    )
    print("✅ ¡Conexión exitosa!")
    conn.close()

except UnicodeDecodeError:
    print("⚠️ Ocurrió el error de la 'ó'. Significa que la DB rechazó la conexión y el mensaje está en español.")
    print("REVISIÓN: ¿Está encendido tu servidor de PostgreSQL/Docker?")
    
except Exception as e:
    # Usamos repr() para imprimir el objeto crudo y evitar que explote por tildes
    print(f"❌ Error de conexión real (Raw): {repr(e)}")