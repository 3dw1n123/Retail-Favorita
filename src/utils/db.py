import os
from contextlib import contextmanager
from typing import Generator

import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.utils.logger import setup_logger

load_dotenv()
logger = setup_logger(__name__)

#Get the env info.
def get_db_url() -> str:
    user = os.getenv('POSTGRES_USER', 'favorita_user')
    password = os.getenv('POSTGRES_PASSWORD','favorita_pass')
    host = os.getenv('POSTGRES_HOST','localhost')
    port = os.getenv('POSTGRES_PORT','5432')
    database = os.getenv('POSTGRES_DB', 'favorita_sales')

    return f'postgresql://{user}:{password}@{host}:{port}/{database}'

#Create a SQLAlchemy engine.
def get_engine()->Engine:
    db_url = get_db_url()
    logger.info(f"Creating Database engine for {db_url.split("@")[1]}")

    engine = create_engine(
        db_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        #client_encoding='latin1'
    )
    return engine

@contextmanager
def get_connection()-> Generator:
    conn = None
    try:
        db_url = get_db_url()
        conn = psycopg2.connect(db_url)
        logger.debug("Database connection established")
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed")

def test_connection()-> bool:
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 'hello world'"))
            version = result.fetchone()[0]
            logger.info(f"Connected to: {version}")
            return True
    
    except Exception as e:
        logger.error(f"Connection test fail: {e}")
        return False
    