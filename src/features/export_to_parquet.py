import sys
import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

sys.path.append(str(Path(__file__).parent.parent.parent))

import polars as pl
from time import time
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

def export_to_parquet_partitioned(output_dir: Optional[Path] = None) -> None:
    if output_dir is None:
        output_dir = Path("data/processed")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    db_uri = f"postgresql://{os.environ.get('POSTGRES_USER')}:{os.environ.get('POSTGRES_PASSWORD')}@{os.environ.get('POSTGRES_HOST')}:{os.environ.get('POSTGRES_PORT')}/{os.environ.get('POSTGRES_DB')}"
    
    years = [2013, 2014, 2015, 2016, 2017]
    total_rows = 0
    
    try:
        logger.info("Starting Partitioned Export to Parquet")
        
        for year in years:
            year_start = time()
            output_path = output_dir / f"master_data_{year}.parquet"
            
            logger.info(f"\nExtracting year {year}...")
            
            # Query master_training_data per year

            query = f"""
                SELECT * FROM master_training_data 
                WHERE date >= '{year}-01-01' AND date <= '{year}-12-31'
                ORDER BY date, store_nbr, item_nbr
            """
            
            df = pl.read_database_uri(
                query=query,
                uri=db_uri,
                engine="connectorx"
            )
            
            rows_in_year = len(df)
            total_rows += rows_in_year
            
            logger.info(f"{year} rows: {rows_in_year:,} (Extracted in {time() - year_start:.2f}s)")
            logger.info(f"Saving to: {output_path.name}")
            
            df.write_parquet(
                output_path,
                compression="snappy",
                statistics=True,
                use_pyarrow=True
            )
            
            # Memory clean
            del df 
            logger.info(f"Year {year} completed.")
            
        logger.info(f"EXPORT COMPLETED")
        logger.info(f"Total rows across all partitions: {total_rows:,}")
        
    except Exception as e:
        logger.error(f"Error during export: {e}")
        raise

if __name__ == "__main__":
    export_to_parquet_partitioned()