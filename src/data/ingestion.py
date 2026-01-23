import os 
from pathlib import Path
from io import StringIO
from typing import Optional

import pandas as pd
from tqdm import tqdm
from src.utils.db import get_connection, get_engine
from src.utils.logger import setup_logger


#Setup logger
logger = setup_logger(__name__, log_file='logs/ingestion.log')

# Paths
project_root = Path(__file__).parent.parent.parent
data_raw = project_root / 'data'/'raw'

#Get total lines
def get_total_lines(filepath:Path)->int:
    with open(filepath, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f) -1



def load_small_table(table_name: str, csv_filename: str, dtype:Optional[dict]=None)->None:
    filepath = data_raw / csv_filename
    if not filepath.exists():
        logger.error(f"File not found in: {filepath}")
        return
    
    logger.info(f"Loading {table_name} from {csv_filename}...")

    try:
        #Read CSV
        df = pd.read_csv(filepath, dtype=dtype)
        logger.info(f"Read {len(df)} rows")

        #Load database
        engine = get_engine()
        df.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )

        logger.info(f"{table_name}loaded successfully")

    except Exception as e:
        logger.error(f"Failed to load {table_name}: {e}")
        raise


def load_train_table(chunksize:int=100000)->None:
    filepath = data_raw / 'train.csv'

    if not filepath.exists():
        logger.error(f"File not found in: {filepath}")
        return
    
    logger.info(f"Loading train data from trainc.csv")
    logger.info(f"This may take several minutes...")

    try:
        logger.info("Counting the total rows...")
        total_lines = get_total_lines(filepath)
        logger.info(f"Total rows: {total_lines}")

        chunks_processed = 0
        rows_processed = 0

        with get_connection() as conn:
            cursor = conn.cursor()

            for chunk in tqdm(
                pd.read_csv(filepath, chunksize=chunksize),
                total=(total_lines//chunksize)+1,
                desc="Loading train data",
                unit="chunk"
            ):
            
                if 'id' in chunk.columns:
                    chunk = chunk.drop('id',axis=1)

                #Convert Dataframe to a CSV String
                buffer = StringIO()
                chunk.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                cursor.copy_from(
                    buffer,
                    'train',
                    sep=",",
                    null = '',
                    columns=chunk.columns.tolist()
                )

                chunks_processed+=1
                rows_processed+=len(chunk)

                if chunks_processed % 10 == 0:
                    conn.commit()
                    logger.info(f"Commited {rows_processed} rows...")
            
            #Final commit
            conn.commit()
            logger.info(f"Train data load successfully ({rows_processed:,} rows)")
        
    except Exception as e:
        logger.error(f"Failed to load train data: {e}")
        raise


def validate_data()->None:
    logger.info("\n--- Validating Data ---")

    engine = get_engine()

    tables = [
        'stores',
        'items', 
        'transactions',
        'oil',
        'holidays_events',
        'train'
    ] 

    for table in tables:
        try:
            query = f"SELECT COUNT(*) FROM {table};"
            result = pd.read_sql(query,engine)
            count = result.iloc[0,0]
            logger.info(f"  {table}: {count:,} rows")
        except Exception as e:
            logger.error(f"  {table}: ERROR - {e}")




def main():
    logger.info("-"*60)
    logger.info("Favorita Data Ingestion")
    logger.info("-"*60)

    #Load small tables
    logger.info("\n--- Loading small tables ---")

    load_small_table('stores', 'stores.csv')
    load_small_table('items', 'items.csv')
    load_small_table('transactions', 'transactions.csv')
    load_small_table('oil', 'oil.csv')
    load_small_table('holidays_events', 'holidays_events.csv')

    #Loading train table
    logger.info("\n--- Loading train table ---")
    load_train_table(chunksize=100000)


    #Validate
    validate_data()
    logger.info(f"\n" + "-"*60)
    logger.info("INGESTION COMPLETE!")
    logger.info(f"\n" + "-"*60)


if __name__ == "__main__":
    main()
