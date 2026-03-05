import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))


from sqlalchemy import text
from src.utils.db import get_engine
from src.utils.logger import setup_logger
from src.data.queries import create_master_training_data_view, validate_master_data,holiday_distribution,payday_impact_preview,top_selling_families


logger = setup_logger(__name__)

def build_master_table()->None:

    engine = get_engine()
    try:
        logger.info("Building master Training Data:")

        with engine.begin() as conn:
            logger.info("Creating master_training_data materialized view...")
            conn.execute(text(create_master_training_data_view))
            logger.info("Materialized view created successfully!")

    except Exception as e:
        logger.error("Error building master table {e}")
        raise
    finally:
        engine.dispose()


def validate_master_table() -> None:
    engine = get_engine()
    try:
        logger.info("Validation Statistics:")

        with engine.connect() as conn:
            logger.info("Charging general info...\n")            
            result = conn.execute(text(validate_master_data))
            stats = result.fetchone()
            logger.info("General statistics:")
            logger.info(f"Total rows: {stats[0]:,}")
            logger.info(f"Unique dates: {stats[1]:,}")
            logger.info(f"Unique stores: {stats[2]:,}")
            logger.info(f"Unique items: {stats[3]:,}")
            logger.info(f"Date range: {stats[4]} to {stats[5]}")
            logger.info(f"Avg sales: {stats[6]}")
            logger.info(f"Avg oil price: ${stats[7]}")
            logger.info(f"Promotion records: {stats[8]:,}")
            logger.info(f"Holiday records: {stats[9]:,}")
            logger.info(f"Weekend records: {stats[10]:,}")
            logger.info(f"Payday records: {stats[11]:,}")
            logger.info(f"Item families: {stats[12]:,}")
            logger.info(f"Store types: {stats[13]:,}\n")

            logger.info("Holidays Distribution: ")
            holidays = conn.execute(text(holiday_distribution))
            for row in holidays:
                logger.info(f"   {row[0]}: {row[1]:,} occurrences, avg sales: {row[3]}")
            print("\n")
            logger.info("Pay day Impact: ")
            payday = conn.execute(text(payday_impact_preview))
            for row in payday:
                day_type = "PAYDAY" if row[0] else "Regular"
                logger.info(f"   {day_type}: {row[1]:,} transactions, avg: {row[2]} (±{row[3]})")
            print("\n")
            logger.info("Top selling families: ")
            families = conn.execute(text(top_selling_families)).fetchmany(5)
            for row in families:
                logger.info(f"   {row[0]}: {row[2]:,} total sales ({row[1]:,} transactions)")
                

    except Exception as e:
        logger.error(f"Validations error: {e}")
        raise
    finally:
        engine.dispose()

def main()-> None:
    logger.info("Starting Feature Engineering Pipeline\n")

    try:
        build_master_table()
        validate_master_table()
    except Exception as e:
        logger.error("\nFeature Engineering Failed: {e}")
        raise



if __name__ == "__main__":
    main()