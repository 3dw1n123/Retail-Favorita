import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.db import get_engine
from src.data.queries import create_oil_clean_view,create_train_clean_view,validate_oil_clean,validate_train_clean,create_holidays_clean_view,validate_holidays_clean
from src.utils.logger import setup_logger
from sqlalchemy import text

logger = setup_logger(__name__)


def clean_oil_prices() -> None:
    engine = get_engine()

    try:
        logger.info("Starting oil price imputation...")

        with engine.begin() as conn:
            #View creation
            conn.execute(text(create_oil_clean_view))

            #Validate results
            result = conn.execute(text(validate_oil_clean))

            stats = result.fetchone()

            logger.info(f"Oil cleaning completed successfully!")
            logger.info(f"Total rows: {stats[0]}")
            logger.info(f"Imputed rows: {stats[1]} ({stats[1]/stats[0]*100:.2f}%)")
            logger.info(f"Date range: {stats[2]} to {stats[3]}")
            logger.info(f"Price range: ${stats[4]:.2f} to ${stats[5]:.2f}")

    except Exception as e:
        logger.error(f"Error during oil cleaning: {e}")
        raise
    finally:
        engine.dispose()

def clean_train() -> None:
    engine = get_engine()

    try:
        logger.info("Starting train cleaning...")
        logger.info("This may take few minutes with +125M records...")


        with engine.begin() as conn:
            #View creation
            conn.execute(text(create_train_clean_view))

            #Validate results
            result = conn.execute(text(validate_train_clean))

            stats = result.fetchone()

            logger.info(f"Train cleaning completed successfully!")
            logger.info(f"Total rows: {stats[0]}")
            logger.info(f"Unique dates: {stats[1]}")
            logger.info(f"Unique stores: {stats[2]:,}")
            logger.info(f"Unique items: {stats[3]:,}")
            logger.info(f"Date range: {stats[4]} to {stats[5]}")
            logger.info(f"Data integrity issues: {stats[6]}")

            if stats[6]>0:
                logger.info(f"Orphan stores issues: {stats[7]}")
                logger.info(f"Orphan items issues: {stats[8]}")


            

    except Exception as e:
        logger.error(f"Error during train cleaning: {e}")
        raise
    finally:
        engine.dispose()



def clean_holidays() -> None:
    engine = get_engine()

    try:
        logger.info("Starting holidays cleaning...")

        with engine.begin() as conn:
            #View creation
            conn.execute(text(create_holidays_clean_view))

            #Validate results
            result = conn.execute(text(validate_holidays_clean))

            stats = result.fetchone()

            logger.info(f"Holidays cleaning completed successfully!")
            logger.info(f"Total rows: {stats[0]}")
            logger.info(f"Unique dates: {stats[1]}")
            logger.info(f"Unique locales: {stats[2]:,}")
            logger.info(f"Unique locations: {stats[3]:,}")
            logger.info(f"Date range: {stats[4]} to {stats[5]}")
            logger.info(f"Multiple holidays dates: {stats[6]}")        

    except Exception as e:
        logger.error(f"Error during holidays cleaning: {e}")
        raise
    finally:
        engine.dispose()


def main()  -> None:
    try:
        clean_oil_prices()
        clean_train()
        clean_holidays()
        logger.info("All cleaning steps completed successfully")
        logger.info("Materialized views created: ")
        logger.info("  - oil_clean")
        logger.info("  - train_clean")
        logger.info("  - holidays_clean")


    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__=="__main__":
    main()