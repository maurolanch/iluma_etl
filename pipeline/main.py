from sqlalchemy import create_engine

from pipeline.config import DB_URL
from db.create_tables import create_tables
from ingestion.ingest_jobs import ingest_raw

from utils.logger import get_logger

logger = get_logger(__name__)

SQL_PATH = "sql/create_raw_tables.sql"
CSV_PATH = "data/data_jobs.csv"


def run_pipeline():
    logger.info("Starting ETL pipeline")

    # Abort early if database URL is missing
    if not DB_URL:
        raise ValueError("DB_URL not found in .env / environment variables")

    engine = create_engine(DB_URL)

    # Create raw schema and tables if they don't exist
    logger.info("Creating tables...")
    create_tables(engine, SQL_PATH)
    logger.info("Tables created successfully")

    # Load CSV into raw layer as-is
    logger.info("Loading CSV into raw layer...")
    ingest_raw(engine, CSV_PATH)

    logger.info("ETL pipeline finished successfully")


if __name__ == "__main__":
    run_pipeline()