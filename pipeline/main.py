import subprocess
from sqlalchemy import create_engine

from pipeline.config import DB_URL
from db.create_tables import create_tables
from ingestion.ingest_jobs import ingest_raw
from utils.logger import get_logger

logger = get_logger(__name__)

SQL_PATH = "sql/create_raw_tables.sql"
CSV_PATH = "data/data_jobs.csv"
DBT_PATH = "dbt"  # ruta relativa al proyecto dbt


def run_dbt():
    """Run full dbt transformation pipeline."""
    result = subprocess.run(
        ["dbt", "run"],
        cwd=DBT_PATH,
        capture_output=True,
        text=True
    )

    # Forward dbt output to our logger
    for line in result.stdout.splitlines():
        logger.info(f"[dbt] {line}")

    if result.returncode != 0:
        for line in result.stderr.splitlines():
            logger.error(f"[dbt] {line}")
        raise RuntimeError("dbt run failed — check logs for details")

    logger.info("dbt transformations complete")


def run_dbt_tests():
    """Run dbt tests after transformation."""
    result = subprocess.run(
        ["dbt", "test"],
        cwd=DBT_PATH,
        capture_output=True,
        text=True
    )

    for line in result.stdout.splitlines():
        logger.info(f"[dbt test] {line}")

    if result.returncode != 0:
        for line in result.stderr.splitlines():
            logger.error(f"[dbt test] {line}")
        raise RuntimeError("dbt tests failed — check logs for details")

    logger.info("dbt tests passed")


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

    # Run dbt transformations
    logger.info("Running dbt transformations...")
    run_dbt()

    # Validate model integrity
    logger.info("Running dbt tests...")
    run_dbt_tests()

    logger.info("ETL pipeline finished successfully")


if __name__ == "__main__":
    run_pipeline()