import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

from db.create_tables import create_tables
from src.ingest_jobs import ingest_data


# =========================
# LOAD ENV
# =========================

load_dotenv()

DB_URL = os.getenv("DB_URL")
CSV_PATH = os.getenv("CSV_PATH")

SQL_PATH = "sql/create_raw_tables.sql"


# =========================
# PIPELINE ORCHESTRATOR
# =========================

def run_pipeline():

    print("\n STARTING ETL PIPELINE\n")

    # 1. DB connection
    engine = create_engine(DB_URL)

    # 2. Create schema
    create_tables(engine, SQL_PATH)

    # 3. Ingest data
    ingest_data(engine, CSV_PATH)

    print("\n✅ PIPELINE FINISHED SUCCESSFULLY\n")


# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    run_pipeline()