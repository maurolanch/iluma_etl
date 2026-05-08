import io
import json
import ast
import pandas as pd
from datetime import datetime
from utils.logger import get_logger

logger = get_logger(__name__)

COLUMNS = [
    "job_title_short", "job_title", "job_location", "job_via",
    "job_schedule_type", "job_work_from_home", "search_location",
    "job_posted_date", "job_no_degree_mention", "job_health_insurance",
    "job_country", "salary_rate", "salary_year_avg", "salary_hour_avg",
    "company_name", "job_skills", "job_type_skills", "ingestion_date"
]

COLUMNS_SQL = ", ".join(COLUMNS)


def sanitize_json_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert Python-formatted strings to valid JSON for JSONB ingestion."""
    nulls_before = df[["job_skills", "job_type_skills"]].isna().sum().sum()

    for col in ["job_skills", "job_type_skills"]:
        def parse(val):
            if val is None or val == "" or val == "nan":
                return None
            try:
                # ast.literal_eval handles single-quoted Python literals
                # json.dumps serializes to valid JSON string for Postgres COPY
                return json.dumps(ast.literal_eval(val))
            except Exception:
                # Corrupted or unparseable value — store as NULL
                return None
        df[col] = df[col].apply(parse)

    # Warn if parsing introduced new nulls — may indicate upstream data issues
    nuevos_nulls = df[["job_skills", "job_type_skills"]].isna().sum().sum() - nulls_before
    if nuevos_nulls > 0:
        logger.warning(f"sanitize_json_columns: {nuevos_nulls} unparseable values set to NULL")

    return df


def ingest_raw(engine, csv_path: str) -> None:
    """
    Load CSV into raw.jobs_raw as-is. No cleaning, no transformations.
    Uses PostgreSQL COPY for high-throughput bulk insertion.
    """
    total = 0
    start = datetime.now()

    with engine.raw_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SET client_encoding TO 'UTF8'")

        # Read in chunks to avoid loading 250MB into memory at once
        reader = pd.read_csv(csv_path, chunksize=50_000, dtype=str)

        for i, df in enumerate(reader):
            # Attach ingestion timestamp as the only added field
            df["ingestion_date"] = datetime.now().isoformat()

            # Ensure column order matches the COPY target schema
            df = df.reindex(columns=COLUMNS)

            # Convert Python list/dict strings to valid JSONB-compatible JSON
            df = sanitize_json_columns(df)

            # Replace NaN with None before JSONB columns hit the buffer
            df = df.where(pd.notnull(df), None)

            # Fill remaining nulls with empty string for TEXT columns
            df = df.fillna("")

            # Write chunk to in-memory CSV buffer for COPY
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            try:
                cursor.copy_expert(
                    f"COPY raw.jobs_raw ({COLUMNS_SQL}) FROM STDIN WITH CSV NULL ''",
                    buffer
                )
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(
                    f"COPY failed on chunk {i+1} (rows {total}–{total+len(df)}): {e}",
                    exc_info=True
                )
                raise

            total += len(df)
            logger.debug(f"chunk {i+1} — {total:,} rows inserted")
            print(f"  chunk {i+1} — {total:,}", end="\r", flush=True)

    elapsed = (datetime.now() - start).seconds
    logger.info(f"Raw ingestion complete → {total:,} rows in {elapsed}s")