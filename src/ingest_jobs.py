import io
import pandas as pd
from datetime import datetime

def ingest_data(engine, csv_path):
    print("Loading CSV...")
    total = 0

    with engine.raw_connection() as conn:
        cursor = conn.cursor()

        reader = pd.read_csv(csv_path, chunksize=50_000)
        for i, df in enumerate(reader):
            df["ingestion_date"] = datetime.now()
            df = df.where(pd.notnull(df), None)

            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            cursor.copy_expert(
                "COPY raw.jobs_raw (job_title_short, job_title, job_location, job_via, "
                "job_schedule_type, job_work_from_home, search_location, job_posted_date, "
                "job_no_degree_mention, job_health_insurance, job_country, salary_rate, "
                "salary_year_avg, salary_hour_avg, company_name, job_skills, "
                "job_type_skills, ingestion_date) "
                "FROM STDIN WITH CSV NULL ''",
                buffer
            )
            conn.commit()
            total += len(df)
            print(f"  chunk {i+1} — {total:,} filas", end="\r")

    print(f"\n⬆️ Listo — {total:,} filas en raw.jobs_raw")