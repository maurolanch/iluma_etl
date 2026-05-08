CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.jobs_raw;

CREATE TABLE raw.jobs_raw (
    id BIGSERIAL PRIMARY KEY,
    job_title_short TEXT,
    job_title TEXT,
    job_location TEXT,
    job_via TEXT,
    job_schedule_type TEXT,
    job_work_from_home TEXT,
    search_location TEXT,
    job_posted_date TEXT,
    job_no_degree_mention TEXT,
    job_health_insurance TEXT,
    job_country TEXT,
    salary_rate TEXT,
    salary_year_avg TEXT,
    salary_hour_avg TEXT,
    company_name TEXT,
    job_skills JSONB,
    job_type_skills JSONB,
    ingestion_date TIMESTAMP
);