CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.jobs_raw;

CREATE TABLE raw.jobs_raw (
    id BIGSERIAL PRIMARY KEY,
    job_title_short TEXT,
    job_title TEXT,
    job_location TEXT,
    job_via TEXT,
    job_schedule_type TEXT,
    job_work_from_home BOOLEAN,
    search_location TEXT,
    job_posted_date TIMESTAMP,
    job_no_degree_mention BOOLEAN,
    job_health_insurance BOOLEAN,
    job_country TEXT,
    salary_rate TEXT,
    salary_year_avg DOUBLE PRECISION,
    salary_hour_avg DOUBLE PRECISION,
    company_name TEXT,
    job_skills TEXT,
    job_type_skills TEXT
);