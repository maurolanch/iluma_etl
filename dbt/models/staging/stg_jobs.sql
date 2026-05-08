select
    job_title_short,
    job_title,

    job_location,
    job_country,
    search_location,

    company_name,
    job_via,
    job_schedule_type,

    job_work_from_home::boolean,
    job_no_degree_mention::boolean,
    job_health_insurance::boolean,

    job_posted_date::timestamp,

    salary_rate,
    salary_year_avg::double precision,
    salary_hour_avg::double precision,

    job_skills::jsonb,
    job_type_skills::jsonb,

    ingestion_date
from raw.jobs_raw