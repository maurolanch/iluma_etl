select distinct on (job_title, company_name, job_posted_date, job_location)

    -- raw lineage
    id as raw_id,

    -- job identifiers
    job_title_short,
    job_title,

    -- location
    job_location,
    job_country,
    search_location,

    -- company
    company_name,
    trim(lower(company_name)) as company_name_normalized,
    job_via,
    job_schedule_type,

    -- flags
    job_work_from_home::boolean,
    job_no_degree_mention::boolean,
    job_health_insurance::boolean,

    -- dates
    job_posted_date::timestamp,
    ingestion_date::timestamp,

    -- salary
    salary_rate,
    nullif(salary_year_avg, '')::double precision as salary_year_avg,
    nullif(salary_hour_avg, '')::double precision as salary_hour_avg,

    -- semi-structured fields as proper JSONB
    coalesce(job_skills::jsonb,      '[]'::jsonb) as job_skills,
    coalesce(job_type_skills::jsonb, '{}'::jsonb) as job_type_skills

from {{ source('raw', 'jobs_raw') }}
where job_title is not null

order by job_title, company_name, job_posted_date, job_location, id