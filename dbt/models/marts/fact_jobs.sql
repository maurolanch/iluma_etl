select distinct on (j.raw_id)
    {{ dbt_utils.generate_surrogate_key(['j.raw_id']) }} as job_id,
    j.raw_id,
    c.company_id,
    l.location_id,
    jt.job_type_id,
    j.job_title_short,
    j.job_title,
    j.job_via,
    j.salary_rate,
    j.salary_year_avg,
    j.salary_hour_avg,
    j.job_posted_date,
    j.ingestion_date

from {{ ref('stg_jobs') }} j

left join {{ ref('dim_companies') }} c
    on trim(lower(j.company_name)) = c.company_name

left join {{ ref('dim_locations') }} l
    on j.job_location = l.job_location

left join {{ ref('dim_job_types') }} jt
    on j.job_schedule_type        = jt.schedule_type
    and j.job_work_from_home      = jt.work_from_home
    and j.job_no_degree_mention   = jt.no_degree_mention
    and j.job_health_insurance    = jt.health_insurance

order by j.raw_id, j.ingestion_date desc