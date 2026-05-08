select
    {{ dbt_utils.generate_surrogate_key([
        'schedule_type',
        'work_from_home',
        'no_degree_mention',
        'health_insurance'
    ]) }} as job_type_id,
    schedule_type,
    work_from_home,
    no_degree_mention,
    health_insurance

from (
    select distinct
        job_schedule_type     as schedule_type,
        job_work_from_home    as work_from_home,
        job_no_degree_mention as no_degree_mention,
        job_health_insurance  as health_insurance
    from {{ ref('stg_jobs') }}
) deduped