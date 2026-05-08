select
    company_name_clean,
    min(company_name)  as display_name,
    count(*)           as job_count

from (
    select
        case
            when company_name_normalized ilike '%confidential%'
              or company_name_normalized ilike '%unknown%'
              or company_name is null
              or company_name = ''
            then 'confidential'
            else company_name_normalized
        end as company_name_clean,
        company_name

    from {{ ref('stg_jobs') }}
) cleaned

group by company_name_clean