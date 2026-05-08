select
    f.job_id,
    s.skill_id,
    i.skill_category

from {{ ref('int_job_type_skills') }} i

inner join {{ ref('fact_jobs') }} f
    on i.raw_id = f.raw_id

inner join {{ ref('dim_skills') }} s
    on i.skill           = s.skill
    and i.skill_category = s.skill_category