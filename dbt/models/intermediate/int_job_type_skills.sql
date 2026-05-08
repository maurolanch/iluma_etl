select
    raw_id,
    job_title,
    company_name,
    job_title_short,
    job_posted_date,
    job_country,
    skill_category,
    trim(skill) as skill

from {{ ref('stg_jobs') }},
lateral jsonb_each(job_type_skills) as t(skill_category, skills_array),
lateral jsonb_array_elements_text(skills_array) as skill
where job_type_skills != '{}'