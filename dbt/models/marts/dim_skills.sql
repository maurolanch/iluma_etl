select
    {{ dbt_utils.generate_surrogate_key(['skill', 'skill_category']) }} as skill_id,
    skill,
    skill_category

from (
    select distinct skill, skill_category
    from {{ ref('int_job_type_skills') }}
    where skill is not null
) deduped