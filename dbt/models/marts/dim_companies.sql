select
    {{ dbt_utils.generate_surrogate_key(['company_name_clean']) }} as company_id,
    company_name_clean  as company_name,
    display_name,
    job_count

from {{ ref('int_companies_deduped') }}
where company_name_clean is not null