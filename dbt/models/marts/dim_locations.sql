select
    {{ dbt_utils.generate_surrogate_key(['job_location']) }} as location_id,
    job_location,
    job_country_clean as job_country

from {{ ref('int_locations_deduped') }}