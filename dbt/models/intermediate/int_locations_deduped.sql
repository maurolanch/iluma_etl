select distinct
    job_location,
    case
        -- Explicit Remote
        when job_location = 'Anywhere' then 'Remote'

        -- Clear US patterns by state
        when job_location ~* ',\s*(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)$'
            then 'United States'

        -- Common European countries patterns by known city
        when job_location ilike '%Germany%'        then 'Germany'
        when job_location ilike '%France%'         then 'France'
        when job_location ilike '%Spain%'          then 'Spain'
        when job_location ilike '%India%'          then 'India'
        when job_location ilike '%United Kingdom%' then 'United Kingdom'
        when job_location ilike '%, UK'            then 'United Kingdom'
        when job_location ilike '%Canada%'         then 'Canada'
        when job_location ilike '%Australia%'      then 'Australia'
        when job_location ilike '%Netherlands%'    then 'Netherlands'
        when job_location ilike '%Poland%'         then 'Poland'
        when job_location ilike '%Brazil%'         then 'Brazil'
        when job_location ilike '%Singapore%'      then 'Singapore'

        -- If job_country is Sudan but job_location doesn't mention Sudan → NULL
        -- it's better honest NULL than incorrect data
        when job_country = 'Sudan'
         and job_location not ilike '%sudan%'      then null

        -- In any other case trust job_country
        else job_country
    end as job_country_clean

from {{ ref('stg_jobs') }}
where job_location is not null