{{ config(materialized="view") }}

SELECT
    --identifiers
    id, 
    rsn AS registration_id,
    --date records
    year_registered,
    year_built,
    year_evaluated,
    evaluation_completed_on,
    --property identifiers
    property_type,
    confirmed_units AS number_of_units,
    --property location
    ward AS ward_number,
    wardname,
    site_address,
    grid,
    latitude,
    longitude,
    --score
    score
FROM 
    {{source("staging","apartment_registration")}}
    WHERE id IS NOT NULL AND rsn IS NOT NULL
    