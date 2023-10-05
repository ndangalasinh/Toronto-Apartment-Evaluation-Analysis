{{ config(materialized="view") }}

select
    -- identifiers
    id,
    rsn,
    -- date records
    year_registered,
    year_built,
    year_evaluated,
    evaluation_completed_on,
    -- property identifiers
    property_type,
    confirmed_units as number_of_units,
    -- property location
    ward as ward_number,
    wardname,
    site_address,
    grid,
    latitude,
    longitude,
    -- score
    score
from {{ source("staging", "apartment_registration") }}
where id is not null and rsn is not null
