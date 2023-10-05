{{ config(materialized="view") }}
select
    _id as id,
    property_type,
    ward_number,
    ward_name,
    operator_registration_number,
    address,
    unit,
    postal_code
from{{ ref("short-term-rental-registrations-data") }}
