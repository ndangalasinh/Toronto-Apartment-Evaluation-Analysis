{{ config(materialized="table") }}
with
    evaluation as (
        select ward_number, count(distinct id) as number_of_houses
        from {{ ref("apartment_registration") }}
        group by ward_number

    ),
    short_term_rentals as (
        select ward_number, count(distinct id) as number_of_strs
        from {{ ref("short_term_rental_registration") }}
        group by ward_number
    ),
    fire_incidents as (
        select incident_ward, count(distinct id) as number_of_fires
        from {{ ref("fire_report_ward") }}
        group by incident_ward
    )
select
    e.ward_number as ward_id,
    {{ get_ward_name("ward_id") }} as ward_descr,
    e.number_of_houses,
    s.number_of_strs,
    f.number_of_fires
from evaluation as e
left join short_term_rentals as s on e.ward_number = s.ward_number
left join fire_incidents as f on e.ward_number = f.incident_ward
