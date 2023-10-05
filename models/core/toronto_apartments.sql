{{ config(materialized="table") }}
with
    evaluation as (
        select
            id,
            rsn as registration_id,
            year_registered,
            year_built,
            year_evaluated,
            -- (year_evaluated-year_built) as house_age
            evaluation_completed_on,
            property_type,
            wardname,
            score
        from {{ ref("apartment_registration") }}
        where
            not (
                id is null
                or rsn is null
                or year_built is null
                or year_registered is null
                or year_evaluated is null
                or evaluation_completed_on is null
                or wardname is null
            )

    ),
    short_term_rentals as (
        select ward_name, id
        from {{ ref("short_term_rental_registration") }}
        where not (id is null or ward_name is null)
    -- group by ward_name
    ),
    fire_incidents as (
        select
            incident_ward,
            id,
            civilian_casualties,
            count_of_persons_rescued,
            estimated_dollar_loss
        from {{ ref("fire_report_ward") }}
        where not (id is null or incident_ward is null)
    -- group by incident_ward
    )
select
    e.wardname as ward,
    e.property_type as type,
    count(e.id) as total_evaluated_houses,
    --sum(e.score) as ward_total_score,
    count(s.id) as ward_total_str_count,
    count(f.id) as ward_total_fires,
    sum(f.civilian_casualties) as ward_total_casualities,
    sum(f.count_of_persons_rescued + civilian_casualties) as ward_total_victims,
    sum(f.estimated_dollar_loss) as ward_total_loss
from evaluation as e
left join short_term_rentals as s on e.wardname = s.ward_name
left join fire_incidents as f on e.wardname = f.incident_ward
group by ward, type
