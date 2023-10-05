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
        from
        from {{ ref("apartment_registration") }}
        where
            not (
                id is null
                or rns is null
                or year_built is null
                or year_registered is null
                or year_evaluated is null
                or evaluation_completed_on is null
                or ward_name is null
            )

    ),
    short_term_rentals as (
        select ward_name, id,
        from {{ ref("short_term_rental_registration") }}
        where not (_id is null or ward_name is null)
    -- group by ward_name
    ),
    fire_incidents as (
        select
            incident_ward,
            id,
            civilian_casualties,
            count_of_persons_rescued,
            estimated_dollar_loss
        from {{ ref("fire_incidents") }}
        where not (_id is null or incident_ward is null)
    -- group by incident_ward
    )
select
    e.ward_name as ward,
    e.property_type as type,
    sum(e.id) as total_evaluated_houses
    sum(e.score) as ward_total_score,
    count(s.id) as ward_total_str_count,
    count(f.id) as ward_total_fires,
    sum(civilian_casualties) as ward_total_casualities
    sum(count_of_persons_rescued + civilian_casualties) as ward_total_victims
    sum(estimated_dollar_loss) as ward_total_loss
from evaluation as e
left join short_term_rentals as s on e.ward_name = s.ward_name
left join fire_incidents as f on e.ward_name = f.incident_ward
group by ward, type
