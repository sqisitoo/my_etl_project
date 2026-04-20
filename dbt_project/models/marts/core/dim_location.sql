with source as (
    select
        location_id,
        city_name,
        latitude,
        longitude
    from {{ ref('stg_internal__locations') }}
)

select * from source