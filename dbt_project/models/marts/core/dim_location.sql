with source as (
    select
        location_id,
        city_name,
        latitude,
        longitude
    from {{ ref('stg_internal_locations') }}
)

select * from source