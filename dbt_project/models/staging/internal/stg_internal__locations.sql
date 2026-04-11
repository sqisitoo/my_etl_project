with source as (
    select name, lat, lon
    from {{ ref('cities_config') }}
),

casted_and_renamed as (
    select
        try_cast(name::string as string) as city_name,
        try_cast(lat::string as float) as latitude,
        try_cast(lon::string as float) as longitude
    from source
),

hashed as (
    select 
        {{ dbt_utils.generate_surrogate_key([
            'city_name',
            'latitude',
            'longitude'
        ]) }} as location_id,
        city_name,
        latitude,
        longitude
    from casted_and_renamed
),

final as (
    select 

        location_id,
        city_name,
        latitude,
        longitude
    from hashed
)

select * from final

