with source as (

    select raw_payload, _source_file
    from {{ source('openweather', 'raw_air_pollution') }}

),

flattened as (

    select
        raw_payload:coord.lat::float as lat,
        raw_payload:coord.lon::float as lon,
        
        to_timestamp_ntz(f.value:dt::number) as observation_ts,

        f.value:main.aqi::int as aqi,

        -- safe cast
        try_cast(f.value:components.co::string as float) as co,
        try_cast(f.value:components.no::string as float) as no,
        try_cast(f.value:components.no2::string as float) as no2,
        try_cast(f.value:components.o3::string as float) as o3,
        try_cast(f.value:components.so2::string as float) as so2,
        try_cast(f.value:components.pm2_5::string as float) as pm2_5,
        try_cast(f.value:components.pm10::string as float) as pm10,
        try_cast(f.value:components.nh3::string as float) as nh3,

        _source_file,
        current_timestamp() as _loaded_at
    from source,
    lateral flatten(input => raw_payload:list) f

),

final as (
    
    select

        md5(concat_ws('||', lat, lon, observation_ts)) as air_quality_id,

        lat,
        lon,
        observation_ts,
        aqi,
        co,
        no,
        no2,
        o3,
        so2,
        pm2_5,
        pm10,
        nh3,
        
        _source_file,
        _loaded_at
        
    from flattened        
)

select * from final