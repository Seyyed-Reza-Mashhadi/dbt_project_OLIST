WITH renamed as (
    select
        safe_cast({{ adapter.quote("geolocation_zip_code_prefix") }} as int64) as zip_code_prefix,
        safe_cast({{ adapter.quote("geolocation_lat") }} as float64) as latitude,
        safe_cast({{ adapter.quote("geolocation_lng") }} as float64) as longitude,
        safe_cast({{ adapter.quote("geolocation_city") }} as string) as city,
        safe_cast({{ adapter.quote("geolocation_state") }} as string) as province
    from {{ source('olist_dataset', 'geolocation') }}
)

select
    zip_code_prefix,
    latitude,
    longitude,
    any_value(city) as city,
    any_value(province) as province
from renamed
-- for removing duplicates
group by      
    zip_code_prefix,
    latitude,
    longitude
