with source as (
        select * from {{ source('olist_dataset', 'geolocation') }}
  ),
  renamed as (
      select
        SAFE_CAST({{ adapter.quote("geolocation_zip_code_prefix") }} AS INT64) AS zip_code_prefix,
        SAFE_CAST({{ adapter.quote("geolocation_lat") }} AS FLOAT64) AS latitude,
        SAFE_CAST({{ adapter.quote("geolocation_lng") }} AS FLOAT64) AS longitude,
        SAFE_CAST({{ adapter.quote("geolocation_city") }} AS STRING) AS city,
        SAFE_CAST({{ adapter.quote("geolocation_state") }} AS STRING) AS province

      from source
  )
  select * from renamed