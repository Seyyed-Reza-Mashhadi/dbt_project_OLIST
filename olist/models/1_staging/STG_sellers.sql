with source as (
        select * from {{ source('olist_dataset', 'sellers') }}
  ),
  renamed as (
      select
        SAFE_CAST({{ adapter.quote("seller_id") }} AS STRING) AS seller_id,
        SAFE_CAST({{ adapter.quote("seller_zip_code_prefix") }} AS INT64) AS zip_code_prefix,
        SAFE_CAST({{ adapter.quote("seller_city") }} AS STRING) AS city,
        SAFE_CAST({{ adapter.quote("seller_state") }} AS STRING) AS province

      from source
  )
  select * from renamed