with source as (
        select * from {{ source('olist_dataset', 'customers') }}
  ),
  renamed as (
      select
        SAFE_CAST({{ adapter.quote("customer_id") }} AS STRING) AS customer_id,
        SAFE_CAST({{ adapter.quote("customer_unique_id") }} AS STRING) AS customer_unique_id,
        SAFE_CAST({{ adapter.quote("customer_zip_code_prefix") }} AS INT64) AS zip_code_prefix,
        SAFE_CAST({{ adapter.quote("customer_city") }} AS STRING) AS city,
        SAFE_CAST({{ adapter.quote("customer_state") }} AS STRING) AS province

      from source
  )
  select * from renamed