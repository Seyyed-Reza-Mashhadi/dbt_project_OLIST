with source as (
        select * from {{ source('olist_dataset', 'order_payments') }}
  ),
  renamed as (
      select
        SAFE_CAST({{ adapter.quote("order_id") }} AS STRING) AS order_id,
        SAFE_CAST({{ adapter.quote("payment_sequential") }} AS INT64) AS payment_sequential,
        SAFE_CAST({{ adapter.quote("payment_type") }} AS STRING) AS payment_type,
        SAFE_CAST({{ adapter.quote("payment_installments") }} AS INT64) AS payment_installments,
        SAFE_CAST({{ adapter.quote("payment_value") }} AS NUMERIC) AS payment_value

      from source
  )
  select * from renamed