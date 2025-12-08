with source as (
        select * from {{ source('olist_dataset', 'orders') }}
  ),
  renamed as (
      select
        SAFE_CAST({{ adapter.quote("order_id") }} AS STRING) AS order_id,
        SAFE_CAST({{ adapter.quote("customer_id") }} AS STRING) AS customer_id,
        SAFE_CAST({{ adapter.quote("order_status") }} AS STRING) AS order_status,
        SAFE_CAST({{ adapter.quote("order_purchase_timestamp") }} AS TIMESTAMP) AS order_purchase_timestamp,
        SAFE_CAST({{ adapter.quote("order_approved_at") }} AS TIMESTAMP) AS order_approved_at,
        SAFE_CAST({{ adapter.quote("order_delivered_carrier_date") }} AS TIMESTAMP) AS order_delivered_carrier_date,
        SAFE_CAST({{ adapter.quote("order_delivered_customer_date") }} AS TIMESTAMP) AS order_delivered_customer_date,
        SAFE_CAST({{ adapter.quote("order_estimated_delivery_date") }} AS TIMESTAMP) AS order_estimated_delivery_date

      from source
  )
  select * from renamed