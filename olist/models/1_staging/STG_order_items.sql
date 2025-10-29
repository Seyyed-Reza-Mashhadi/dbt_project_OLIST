with source as (
    select * from {{ source('olist_dataset', 'order_items') }}
),

renamed as (
    select
        SAFE_CAST({{ adapter.quote("order_id") }} AS STRING) AS order_id,
        SAFE_CAST({{ adapter.quote("order_item_id") }} AS INT64) AS order_item_id,
        SAFE_CAST({{ adapter.quote("product_id") }} AS STRING) AS product_id,
        SAFE_CAST({{ adapter.quote("seller_id") }} AS STRING) AS seller_id,
        SAFE_CAST({{ adapter.quote("shipping_limit_date") }} AS TIMESTAMP) AS shipping_limit_date,
        SAFE_CAST({{ adapter.quote("price") }} AS NUMERIC) AS price,
        SAFE_CAST({{ adapter.quote("freight_value") }} AS NUMERIC) AS freight_value
        
    from source
)

select * from renamed