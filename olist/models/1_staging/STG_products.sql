with source as (
        select * from {{ source('olist_dataset', 'products') }}
  ),
  renamed as (
      select
        SAFE_CAST({{ adapter.quote("product_id") }} AS STRING) AS product_id,
        SAFE_CAST({{ adapter.quote("product_category_name") }} AS STRING) AS product_category_name,
        SAFE_CAST({{ adapter.quote("product_name_lenght") }} AS INT64) AS product_name_length,
        SAFE_CAST({{ adapter.quote("product_description_lenght") }} AS INT64) AS product_description_length,
        SAFE_CAST({{ adapter.quote("product_photos_qty") }} AS INT64) AS product_photos_qty,
        SAFE_CAST({{ adapter.quote("product_weight_g") }} AS FLOAT64) AS product_weight_g,
        SAFE_CAST({{ adapter.quote("product_length_cm") }} AS FLOAT64) AS product_length_cm,
        SAFE_CAST({{ adapter.quote("product_height_cm") }} AS FLOAT64) AS product_height_cm,
        SAFE_CAST({{ adapter.quote("product_width_cm") }} AS FLOAT64) AS product_width_cm

      from source
  )
  select * from renamed