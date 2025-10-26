with source as (
        select * from {{ source('olist_dataset', 'order_reviews') }}
  ),
  renamed as (
      select
        CAST({{ adapter.quote("review_id") }} AS STRING) AS review_id,
        CAST({{ adapter.quote("order_id") }} AS STRING) AS order_id,
        CAST({{ adapter.quote("review_score") }} AS FLOAT64) AS review_score,
        CAST({{ adapter.quote("review_creation_date") }} AS TIMESTAMP) AS review_creation_date,
        CAST({{ adapter.quote("commented") }} AS BOOLEAN) AS commented

      from source
  )
  select * from renamed