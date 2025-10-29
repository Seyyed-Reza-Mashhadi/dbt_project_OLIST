SELECT
  review_id,
  order_id,
  review_score,
  review_creation_date,
  commented

FROM {{ ref('STG_order_reviews') }}