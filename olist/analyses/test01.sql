select 
    order_id,
from {{ ref('STG_orders') }}
limit 10