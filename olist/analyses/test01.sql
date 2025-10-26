select 
    order_status,
    count(*) as total_orders
from {{ source('olist_dataset', 'orders') }}
group by order_status