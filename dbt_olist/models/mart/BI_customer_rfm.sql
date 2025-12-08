-- ============================================
-- CONFIGURATION: Analysis Period
-- ============================================
{%- set analysis_end_date = '2018-11-01' -%}
{%- set analysis_start_date = '2017-11-01' -%}
-- ============================================

with orders_12m as (
  select 
    customer_unique_id,
    order_id,
    order_purchase_timestamp,
    payment_value
  from {{ ref('INT_customers_finalized_orders') }}  
  where DATE(order_purchase_timestamp) >= DATE '{{ analysis_start_date }}'
    and DATE(order_purchase_timestamp) <= DATE '{{ analysis_end_date }}'
),

customer_summary as (
  select
    customer_unique_id,
    count(distinct order_id) as total_orders,
    sum(payment_value) as total_spent,
    min(order_purchase_timestamp) as first_order_date,
    max(order_purchase_timestamp) as last_order_date,
    DATE_DIFF(DATE '{{ analysis_end_date }}', DATE(max(order_purchase_timestamp)), DAY) as recency_days
  from orders_12m
  group by customer_unique_id
),

rfm_rank as (
  select
    customer_unique_id,
    total_orders,
    total_spent,
    recency_days,
    ntile(5) over (order by recency_days desc) as r_quintile_raw,
    ntile(5) over (order by total_spent) as m_quintile_raw
  from customer_summary
),

rfm_scores as (
  select
    customer_unique_id,
    total_orders,
    total_spent,
    recency_days,

    6 - r_quintile_raw as r_score,

    -- rule-based frequency scoring (for heavily skewed data) - otherwise, use ntile(5) over (order by total_orders)
    case 
      when total_orders = 1 then 1
      when total_orders = 2 then 3
      when total_orders between 3 and 5 then 4
      when total_orders > 5 then 5
    end as f_score,

    m_quintile_raw as m_score,

    (6 - r_quintile_raw)
    + case 
        when total_orders = 1 then 1
        when total_orders = 2 then 3
        when total_orders between 3 and 5 then 4
        when total_orders > 5 then 5
      end
    + m_quintile_raw as rfm_score
  from rfm_rank
)

select
  r.customer_unique_id,
  r.total_orders,
  r.total_spent,
  r.recency_days,
  r.r_score,
  r.f_score,
  r.m_score,
  r.rfm_score,
  concat(r.r_score, r.f_score, r.m_score) as rfm_label,

  case 
    when r.rfm_score >= 13 then 'Champions'
    when r.rfm_score between 10 and 12 then 'Loyal Customers'
    when r.rfm_score between 7 and 9 then 'Potential Loyalists'
    when r.rfm_score between 4 and 6 then 'At Risk'
    else 'Lost'
  end as rfm_segment
from rfm_scores as r