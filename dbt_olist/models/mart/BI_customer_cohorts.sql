{{
    config(
        materialized='table',
        description='Cohort retention and revenue analysis showing how customer groups behave over time'
    )
}}

/*
  CONFIGURATION
  - START_DATE: inclusive start of analysis (YYYY-MM-DD)
  - END_DATE: inclusive end of analysis (YYYY-MM-DD)
  - COHORT_PERIOD: WEEK, MONTH, QUARTER or YEAR  (no quotes when referenced)
*/

{% set START_DATE = '2017-11-01' %}
{% set END_DATE = '2018-11-01' %}
{% set COHORT_PERIOD = 'MONTH' %}  -- Options: WEEK, MONTH, QUARTER, YEAR


/* ============================================================================
   BASE DATA
============================================================================ */

with source_data as (
    select
        customer_unique_id,
        order_purchase_timestamp,
        payment_value
    from {{ ref('INT_customers_finalized_orders') }}
    where date(order_purchase_timestamp)
          between date('{{ START_DATE }}') and date('{{ END_DATE }}')
),

customer_cohorts as (
    -- Assign each customer to cohort based on first purchase timestamp
    select
        customer_unique_id,
        date_trunc(date(min(order_purchase_timestamp)), {{ COHORT_PERIOD }}) as cohort_period
    from source_data
    group by customer_unique_id
),

orders as (
    -- All orders aligned to cohort and order-period
    select
        o.customer_unique_id,
        c.cohort_period,
        date_trunc(date(o.order_purchase_timestamp), {{ COHORT_PERIOD }}) as order_period,
        sum(o.payment_value) as total_spent
    from source_data o
    inner join customer_cohorts c
        on o.customer_unique_id = c.customer_unique_id
    group by o.customer_unique_id, c.cohort_period, date_trunc(date(o.order_purchase_timestamp), {{ COHORT_PERIOD }})
),

cohort_size as (
    -- Total customers in each cohort
    select
        cohort_period,
        count(distinct customer_unique_id) as customers_in_cohort
    from customer_cohorts
    group by cohort_period
),

activity as (
    -- Activity of each cohort per period
    select
        cohort_period,
        order_period,
        count(distinct customer_unique_id) as active_customers,
        sum(total_spent) as total_spent
    from orders
    group by cohort_period, order_period
),

retention as (
    -- Full retention + revenue metrics for each cohort & period
    select
        a.cohort_period,
        a.order_period,
        c.customers_in_cohort,
        a.active_customers,
        round(
            safe_divide(
                cast(a.active_customers as float64),
                cast(c.customers_in_cohort as float64)
            ), 4
        ) as retention_rate,
        a.total_spent,
        round(
            safe_divide(
                cast(a.total_spent as float64),
                nullif(cast(a.active_customers as float64), 0)
            ), 2
        ) as avg_spent_per_customer,
        date_diff(a.order_period, a.cohort_period, {{ COHORT_PERIOD }}) as period_index
    from activity a
    inner join cohort_size c
        on a.cohort_period = c.cohort_period
    where a.order_period >= a.cohort_period
),

cohort_data as (
    -- Individual cohort rows
    select
        cohort_period,
        order_period,
        customers_in_cohort,
        active_customers,
        retention_rate,
        total_spent,
        period_index,
        FALSE as is_weighted_average
    from retention
),

weighted_averages as (
    /*
      Weighted averages aligned by period_index:

      - For each period_index, calculate total cohort size (weights)
      - Weighted retention = SUM(active) / SUM(customers_in_cohort)
      - Weighted revenue metrics similarly aggregated
    */
    select
        cast(null as date) as cohort_period,
        cast(null as date) as order_period,
        sum(customers_in_cohort) as customers_in_cohort,
        sum(active_customers) as active_customers,
        round(
            safe_divide(
                cast(sum(active_customers) as float64),
                cast(sum(customers_in_cohort) as float64)
            ), 4
        ) as retention_rate,
        round(sum(total_spent), 2) as total_spent,
        period_index,
        TRUE as is_weighted_average
    from retention
    group by period_index
)

select * from cohort_data
union all
select * from weighted_averages
order by is_weighted_average desc, cohort_period, period_index