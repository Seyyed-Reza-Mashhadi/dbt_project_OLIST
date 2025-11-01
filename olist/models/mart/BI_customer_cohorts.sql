{{
    config(
        materialized='table',
        description='Cohort retention and revenue analysis showing how customer groups behave over time'
    )
}}

/*
============================================================================
COHORT RETENTION & REVENUE ANALYSIS
============================================================================

PURPOSE:
This model analyzes customer cohorts based on their first purchase date and 
tracks their retention and spending behavior over time. It includes both 
individual cohort performance and weighted averages across all cohorts.

CONFIGURATION - EDIT THESE VARIABLES:
- START_DATE: Beginning of analysis period
- END_DATE: End of analysis period  
- COHORT_PERIOD: Time granularity (WEEK, MONTH, QUARTER, or YEAR)

OUTPUTS:
- cohort_period: Date when customers made their first purchase
- order_period: Date of the activity period being measured
- customers_in_cohort: Total unique customers in this cohort
- active_customers: Number of cohort customers active in this period
- retention_rate: Percentage of cohort still active (1.0 = 100%)
- total_spent: Total revenue from cohort customers in this period
- avg_spent_per_customer: Average spend per active customer in this period
- period_index: Number of periods since cohort start (0 = first period)
- is_weighted_average: TRUE for summary rows, FALSE for individual cohorts

INTERPRETATION:
- period_index 0 always has 100% retention (customers' first purchase)
- Retention typically decreases in subsequent periods
- Weighted averages help identify overall trends across all cohorts
- Compare cohorts to see if newer/older groups perform differently

============================================================================
*/

{% set START_DATE = '2017-11-01' %}
{% set END_DATE = '2018-11-01' %}
{% set COHORT_PERIOD = 'MONTH' %}  -- Options: WEEK, MONTH, QUARTER, YEAR

/*
============================================================================
*/

with source_data as (
    -- Filter orders to analysis date range
    select
        customer_unique_id,
        order_purchase_timestamp,
        payment_value
    from {{ ref('INT_customers_finalized_orders') }}
    where date(order_purchase_timestamp) between date('{{ START_DATE }}') and date('{{ END_DATE }}')
),

customer_cohorts as (
    -- Assign each customer to their cohort based on first purchase date
    select
        customer_unique_id,
        date_trunc(date(min(order_purchase_timestamp)), {{ COHORT_PERIOD }}) as cohort_period
    from source_data
    group by customer_unique_id
),

orders as (
    -- Aggregate orders by customer, cohort, and order period
    select
        o.customer_unique_id,
        c.cohort_period,
        date_trunc(date(o.order_purchase_timestamp), {{ COHORT_PERIOD }}) as order_period,
        sum(o.payment_value) as total_spent
    from source_data o
    inner join customer_cohorts c on o.customer_unique_id = c.customer_unique_id
    group by o.customer_unique_id, c.cohort_period, date_trunc(date(o.order_purchase_timestamp), {{ COHORT_PERIOD }})
),

cohort_size as (
    -- Calculate total size of each cohort
    select
        cohort_period,
        count(distinct customer_unique_id) as customers_in_cohort
    from customer_cohorts
    group by cohort_period
),

activity as (
    -- Aggregate customer activity and spending by cohort and period
    select
        cohort_period,
        order_period,
        count(distinct customer_unique_id) as active_customers,
        sum(total_spent) as total_spent
    from orders
    group by cohort_period, order_period
),

retention as (
    -- Calculate retention metrics for each cohort-period combination
    select
        a.cohort_period,
        a.order_period,
        c.customers_in_cohort,
        a.active_customers,
        round(safe_divide(cast(a.active_customers as float64), cast(c.customers_in_cohort as float64)), 4) as retention_rate,
        a.total_spent,
        round(safe_divide(a.total_spent, cast(a.active_customers as float64)), 2) as avg_spent_per_customer,
        date_diff(a.order_period, a.cohort_period, {{ COHORT_PERIOD }}) as period_index
    from activity a
    inner join cohort_size c on a.cohort_period = c.cohort_period
    where a.order_period >= a.cohort_period  -- Only include periods after or during cohort start
),

cohort_data as (
    -- Individual cohort performance data
    select
        cohort_period,
        order_period,
        customers_in_cohort,
        active_customers,
        retention_rate,
        total_spent,
        avg_spent_per_customer,
        period_index,
        FALSE as is_weighted_average
    from retention
),

weighted_averages as (
    -- Calculate weighted averages across all cohorts for each order_period
    -- Weighted by cohort size so larger cohorts have more influence
    -- This shows the overall performance trend across all cohorts over time
    select
        cast(null as date) as cohort_period,
        order_period,
        sum(customers_in_cohort) as customers_in_cohort,
        round(safe_divide(
            sum(cast(active_customers as float64) * cast(customers_in_cohort as float64)),
            sum(cast(customers_in_cohort as float64))
        ), 2) as active_customers,
        round(safe_divide(
            sum(retention_rate * cast(customers_in_cohort as float64)),
            sum(cast(customers_in_cohort as float64))
        ), 4) as retention_rate,
        round(safe_divide(
            sum(total_spent * cast(customers_in_cohort as float64)),
            sum(cast(customers_in_cohort as float64))
        ), 2) as total_spent,
        round(safe_divide(
            sum(avg_spent_per_customer * cast(customers_in_cohort as float64)),
            sum(cast(customers_in_cohort as float64))
        ), 2) as avg_spent_per_customer,
        cast(null as int64) as period_index,
        TRUE as is_weighted_average
    from retention
    group by order_period
)

-- Combine individual cohort data with weighted averages
select * from cohort_data
union all
select * from weighted_averages
order by is_weighted_average desc, cohort_period, order_period