{{ config(
    materialized = 'table'
) }}

{% set start_date = "'2020-01-01'" %}
{% set end_date = "'2030-12-31'" %}

with date_spine as (
    {{ dbt_date.date_spine(
        datepart="day",
        start_date=start_date,
        end_date=end_date
    ) }}
)

select
    cast(date_day as date) as date,
    cast(extract(year from date_day) as int64) as year,
    cast(extract(month from date_day) as int64) as month,
    cast(format_date('%B', date_day) as string) as month_name,
    cast(extract(quarter from date_day) as int64) as quarter,
    cast(extract(dayofweek from date_day) as int64) as day_of_week,
    cast(format_date('%A', date_day) as string) as day_name,
    cast(case when extract(dayofweek from date_day) in (1,7) then true else false end as bool) as is_weekend
from date_spine
order by date_day
