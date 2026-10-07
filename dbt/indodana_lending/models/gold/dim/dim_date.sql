{{ config(materialized='table') }}

-- Conformed date dimension covering the full observable window:
-- the earliest application through the latest instalment maturity.
-- Built from a generator spine rather than a seed file so it extends
-- automatically as the loan book ages.

with bounds as (

    select
        least(
            (select min(applied_date)    from {{ ref('fct_loan_application') }}),
            (select min(disbursed_date)  from {{ ref('fct_loan') }})
        )                                               as min_date,
        greatest(
            (select max(due_date)        from {{ ref('fct_installment') }}),
            current_date()
        )                                               as max_date
),

spine as (

    -- generator rowcount cannot be dynamic, so overshoot and clamp.
    -- The clamp is a plain WHERE on a subquery: QUALIFY needs a window
    -- function in scope, and there isn't one here.
    select date_day
    from (
        select dateadd(day, seq4(), (select min_date from bounds)) as date_day
        from table(generator(rowcount => 10000))
    )
    where date_day <= (select max_date from bounds)

)

select
    date_day,
    to_number(to_char(date_day, 'YYYYMMDD'))        as date_key,
    year(date_day)                                  as year_number,
    quarter(date_day)                               as quarter_number,
    month(date_day)                                 as month_number,
    monthname(date_day)                             as month_name,
    day(date_day)                                   as day_of_month,
    dayofweek(date_day)                             as day_of_week,
    dayname(date_day)                               as day_name,
    weekofyear(date_day)                            as week_of_year,
    date_trunc('month',   date_day)::date           as month_start_date,
    last_day(date_day, 'month')                     as month_end_date,
    date_trunc('quarter', date_day)::date           as quarter_start_date,
    to_char(date_day, 'YYYY-MM')                    as year_month,
    dayofweek(date_day) in (0, 6)                   as is_weekend,
    date_day = current_date()                       as is_today,
    date_day <= current_date()                      as is_past_or_today

from spine
