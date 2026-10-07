{{ config(materialized='table') }}

-- Vintage triangle: cumulative delinquency by disbursement cohort and
-- months on book.
--
-- Grain: disbursed_month x product_type x months_on_book
--
-- This is a genuine vintage CURVE, not a cohort snapshot. The difference
-- matters: comparing a 20-month-old cohort's lifetime bad rate against a
-- 2-month-old cohort's is meaningless, because the young cohort has not
-- had time to go bad. Here each cohort is measured at the SAME age.
--
-- The age at which a loan first went 30+/90+ is reconstructed from the
-- instalment number it happened on: instalment N falls due roughly N
-- months after disbursement, so the first delinquent instalment number is
-- the months-on-book at first delinquency.
--
-- Cells beyond a cohort's observed age are returned as NULL rather than 0.
-- Zero would read as "no defaults yet", when the truth is "not observed
-- yet" — the single most common way a vintage report misleads.

with first_bad as (

    select
        i.loan_id,
        min(iff(coalesce(i.settled_days_late, 0) >= 30 or i.dpd >= 30,
                i.installment_no, null))                    as first_30_mob,
        min(iff(coalesce(i.settled_days_late, 0) >= {{ var('npl_dpd_threshold') }}
                or i.dpd >= {{ var('npl_dpd_threshold') }},
                i.installment_no, null))                    as first_90_mob
    from {{ ref('fct_installment') }} i
    group by i.loan_id

),

cohort as (

    select
        f.disbursed_month,
        f.product_type,
        f.loan_id,
        f.principal_amount,
        f.months_on_book,
        b.first_30_mob,
        b.first_90_mob
    from {{ ref('fct_loan') }} f
    left join first_bad b on b.loan_id = f.loan_id

),

cohort_totals as (

    select
        disbursed_month,
        product_type,
        count(*)                        as cohort_loans,
        sum(principal_amount)           as cohort_principal,
        -- the oldest loan in the cohort bounds how far the curve is observable
        max(months_on_book)             as cohort_max_mob
    from cohort
    group by 1, 2

),

mob_spine as (

    select i as months_on_book
    from (select seq4() + 1 as i from table(generator(rowcount => 60)))
    where i <= {{ var('vintage_max_months_on_book') }}

),

grid as (

    select
        t.disbursed_month,
        t.product_type,
        t.cohort_loans,
        t.cohort_principal,
        t.cohort_max_mob,
        m.months_on_book
    from cohort_totals t
    cross join mob_spine m

)

select
    g.disbursed_month,
    g.product_type,
    g.months_on_book,
    g.cohort_loans,
    g.cohort_principal,
    g.cohort_max_mob,

    -- is this cell actually observable for this cohort yet
    g.months_on_book <= g.cohort_max_mob                    as is_observed,

    iff(g.months_on_book <= g.cohort_max_mob,
        count_if(c.first_30_mob <= g.months_on_book), null)  as loans_ever_30_by_mob,
    iff(g.months_on_book <= g.cohort_max_mob,
        count_if(c.first_90_mob <= g.months_on_book), null)  as loans_ever_90_by_mob,

    iff(g.months_on_book <= g.cohort_max_mob,
        round(div0(count_if(c.first_30_mob <= g.months_on_book),
                   g.cohort_loans) * 100, 2), null)          as ever_30_rate_pct,
    iff(g.months_on_book <= g.cohort_max_mob,
        round(div0(count_if(c.first_90_mob <= g.months_on_book),
                   g.cohort_loans) * 100, 2), null)          as ever_90_rate_pct,

    iff(g.months_on_book <= g.cohort_max_mob,
        sum(iff(c.first_90_mob <= g.months_on_book,
                c.principal_amount, 0)), null)               as principal_ever_90_by_mob,
    iff(g.months_on_book <= g.cohort_max_mob,
        round(div0(sum(iff(c.first_90_mob <= g.months_on_book,
                           c.principal_amount, 0)),
                   g.cohort_principal) * 100, 2), null)      as ever_90_rate_by_value_pct

from grid g
join cohort c
      on  c.disbursed_month = g.disbursed_month
      and c.product_type    = g.product_type
group by
    g.disbursed_month, g.product_type, g.months_on_book,
    g.cohort_loans, g.cohort_principal, g.cohort_max_mob
order by g.disbursed_month desc, g.product_type, g.months_on_book
