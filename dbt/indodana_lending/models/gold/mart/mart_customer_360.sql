{{ config(materialized='table') }}

-- One row per customer: the full relationship, acquisition through
-- repayment behaviour. Feeds limit-increase campaigns, cross-sell
-- targeting and churn work.
--
-- Grain: customer_id

with loan_agg as (

    select
        customer_id,
        count(*)                                as loans_total,
        count_if(loan_status = 'ACTIVE')         as loans_active,
        count_if(is_paid_off)                    as loans_paid_off,
        count_if(loan_status = 'DELINQUENT')     as loans_delinquent,
        count_if(is_written_off)                 as loans_written_off,
        sum(principal_amount)                    as lifetime_principal,
        sum(outstanding_principal)               as current_outstanding,
        sum(total_collected)                     as lifetime_collected,
        sum(interest_collected)                  as lifetime_interest,
        sum(late_fees_collected)                 as lifetime_late_fees,
        max(max_dpd_ever)                        as worst_dpd_ever,
        max(current_dpd)                         as current_max_dpd,
        count_if(is_ever_30_plus)                as loans_ever_30_plus,
        min(disbursed_date)                      as first_disbursed_date,
        max(disbursed_date)                      as last_disbursed_date,
        max(last_payment_at)                     as last_payment_at,
        sum(payment_count)                       as payments_made,
        sum(late_payment_count)                  as late_payments_made,
        count(distinct product_type)             as products_held
    from {{ ref('fct_loan') }}
    group by customer_id

),

app_agg as (

    select
        customer_id,
        count(*)                                as applications_total,
        count_if(is_approved)                   as applications_approved,
        count_if(is_declined)                   as applications_declined,
        max(applied_at)                         as last_applied_at,
        max(internal_score)                     as latest_internal_score,
        max(bureau_score)                       as latest_bureau_score
    from {{ ref('fct_loan_application') }}
    group by customer_id

),

limit_changes as (

    select
        customer_id,
        count(*)                                as credit_limit_versions,
        min(credit_limit)                       as lowest_limit_ever,
        max(credit_limit)                       as highest_limit_ever
    from {{ ref('dim_customer_credit_history') }}
    group by customer_id

)

select
    c.customer_id,
    c.full_name,
    c.city,
    c.province,
    c.age_band,
    c.gender,
    c.occupation,
    c.income_band,
    c.monthly_income,
    c.kyc_status,
    c.risk_segment,
    c.credit_limit,
    c.acquisition_platform,
    c.acquisition_lead_source,
    c.registered_date,
    c.tenure_days,

    coalesce(a.applications_total, 0)           as applications_total,
    coalesce(a.applications_approved, 0)        as applications_approved,
    coalesce(a.applications_declined, 0)        as applications_declined,
    a.last_applied_at,
    a.latest_internal_score,
    a.latest_bureau_score,

    coalesce(l.loans_total, 0)                  as loans_total,
    coalesce(l.loans_active, 0)                 as loans_active,
    coalesce(l.loans_paid_off, 0)               as loans_paid_off,
    coalesce(l.loans_delinquent, 0)             as loans_delinquent,
    coalesce(l.loans_written_off, 0)            as loans_written_off,
    coalesce(l.products_held, 0)                as products_held,
    coalesce(l.lifetime_principal, 0)           as lifetime_principal,
    coalesce(l.current_outstanding, 0)          as current_outstanding,
    coalesce(l.lifetime_collected, 0)           as lifetime_collected,
    coalesce(l.lifetime_interest, 0)            as lifetime_interest,
    coalesce(l.lifetime_late_fees, 0)           as lifetime_late_fees,
    -- revenue proxy: interest plus fees actually received
    coalesce(l.lifetime_interest, 0)
      + coalesce(l.lifetime_late_fees, 0)       as lifetime_revenue,

    l.first_disbursed_date,
    l.last_disbursed_date,
    l.last_payment_at,
    coalesce(l.payments_made, 0)                as payments_made,
    coalesce(l.late_payments_made, 0)           as late_payments_made,
    round(div0(coalesce(l.payments_made, 0) - coalesce(l.late_payments_made, 0),
               coalesce(l.payments_made, 0)) * 100, 2) as on_time_payment_pct,

    coalesce(l.worst_dpd_ever, 0)               as worst_dpd_ever,
    coalesce(l.current_max_dpd, 0)              as current_max_dpd,
    {{ dpd_bucket('coalesce(l.current_max_dpd, 0)') }} as current_dpd_bucket,
    coalesce(l.loans_ever_30_plus, 0)           as loans_ever_30_plus,

    coalesce(h.credit_limit_versions, 1)        as credit_limit_versions,
    h.lowest_limit_ever,
    h.highest_limit_ever,
    c.credit_limit - coalesce(l.current_outstanding, 0) as available_limit,
    round(div0(coalesce(l.current_outstanding, 0), c.credit_limit) * 100, 2)
                                                as limit_utilisation_pct,

    -- Relationship state. Ordered so the worst signal wins: a customer with
    -- a write-off is NOT "dormant" just because they stopped transacting.
    case
        when coalesce(l.loans_written_off, 0) > 0        then 'WRITTEN_OFF'
        when coalesce(l.loans_delinquent, 0)  > 0        then 'DELINQUENT'
        when coalesce(l.loans_active, 0)      > 0        then 'ACTIVE_BORROWER'
        when coalesce(l.loans_paid_off, 0)    > 0        then 'DORMANT_GOOD'
        when coalesce(a.applications_total, 0) > 0       then 'NEVER_BORROWED'
        else 'REGISTERED_ONLY'
    end                                         as relationship_status,

    -- Upsell gate: clean history, real headroom, no live delinquency.
    coalesce(l.loans_paid_off, 0) >= 1
      and coalesce(l.worst_dpd_ever, 0) <= 5
      and coalesce(l.current_max_dpd, 0) = 0
      and c.kyc_status = 'VERIFIED'             as is_limit_increase_candidate

from {{ ref('dim_customer') }} c
left join loan_agg      l on l.customer_id = c.customer_id
left join app_agg       a on a.customer_id = c.customer_id
left join limit_changes h on h.customer_id = c.customer_id
