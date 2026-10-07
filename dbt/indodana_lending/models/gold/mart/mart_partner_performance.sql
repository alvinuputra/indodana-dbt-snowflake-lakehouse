{{ config(materialized='table') }}

-- Cermati marketplace partner economics.
--
-- Grain: partner_id x period_month
--
-- Marketplace revenue is commission on disbursed volume; marketplace cost
-- is lead spend. Both are needed in one place, because a partner with high
-- commission that only converts expensive paid-search leads can be less
-- profitable than a low-commission partner fed by organic traffic.
--
-- Credit performance sits in the same row deliberately: a partner driving
-- high volume at a 15% NPL rate is not a good partner regardless of its
-- commission rate.

with lead_spend as (

    select
        partner_id,
        created_month                       as period_month,
        count(*)                            as leads,
        sum(cost_per_lead)                  as lead_spend
    from {{ ref('slv_leads') }}
    where not is_deleted
    group by 1, 2

),

loan_perf as (

    select
        partner_id,
        disbursed_month                     as period_month,
        count(*)                            as loans_disbursed,
        sum(principal_amount)               as principal_disbursed,
        sum(outstanding_principal)          as outstanding_principal,
        sum(total_collected)                as total_collected,
        count_if(is_npl)                    as npl_loans,
        sum(iff(is_npl, outstanding_principal, 0)) as npl_outstanding,
        count_if(is_ever_30_plus)           as ever_30_plus_loans,
        count_if(is_paid_off)               as paid_off_loans,
        avg(months_on_book)                 as avg_months_on_book
    from {{ ref('fct_loan') }}
    group by 1, 2

)

select
    coalesce(p.partner_id, s.partner_id)                as partner_id,
    d.partner_name,
    d.partner_type,
    d.commission_rate,
    d.is_active,
    coalesce(p.period_month, s.period_month)            as period_month,

    coalesce(s.leads, 0)                                as leads,
    coalesce(s.lead_spend, 0)                           as lead_spend,
    coalesce(p.loans_disbursed, 0)                      as loans_disbursed,
    coalesce(p.principal_disbursed, 0)                  as principal_disbursed,
    coalesce(p.outstanding_principal, 0)                as outstanding_principal,
    coalesce(p.total_collected, 0)                      as total_collected,

    -- commission earned on disbursed volume
    round(coalesce(p.principal_disbursed, 0) * coalesce(d.commission_rate, 0))
                                                        as commission_revenue,
    round(coalesce(p.principal_disbursed, 0) * coalesce(d.commission_rate, 0)
          - coalesce(s.lead_spend, 0))                  as net_marketplace_margin,

    round(div0(coalesce(p.loans_disbursed, 0), coalesce(s.leads, 0)) * 100, 2)
                                                        as lead_conversion_pct,
    round(div0(coalesce(s.lead_spend, 0), coalesce(p.loans_disbursed, 0)))
                                                        as cost_per_disbursed_loan,
    round(div0(coalesce(p.principal_disbursed, 0), coalesce(p.loans_disbursed, 0)))
                                                        as avg_ticket_size,

    coalesce(p.npl_loans, 0)                            as npl_loans,
    coalesce(p.npl_outstanding, 0)                      as npl_outstanding,
    round(div0(coalesce(p.npl_outstanding, 0),
               coalesce(p.outstanding_principal, 0)) * 100, 2) as npl_ratio_pct,
    round(div0(coalesce(p.ever_30_plus_loans, 0),
               coalesce(p.loans_disbursed, 0)) * 100, 2)       as ever_30_plus_pct,
    coalesce(p.paid_off_loans, 0)                       as paid_off_loans,
    round(p.avg_months_on_book, 1)                      as avg_months_on_book

from loan_perf p
full outer join lead_spend s
     on  s.partner_id   = p.partner_id
     and s.period_month = p.period_month
left join {{ ref('dim_partner') }} d
     on  d.partner_id = coalesce(p.partner_id, s.partner_id)
order by period_month desc, principal_disbursed desc
