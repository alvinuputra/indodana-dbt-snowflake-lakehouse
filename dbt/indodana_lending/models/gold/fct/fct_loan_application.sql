{{ config(materialized='table') }}

-- Grain: one row per loan application.
-- The origination funnel fact: every decision the underwriting engine made,
-- joined to the lead that produced it so acquisition cost can be attributed
-- all the way down to a disbursed loan.

select
    a.application_id,
    a.customer_id,
    a.lead_id,
    l.partner_id,
    a.product_type,
    a.tenor_months,

    a.applied_at,
    a.applied_date,
    a.applied_month,
    a.decision_at,
    a.decision_minutes,

    a.requested_amount,
    a.approved_amount,
    a.approved_rate_pa,
    a.bureau_score,
    a.bureau_score_band,
    a.has_bureau_file,
    a.internal_score,

    a.decision,
    a.decline_reason,
    a.decision = 'APPROVED'                         as is_approved,
    a.decision = 'DECLINED'                         as is_declined,
    a.decision = 'PENDING'                          as is_pending,

    -- Attribution: a lead-sourced application is a marketplace conversion.
    -- cost_per_lead is carried on the application so the funnel mart can
    -- divide acquisition spend by disbursed loans without a second join.
    l.lead_source,
    l.campaign_code,
    l.cost_per_lead,
    a.lead_id is not null                           as is_marketplace_sourced,

    -- did this application turn into money out the door
    ln.loan_id,
    ln.loan_id is not null                          as is_disbursed,
    ln.principal_amount                             as disbursed_amount,

    c.risk_segment,
    c.province,
    c.income_band

from {{ ref('slv_loan_applications') }} a
left join {{ ref('slv_leads') }} l
       on l.lead_id = a.lead_id
      and not l.is_deleted
left join {{ ref('slv_loans') }} ln
       on ln.application_id = a.application_id
      and not ln.is_deleted
left join {{ ref('dim_customer') }} c
       on c.customer_id = a.customer_id
where not a.is_deleted
