{{ config(materialized='table') }}

-- Origination funnel by month, product and acquisition platform.
--
-- Grain: applied_month x product_type x origination_platform x lead_source
--
-- Leads are counted on their own creation month, not the application month,
-- because a lead that has not applied yet still cost money to acquire —
-- counting it on the application month would understate spend in the
-- current period and flatter the cost-per-disbursed-loan metric.

with leads as (

    select
        created_month                                   as period_month,
        product_interest                                as product_type,
        'CERMATI_MARKETPLACE'                           as origination_platform,
        lead_source,
        count(*)                                        as leads,
        sum(cost_per_lead)                              as lead_spend
    from {{ ref('slv_leads') }}
    where not is_deleted
    group by 1, 2, 3, 4

),

apps as (

    select
        applied_month                                   as period_month,
        product_type,
        case when is_marketplace_sourced then 'CERMATI_MARKETPLACE'
             else 'INDODANA_DIRECT' end                 as origination_platform,
        coalesce(lead_source, 'DIRECT_APP')             as lead_source,
        count(*)                                        as applications,
        count_if(is_approved)                           as approved,
        count_if(is_declined)                           as declined,
        count_if(is_pending)                            as pending,
        count_if(is_disbursed)                          as disbursed,
        sum(requested_amount)                           as requested_amount,
        sum(coalesce(approved_amount, 0))               as approved_amount,
        sum(coalesce(disbursed_amount, 0))              as disbursed_amount,
        avg(decision_minutes)                           as avg_decision_minutes,
        avg(internal_score)                             as avg_internal_score,
        avg(bureau_score)                               as avg_bureau_score
    from {{ ref('fct_loan_application') }}
    group by 1, 2, 3, 4

),

combined as (

    select
        coalesce(a.period_month, l.period_month)            as period_month,
        coalesce(a.product_type, l.product_type)            as product_type,
        coalesce(a.origination_platform, l.origination_platform) as origination_platform,
        coalesce(a.lead_source, l.lead_source)              as lead_source,
        coalesce(l.leads, 0)                                as leads,
        coalesce(l.lead_spend, 0)                           as lead_spend,
        coalesce(a.applications, 0)                         as applications,
        coalesce(a.approved, 0)                             as approved,
        coalesce(a.declined, 0)                             as declined,
        coalesce(a.pending, 0)                              as pending,
        coalesce(a.disbursed, 0)                            as disbursed,
        coalesce(a.requested_amount, 0)                     as requested_amount,
        coalesce(a.approved_amount, 0)                      as approved_amount,
        coalesce(a.disbursed_amount, 0)                     as disbursed_amount,
        a.avg_decision_minutes,
        a.avg_internal_score,
        a.avg_bureau_score
    from apps a
    full outer join leads l
         on  l.period_month          = a.period_month
         and l.product_type          = a.product_type
         and l.origination_platform  = a.origination_platform
         and l.lead_source           = a.lead_source

)

select
    period_month,
    product_type,
    origination_platform,
    lead_source,

    leads,
    applications,
    approved,
    declined,
    pending,
    disbursed,

    requested_amount,
    approved_amount,
    disbursed_amount,
    lead_spend,

    -- Funnel conversion. div0 everywhere: a brand-new lead source has
    -- zero applications in its first month and would otherwise blow up.
    round(div0(applications, leads)            * 100, 2) as lead_to_application_pct,
    round(div0(approved,     applications)     * 100, 2) as approval_pct,
    round(div0(disbursed,    approved)         * 100, 2) as disbursal_pct,
    round(div0(disbursed,    applications)     * 100, 2) as application_to_disbursed_pct,
    round(div0(disbursed,    leads)            * 100, 2) as lead_to_disbursed_pct,

    round(div0(disbursed_amount, disbursed))             as avg_ticket_size,
    round(div0(approved_amount,  approved))              as avg_approved_amount,
    -- acquisition cost per funded loan: the number that decides whether a
    -- marketing channel is worth keeping
    round(div0(lead_spend, disbursed))                   as cost_per_disbursed_loan,

    round(avg_decision_minutes, 1)                       as avg_decision_minutes,
    round(avg_internal_score, 1)                         as avg_internal_score,
    round(avg_bureau_score, 1)                           as avg_bureau_score

from combined
order by period_month desc, product_type, origination_platform, lead_source
