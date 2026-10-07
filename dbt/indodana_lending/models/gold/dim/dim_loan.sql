{{ config(materialized='table') }}

-- Current-state loan dimension: the descriptive attributes of a loan.
-- Balances, DPD and risk flags are measures and live in fct_loan.

select
    l.loan_id,
    l.application_id,
    l.customer_id,
    l.merchant_id,
    l.partner_id,
    l.product_type,
    l.tenor_months,
    case
        when l.tenor_months <=  6 then 'SHORT (<=6m)'
        when l.tenor_months <= 12 then 'MEDIUM (7-12m)'
        else                           'LONG (>12m)'
    end                                             as tenor_band,
    l.interest_rate_pa,
    l.admin_fee,
    l.ticket_size_band,
    l.loan_status,
    l.loan_status in ('DELINQUENT', 'WRITTEN_OFF')  as is_non_performing,
    l.loan_status = 'PAID_OFF'                      as is_closed,
    l.disbursed_at,
    l.disbursed_date,
    l.disbursed_month,
    l.first_due_date,
    l.maturity_date,
    l.months_on_book,
    l.maturity_date < current_date()                as is_past_maturity,

    -- origination channel, carried down from the application
    a.decision,
    a.bureau_score_band,
    a.lead_id,
    case when a.lead_id is not null then 'CERMATI_MARKETPLACE'
         else 'INDODANA_DIRECT' end                 as origination_platform,

    l.cdc_commit_ts                                 as last_changed_at,
    l.cdc_batch_id                                  as last_changed_batch_id

from {{ ref('slv_loans') }} l
left join {{ ref('slv_loan_applications') }} a
       on a.application_id = l.application_id
      and not a.is_deleted
where not l.is_deleted
