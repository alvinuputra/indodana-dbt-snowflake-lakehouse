{{ config(materialized='table') }}

-- Grain: one row per disbursed loan.
--
-- Carries the risk measures that every portfolio question needs:
-- current DPD, worst-ever DPD, and the ever-30/ever-90 flags that
-- define vintage performance.
--
-- "Ever" delinquency cannot come from the current loan status alone — a
-- loan that was 45 days late and then caught up shows as ACTIVE today.
-- It is reconstructed from two sources: the lateness recorded on each
-- settled payment, and the live DPD of instalments still unpaid.

with inst_agg as (

    select
        loan_id,
        count(*)                                            as installment_count,
        count_if(installment_status = 'PAID')               as installments_paid,
        count_if(installment_status = 'UNPAID')             as installments_unpaid,
        count_if(installment_status = 'UNPAID'
                 and due_date < current_date())             as installments_overdue,
        sum(total_due)                                      as total_scheduled,
        sum(iff(installment_status = 'UNPAID', principal_due, 0)) as unpaid_principal,
        sum(iff(installment_status = 'UNPAID', total_due, 0))     as unpaid_total,
        -- live DPD across still-unpaid instalments
        max(dpd)                                            as current_max_dpd,
        min(iff(installment_status = 'UNPAID' and due_date < current_date(),
                due_date, null))                            as oldest_overdue_date
    from {{ ref('slv_installments') }}
    where not is_deleted
    group by loan_id

),

pay_agg as (

    select
        loan_id,
        count(*)                                            as payment_count,
        sum(amount)                                         as total_collected,
        sum(principal_portion)                              as principal_collected,
        sum(interest_portion)                               as interest_collected,
        sum(late_fee)                                       as late_fees_collected,
        count_if(is_late_payment)                           as late_payment_count,
        max(days_late)                                      as worst_payment_days_late,
        max(paid_at)                                        as last_payment_at
    from {{ ref('slv_payments') }}
    where not is_deleted
    group by loan_id

),

coll_agg as (

    select
        loan_id,
        count(*)                                            as collection_contact_count,
        count_if(is_contact_made)                           as collection_reached_count,
        count_if(is_promise_to_pay)                         as promise_to_pay_count,
        count_if(is_field_visit)                            as field_visit_count,
        max(activity_at)                                    as last_collection_at
    from {{ ref('slv_collection_activities') }}
    where not is_deleted
    group by loan_id

)

select
    d.loan_id,
    d.application_id,
    d.customer_id,
    d.merchant_id,
    d.partner_id,
    d.product_type,
    d.tenor_band,
    d.tenor_months,
    d.ticket_size_band,
    d.origination_platform,
    d.bureau_score_band,
    d.loan_status,
    d.disbursed_at,
    d.disbursed_date,
    d.disbursed_month,
    d.first_due_date,
    d.maturity_date,
    d.months_on_book,

    -- exposure
    l.principal_amount,
    l.interest_rate_pa,
    l.admin_fee,
    l.outstanding_principal,
    coalesce(i.total_scheduled, 0)                          as total_scheduled,
    coalesce(i.unpaid_total, 0)                             as unpaid_total,
    coalesce(p.total_collected, 0)                          as total_collected,
    coalesce(p.principal_collected, 0)                      as principal_collected,
    coalesce(p.interest_collected, 0)                       as interest_collected,
    coalesce(p.late_fees_collected, 0)                      as late_fees_collected,

    -- repayment progress
    coalesce(i.installment_count, 0)                        as installment_count,
    coalesce(i.installments_paid, 0)                        as installments_paid,
    coalesce(i.installments_unpaid, 0)                      as installments_unpaid,
    coalesce(i.installments_overdue, 0)                     as installments_overdue,
    coalesce(p.payment_count, 0)                            as payment_count,
    coalesce(p.late_payment_count, 0)                       as late_payment_count,
    div0(coalesce(i.installments_paid, 0),
         coalesce(i.installment_count, 0))                  as pct_installments_paid,
    p.last_payment_at,

    -- delinquency
    coalesce(i.current_max_dpd, 0)                          as current_dpd,
    {{ dpd_bucket('coalesce(i.current_max_dpd, 0)') }}      as current_dpd_bucket,
    {{ dpd_bucket_sort('coalesce(i.current_max_dpd, 0)') }} as current_dpd_bucket_sort,
    i.oldest_overdue_date,

    greatest(coalesce(i.current_max_dpd, 0),
             coalesce(p.worst_payment_days_late, 0))        as max_dpd_ever,
    greatest(coalesce(i.current_max_dpd, 0),
             coalesce(p.worst_payment_days_late, 0)) >= 30  as is_ever_30_plus,
    greatest(coalesce(i.current_max_dpd, 0),
             coalesce(p.worst_payment_days_late, 0))
        >= {{ var('npl_dpd_threshold') }}                   as is_ever_90_plus,

    -- Non-performing on the balance-sheet definition: still owing AND
    -- currently past the NPL threshold. A loan that was once 90+ but has
    -- since been paid off is not an NPL today.
    coalesce(i.current_max_dpd, 0) >= {{ var('npl_dpd_threshold') }}
        and d.loan_status <> 'PAID_OFF'                     as is_npl,
    d.loan_status = 'WRITTEN_OFF'                           as is_written_off,
    d.loan_status = 'PAID_OFF'                              as is_paid_off,

    -- collection effort
    coalesce(c.collection_contact_count, 0)                 as collection_contact_count,
    coalesce(c.collection_reached_count, 0)                 as collection_reached_count,
    coalesce(c.promise_to_pay_count, 0)                     as promise_to_pay_count,
    coalesce(c.field_visit_count, 0)                        as field_visit_count,
    c.last_collection_at

from {{ ref('dim_loan') }} d
join      {{ ref('slv_loans') }} l on l.loan_id = d.loan_id and not l.is_deleted
left join inst_agg i on i.loan_id = d.loan_id
left join pay_agg  p on p.loan_id = d.loan_id
left join coll_agg c on c.loan_id = d.loan_id
