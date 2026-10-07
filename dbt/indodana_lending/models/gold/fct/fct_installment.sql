{{ config(materialized='table') }}

-- Grain: one row per instalment.
-- The aging fact: every scheduled amount with its bucket as at today.
-- This is what an aging report or a provisioning calculation reads.

select
    i.installment_id,
    i.loan_id,
    l.customer_id,
    l.partner_id,
    l.product_type,
    l.disbursed_month,

    i.installment_no,
    i.due_date,
    i.due_month,
    i.due_date < current_date()                     as is_due,

    i.principal_due,
    i.interest_due,
    i.fee_due,
    i.total_due,
    i.amount_paid,
    greatest(0, i.total_due - i.amount_paid)        as amount_outstanding,

    i.installment_status,
    i.installment_status = 'PAID'                   as is_paid,
    i.paid_at,
    -- lateness of a SETTLED instalment comes off the payment, not from
    -- today's date — the instalment is no longer accruing DPD
    p.days_late                                     as settled_days_late,

    i.dpd,
    {{ dpd_bucket('i.dpd') }}                       as dpd_bucket,
    {{ dpd_bucket_sort('i.dpd') }}                  as dpd_bucket_sort,
    i.dpd >= {{ var('npl_dpd_threshold') }}         as is_npl_installment

from {{ ref('slv_installments') }} i
join {{ ref('slv_loans') }} l
       on l.loan_id = i.loan_id
      and not l.is_deleted
left join {{ ref('slv_payments') }} p
       on p.installment_id = i.installment_id
      and not p.is_deleted
where not i.is_deleted
