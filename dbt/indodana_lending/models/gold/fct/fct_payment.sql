{{ config(materialized='table') }}

-- Grain: one row per payment.
-- Cash-in fact: collections by channel, product and lateness.

select
    p.payment_id,
    p.loan_id,
    p.installment_id,
    p.customer_id,
    l.partner_id,
    l.merchant_id,
    l.product_type,
    l.disbursed_month,

    p.paid_at,
    p.paid_date,
    p.paid_month,

    p.amount,
    p.principal_portion,
    p.interest_portion,
    p.late_fee,

    p.payment_channel,
    p.channel_group,
    p.payment_type,
    p.days_late,
    p.is_late_payment,
    {{ dpd_bucket('p.days_late') }}                 as paid_from_bucket,

    i.due_date,
    i.installment_no,
    i.total_due,
    -- a short payment means the instalment is only partially settled
    p.amount < (i.total_due + p.late_fee)           as is_underpayment

from {{ ref('slv_payments') }} p
join {{ ref('slv_loans') }} l
       on l.loan_id = p.loan_id
      and not l.is_deleted
left join {{ ref('slv_installments') }} i
       on i.installment_id = p.installment_id
      and not i.is_deleted
where not p.is_deleted
