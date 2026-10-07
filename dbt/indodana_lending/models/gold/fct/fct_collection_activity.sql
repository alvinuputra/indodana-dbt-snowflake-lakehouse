{{ config(materialized='table') }}

-- Grain: one row per collection contact attempt.
--
-- `is_cured_within_14d` is the only measure here that is not a plain
-- passthrough: it asks whether money actually arrived on the loan within
-- two weeks of the contact. That is the metric collection teams are
-- managed on — contact volume alone says nothing about effectiveness.

with cured as (

    select
        a.activity_id,
        max(iff(p.paid_at > a.activity_at
                and p.paid_at <= dateadd(day, 14, a.activity_at), 1, 0)) as cured_flag,
        sum(iff(p.paid_at > a.activity_at
                and p.paid_at <= dateadd(day, 14, a.activity_at),
                p.amount, 0))                                            as amount_cured
    from {{ ref('slv_collection_activities') }} a
    left join {{ ref('slv_payments') }} p
           on p.loan_id = a.loan_id
          and not p.is_deleted
    where not a.is_deleted
    group by a.activity_id

)

select
    a.activity_id,
    a.loan_id,
    a.customer_id,
    a.collector_id,
    l.product_type,
    l.partner_id,
    l.disbursed_month,

    a.activity_at,
    a.activity_date,
    a.activity_month,

    a.channel,
    a.action_code,
    a.outcome,
    a.collection_bucket,
    a.dpd_at_contact,
    {{ dpd_bucket('a.dpd_at_contact') }}            as dpd_bucket_at_contact,
    {{ dpd_bucket_sort('a.dpd_at_contact') }}       as dpd_bucket_at_contact_sort,

    a.is_contact_made,
    a.is_promise_to_pay,
    a.is_positive_outcome,
    a.is_field_visit,
    a.promise_amount,
    a.promise_date,

    coalesce(c.cured_flag, 0) = 1                   as is_cured_within_14d,
    coalesce(c.amount_cured, 0)                     as amount_cured_within_14d

from {{ ref('slv_collection_activities') }} a
join {{ ref('slv_loans') }} l
       on l.loan_id = a.loan_id
      and not l.is_deleted
left join cured c on c.activity_id = a.activity_id
where not a.is_deleted
