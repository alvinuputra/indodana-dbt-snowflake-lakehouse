{% snapshot snap_loan_status %}

{{
    config(
        target_schema='SILVER',
        unique_key='loan_id',
        strategy='check',
        check_cols=['loan_status', 'outstanding_principal'],
        invalidate_hard_deletes=True
    )
}}

-- SCD2 history of loan status and outstanding balance.
--
-- This is the backbone of roll-rate and migration analysis: "how many
-- loans that were 1-30 last month are 31-60 this month" is unanswerable
-- from a current-state table alone. Each status transition becomes its own
-- version row with a valid-from/valid-to window.

select
    loan_id,
    loan_status,
    outstanding_principal,
    cdc_commit_ts as source_commit_ts

from {{ ref('slv_loans') }}
where not is_deleted

{% endsnapshot %}
