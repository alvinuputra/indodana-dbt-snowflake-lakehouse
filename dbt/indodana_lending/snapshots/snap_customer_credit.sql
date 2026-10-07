{% snapshot snap_customer_credit %}

{{
    config(
        target_schema='SILVER',
        unique_key='customer_id',
        strategy='check',
        check_cols=['credit_limit', 'risk_segment', 'kyc_status', 'monthly_income'],
        invalidate_hard_deletes=True
    )
}}

-- SCD2 history of the customer credit profile.
--
-- Why a snapshot and not just the silver table: silver holds only the
-- CURRENT limit and segment. Credit risk needs the limit as it stood when
-- a loan was approved, and regulators ask when a customer was re-graded.
-- Bronze does technically carry every version, but reconstructing valid-from
-- and valid-to windows from a raw change stream on every query is both slow
-- and easy to get wrong — the snapshot materialises it once.
--
-- `check` strategy rather than `timestamp`: the source emits no reliable
-- last-modified column on these attributes, and we only want a new version
-- when a tracked attribute actually moves — not on every unrelated update
-- to the customer row.

select
    customer_id,
    credit_limit,
    risk_segment,
    kyc_status,
    monthly_income,
    cdc_commit_ts as source_commit_ts

from {{ ref('slv_customers') }}
where not is_deleted

{% endsnapshot %}
