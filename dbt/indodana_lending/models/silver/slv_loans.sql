{{
    config(
        materialized='incremental',
        unique_key='loan_id',
        incremental_strategy='merge'
    )
}}

-- Current state of every disbursed loan. Highest churn table in the feed:
-- status and outstanding principal are rewritten on every payment.
--
-- Tombstoned rows are retained with is_deleted = true rather than removed,
-- so the gold layer filters them explicitly and the audit trail survives.

{{ cdc_current(ref('stg_loans'), 'loan_id') }}
