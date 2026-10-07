{{
    config(
        materialized='incremental',
        unique_key='application_id',
        incremental_strategy='merge'
    )
}}

-- Current state of every loan application and its underwriting decision.
--
-- Tombstoned rows are retained with is_deleted = true rather than removed,
-- so the gold layer filters them explicitly and the audit trail survives.

{{ cdc_current(ref('stg_loan_applications'), 'application_id') }}
