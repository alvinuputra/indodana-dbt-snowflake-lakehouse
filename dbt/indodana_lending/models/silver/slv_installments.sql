{{
    config(
        materialized='incremental',
        unique_key='installment_id',
        incremental_strategy='merge'
    )
}}

-- Current state of the installment schedule across all loans.
--
-- Tombstoned rows are retained with is_deleted = true rather than removed,
-- so the gold layer filters them explicitly and the audit trail survives.

{{ cdc_current(ref('stg_installments'), 'installment_id') }}
