{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        incremental_strategy='merge'
    )
}}

-- Payment events. Immutable in the source, but resolved through the same
-- CDC pattern as everything else so a replayed batch cannot duplicate rows.
--
-- Tombstoned rows are retained with is_deleted = true rather than removed,
-- so the gold layer filters them explicitly and the audit trail survives.

{{ cdc_current(ref('stg_payments'), 'payment_id') }}
