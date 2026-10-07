{{
    config(
        materialized='incremental',
        unique_key='lead_id',
        incremental_strategy='merge'
    )
}}

-- Current state of every Cermati marketplace lead.
--
-- Tombstoned rows are retained with is_deleted = true rather than removed,
-- so the gold layer filters them explicitly and the audit trail survives.

{{ cdc_current(ref('stg_leads'), 'lead_id') }}
