{{
    config(
        materialized='incremental',
        unique_key='activity_id',
        incremental_strategy='merge'
    )
}}

-- Collection contact attempts and their outcomes.
--
-- Tombstoned rows are retained with is_deleted = true rather than removed,
-- so the gold layer filters them explicitly and the audit trail survives.

{{ cdc_current(ref('stg_collection_activities'), 'activity_id') }}
