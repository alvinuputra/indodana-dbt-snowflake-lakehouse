{{ config(materialized='view') }}

select
    partner_id,
    partner_name,
    partner_type,
    commission_rate,
    payout_terms_days,
    is_active,

    _op                                          as cdc_op,
    _commit_ts                                   as cdc_commit_ts,
    _seq                                         as cdc_seq,
    _batch_id                                    as cdc_batch_id,
    _source                                      as cdc_source,
    _ingested_at                                 as cdc_ingested_at,
    _op = 'D'                                    as is_deleted

from {{ source('bronze', 'raw_partners') }}
