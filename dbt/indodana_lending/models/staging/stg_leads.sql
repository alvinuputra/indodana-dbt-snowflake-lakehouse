{{ config(materialized='view') }}

select
    lead_id,
    customer_id,
    lead_source,
    campaign_code,
    product_interest,
    partner_id,
    lead_status,
    cost_per_lead,
    created_at,
    created_at::date                            as created_date,
    date_trunc('month', created_at)::date       as created_month,

    _op                                         as cdc_op,
    _commit_ts                                  as cdc_commit_ts,
    _seq                                        as cdc_seq,
    _batch_id                                   as cdc_batch_id,
    _source                                     as cdc_source,
    _ingested_at                                as cdc_ingested_at,
    _op = 'D'                                   as is_deleted

from {{ source('bronze', 'raw_leads') }}
