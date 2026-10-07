{{ config(materialized='view') }}

select
    activity_id,
    loan_id,
    customer_id,
    collector_id,
    collection_bucket,
    channel,
    action_code,
    outcome,
    dpd_at_contact,
    promise_amount,
    promise_date,
    activity_at,
    activity_at::date                            as activity_date,
    date_trunc('month', activity_at)::date       as activity_month,

    outcome = 'PROMISE_TO_PAY'                   as is_promise_to_pay,
    outcome in ('PAID', 'PROMISE_TO_PAY')        as is_positive_outcome,
    -- a contact only counts as "reached" if someone actually engaged
    outcome not in ('NO_ANSWER', 'WRONG_NUMBER') as is_contact_made,
    channel = 'FIELD_VISIT'                      as is_field_visit,

    _op                                          as cdc_op,
    _commit_ts                                   as cdc_commit_ts,
    _seq                                         as cdc_seq,
    _batch_id                                    as cdc_batch_id,
    _source                                      as cdc_source,
    _ingested_at                                 as cdc_ingested_at,
    _op = 'D'                                    as is_deleted

from {{ source('bronze', 'raw_collection_activities') }}
