{{ config(materialized='view') }}

select
    application_id,
    customer_id,
    lead_id,
    product_type,
    requested_amount,
    tenor_months,
    bureau_score,
    has_bureau_file,
    internal_score,
    decision,
    decline_reason,
    approved_amount,
    approved_rate_pa,
    applied_at,
    applied_at::date                            as applied_date,
    date_trunc('month', applied_at)::date       as applied_month,
    decision_at,
    -- time-to-decision is the headline origination SLA metric
    datediff(minute, applied_at, decision_at)    as decision_minutes,

    -- banded bureau score, the x-axis of every scorecard chart
    case
        when bureau_score is null  then 'NO_FILE'
        when bureau_score <  580   then '<580'
        when bureau_score <  650   then '580-649'
        when bureau_score <  700   then '650-699'
        when bureau_score <  750   then '700-749'
        else                            '750+'
    end                                         as bureau_score_band,

    _op                                         as cdc_op,
    _commit_ts                                  as cdc_commit_ts,
    _seq                                        as cdc_seq,
    _batch_id                                   as cdc_batch_id,
    _source                                     as cdc_source,
    _ingested_at                                as cdc_ingested_at,
    _op = 'D'                                   as is_deleted

from {{ source('bronze', 'raw_loan_applications') }}
