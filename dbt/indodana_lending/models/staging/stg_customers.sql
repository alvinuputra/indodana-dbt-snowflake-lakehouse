{{ config(materialized='view') }}

-- Typed, renamed passthrough of the customer change stream.
-- Deliberately NOT deduplicated: staging preserves every version so the
-- silver layer can resolve current state and the snapshots can build
-- history from the same source of truth.

select
    customer_id,
    full_name,
    nik_masked,
    lower(email)                                as email,
    phone,
    city,
    province,
    birth_date,
    datediff(year, birth_date, current_date())   as age_years,
    gender,
    occupation,
    monthly_income,
    kyc_status,
    risk_segment,
    credit_limit,
    acquisition_channel,
    -- marketplace-sourced customers carry the Cermati lead channel in the
    -- acquisition string; split it out so attribution is queryable
    case
        when acquisition_channel like 'CERMATI_%' then 'CERMATI_MARKETPLACE'
        else 'INDODANA_DIRECT'
    end                                         as acquisition_platform,
    case
        when acquisition_channel like 'CERMATI_%'
            then substr(acquisition_channel, 9)
        else null
    end                                         as acquisition_lead_source,
    registered_at,
    registered_at::date                         as registered_date,

    _op                                         as cdc_op,
    _commit_ts                                  as cdc_commit_ts,
    _seq                                        as cdc_seq,
    _batch_id                                   as cdc_batch_id,
    _source                                     as cdc_source,
    _ingested_at                                as cdc_ingested_at,
    _op = 'D'                                   as is_deleted

from {{ source('bronze', 'raw_customers') }}
