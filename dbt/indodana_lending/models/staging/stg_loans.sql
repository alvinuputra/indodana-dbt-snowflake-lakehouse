{{ config(materialized='view') }}

select
    loan_id,
    application_id,
    customer_id,
    merchant_id,
    partner_id,
    product_type,
    principal_amount,
    interest_rate_pa,
    admin_fee,
    tenor_months,
    loan_status,
    outstanding_principal,
    disbursed_at,
    disbursed_at::date                          as disbursed_date,
    date_trunc('month', disbursed_at)::date     as disbursed_month,
    first_due_date,
    maturity_date,

    -- months on book drives vintage analysis
    datediff(month, disbursed_at::date, current_date()) as months_on_book,

    case
        when principal_amount <   2000000 then '<2m'
        when principal_amount <   5000000 then '2m-5m'
        when principal_amount <  15000000 then '5m-15m'
        when principal_amount <  50000000 then '15m-50m'
        else                                   '50m+'
    end                                         as ticket_size_band,

    _op                                         as cdc_op,
    _commit_ts                                  as cdc_commit_ts,
    _seq                                        as cdc_seq,
    _batch_id                                   as cdc_batch_id,
    _source                                     as cdc_source,
    _ingested_at                                as cdc_ingested_at,
    _op = 'D'                                   as is_deleted

from {{ source('bronze', 'raw_loans') }}
