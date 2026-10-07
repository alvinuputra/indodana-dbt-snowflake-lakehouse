{{ config(materialized='view') }}

select
    payment_id,
    loan_id,
    installment_id,
    customer_id,
    paid_at,
    paid_at::date                                as paid_date,
    date_trunc('month', paid_at)::date           as paid_month,
    amount,
    principal_portion,
    interest_portion,
    late_fee,
    payment_channel,
    payment_type,
    days_late,
    days_late > 0                                as is_late_payment,

    case
        when payment_channel like 'VA_%'                     then 'VIRTUAL_ACCOUNT'
        when payment_channel in ('GOPAY', 'OVO')             then 'E_WALLET'
        when payment_channel in ('ALFAMART', 'INDOMARET')    then 'RETAIL_OUTLET'
        when payment_channel = 'AUTODEBIT'                   then 'AUTO_DEBIT'
        else 'OTHER'
    end                                          as channel_group,

    _op                                          as cdc_op,
    _commit_ts                                   as cdc_commit_ts,
    _seq                                         as cdc_seq,
    _batch_id                                    as cdc_batch_id,
    _source                                      as cdc_source,
    _ingested_at                                 as cdc_ingested_at,
    _op = 'D'                                    as is_deleted

from {{ source('bronze', 'raw_payments') }}
