{{ config(materialized='view') }}

select
    installment_id,
    loan_id,
    installment_no,
    due_date,
    date_trunc('month', due_date)::date          as due_month,
    principal_due,
    interest_due,
    fee_due,
    total_due,
    amount_paid,
    installment_status,
    paid_at,

    -- Days past due as at today, for unpaid instalments only.
    -- A paid instalment has no live DPD; its lateness lives on the
    -- payment record instead (days_late in stg_payments).
    case
        when installment_status = 'PAID' then 0
        when due_date < current_date()
            then datediff(day, due_date, current_date())
        else 0
    end                                          as dpd,

    _op                                          as cdc_op,
    _commit_ts                                   as cdc_commit_ts,
    _seq                                         as cdc_seq,
    _batch_id                                    as cdc_batch_id,
    _source                                      as cdc_source,
    _ingested_at                                 as cdc_ingested_at,
    _op = 'D'                                    as is_deleted

from {{ source('bronze', 'raw_installments') }}
