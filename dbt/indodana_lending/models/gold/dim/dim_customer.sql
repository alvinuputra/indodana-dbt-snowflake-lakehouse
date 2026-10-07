{{ config(materialized='table') }}

-- Current-state customer dimension. One row per live customer.
-- Tombstoned customers are excluded here; they remain inspectable in
-- the silver layer with is_deleted = true.

select
    customer_id,
    full_name,
    nik_masked,
    email,
    phone,
    city,
    province,
    birth_date,
    age_years,
    case
        when age_years <  25 then '18-24'
        when age_years <  35 then '25-34'
        when age_years <  45 then '35-44'
        when age_years <  55 then '45-54'
        else                      '55+'
    end                                             as age_band,
    gender,
    occupation,
    monthly_income,
    case
        when monthly_income <   5000000 then '<5m'
        when monthly_income <  10000000 then '5m-10m'
        when monthly_income <  20000000 then '10m-20m'
        when monthly_income <  40000000 then '20m-40m'
        else                                 '40m+'
    end                                             as income_band,
    kyc_status,
    risk_segment,
    credit_limit,
    acquisition_platform,
    acquisition_lead_source,
    acquisition_channel,
    registered_at,
    registered_date,
    date_trunc('month', registered_date)::date      as registered_month,
    datediff(day, registered_date, current_date())  as tenure_days,

    cdc_commit_ts                                   as last_changed_at,
    cdc_batch_id                                    as last_changed_batch_id

from {{ ref('slv_customers') }}
where not is_deleted
