{{ config(materialized='table') }}

-- Type-2 customer credit dimension built on top of the dbt snapshot.
--
-- Use this instead of dim_customer whenever a question is time-qualified:
-- "what was the limit when the loan was approved", "when was this customer
-- downgraded to segment E". Join with a BETWEEN on the validity window.

select
    -- No dbt_utils dependency: the project deploys to Snowflake without an
    -- External Access Integration, so packages cannot be resolved at runtime.
    md5(customer_id || '|' || dbt_valid_from::varchar)
                                                    as customer_credit_sk,
    customer_id,
    credit_limit,
    risk_segment,
    kyc_status,
    monthly_income,
    source_commit_ts,

    dbt_valid_from                                  as valid_from,
    -- open-ended current version: a NULL valid_to breaks BETWEEN joins,
    -- so it is capped at the far future instead
    coalesce(dbt_valid_to, '9999-12-31'::timestamp_ntz) as valid_to,
    dbt_valid_to is null                            as is_current,

    row_number() over (
        partition by customer_id order by dbt_valid_from
    )                                               as version_number

from {{ ref('snap_customer_credit') }}
