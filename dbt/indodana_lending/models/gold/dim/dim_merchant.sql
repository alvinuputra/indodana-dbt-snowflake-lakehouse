{{ config(materialized='table') }}

select
    merchant_id,
    merchant_name,
    merchant_category,
    mdr_rate,
    city,
    is_active
from {{ ref('slv_merchants') }}
where not is_deleted
