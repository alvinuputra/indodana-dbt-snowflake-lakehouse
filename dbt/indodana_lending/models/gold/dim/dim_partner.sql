{{ config(materialized='table') }}

select
    partner_id,
    partner_name,
    partner_type,
    commission_rate,
    payout_terms_days,
    is_active,
    partner_id = 'PTR009'                           as is_own_balance_sheet
from {{ ref('slv_partners') }}
where not is_deleted
