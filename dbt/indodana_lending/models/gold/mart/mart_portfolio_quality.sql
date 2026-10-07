{{ config(materialized='table') }}

-- Portfolio asset quality as at today.
--
-- Grain: product_type x risk_segment x partner_id
--
-- NPL is computed on outstanding exposure, not loan count. A 90+ book of
-- ten IDR 100m loans and one of ten IDR 1m loans are the same count and
-- very different problems, so the headline ratio is value-weighted. The
-- count-based ratio is kept alongside it because collections teams are
-- staffed by case volume rather than by rupiah.

with live as (

    select
        f.*,
        c.risk_segment,
        c.province
    from {{ ref('fct_loan') }} f
    left join {{ ref('dim_customer') }} c on c.customer_id = f.customer_id
    -- closed loans carry no exposure and would dilute every ratio
    where f.loan_status <> 'PAID_OFF'

)

select
    product_type,
    coalesce(risk_segment, 'UNKNOWN')                   as risk_segment,
    partner_id,

    count(*)                                            as loan_count,
    sum(principal_amount)                               as principal_disbursed,
    sum(outstanding_principal)                          as outstanding_principal,
    sum(unpaid_total)                                   as total_amount_due,
    sum(total_collected)                                as total_collected,

    -- bucket distribution by exposure
    sum(iff(current_dpd_bucket = 'X (current)', outstanding_principal, 0)) as os_current,
    sum(iff(current_dpd_bucket = '1-30',        outstanding_principal, 0)) as os_dpd_1_30,
    sum(iff(current_dpd_bucket = '31-60',       outstanding_principal, 0)) as os_dpd_31_60,
    sum(iff(current_dpd_bucket = '61-90',       outstanding_principal, 0)) as os_dpd_61_90,
    sum(iff(current_dpd_bucket = '91-180',      outstanding_principal, 0)) as os_dpd_91_180,
    sum(iff(current_dpd_bucket = '180+',        outstanding_principal, 0)) as os_dpd_180_plus,

    -- bucket distribution by count
    count_if(current_dpd_bucket = 'X (current)')        as cnt_current,
    count_if(current_dpd_bucket = '1-30')               as cnt_dpd_1_30,
    count_if(current_dpd_bucket = '31-60')              as cnt_dpd_31_60,
    count_if(current_dpd_bucket = '61-90')              as cnt_dpd_61_90,
    count_if(current_dpd_bucket = '91-180')             as cnt_dpd_91_180,
    count_if(current_dpd_bucket = '180+')               as cnt_dpd_180_plus,

    count_if(is_npl)                                    as npl_count,
    sum(iff(is_npl, outstanding_principal, 0))          as npl_outstanding,
    count_if(is_written_off)                            as written_off_count,
    sum(iff(is_written_off, outstanding_principal, 0))  as written_off_outstanding,
    count_if(is_ever_30_plus)                           as ever_30_plus_count,
    count_if(is_ever_90_plus)                           as ever_90_plus_count,

    -- headline ratios
    round(div0(sum(iff(is_npl, outstanding_principal, 0)),
               sum(outstanding_principal))        * 100, 2) as npl_ratio_by_value_pct,
    round(div0(count_if(is_npl), count(*))        * 100, 2) as npl_ratio_by_count_pct,
    round(div0(sum(iff(current_dpd > 0, outstanding_principal, 0)),
               sum(outstanding_principal))        * 100, 2) as par_1_plus_pct,
    round(div0(sum(iff(current_dpd > 30, outstanding_principal, 0)),
               sum(outstanding_principal))        * 100, 2) as par_30_plus_pct,
    round(div0(count_if(is_ever_30_plus), count(*)) * 100, 2) as ever_30_plus_pct,
    round(div0(count_if(is_ever_90_plus), count(*)) * 100, 2) as ever_90_plus_pct,

    round(avg(current_dpd), 1)                          as avg_current_dpd,
    round(avg(months_on_book), 1)                       as avg_months_on_book,
    round(avg(interest_rate_pa) * 100, 2)               as avg_rate_pct,

    current_timestamp()::timestamp_ntz                  as reported_at

from live
group by 1, 2, 3
order by outstanding_principal desc
