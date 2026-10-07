{{ config(materialized='table') }}

-- Collection effectiveness by bucket, channel and month.
--
-- Grain: activity_month x dpd_bucket_at_contact x channel
--
-- Contact volume is a vanity metric. The two columns that matter are
-- contact_rate (did anyone actually pick up) and cure_rate_pct (did money
-- arrive within 14 days). A channel can have a great contact rate and a
-- terrible cure rate — that is a script problem, not a reach problem, and
-- the two have to be visible separately to tell them apart.

select
    activity_month,
    dpd_bucket_at_contact,
    max(dpd_bucket_at_contact_sort)                     as dpd_bucket_sort,
    channel,
    action_code,

    count(*)                                            as contact_attempts,
    count(distinct loan_id)                             as loans_touched,
    count(distinct collector_id)                        as collectors_active,

    count_if(is_contact_made)                           as contacts_made,
    count_if(is_promise_to_pay)                         as promises_to_pay,
    count_if(outcome = 'PAID')                          as paid_on_contact,
    count_if(outcome = 'NO_ANSWER')                     as no_answer,
    count_if(outcome = 'WRONG_NUMBER')                  as wrong_number,
    count_if(outcome = 'REFUSED')                       as refused,
    count_if(outcome = 'DISPUTE')                       as disputes,

    count_if(is_cured_within_14d)                       as cured_within_14d,
    sum(amount_cured_within_14d)                        as amount_cured_within_14d,
    sum(coalesce(promise_amount, 0))                    as promised_amount,

    round(div0(count_if(is_contact_made), count(*))       * 100, 2) as contact_rate_pct,
    round(div0(count_if(is_promise_to_pay),
               nullif(count_if(is_contact_made), 0))      * 100, 2) as ptp_rate_pct,
    round(div0(count_if(is_cured_within_14d), count(*))   * 100, 2) as cure_rate_pct,
    -- did the promise convert into cash
    round(div0(sum(amount_cured_within_14d),
               sum(coalesce(promise_amount, 0)))          * 100, 2) as promise_kept_pct,
    round(div0(sum(amount_cured_within_14d), count(*)))             as amount_cured_per_attempt,
    round(avg(dpd_at_contact), 1)                                   as avg_dpd_at_contact

from {{ ref('fct_collection_activity') }}
group by activity_month, dpd_bucket_at_contact, channel, action_code
order by activity_month desc, dpd_bucket_sort, channel
