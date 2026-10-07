{#
    Standard Indonesian multifinance collection buckets (OJK-aligned).
    Used by every model that reports delinquency so the bucket edges
    cannot drift apart between marts.
#}
{% macro dpd_bucket(dpd_col) %}
    case
        when {{ dpd_col }} is null or {{ dpd_col }} <= 0 then 'X (current)'
        when {{ dpd_col }} <=  30 then '1-30'
        when {{ dpd_col }} <=  60 then '31-60'
        when {{ dpd_col }} <=  90 then '61-90'
        when {{ dpd_col }} <= 180 then '91-180'
        else '180+'
    end
{% endmacro %}

{% macro dpd_bucket_sort(dpd_col) %}
    case
        when {{ dpd_col }} is null or {{ dpd_col }} <= 0 then 0
        when {{ dpd_col }} <=  30 then 1
        when {{ dpd_col }} <=  60 then 2
        when {{ dpd_col }} <=  90 then 3
        when {{ dpd_col }} <= 180 then 4
        else 5
    end
{% endmacro %}
