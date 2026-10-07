{#
    Resolve the current state of an entity from a raw CDC change stream.

    Bronze keeps every version of every row, so "current" means: the
    highest (commit_ts, seq) version per business key.

    Three things this has to get right, and each is easy to get wrong:

    1. WATERMARK ON cdc_ingested_at, NOT cdc_commit_ts.
       Source commit timestamps are backdated (a replayed or backfilled
       batch carries old business timestamps). Watermarking on commit_ts
       would silently skip those rows forever. ingested_at is wall-clock
       at landing and is the only monotonic column available.

    2. ORDER BY cdc_commit_ts, NOT cdc_ingested_at.
       Which version wins is a question about the source system's
       timeline, not about when we happened to load it.

    3. NEVER LET AN OLDER VERSION OVERWRITE A NEWER ONE.
       A late-arriving batch can carry a version older than what is
       already merged. The merge is keyed on the business key alone, so
       without the explicit version guard below, dbt would happily
       regress the row.

    Tombstones are kept and flagged via is_deleted rather than deleted
    outright — downstream models filter them, and the audit trail stays
    intact.
#}
{% macro cdc_current(stg_relation, key_col) %}

with incoming as (

    select * from {{ stg_relation }}

    {% if is_incremental() %}
    where cdc_ingested_at >= (
        select coalesce(max(cdc_ingested_at), '1900-01-01'::timestamp_ntz)
        from {{ this }}
    )
    {% endif %}

),

latest as (

    select *
    from incoming
    qualify row_number() over (
        partition by {{ key_col }}
        order by cdc_commit_ts desc, cdc_seq desc
    ) = 1

)

{% if is_incremental() %}

select l.*
from latest l
left join {{ this }} e
       on e.{{ key_col }} = l.{{ key_col }}
where e.{{ key_col }} is null
   or l.cdc_commit_ts > e.cdc_commit_ts
   or (l.cdc_commit_ts = e.cdc_commit_ts and l.cdc_seq > e.cdc_seq)

{% else %}

select l.* from latest l

{% endif %}

{% endmacro %}
