# Indodana / Cermati — Lending Lakehouse (dbt on Snowflake + CDC ingest app)

Bronze → Silver → Gold over the full consumer-lending lifecycle, built as a
dbt project that runs **inside Snowflake**, plus a local HTML control panel
that lands new CDC events into Bronze on a button click.

```
Account   <YOUR_SNOWFLAKE_ACCOUNT>
Database  DEMO_INDODANA
Schemas   BRONZE · SILVER · GOLD · DBT · GEN
Project   DEMO_INDODANA.DBT.INDODANA_LENDING
```

---

## Business domain

The full lending lifecycle, spanning both sides of the Indodana/Cermati
business — the marketplace that acquires demand and the lender that
underwrites it:

```
Cermati lead  →  application  →  underwriting decision  →  disbursement
              →  instalment schedule  →  payments  →  collections
```

Nine source entities across four simulated source systems:

| Source system         | Entities                                      |
|-----------------------|-----------------------------------------------|
| `cermati_marketplace` | leads, partners                               |
| `core_lending`        | customers, loan applications, loans, merchants |
| `lms`                 | instalments, payments                         |
| `collection_crm`      | collection activities                         |

Products: `PAYLATER` (BNPL), `KTA` (unsecured personal loan), `MULTIGUNA`,
`CREDIT_CARD`. Flat-rate amortisation, IDR-rounded, OJK-aligned DPD buckets.

---

## Layout

```
INDODANA/
├── sql/
│   ├── 00_setup.sql            database, schemas, Bronze landing tables
│   ├── 01_generator.sql        GEN schema, reference pools, SP_LOAD_REFERENCE
│   ├── 02_pools.sql            weighted pick pools for the generator
│   ├── 03_sp_ingest_batch.sql  SP_INGEST_BATCH — one full lifecycle cohort
│   ├── 04_sp_cdc.sql           SP_CDC_UPDATE / SP_CDC_DELETE / SP_SEED_HISTORY
│   └── 05_scheduling.sql       streams + task graph (event-driven & nightly)
├── dbt/indodana_lending/       the dbt project (deployed to Snowflake)
└── ingest-app/
    ├── server.js               local Node API (no dependencies)
    └── public/index.html       control panel UI
```

---

## The CDC design

Bronze is a **raw append-only change stream**, not a mirror. Every row is one
immutable change event:

| Column         | Meaning                                                  |
|----------------|----------------------------------------------------------|
| `_OP`          | `I` insert · `U` update · `D` soft delete                |
| `_COMMIT_TS`   | commit time in the **source** system — may be backdated  |
| `_SEQ`         | monotonic tie-breaker within the same commit timestamp   |
| `_BATCH_ID`    | ingest batch that carried the row                        |
| `_INGESTED_AT` | wall-clock landing time                                  |

Silver resolves this into current state with `macros/cdc_current.sql`. Three
things that pattern has to get right, and each is easy to get wrong:

1. **Watermark on `_INGESTED_AT`, never `_COMMIT_TS`.** Replayed and
   backfilled batches carry old business timestamps. Watermarking on
   commit time would skip those rows permanently.
2. **Order by `_COMMIT_TS`, not `_INGESTED_AT`.** Which version wins is a
   question about the source system's timeline, not our load order.
3. **Never let an older version overwrite a newer one.** dbt's merge is
   keyed on the business key alone, so a late-arriving batch would
   otherwise regress the row. The macro adds an explicit version guard.

Tombstones are **kept** with `is_deleted = true` rather than deleted. Gold
filters them; the audit trail survives.

### Two CDC patterns, by purpose

| Pattern                | Models                        | Answers |
|------------------------|-------------------------------|---------|
| Incremental merge      | `slv_*` (9 models)            | What is true *now* |
| Snapshot SCD2          | `snap_customer_credit`, `snap_loan_status` | What was true *then* |

The snapshots are not redundant. Credit risk needs the limit as it stood
when a loan was approved, and roll-rate analysis needs status transitions —
neither is answerable from a current-state table.

---

## Model graph

**35 models · 2 snapshots · 99 data tests**

```
BRONZE (9 raw tables)
   ↓  staging — 9 views, typed/renamed, every version preserved
   ↓  silver  — 9 incremental merge models, one row per business key
   ↓           + 2 snapshots (SCD2 history)
GOLD
   ├── dim_date  dim_customer  dim_customer_credit_history
   │   dim_loan  dim_partner   dim_merchant
   ├── fct_loan_application  fct_loan  fct_installment
   │   fct_payment           fct_collection_activity
   └── mart_origination_funnel        lead→application→approval→disbursement,
       │                              with cost per disbursed loan
       mart_portfolio_quality         value-weighted NPL by product/segment/partner
       mart_vintage_analysis          ever-30 / ever-90 triangle by cohort × MOB
       mart_collection_effectiveness  contact rate, PTP rate, 14-day cure rate
       mart_partner_performance       lead spend vs commission vs credit quality
       mart_customer_360              one row per customer
```

### Notes on two models worth reading

**`mart_vintage_analysis`** is a real vintage *curve*, not a cohort snapshot.
Each cohort is measured at the same age, so a 20-month-old book is not
compared against a 2-month-old one that has not had time to go bad. Months
on book at first delinquency is reconstructed from the instalment number it
happened on. Cells beyond a cohort's observed age return `NULL`, not `0` —
zero reads as "no defaults", when the truth is "not observed yet", and that
is the single most common way a vintage report misleads.

**`mart_portfolio_quality`** reports NPL value-weighted on outstanding
exposure, because ten IDR 100m loans and ten IDR 1m loans are the same
count and very different problems. The count-based ratio sits beside it,
since collections teams are staffed by case volume rather than by rupiah.

### Tests that check arithmetic, not just structure

Beyond uniqueness / not-null / relationships / accepted-values, two singular
tests assert the numbers actually tie:

- `assert_installment_principal_ties_to_loan` — the schedule must amortise
  exactly the disbursed principal (±IDR 1). Catches rounding bugs and
  schedules cut against a stale principal.
- `assert_loan_status_consistent_with_installments` — status (core lending)
  and balance (LMS) arrive as independent CDC streams and can disagree.
  When they do, every NPL ratio is wrong, because closed loans are excluded
  from the denominator.

---

## Setup from scratch

```bash
cd INDODANA
CONN=<your_snow_cli_connection>

snow sql -c $CONN -f sql/00_setup.sql
snow sql -c $CONN -f sql/01_generator.sql
snow sql -c $CONN -f sql/02_pools.sql
snow sql -c $CONN -f sql/03_sp_ingest_batch.sql
snow sql -c $CONN -f sql/04_sp_cdc.sql

snow sql -c $CONN -q "CALL DEMO_INDODANA.GEN.SP_LOAD_REFERENCE()"
snow sql -c $CONN -q "CALL DEMO_INDODANA.GEN.SP_SEED_HISTORY(20, 260)"
```

Deploy and run the dbt project:

```bash
cd dbt
snow dbt deploy INDODANA_LENDING --source indodana_lending \
  --database DEMO_INDODANA --schema DBT -c $CONN

snow dbt execute -c $CONN --database DEMO_INDODANA --schema DBT \
  INDODANA_LENDING build
```

Or from SQL / a task:

```sql
EXECUTE DBT PROJECT DEMO_INDODANA.DBT.INDODANA_LENDING ARGS='build';
```

Re-deploying the same project name creates a new version (`VERSION$2`, …).

---

## The ingest control panel

```bash
cd ingest-app
SNOW_CONNECTION=<your_snow_cli_connection> node server.js   # → http://127.0.0.1:5190
```

`SNOW_CONNECTION` is required — the server exits immediately without it.
Optional: `SNOW_WAREHOUSE` (default `DEFAULT_WH`), `PORT` (default `5190`).

Zero npm dependencies. It shells out to the `snow` CLI using the existing
named connection, which is deliberate: the account uses OAuth
authorization-code auth that the Node driver cannot complete headless.
No credential ever lives in this process, in the browser, or on disk here.
SQL goes to a temp file and is passed with `-f`, sidestepping shell quoting;
every client value is integer-validated or resolved against a server-side
whitelist, so no user input is concatenated into SQL.

| Button | What lands in Bronze |
|--------|----------------------|
| **Ingest batch baru** | One full lifecycle cohort as `_OP='I'` events |
| **CDC update** | Limit/segment re-grade, instalments settling, loan status recalc — `_OP='U'` |
| **CDC delete** | Duplicate leads merged, fraud applications purged — `_OP='D'` tombstones |
| **Run dbt** | `EXECUTE DBT PROJECT` with one of 8 presets |
| **Jalankan siklus lengkap** | ingest → update → delete → `dbt build`, in one click |

The panel shows a **Bronze→Silver pending badge** (rows landed but not yet
merged) and a **CDC pressure table** (`raw_rows − distinct keys = extra
versions`). Those two make the pipeline observable instead of something you
have to take on trust: ingest and the badge goes red, run dbt and it goes
green; run a CDC update and extra versions climb.

If the CLI session has expired, re-auth once:

```bash
snow connection test -c <your_snow_cli_connection>
```

---

## Scheduling

Two independent triggers on the same dbt project (`sql/05_scheduling.sql`).

### Event-driven (primary)

```
new CDC events land in Bronze
          ↓
T_DBT_BUILD      SCHEDULE '1 MINUTE', WHEN any of 5 Bronze streams has data
          ↓      AS EXECUTE DBT PROJECT ... ARGS='build'
T_DRAIN_STREAMS  AFTER T_DBT_BUILD — consumes the streams
```

Costs nothing while Bronze is quiet: a task whose `WHEN` is false is
SKIPPED in ~1s without starting the warehouse.

**The non-obvious part — streams must be drained explicitly.**
`EXECUTE DBT PROJECT` reads the Bronze *tables*, not the streams, so it
never advances a stream offset. Without `T_DRAIN_STREAMS`, the `WHEN`
condition stays true forever and the task re-fires every minute for the
rest of time. The drain task only runs on a *successful* build, which
gives free retry semantics: a failed build leaves the streams full and
the next minute tries again.

Streams are `APPEND_ONLY = TRUE` — correct and cheaper here, since Bronze
is insert-only by design and there are no updates or deletes to reconcile.

Five streams (`customers, loans, payments, leads, applications`) cover all
three ingest procedures: `SP_INGEST_BATCH` and `SP_CDC_UPDATE` touch the
first three, `SP_CDC_DELETE` touches the last two.

### Nightly (safety net)

`T_DBT_NIGHTLY` — full build at 02:00 Asia/Jakarta. Not redundant with the
event path: DPD and aging are computed against `CURRENT_DATE()`, so gold
is stale at midnight **even if no new row arrived**. It also re-runs all
99 tests daily. Set `WRITEBACK = FALSE` so it cannot contend with an
event-driven run writing `target/` artifacts to the live version.

### Privileges this required

Tasks run as their **owner role**, not the caller — so a grant that an
interactive session resolves through role inheritance is not enough:

```sql
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE <YOUR_ROLE>;
GRANT USAGE ON WAREHOUSE DEFAULT_WH TO ROLE <YOUR_ROLE>;  -- explicit, required
```

`DEFAULT_WH` is owned by SYSADMIN with no USAGE grants. Everything worked
interactively, but the task failed every minute with *"USAGE privilege on
the task's warehouse must be granted"* until the explicit grant was added.

The task owner role is whichever role created the tasks. Substitute it for
`<YOUR_ROLE>` above.

`SUSPEND_TASK_AFTER_NUM_FAILURES` is raised to 30 (default 10). At a
1-minute cadence the default silently kills the pipeline after only 10
minutes of transient failures — which is exactly what happened during
setup.

### Operating it

```sql
-- stop / start (root last on resume)
ALTER TASK DEMO_INDODANA.BRONZE.T_DBT_BUILD SUSPEND;
SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('DEMO_INDODANA.BRONZE.T_DBT_BUILD');

-- run history
SELECT NAME, STATE, SCHEDULED_TIME, ERROR_MESSAGE
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.TASK_HISTORY(
  SCHEDULED_TIME_RANGE_START => DATEADD('hour',-24,CURRENT_TIMESTAMP()),
  SCHEDULED_TIME_RANGE_END   => CURRENT_TIMESTAMP(),
  RESULT_LIMIT => 10000))
WHERE DATABASE_NAME='DEMO_INDODANA' ORDER BY SCHEDULED_TIME DESC;

-- what each triggered run picked up
SELECT * FROM DEMO_INDODANA.BRONZE.PIPELINE_RUN_LOG ORDER BY LOGGED_AT DESC;
```

**No failure notifications yet.** Task `ERROR_INTEGRATION` requires a
QUEUE-type notification integration (SNS / SQS / Event Grid / Pub-Sub);
EMAIL integrations are rejected. `DEMO_INDODANA_EMAIL_INT` exists and is
usable from an ALERT, which is the supported path for email alerting on
task failures.

---

## Verified state

```
dbt build                     PASS=136  WARN=0  ERROR=0  SKIP=0
Bronze                        7,644 customers · 3,222 loans · ~44k instalments
Approval rate                 44.5%
NPL (DPD 90+, by value)       3.46%
Vintage (KTA, MOB 12)         ever-30 ≈ 12-15%, ever-90 ≈ 5%
```

CDC resolution verified end to end: 7,914 Bronze customer versions collapsed
to 7,764 Silver rows (= 7,764 distinct keys); 3,492 loan versions to 3,292;
20 tombstoned leads retained in Silver and excluded from Gold; the loan
snapshot produced exactly 200 second versions for the 200 loans updated.

Scheduling verified end to end, with no manual step:

```
01:09:29  T_DBT_BUILD      SUCCEEDED  29s   stream had data -> dbt build
01:09:58  T_DRAIN_STREAMS  SUCCEEDED   4s   stream offsets advanced
01:10:29  T_DBT_BUILD      SKIPPED     1s   WHEN false -> warehouse never started
```

Afterwards Bronze = Silver = Gold at 3,382 loans, and `PIPELINE_RUN_LOG`
recorded exactly the batch that triggered it (90 customers, 56 leads,
90 applications, 37 loans, 0 payments).

---

## Known limits of the synthetic data

Worth stating plainly, since credit figures invite over-reading:

- **Repayment behaviour is assigned, not simulated.** Each loan gets a
  profile (`GOOD` / `LATE` / `DELINQUENT` / `WRITEOFF`) drawn against its
  risk segment at disbursement. Real borrowers respond to shocks; these do
  not. Segment-level bad rates are therefore plausible by construction
  rather than emergent.
- **No restructuring path.** `RESTRUCTURED` exists in the status enum but is
  never emitted, so roll-rate analysis has no cure-by-restructure branch.
- **Collection activity does not change repayment.** Contacts are generated
  from DPD, and `is_cured_within_14d` observes payments that were already
  going to happen. Cure rates are descriptive, not causal — do not read the
  collection mart as a measure of collector effectiveness.
- **One application per customer per cohort.** Repeat-borrower behaviour is
  thin, so `mart_customer_360` cross-sell signals are weak by construction.
