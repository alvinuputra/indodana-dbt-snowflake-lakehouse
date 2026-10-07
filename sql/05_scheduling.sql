-- =====================================================================
-- 05_scheduling.sql : event-driven + nightly orchestration
-- =====================================================================
-- Two independent triggers for the same dbt project:
--
--   1. EVENT-DRIVEN (primary)  T_DBT_BUILD + T_DRAIN_STREAMS
--      Checks every minute whether anything new landed in Bronze.
--      Costs nothing when Bronze is quiet — a task whose WHEN condition
--      is false is skipped without starting the warehouse.
--
--   2. NIGHTLY (safety net)    T_DBT_NIGHTLY
--      Runs a full build at 02:00 Jakarta regardless of new data. This
--      is not redundant: DPD and aging are computed against
--      CURRENT_DATE(), so yesterday's gold tables are stale at midnight
--      even if not a single new row arrived. It also re-runs all 99
--      tests daily, which catches drift the event path would miss.
--
-- ---------------------------------------------------------------------
-- THE NON-OBVIOUS PART: streams must be drained explicitly.
--
-- EXECUTE DBT PROJECT reads the Bronze *tables*, not the streams. It
-- therefore never advances a stream offset. Left alone, the WHEN
-- condition stays true forever and the task re-fires every minute for
-- the rest of time.
--
-- T_DRAIN_STREAMS fixes this: it runs after a SUCCESSFUL dbt build and
-- consumes all five streams in one DML, advancing their offsets. Because
-- it only runs on success, a failed build leaves the streams un-drained
-- and the work is automatically retried a minute later.
-- =====================================================================

USE SCHEMA DEMO_INDODANA.BRONZE;

-- ---------------------------------------------------------------------
-- Run log — also the mechanism that consumes the streams
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS PIPELINE_RUN_LOG (
    LOGGED_AT     TIMESTAMP_NTZ,
    TRIGGERED_BY  VARCHAR(40),
    ENTITY        VARCHAR(40),
    ROWS_SEEN     NUMBER(38,0),
    INSERTS_SEEN  NUMBER(38,0),
    UPDATES_SEEN  NUMBER(38,0)
);

-- ---------------------------------------------------------------------
-- Streams on the Bronze tables that every ingest path writes to.
--
-- Five, not nine: SP_INGEST_BATCH touches customers/loans/payments,
-- SP_CDC_UPDATE touches customers/loans/payments, and SP_CDC_DELETE
-- touches leads/applications. These five cover all three procedures.
-- Reference tables (partners, merchants) are static and excluded.
--
-- APPEND_ONLY = TRUE is correct here and cheaper than a standard
-- stream: Bronze is insert-only by design, so there are no updates or
-- deletes for the stream to reconcile.
-- ---------------------------------------------------------------------
CREATE STREAM IF NOT EXISTS STR_RAW_CUSTOMERS
    ON TABLE RAW_CUSTOMERS            APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS STR_RAW_LOANS
    ON TABLE RAW_LOANS                APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS STR_RAW_PAYMENTS
    ON TABLE RAW_PAYMENTS             APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS STR_RAW_LEADS
    ON TABLE RAW_LEADS                APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS STR_RAW_APPLICATIONS
    ON TABLE RAW_LOAN_APPLICATIONS    APPEND_ONLY = TRUE;

-- =====================================================================
-- GRAPH 1 — event-driven
-- =====================================================================

-- Root. No warehouse override: dbt itself picks the warehouse from
-- profiles.yml. The task warehouse only runs the EXECUTE statement.
CREATE OR REPLACE TASK T_DBT_BUILD
    WAREHOUSE = DEFAULT_WH
    SCHEDULE  = '1 MINUTE'
    USER_TASK_TIMEOUT_MS = 1800000          -- 30 min ceiling
    -- No ERROR_INTEGRATION: task failure notifications require a QUEUE-type
    -- integration (SNS / SQS / Event Grid / Pub-Sub). EMAIL integrations are
    -- rejected here. Failure alerting is handled by an ALERT instead.
    COMMENT = 'Bronze->Silver->Gold whenever new CDC events land in Bronze'
WHEN
       SYSTEM$STREAM_HAS_DATA('DEMO_INDODANA.BRONZE.STR_RAW_CUSTOMERS')
    OR SYSTEM$STREAM_HAS_DATA('DEMO_INDODANA.BRONZE.STR_RAW_LOANS')
    OR SYSTEM$STREAM_HAS_DATA('DEMO_INDODANA.BRONZE.STR_RAW_PAYMENTS')
    OR SYSTEM$STREAM_HAS_DATA('DEMO_INDODANA.BRONZE.STR_RAW_LEADS')
    OR SYSTEM$STREAM_HAS_DATA('DEMO_INDODANA.BRONZE.STR_RAW_APPLICATIONS')
AS
EXECUTE DBT PROJECT DEMO_INDODANA.DBT.INDODANA_LENDING ARGS = 'build';

-- Child. Runs only if the build succeeded, which is exactly the retry
-- semantics we want: failure leaves the streams full and the next
-- minute tries again.
CREATE OR REPLACE TASK T_DRAIN_STREAMS
    WAREHOUSE = DEFAULT_WH
    COMMENT = 'Consume Bronze streams to advance offsets after a successful build'
    AFTER T_DBT_BUILD
AS
INSERT INTO DEMO_INDODANA.BRONZE.PIPELINE_RUN_LOG
    (LOGGED_AT, TRIGGERED_BY, ENTITY, ROWS_SEEN, INSERTS_SEEN, UPDATES_SEEN)
SELECT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, 'STREAM_TRIGGER', 'customers',
       COUNT(*), COUNT_IF(_OP = 'I'), COUNT_IF(_OP = 'U')
FROM DEMO_INDODANA.BRONZE.STR_RAW_CUSTOMERS
UNION ALL
SELECT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, 'STREAM_TRIGGER', 'loans',
       COUNT(*), COUNT_IF(_OP = 'I'), COUNT_IF(_OP = 'U')
FROM DEMO_INDODANA.BRONZE.STR_RAW_LOANS
UNION ALL
SELECT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, 'STREAM_TRIGGER', 'payments',
       COUNT(*), COUNT_IF(_OP = 'I'), COUNT_IF(_OP = 'U')
FROM DEMO_INDODANA.BRONZE.STR_RAW_PAYMENTS
UNION ALL
SELECT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, 'STREAM_TRIGGER', 'leads',
       COUNT(*), COUNT_IF(_OP = 'I'), COUNT_IF(_OP = 'U')
FROM DEMO_INDODANA.BRONZE.STR_RAW_LEADS
UNION ALL
SELECT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, 'STREAM_TRIGGER', 'applications',
       COUNT(*), COUNT_IF(_OP = 'I'), COUNT_IF(_OP = 'U')
FROM DEMO_INDODANA.BRONZE.STR_RAW_APPLICATIONS;

-- =====================================================================
-- GRAPH 2 — nightly safety net
-- =====================================================================
-- Scheduled at 02:00 Jakarta, deliberately far from demo hours, because
-- two concurrent EXECUTE DBT PROJECT runs on the same project object
-- would both try to write target/ and log/ artifacts back to the live
-- version. WRITEBACK = FALSE removes that contention entirely — the
-- nightly run still produces per-query results, it just doesn't update
-- the live version's artifacts.
CREATE OR REPLACE TASK T_DBT_NIGHTLY
    WAREHOUSE = DEFAULT_WH
    SCHEDULE  = 'USING CRON 0 2 * * * Asia/Jakarta'
    USER_TASK_TIMEOUT_MS = 3600000          -- 60 min ceiling
    COMMENT = 'Nightly full build: refreshes date-dependent DPD/aging and re-runs all tests'
AS
EXECUTE DBT PROJECT DEMO_INDODANA.DBT.INDODANA_LENDING
    ARGS = 'build'
    WRITEBACK = FALSE;
