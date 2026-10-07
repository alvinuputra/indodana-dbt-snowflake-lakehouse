-- =====================================================================
-- 04_sp_cdc.sql : update / delete change events + historical seeding
-- =====================================================================
-- These are what make the Bronze layer a real CDC feed rather than an
-- append-only log of inserts. Silver resolves them with dbt incremental
-- merges (current state) and dbt snapshots (SCD2 history).
-- =====================================================================
USE SCHEMA DEMO_INDODANA.GEN;

-- ---------------------------------------------------------------------
-- SP_CDC_UPDATE — mutate existing entities and emit 'U' events
-- ---------------------------------------------------------------------
-- Models the four mutations an operational lending stack actually emits:
--   1. credit limit / risk re-grade on the customer master
--   2. loan status + outstanding principal recalculation
--   3. installment settling as payments post
--   4. KYC completing for pending customers
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_CDC_UPDATE(P_CUSTOMERS NUMBER, P_LOANS NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  v_batch VARCHAR;
  v_cu NUMBER DEFAULT 0; v_lu NUMBER DEFAULT 0;
  v_iu NUMBER DEFAULT 0; v_pu NUMBER DEFAULT 0;
BEGIN
  v_batch := 'B-' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS')
                  || '-' || LEFT(REPLACE(UUID_STRING(), '-', ''), 6);

  INSERT INTO DEMO_INDODANA.BRONZE.BATCH_LOG (BATCH_ID, BATCH_TYPE, STARTED_AT, DETAIL)
  VALUES (:v_batch, 'UPDATE', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
          'customers=' || :P_CUSTOMERS || ' loans=' || :P_LOANS);

  -- -------------------------------------------------------------------
  -- Current state must be resolved first: Bronze holds every version of
  -- a row, so "latest per key" is the only meaningful starting point.
  -- -------------------------------------------------------------------
  CREATE OR REPLACE TEMPORARY TABLE T_CUR_CUST AS
  SELECT * EXCLUDE (RN) FROM (
    SELECT c.*, ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID
                                   ORDER BY _COMMIT_TS DESC, _SEQ DESC) AS RN
    FROM DEMO_INDODANA.BRONZE.RAW_CUSTOMERS c
  ) WHERE RN = 1 AND _OP <> 'D';

  CREATE OR REPLACE TEMPORARY TABLE T_CUR_LOAN AS
  SELECT * EXCLUDE (RN) FROM (
    SELECT l.*, ROW_NUMBER() OVER (PARTITION BY LOAN_ID
                                   ORDER BY _COMMIT_TS DESC, _SEQ DESC) AS RN
    FROM DEMO_INDODANA.BRONZE.RAW_LOANS l
  ) WHERE RN = 1 AND _OP <> 'D';

  -- -------------------------------------------------------------------
  -- 1. Customer re-grade: limit increase for good payers, cut for bad
  -- -------------------------------------------------------------------
  INSERT INTO DEMO_INDODANA.BRONZE.RAW_CUSTOMERS
    (CUSTOMER_ID, FULL_NAME, NIK_MASKED, EMAIL, PHONE, CITY, PROVINCE, BIRTH_DATE,
     GENDER, OCCUPATION, MONTHLY_INCOME, KYC_STATUS, RISK_SEGMENT, CREDIT_LIMIT,
     ACQUISITION_CHANNEL, REGISTERED_AT, _OP, _COMMIT_TS, _SEQ, _BATCH_ID, _SOURCE)
  WITH pick AS (
    SELECT c.*,
           -- repayment track record drives the re-grade direction
           COALESCE(b.ON_TIME_RATIO, 0.5)        AS ON_TIME_RATIO,
           COALESCE(b.MAX_DPD, 0)                AS MAX_DPD
    FROM T_CUR_CUST c
    LEFT JOIN (
      SELECT l.CUSTOMER_ID,
             AVG(IFF(p.DAYS_LATE = 0, 1, 0))     AS ON_TIME_RATIO,
             MAX(p.DAYS_LATE)                    AS MAX_DPD
      FROM T_CUR_LOAN l
      JOIN DEMO_INDODANA.BRONZE.RAW_PAYMENTS p ON p.LOAN_ID = l.LOAN_ID
      GROUP BY l.CUSTOMER_ID
    ) b ON b.CUSTOMER_ID = c.CUSTOMER_ID
    QUALIFY ROW_NUMBER() OVER (ORDER BY RANDOM()) <= :P_CUSTOMERS
  )
  SELECT
    CUSTOMER_ID, FULL_NAME, NIK_MASKED, EMAIL, PHONE, CITY, PROVINCE, BIRTH_DATE,
    GENDER, OCCUPATION, MONTHLY_INCOME,
    IFF(KYC_STATUS = 'PENDING', 'VERIFIED', KYC_STATUS)      AS KYC_STATUS,
    CASE WHEN MAX_DPD > 60                     THEN LEAST('E', CHR(ASCII(RISK_SEGMENT) + 1))
         WHEN ON_TIME_RATIO >= 0.95 AND MAX_DPD = 0
                                               THEN GREATEST('A', CHR(ASCII(RISK_SEGMENT) - 1))
         ELSE RISK_SEGMENT END                               AS RISK_SEGMENT,
    LEAST(200000000, GREATEST(1000000, ROUND(
      CREDIT_LIMIT * CASE WHEN MAX_DPD > 60                        THEN 0.60
                          WHEN MAX_DPD > 15                        THEN 0.85
                          WHEN ON_TIME_RATIO >= 0.95 AND MAX_DPD=0 THEN 1.30
                          ELSE 1.00 END / 500000) * 500000))       AS CREDIT_LIMIT,
    ACQUISITION_CHANNEL, REGISTERED_AT,
    'U', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    DEMO_INDODANA.BRONZE.SEQ_CDC.NEXTVAL, :v_batch, 'core_lending'
  FROM pick;
  v_cu := SQLROWCOUNT;

  -- -------------------------------------------------------------------
  -- 2. Installments settling: pick unpaid, now-due instalments and pay
  --    them. Emits both a 'U' on the instalment and an 'I' payment.
  -- -------------------------------------------------------------------
  CREATE OR REPLACE TEMPORARY TABLE T_CUR_INST AS
  SELECT * EXCLUDE (RN) FROM (
    SELECT i.*, ROW_NUMBER() OVER (PARTITION BY INSTALLMENT_ID
                                   ORDER BY _COMMIT_TS DESC, _SEQ DESC) AS RN
    FROM DEMO_INDODANA.BRONZE.RAW_INSTALLMENTS i
  ) WHERE RN = 1 AND _OP <> 'D';

  CREATE OR REPLACE TEMPORARY TABLE T_SETTLE AS
  SELECT i.INSTALLMENT_ID, i.LOAN_ID, i.INSTALLMENT_NO, i.DUE_DATE,
         i.PRINCIPAL_DUE, i.INTEREST_DUE, i.FEE_DUE, i.TOTAL_DUE,
         l.CUSTOMER_ID,
         GREATEST(0, DATEDIFF(day, i.DUE_DATE, CURRENT_DATE())) AS DAYS_LATE,
         UNIFORM(1, 100, RANDOM())                              AS CHANNEL_IDX
  FROM T_CUR_INST i
  JOIN T_CUR_LOAN l ON l.LOAN_ID = i.LOAN_ID
  WHERE i.INSTALLMENT_STATUS = 'UNPAID'
    AND i.DUE_DATE <= CURRENT_DATE()
    AND l.LOAN_STATUS IN ('ACTIVE', 'DELINQUENT')
  QUALIFY ROW_NUMBER() OVER (ORDER BY RANDOM()) <= :P_LOANS;

  INSERT INTO DEMO_INDODANA.BRONZE.RAW_PAYMENTS
    (PAYMENT_ID, LOAN_ID, INSTALLMENT_ID, CUSTOMER_ID, PAID_AT, AMOUNT,
     PRINCIPAL_PORTION, INTEREST_PORTION, LATE_FEE, PAYMENT_CHANNEL, PAYMENT_TYPE,
     DAYS_LATE, _OP, _COMMIT_TS, _SEQ, _BATCH_ID, _SOURCE)
  SELECT
    'PAY' || LPAD(DEMO_INDODANA.GEN.SEQ_PAYMENT.NEXTVAL, 11, '0'),
    s.LOAN_ID, s.INSTALLMENT_ID, s.CUSTOMER_ID,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    s.TOTAL_DUE + LEAST(ROUND(s.TOTAL_DUE * 0.10 / 1000) * 1000, s.DAYS_LATE * 5000),
    s.PRINCIPAL_DUE, s.INTEREST_DUE,
    LEAST(ROUND(s.TOTAL_DUE * 0.10 / 1000) * 1000, s.DAYS_LATE * 5000),
    ch.PAYMENT_CHANNEL,
    IFF(s.DAYS_LATE > 0, 'LATE', 'SCHEDULED'),
    s.DAYS_LATE, 'I', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    DEMO_INDODANA.BRONZE.SEQ_CDC.NEXTVAL, :v_batch, 'lms'
  FROM T_SETTLE s
  JOIN DEMO_INDODANA.GEN.POOL_CHANNEL ch ON ch.IDX = s.CHANNEL_IDX;
  v_pu := SQLROWCOUNT;

  INSERT INTO DEMO_INDODANA.BRONZE.RAW_INSTALLMENTS
    (INSTALLMENT_ID, LOAN_ID, INSTALLMENT_NO, DUE_DATE, PRINCIPAL_DUE, INTEREST_DUE,
     FEE_DUE, TOTAL_DUE, AMOUNT_PAID, INSTALLMENT_STATUS, PAID_AT,
     _OP, _COMMIT_TS, _SEQ, _BATCH_ID, _SOURCE)
  SELECT
    s.INSTALLMENT_ID, s.LOAN_ID, s.INSTALLMENT_NO, s.DUE_DATE, s.PRINCIPAL_DUE,
    s.INTEREST_DUE, s.FEE_DUE, s.TOTAL_DUE,
    s.TOTAL_DUE + LEAST(ROUND(s.TOTAL_DUE * 0.10 / 1000) * 1000, s.DAYS_LATE * 5000),
    'PAID', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    'U', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    DEMO_INDODANA.BRONZE.SEQ_CDC.NEXTVAL, :v_batch, 'lms'
  FROM T_SETTLE s;
  v_iu := SQLROWCOUNT;

  -- -------------------------------------------------------------------
  -- 3. Loan status + outstanding recalculated from the new instalment
  --    state, for every loan touched above.
  -- -------------------------------------------------------------------
  CREATE OR REPLACE TEMPORARY TABLE T_INST_AFTER AS
  SELECT i.LOAN_ID, i.INSTALLMENT_ID, i.DUE_DATE, i.PRINCIPAL_DUE,
         IFF(s.INSTALLMENT_ID IS NOT NULL, 'PAID', i.INSTALLMENT_STATUS) AS INSTALLMENT_STATUS
  FROM T_CUR_INST i
  LEFT JOIN T_SETTLE s ON s.INSTALLMENT_ID = i.INSTALLMENT_ID
  WHERE i.LOAN_ID IN (SELECT LOAN_ID FROM T_SETTLE);

  INSERT INTO DEMO_INDODANA.BRONZE.RAW_LOANS
    (LOAN_ID, APPLICATION_ID, CUSTOMER_ID, MERCHANT_ID, PARTNER_ID, PRODUCT_TYPE,
     PRINCIPAL_AMOUNT, INTEREST_RATE_PA, ADMIN_FEE, TENOR_MONTHS, LOAN_STATUS,
     OUTSTANDING_PRINCIPAL, DISBURSED_AT, FIRST_DUE_DATE, MATURITY_DATE,
     _OP, _COMMIT_TS, _SEQ, _BATCH_ID, _SOURCE)
  SELECT
    l.LOAN_ID, l.APPLICATION_ID, l.CUSTOMER_ID, l.MERCHANT_ID, l.PARTNER_ID,
    l.PRODUCT_TYPE, l.PRINCIPAL_AMOUNT, l.INTEREST_RATE_PA, l.ADMIN_FEE,
    l.TENOR_MONTHS,
    CASE WHEN a.UNPAID_CNT = 0 THEN 'PAID_OFF'
         WHEN a.MAX_DPD > 180  THEN 'WRITTEN_OFF'
         WHEN a.MAX_DPD > 0    THEN 'DELINQUENT'
         ELSE 'ACTIVE' END,
    a.OUTSTANDING_PRINCIPAL,
    l.DISBURSED_AT, l.FIRST_DUE_DATE, l.MATURITY_DATE,
    'U', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    DEMO_INDODANA.BRONZE.SEQ_CDC.NEXTVAL, :v_batch, 'core_lending'
  FROM T_CUR_LOAN l
  JOIN (
    SELECT LOAN_ID,
           COUNT_IF(INSTALLMENT_STATUS = 'UNPAID')                   AS UNPAID_CNT,
           MAX(IFF(INSTALLMENT_STATUS = 'UNPAID' AND DUE_DATE < CURRENT_DATE(),
                   DATEDIFF(day, DUE_DATE, CURRENT_DATE()), 0))      AS MAX_DPD,
           SUM(IFF(INSTALLMENT_STATUS = 'UNPAID', PRINCIPAL_DUE, 0)) AS OUTSTANDING_PRINCIPAL
    FROM T_INST_AFTER GROUP BY LOAN_ID
  ) a ON a.LOAN_ID = l.LOAN_ID;
  v_lu := SQLROWCOUNT;

  UPDATE DEMO_INDODANA.BRONZE.BATCH_LOG
     SET FINISHED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
         ROWS_LANDED = :v_cu + :v_lu + :v_iu + :v_pu,
         DETAIL = DETAIL || ' | cust_upd=' || :v_cu || ' loan_upd=' || :v_lu
                  || ' inst_upd=' || :v_iu || ' new_pay=' || :v_pu
   WHERE BATCH_ID = :v_batch;

  RETURN :v_batch || ' | CDC updates: customers=' || :v_cu
      || ' loans=' || :v_lu || ' installments=' || :v_iu
      || ' payments=' || :v_pu;
END;
$$;

-- ---------------------------------------------------------------------
-- SP_CDC_DELETE — soft deletes ('D' tombstones)
-- ---------------------------------------------------------------------
-- Real-world sources do delete: duplicate leads get merged away and
-- fraud-flagged applications are purged for compliance. Downstream
-- models must honour the tombstone instead of keeping a ghost row.
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_CDC_DELETE(P_LEADS NUMBER, P_APPLICATIONS NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  v_batch VARCHAR;
  v_dl NUMBER DEFAULT 0; v_da NUMBER DEFAULT 0;
BEGIN
  v_batch := 'B-' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS')
                  || '-' || LEFT(REPLACE(UUID_STRING(), '-', ''), 6);

  INSERT INTO DEMO_INDODANA.BRONZE.BATCH_LOG (BATCH_ID, BATCH_TYPE, STARTED_AT, DETAIL)
  VALUES (:v_batch, 'DELETE', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
          'leads=' || :P_LEADS || ' applications=' || :P_APPLICATIONS);

  -- Duplicate leads merged away by the marketplace dedupe job.
  -- Only leads that never converted are eligible — deleting a lead that
  -- produced a disbursed loan would orphan the attribution chain.
  INSERT INTO DEMO_INDODANA.BRONZE.RAW_LEADS
    (LEAD_ID, CUSTOMER_ID, LEAD_SOURCE, CAMPAIGN_CODE, PRODUCT_INTEREST, PARTNER_ID,
     LEAD_STATUS, COST_PER_LEAD, CREATED_AT, _OP, _COMMIT_TS, _SEQ, _BATCH_ID, _SOURCE)
  WITH cur AS (
    SELECT * EXCLUDE (RN) FROM (
      SELECT l.*, ROW_NUMBER() OVER (PARTITION BY LEAD_ID
                                     ORDER BY _COMMIT_TS DESC, _SEQ DESC) AS RN
      FROM DEMO_INDODANA.BRONZE.RAW_LEADS l
    ) WHERE RN = 1 AND _OP <> 'D'
  )
  SELECT c.LEAD_ID, c.CUSTOMER_ID, c.LEAD_SOURCE, c.CAMPAIGN_CODE, c.PRODUCT_INTEREST,
         c.PARTNER_ID, 'DROPPED', c.COST_PER_LEAD, c.CREATED_AT,
         'D', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
         DEMO_INDODANA.BRONZE.SEQ_CDC.NEXTVAL, :v_batch, 'cermati_marketplace'
  FROM cur c
  WHERE NOT EXISTS (
          SELECT 1 FROM DEMO_INDODANA.BRONZE.RAW_LOANS ln
          JOIN DEMO_INDODANA.BRONZE.RAW_LOAN_APPLICATIONS ap
            ON ap.APPLICATION_ID = ln.APPLICATION_ID
          WHERE ap.LEAD_ID = c.LEAD_ID)
  QUALIFY ROW_NUMBER() OVER (ORDER BY RANDOM()) <= :P_LEADS;
  v_dl := SQLROWCOUNT;

  -- Fraud-flagged / withdrawn applications purged. Declined-only, so no
  -- disbursed loan is ever left pointing at a tombstoned application.
  INSERT INTO DEMO_INDODANA.BRONZE.RAW_LOAN_APPLICATIONS
    (APPLICATION_ID, CUSTOMER_ID, LEAD_ID, PRODUCT_TYPE, REQUESTED_AMOUNT, TENOR_MONTHS,
     BUREAU_SCORE, HAS_BUREAU_FILE, INTERNAL_SCORE, DECISION, DECLINE_REASON,
     APPROVED_AMOUNT, APPROVED_RATE_PA, APPLIED_AT, DECISION_AT,
     _OP, _COMMIT_TS, _SEQ, _BATCH_ID, _SOURCE)
  WITH cur AS (
    SELECT * EXCLUDE (RN) FROM (
      SELECT a.*, ROW_NUMBER() OVER (PARTITION BY APPLICATION_ID
                                     ORDER BY _COMMIT_TS DESC, _SEQ DESC) AS RN
      FROM DEMO_INDODANA.BRONZE.RAW_LOAN_APPLICATIONS a
    ) WHERE RN = 1 AND _OP <> 'D'
  )
  SELECT c.APPLICATION_ID, c.CUSTOMER_ID, c.LEAD_ID, c.PRODUCT_TYPE, c.REQUESTED_AMOUNT,
         c.TENOR_MONTHS, c.BUREAU_SCORE, c.HAS_BUREAU_FILE, c.INTERNAL_SCORE,
         'CANCELLED', c.DECLINE_REASON, c.APPROVED_AMOUNT, c.APPROVED_RATE_PA,
         c.APPLIED_AT, c.DECISION_AT,
         'D', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
         DEMO_INDODANA.BRONZE.SEQ_CDC.NEXTVAL, :v_batch, 'core_lending'
  FROM cur c
  WHERE c.DECISION = 'DECLINED'
  QUALIFY ROW_NUMBER() OVER (ORDER BY RANDOM()) <= :P_APPLICATIONS;
  v_da := SQLROWCOUNT;

  UPDATE DEMO_INDODANA.BRONZE.BATCH_LOG
     SET FINISHED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
         ROWS_LANDED = :v_dl + :v_da,
         DETAIL = DETAIL || ' | lead_del=' || :v_dl || ' app_del=' || :v_da
   WHERE BATCH_ID = :v_batch;

  RETURN :v_batch || ' | CDC deletes: leads=' || :v_dl
      || ' applications=' || :v_da;
END;
$$;

-- ---------------------------------------------------------------------
-- SP_SEED_HISTORY — build P_MONTHS of monthly cohorts in one shot
-- ---------------------------------------------------------------------
-- Cohorts are seeded month by month so the gold vintage mart has real
-- vintages: older cohorts have run far enough into their tenor to
-- produce write-offs, newer ones are still clean.
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_SEED_HISTORY(P_MONTHS NUMBER, P_PER_MONTH NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  v_m     NUMBER DEFAULT 0;
  v_total NUMBER DEFAULT 0;
  v_res   VARCHAR;
  v_as_of DATE;
  v_size  NUMBER;
BEGIN
  v_m := :P_MONTHS;
  WHILE (:v_m >= 0) DO
    v_as_of := DATE_TRUNC('month', DATEADD(month, -:v_m, CURRENT_DATE()))::DATE;
    -- modest month-on-month growth so the origination trend is not flat
    v_size  := GREATEST(10, ROUND(:P_PER_MONTH * (1 + 0.04 * (:P_MONTHS - :v_m))));
    CALL DEMO_INDODANA.GEN.SP_INGEST_BATCH(:v_size, :v_as_of);
    v_total := :v_total + :v_size;
    v_m := :v_m - 1;
  END WHILE;
  RETURN 'Seeded ' || (:P_MONTHS + 1) || ' monthly cohorts, '
      || :v_total || ' customers total.';
END;
$$;
