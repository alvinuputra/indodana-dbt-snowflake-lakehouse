-- =====================================================================
-- 03_sp_ingest_batch.sql
-- Emits one full cohort of lending-lifecycle CDC events into BRONZE:
--   lead -> customer -> application -> decision -> disbursement
--        -> installment schedule -> payments -> collection activity
--
-- P_CUSTOMERS : cohort size
-- P_AS_OF     : month the cohort enters the funnel (drives vintages)
--
-- Downstream behaviour is correlated, not independently random:
-- income drives a latent credit `QUALITY`, which drives risk segment,
-- credit limit, bureau score, the approval decision AND the repayment
-- behaviour profile. Without that correlation the gold-layer risk
-- marts would be statistical noise and every scorecard would look flat.
-- =====================================================================
USE SCHEMA DEMO_INDODANA.GEN;

CREATE OR REPLACE PROCEDURE SP_INGEST_BATCH(P_CUSTOMERS NUMBER, P_AS_OF DATE)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  v_batch VARCHAR;
  v_rows  NUMBER DEFAULT 0;
  v_c NUMBER DEFAULT 0; v_l NUMBER DEFAULT 0; v_a NUMBER DEFAULT 0;
  v_ln NUMBER DEFAULT 0; v_i NUMBER DEFAULT 0; v_p NUMBER DEFAULT 0; v_x NUMBER DEFAULT 0;
BEGIN
  -- A sequence NEXTVAL is not allowed in a procedure assignment expression,
  -- so the batch suffix comes from UUID_STRING instead.
  v_batch := 'B-' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS')
                  || '-' || LEFT(REPLACE(UUID_STRING(), '-', ''), 6);

  INSERT INTO DEMO_INDODANA.BRONZE.BATCH_LOG (BATCH_ID, BATCH_TYPE, STARTED_AT, DETAIL)
  VALUES (:v_batch, 'INSERT', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
          'cohort=' || :P_CUSTOMERS || ' as_of=' || :P_AS_OF);

  -- ===================================================================
  -- 1. Customers + their latent credit quality
  -- ===================================================================
  CREATE OR REPLACE TEMPORARY TABLE T_CUST AS
  WITH r AS (
    SELECT n.I AS RN,
           UNIFORM(1, 123, RANDOM())              AS CITY_IDX,
           UNIFORM(1,  12, RANDOM())              AS OCC_IDX,
           UNIFORM(1,  30, RANDOM())              AS FN_IDX,
           UNIFORM(1,  28, RANDOM())              AS LN_IDX,
           UNIFORM(1, 100, RANDOM())              AS SRC_IDX,
           UNIFORM(1, 100, RANDOM())              AS PROD_IDX,
           UNIFORM(1, 100, RANDOM())             AS DECLINE_IDX,
           UNIFORM(14.0::FLOAT, 17.6::FLOAT, RANDOM()) AS R_INC,
           UNIFORM(0::FLOAT, 1::FLOAT, RANDOM())  AS R_Q,
           UNIFORM(0::FLOAT, 1::FLOAT, RANDOM())  AS R_KYC,
           UNIFORM(0::FLOAT, 1::FLOAT, RANDOM())  AS R_LEAD,
           UNIFORM(0::FLOAT, 1::FLOAT, RANDOM())  AS R_BEH,
           UNIFORM(0::FLOAT, 1::FLOAT, RANDOM())  AS R_BUREAU,
           UNIFORM(0::FLOAT, 1::FLOAT, RANDOM())  AS R_DISBURSE,
           UNIFORM(0::FLOAT, 1::FLOAT, RANDOM())  AS R_AMT,
           UNIFORM(-45, 45, RANDOM())             AS SCORE_NOISE,
           UNIFORM(-35, 35, RANDOM())             AS BUREAU_NOISE,
           UNIFORM(0,  27, RANDOM())              AS DAY_IN_MONTH,
           UNIFORM(0,  3,  RANDOM())              AS SIGNUP_LAG,
           UNIFORM(0,  5,  RANDOM())              AS APPLY_LAG,
           UNIFORM(2,  36, RANDOM())              AS DISBURSE_LAG_H,
           UNIFORM(5,  90, RANDOM())              AS DECISION_LAG_MIN,
           UNIFORM(28, 35, RANDOM())              AS FIRST_DUE_LAG,
           UNIFORM(21, 55, RANDOM())              AS AGE_Y,
           UNIFORM(0,  364,RANDOM())              AS AGE_D,
           UNIFORM(0, 86399, RANDOM())            AS SEC_OF_DAY,
           UNIFORM(1,  10, RANDOM())              AS PARTNER_N,
           UNIFORM(1,  15, RANDOM())              AS MERCHANT_N
    FROM DEMO_INDODANA.GEN.NUMBERS n
    WHERE n.I <= :P_CUSTOMERS
  ),
  inc AS (
    SELECT r.*,
           c.CITY, c.PROVINCE, o.OCCUPATION, f.NAME AS FIRST_NAME, f.GENDER,
           l.NAME AS LAST_NAME, s.LEAD_SOURCE, s.CPL,
           p.PRODUCT_TYPE, p.MIN_AMT, p.MAX_AMT, p.RATE_PA,
           GREATEST(2000000,
             ROUND(EXP(r.R_INC) * o.INCOME_MULT / 100000) * 100000) AS MONTHLY_INCOME
    FROM r
    JOIN DEMO_INDODANA.GEN.POOL_CITY        c ON c.IDX = r.CITY_IDX
    JOIN DEMO_INDODANA.GEN.POOL_OCCUPATION  o ON o.IDX = r.OCC_IDX
    JOIN DEMO_INDODANA.GEN.POOL_FIRST       f ON f.IDX = r.FN_IDX
    JOIN DEMO_INDODANA.GEN.POOL_LAST        l ON l.IDX = r.LN_IDX
    JOIN DEMO_INDODANA.GEN.POOL_LEAD_SOURCE s ON s.IDX = r.SRC_IDX
    JOIN DEMO_INDODANA.GEN.POOL_PRODUCT     p ON p.IDX = r.PROD_IDX
  ),
  q AS (
    SELECT inc.*,
           -- Latent credit quality in [0,1): 55% income-driven, 45% idiosyncratic.
           -- Income is log-scaled across the IDR 2m..60m band — a linear scale
           -- would squash the heavily right-skewed income distribution into the
           -- bottom decile and decline almost every applicant.
           0.55 * LEAST(1, GREATEST(0,
                    (LN(MONTHLY_INCOME) - LN(2000000)) / (LN(60000000) - LN(2000000))))
         + 0.45 * R_Q AS QUALITY,
           DATEADD(day, DAY_IN_MONTH, :P_AS_OF) AS FUNNEL_DATE
    FROM inc
  )
  SELECT
    'CUS' || LPAD(DEMO_INDODANA.GEN.SEQ_CUSTOMER.NEXTVAL, 9, '0') AS CUSTOMER_ID,
    FIRST_NAME || ' ' || LAST_NAME                     AS FULL_NAME,
    '32' || REPEAT('*', 10) || LPAD(UNIFORM(1000, 9999, RANDOM()), 4, '0') AS NIK_MASKED,
    LOWER(FIRST_NAME) || '.' || LOWER(LAST_NAME) || RN || '@'
      || ARRAY_CONSTRUCT('gmail.com','yahoo.co.id','outlook.com','icloud.com')
         [UNIFORM(0, 3, RANDOM())]::VARCHAR                AS EMAIL,
    '+62' || ARRAY_CONSTRUCT('811','812','813','817','821','822','838','851')
             [UNIFORM(0, 7, RANDOM())]::VARCHAR
          || LPAD(UNIFORM(1000000, 9999999, RANDOM()), 7, '0') AS PHONE,
    CITY, PROVINCE,
    DATEADD(day, -AGE_D, DATEADD(year, -AGE_Y, FUNNEL_DATE))  AS BIRTH_DATE,
    IFF(GENDER = 'M', 'MALE', 'FEMALE')                 AS GENDER,
    OCCUPATION, MONTHLY_INCOME,
    CASE WHEN R_KYC < 0.88 THEN 'VERIFIED'
         WHEN R_KYC < 0.96 THEN 'PENDING'
         ELSE 'REJECTED' END                            AS KYC_STATUS,
    CASE WHEN QUALITY >= 0.72 THEN 'A'
         WHEN QUALITY >= 0.58 THEN 'B'
         WHEN QUALITY >= 0.44 THEN 'C'
         WHEN QUALITY >= 0.30 THEN 'D'
         ELSE 'E' END                                   AS RISK_SEGMENT,
    LEAST(200000000, GREATEST(1000000,
      ROUND(MONTHLY_INCOME * CASE WHEN QUALITY >= 0.72 THEN 3.0
                                  WHEN QUALITY >= 0.58 THEN 2.5
                                  WHEN QUALITY >= 0.44 THEN 2.0
                                  WHEN QUALITY >= 0.30 THEN 1.2
                                  ELSE 0.8 END / 500000) * 500000))   AS CREDIT_LIMIT,
    IFF(R_LEAD < 0.70, 'CERMATI_' || LEAD_SOURCE, 'INDODANA_APP')     AS ACQUISITION_CHANNEL,
    LEAST(CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
          DATEADD(second, SEC_OF_DAY,
                  DATEADD(day, SIGNUP_LAG, FUNNEL_DATE)::TIMESTAMP_NTZ)) AS REGISTERED_AT,
    -- helper columns consumed by later steps, never landed in Bronze
    QUALITY, FUNNEL_DATE,
    R_LEAD < 0.70                                       AS HAS_LEAD,
    LEAD_SOURCE, CPL,
    PRODUCT_TYPE, MIN_AMT, MAX_AMT, RATE_PA,
    R_BEH, R_BUREAU, R_DISBURSE, R_AMT, SCORE_NOISE, BUREAU_NOISE,
    DECLINE_IDX, SIGNUP_LAG, APPLY_LAG, DECISION_LAG_MIN, DISBURSE_LAG_H, FIRST_DUE_LAG,
    'PTR' || LPAD(PARTNER_N, 3, '0')                    AS PARTNER_ID,
    'MCH' || LPAD(MERCHANT_N, 3, '0')                   AS MERCHANT_ID
  FROM q;

  -- ===================================================================
  -- 2. Leads (marketplace-sourced customers only)
  -- ===================================================================
  CREATE OR REPLACE TEMPORARY TABLE T_LEAD AS
  SELECT
    'LED' || LPAD(DEMO_INDODANA.GEN.SEQ_LEAD.NEXTVAL, 9, '0') AS LEAD_ID,
    c.CUSTOMER_ID,
    c.LEAD_SOURCE,
    'CMP-' || TO_CHAR(c.FUNNEL_DATE, 'YYYYMM') || '-'
           || UPPER(LEFT(c.LEAD_SOURCE, 3))                   AS CAMPAIGN_CODE,
    c.PRODUCT_TYPE                                            AS PRODUCT_INTEREST,
    c.PARTNER_ID,
    'APPLIED'                                                 AS LEAD_STATUS,
    c.CPL                                                     AS COST_PER_LEAD,
    LEAST(CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
          c.FUNNEL_DATE::TIMESTAMP_NTZ)                       AS CREATED_AT
  FROM T_CUST c
  WHERE c.HAS_LEAD;

  -- ===================================================================
  -- 3. Applications + underwriting decision
  -- ===================================================================
  CREATE OR REPLACE TEMPORARY TABLE T_APP AS
  WITH s AS (
    SELECT
      c.*,
      l.LEAD_ID,
      LEAST(850, GREATEST(300,
        ROUND(470 + c.QUALITY * 330 + c.SCORE_NOISE)))         AS INTERNAL_SCORE,
      c.R_BUREAU < 0.72                                        AS HAS_BUREAU_FILE,
      GREATEST(c.MIN_AMT,
        ROUND((c.MIN_AMT + c.R_AMT * (c.MAX_AMT - c.MIN_AMT)) / 500000)
        * 500000)                                              AS REQUESTED_AMOUNT,
      CASE WHEN c.PRODUCT_TYPE = 'PAYLATER'
           THEN ARRAY_CONSTRUCT(3, 6, 12)[UNIFORM(0, 2, RANDOM())]::NUMBER
           ELSE ARRAY_CONSTRUCT(6, 12, 18, 24, 36)[UNIFORM(0, 4, RANDOM())]::NUMBER
      END                                                      AS TENOR_MONTHS,
      LEAST(CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
            DATEADD(second, UNIFORM(0, 86399, RANDOM()),
                    DATEADD(day, c.APPLY_LAG + c.SIGNUP_LAG,
                            c.FUNNEL_DATE)::TIMESTAMP_NTZ))     AS APPLIED_AT
    FROM T_CUST c
    LEFT JOIN T_LEAD l ON l.CUSTOMER_ID = c.CUSTOMER_ID
  ),
  d AS (
    SELECT s.*,
           LEAST(CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                 DATEADD(minute, s.DECISION_LAG_MIN, s.APPLIED_AT)) AS DECISION_AT,
           CASE
             WHEN s.KYC_STATUS = 'REJECTED'                       THEN 'DECLINED'
             WHEN s.KYC_STATUS = 'PENDING'                        THEN 'PENDING'
             WHEN s.INTERNAL_SCORE < 600                          THEN 'DECLINED'
             WHEN NOT s.HAS_BUREAU_FILE AND s.QUALITY < 0.48      THEN 'DECLINED'
             ELSE 'APPROVED'
           END                                                    AS DECISION
    FROM s
  )
  SELECT
    'APP' || LPAD(DEMO_INDODANA.GEN.SEQ_APP.NEXTVAL, 9, '0') AS APPLICATION_ID,
    d.CUSTOMER_ID, d.LEAD_ID, d.PRODUCT_TYPE, d.REQUESTED_AMOUNT, d.TENOR_MONTHS,
    IFF(d.HAS_BUREAU_FILE,
        LEAST(850, GREATEST(300, d.INTERNAL_SCORE + d.BUREAU_NOISE)),
        NULL)                                                 AS BUREAU_SCORE,
    d.HAS_BUREAU_FILE, d.INTERNAL_SCORE, d.DECISION,
    IFF(d.DECISION = 'DECLINED', dr.REASON, NULL)             AS DECLINE_REASON,
    IFF(d.DECISION = 'APPROVED',
        GREATEST(500000,
          FLOOR(LEAST(d.REQUESTED_AMOUNT, d.CREDIT_LIMIT) / 500000) * 500000),
        NULL)                                                 AS APPROVED_AMOUNT,
    IFF(d.DECISION = 'APPROVED',
        -- risk-based pricing: weaker segments pay a spread over the base rate
        ROUND(d.RATE_PA * CASE WHEN d.RISK_SEGMENT = 'A' THEN 0.88
                               WHEN d.RISK_SEGMENT = 'B' THEN 0.96
                               WHEN d.RISK_SEGMENT = 'C' THEN 1.00
                               WHEN d.RISK_SEGMENT = 'D' THEN 1.12
                               ELSE 1.22 END, 4),
        NULL)                                                 AS APPROVED_RATE_PA,
    d.APPLIED_AT,
    IFF(d.DECISION = 'PENDING', NULL, d.DECISION_AT)          AS DECISION_AT,
    -- helpers
    d.QUALITY, d.RISK_SEGMENT, d.R_BEH, d.R_DISBURSE,
    d.DISBURSE_LAG_H, d.FIRST_DUE_LAG, d.PARTNER_ID, d.MERCHANT_ID, d.DECISION_AT AS DEC_TS
  FROM d
  LEFT JOIN DEMO_INDODANA.GEN.POOL_DECLINE dr ON dr.IDX = d.DECLINE_IDX;

  -- ===================================================================
  -- 4. Loans — approved applications that actually disburse (~94%)
  -- ===================================================================
  CREATE OR REPLACE TEMPORARY TABLE T_LOAN AS
  SELECT
    'LON' || LPAD(DEMO_INDODANA.GEN.SEQ_LOAN.NEXTVAL, 9, '0') AS LOAN_ID,
    a.APPLICATION_ID, a.CUSTOMER_ID,
    IFF(a.PRODUCT_TYPE = 'PAYLATER', a.MERCHANT_ID, NULL)     AS MERCHANT_ID,
    a.PARTNER_ID, a.PRODUCT_TYPE,
    a.APPROVED_AMOUNT                                         AS PRINCIPAL_AMOUNT,
    a.APPROVED_RATE_PA                                        AS INTEREST_RATE_PA,
    ROUND(a.APPROVED_AMOUNT * 0.01 / 1000) * 1000             AS ADMIN_FEE,
    a.TENOR_MONTHS,
    LEAST(CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
          DATEADD(hour, a.DISBURSE_LAG_H, a.DEC_TS))          AS DISBURSED_AT,
    a.QUALITY, a.RISK_SEGMENT, a.R_BEH, a.FIRST_DUE_LAG
  FROM T_APP a
  WHERE a.DECISION = 'APPROVED'
    AND a.APPROVED_AMOUNT >= 500000
    AND a.R_DISBURSE < 0.94;

  -- Repayment behaviour profile, conditioned on risk segment.
  -- GOOD pays early/on time, LATE always pays but days late, DELINQUENT
  -- stops partway, WRITEOFF stops almost immediately.
  CREATE OR REPLACE TEMPORARY TABLE T_LOAN_BEH AS
  SELECT l.*,
         DATEADD(day, l.FIRST_DUE_LAG, l.DISBURSED_AT::DATE)  AS FIRST_DUE_DATE,
         CASE l.RISK_SEGMENT
           WHEN 'A' THEN CASE WHEN l.R_BEH < 0.930 THEN 'GOOD' WHEN l.R_BEH < 0.988 THEN 'LATE' WHEN l.R_BEH < 0.998 THEN 'DELINQUENT' ELSE 'WRITEOFF' END
           WHEN 'B' THEN CASE WHEN l.R_BEH < 0.865 THEN 'GOOD' WHEN l.R_BEH < 0.960 THEN 'LATE' WHEN l.R_BEH < 0.993 THEN 'DELINQUENT' ELSE 'WRITEOFF' END
           WHEN 'C' THEN CASE WHEN l.R_BEH < 0.745 THEN 'GOOD' WHEN l.R_BEH < 0.910 THEN 'LATE' WHEN l.R_BEH < 0.978 THEN 'DELINQUENT' ELSE 'WRITEOFF' END
           WHEN 'D' THEN CASE WHEN l.R_BEH < 0.600 THEN 'GOOD' WHEN l.R_BEH < 0.830 THEN 'LATE' WHEN l.R_BEH < 0.952 THEN 'DELINQUENT' ELSE 'WRITEOFF' END
           ELSE          CASE WHEN l.R_BEH < 0.440 THEN 'GOOD' WHEN l.R_BEH < 0.720 THEN 'LATE' WHEN l.R_BEH < 0.910 THEN 'DELINQUENT' ELSE 'WRITEOFF' END
         END                                                   AS BEHAVIOUR,
         -- LATE payers run up to ~8 weeks behind, which means some of them
         -- cross 30 DPD and then cure. Capping this below 30 would make
         -- ever-30 and ever-90 identical for every mature cohort and
         -- collapse the vintage mart into a single degenerate curve.
         UNIFORM(1, 55, RANDOM())                              AS LATE_DAYS_BASE
  FROM T_LOAN l;

  ALTER TABLE T_LOAN_BEH ADD COLUMN STOP_AFTER_N NUMBER;
  UPDATE T_LOAN_BEH
     SET STOP_AFTER_N = CASE BEHAVIOUR
           WHEN 'GOOD'       THEN TENOR_MONTHS
           WHEN 'LATE'       THEN TENOR_MONTHS
           WHEN 'DELINQUENT' THEN GREATEST(1, FLOOR(TENOR_MONTHS * UNIFORM(0.30::FLOAT, 0.80::FLOAT, RANDOM())))
           ELSE                   GREATEST(0, FLOOR(TENOR_MONTHS * UNIFORM(0.00::FLOAT, 0.25::FLOAT, RANDOM())))
         END;

  -- ===================================================================
  -- 5. Installment schedule (flat-rate amortisation, IDR-rounded)
  -- ===================================================================
  CREATE OR REPLACE TEMPORARY TABLE T_INST AS
  WITH sched AS (
    SELECT
      l.LOAN_ID, l.CUSTOMER_ID, l.TENOR_MONTHS, l.PRINCIPAL_AMOUNT,
      l.BEHAVIOUR, l.STOP_AFTER_N, l.LATE_DAYS_BASE,
      n.I                                                       AS INSTALLMENT_NO,
      DATEADD(month, n.I - 1, l.FIRST_DUE_DATE)                 AS DUE_DATE,
      ROUND(l.PRINCIPAL_AMOUNT / l.TENOR_MONTHS / 1000) * 1000  AS PRINCIPAL_STD,
      ROUND(l.PRINCIPAL_AMOUNT * l.INTEREST_RATE_PA / 12 / 1000) * 1000 AS INTEREST_DUE,
      IFF(n.I = 1, l.ADMIN_FEE, 0)                              AS FEE_DUE
    FROM T_LOAN_BEH l
    JOIN DEMO_INDODANA.GEN.NUMBERS n ON n.I <= l.TENOR_MONTHS
  )
  SELECT
    LOAN_ID || '-' || LPAD(INSTALLMENT_NO, 2, '0')              AS INSTALLMENT_ID,
    LOAN_ID, CUSTOMER_ID, INSTALLMENT_NO, DUE_DATE,
    -- final instalment absorbs the rounding residual so principal ties exactly
    IFF(INSTALLMENT_NO = TENOR_MONTHS,
        PRINCIPAL_AMOUNT - PRINCIPAL_STD * (TENOR_MONTHS - 1),
        PRINCIPAL_STD)                                          AS PRINCIPAL_DUE,
    INTEREST_DUE, FEE_DUE,
    IFF(INSTALLMENT_NO = TENOR_MONTHS,
        PRINCIPAL_AMOUNT - PRINCIPAL_STD * (TENOR_MONTHS - 1),
        PRINCIPAL_STD) + INTEREST_DUE + FEE_DUE                 AS TOTAL_DUE,
    TENOR_MONTHS, BEHAVIOUR, STOP_AFTER_N, LATE_DAYS_BASE
  FROM sched;

  -- ===================================================================
  -- 6. Payments — only those that have already happened by now
  -- ===================================================================
  CREATE OR REPLACE TEMPORARY TABLE T_PAY AS
  WITH intent AS (
    SELECT i.*,
           CASE i.BEHAVIOUR
             WHEN 'GOOD' THEN DATEADD(day, -UNIFORM(0, 3, RANDOM()), i.DUE_DATE)
             WHEN 'LATE' THEN DATEADD(day,  i.LATE_DAYS_BASE,        i.DUE_DATE)
             ELSE IFF(i.INSTALLMENT_NO <= i.STOP_AFTER_N,
                      DATEADD(day, UNIFORM(0, 14, RANDOM()), i.DUE_DATE),
                      NULL)
           END AS PAY_DATE
    FROM T_INST i
  ),
  due AS (
    SELECT intent.*,
           DATEADD(second, UNIFORM(0, 86399, RANDOM()),
                   PAY_DATE::TIMESTAMP_NTZ)         AS PAID_AT,
           UNIFORM(1, 100, RANDOM())                AS CHANNEL_IDX
    FROM intent
    WHERE PAY_DATE IS NOT NULL
      AND PAY_DATE <= CURRENT_DATE()
  )
  SELECT
    'PAY' || LPAD(DEMO_INDODANA.GEN.SEQ_PAYMENT.NEXTVAL, 11, '0') AS PAYMENT_ID,
    d.LOAN_ID, d.INSTALLMENT_ID, d.CUSTOMER_ID, d.PAID_AT,
    d.PRINCIPAL_DUE                                   AS PRINCIPAL_PORTION,
    d.INTEREST_DUE                                    AS INTEREST_PORTION,
    GREATEST(0, DATEDIFF(day, d.DUE_DATE, d.PAID_AT)) AS DAYS_LATE,
    LEAST(ROUND(d.TOTAL_DUE * 0.10 / 1000) * 1000,
          GREATEST(0, DATEDIFF(day, d.DUE_DATE, d.PAID_AT)) * 5000) AS LATE_FEE,
    ch.PAYMENT_CHANNEL,
    CASE WHEN d.PAID_AT::DATE <  d.DUE_DATE THEN 'EARLY'
         WHEN d.PAID_AT::DATE =  d.DUE_DATE THEN 'SCHEDULED'
         ELSE 'LATE' END                              AS PAYMENT_TYPE,
    d.TOTAL_DUE
  FROM due d
  JOIN DEMO_INDODANA.GEN.POOL_CHANNEL ch ON ch.IDX = d.CHANNEL_IDX;

  -- ===================================================================
  -- 7. Resolve installment + loan state as at today
  -- ===================================================================
  CREATE OR REPLACE TEMPORARY TABLE T_INST_FINAL AS
  SELECT
    i.INSTALLMENT_ID, i.LOAN_ID, i.INSTALLMENT_NO, i.DUE_DATE,
    i.PRINCIPAL_DUE, i.INTEREST_DUE, i.FEE_DUE, i.TOTAL_DUE,
    COALESCE(p.PRINCIPAL_PORTION + p.INTEREST_PORTION + i.FEE_DUE + p.LATE_FEE, 0) AS AMOUNT_PAID,
    IFF(p.PAYMENT_ID IS NOT NULL, 'PAID', 'UNPAID') AS INSTALLMENT_STATUS,
    p.PAID_AT,
    IFF(p.PAYMENT_ID IS NULL AND i.DUE_DATE < CURRENT_DATE(),
        DATEDIFF(day, i.DUE_DATE, CURRENT_DATE()), 0) AS DPD
  FROM T_INST i
  LEFT JOIN T_PAY p ON p.INSTALLMENT_ID = i.INSTALLMENT_ID;

  CREATE OR REPLACE TEMPORARY TABLE T_LOAN_FINAL AS
  SELECT
    l.LOAN_ID, l.APPLICATION_ID, l.CUSTOMER_ID, l.MERCHANT_ID, l.PARTNER_ID,
    l.PRODUCT_TYPE, l.PRINCIPAL_AMOUNT, l.INTEREST_RATE_PA, l.ADMIN_FEE,
    l.TENOR_MONTHS, l.DISBURSED_AT, l.FIRST_DUE_DATE,
    DATEADD(month, l.TENOR_MONTHS - 1, l.FIRST_DUE_DATE) AS MATURITY_DATE,
    s.MAX_DPD,
    CASE WHEN s.UNPAID_CNT = 0       THEN 'PAID_OFF'
         WHEN s.MAX_DPD  >  180      THEN 'WRITTEN_OFF'
         WHEN s.MAX_DPD  >    0      THEN 'DELINQUENT'
         ELSE 'ACTIVE' END                                AS LOAN_STATUS,
    s.OUTSTANDING_PRINCIPAL
  FROM T_LOAN_BEH l
  JOIN (
    SELECT LOAN_ID,
           MAX(DPD)                                              AS MAX_DPD,
           COUNT_IF(INSTALLMENT_STATUS = 'UNPAID')               AS UNPAID_CNT,
           SUM(IFF(INSTALLMENT_STATUS = 'UNPAID', PRINCIPAL_DUE, 0)) AS OUTSTANDING_PRINCIPAL
    FROM T_INST_FINAL GROUP BY LOAN_ID
  ) s ON s.LOAN_ID = l.LOAN_ID;

  -- ===================================================================
  -- 8. Collection activities for everything past due
  -- ===================================================================
  CREATE OR REPLACE TEMPORARY TABLE T_COLL AS
  WITH overdue AS (
    SELECT f.LOAN_ID, f.CUSTOMER_ID, f.MAX_DPD,
           MIN(i.DUE_DATE) AS FIRST_OVERDUE_DATE,
           LEAST(8, GREATEST(1, CEIL(f.MAX_DPD / 18.0))) AS N_ACT
    FROM T_LOAN_FINAL f
    JOIN T_INST_FINAL i
      ON i.LOAN_ID = f.LOAN_ID AND i.INSTALLMENT_STATUS = 'UNPAID' AND i.DPD > 0
    WHERE f.MAX_DPD > 0
    GROUP BY f.LOAN_ID, f.CUSTOMER_ID, f.MAX_DPD
  ),
  acts AS (
    SELECT o.LOAN_ID, o.CUSTOMER_ID, n.I AS ACT_NO,
           DATEADD(day, FLOOR(o.MAX_DPD * n.I / (o.N_ACT + 1.0)),
                   o.FIRST_OVERDUE_DATE)                 AS ACT_DATE,
           UNIFORM(0::FLOAT, 1::FLOAT, RANDOM())         AS R_OUT,
           UNIFORM(1, 24, RANDOM())                      AS COLLECTOR_N,
           UNIFORM(1, 20, RANDOM())                      AS PROMISE_LAG
    FROM overdue o
    JOIN DEMO_INDODANA.GEN.NUMBERS n ON n.I <= o.N_ACT
  )
  SELECT
    'ACT' || LPAD(DEMO_INDODANA.GEN.SEQ_ACTIVITY.NEXTVAL, 11, '0') AS ACTIVITY_ID,
    a.LOAN_ID, a.CUSTOMER_ID,
    'COL' || LPAD(a.COLLECTOR_N, 3, '0')                 AS COLLECTOR_ID,
    DATEDIFF(day, o.FIRST_OVERDUE_DATE, a.ACT_DATE)      AS DPD_AT_CONTACT,
    -- escalation ladder: soft digital nudges first, field visit last
    CASE WHEN DATEDIFF(day, o.FIRST_OVERDUE_DATE, a.ACT_DATE) <= 7  THEN 'SMS'
         WHEN DATEDIFF(day, o.FIRST_OVERDUE_DATE, a.ACT_DATE) <= 20 THEN 'WHATSAPP'
         WHEN DATEDIFF(day, o.FIRST_OVERDUE_DATE, a.ACT_DATE) <= 45 THEN 'CALL'
         WHEN DATEDIFF(day, o.FIRST_OVERDUE_DATE, a.ACT_DATE) <= 90 THEN 'CALL'
         ELSE 'FIELD_VISIT' END                          AS CHANNEL,
    CASE WHEN DATEDIFF(day, o.FIRST_OVERDUE_DATE, a.ACT_DATE) <= 7  THEN 'SOFT_REMINDER'
         WHEN DATEDIFF(day, o.FIRST_OVERDUE_DATE, a.ACT_DATE) <= 30 THEN 'FIRST_DEMAND'
         WHEN DATEDIFF(day, o.FIRST_OVERDUE_DATE, a.ACT_DATE) <= 60 THEN 'SECOND_DEMAND'
         WHEN DATEDIFF(day, o.FIRST_OVERDUE_DATE, a.ACT_DATE) <= 90 THEN 'FINAL_NOTICE'
         ELSE 'LEGAL_WARNING' END                        AS ACTION_CODE,
    CASE WHEN a.R_OUT < 0.34 THEN 'NO_ANSWER'
         WHEN a.R_OUT < 0.58 THEN 'PROMISE_TO_PAY'
         WHEN a.R_OUT < 0.70 THEN 'PAID'
         WHEN a.R_OUT < 0.80 THEN 'REFUSED'
         WHEN a.R_OUT < 0.90 THEN 'DISPUTE'
         ELSE 'WRONG_NUMBER' END                         AS OUTCOME,
    a.R_OUT, a.PROMISE_LAG, a.ACT_DATE
  FROM acts a
  JOIN overdue o ON o.LOAN_ID = a.LOAN_ID
  WHERE a.ACT_DATE <= CURRENT_DATE();

  -- ===================================================================
  -- 9. Land everything in Bronze as insert events
  -- ===================================================================
  INSERT INTO DEMO_INDODANA.BRONZE.RAW_CUSTOMERS
    (CUSTOMER_ID, FULL_NAME, NIK_MASKED, EMAIL, PHONE, CITY, PROVINCE, BIRTH_DATE,
     GENDER, OCCUPATION, MONTHLY_INCOME, KYC_STATUS, RISK_SEGMENT, CREDIT_LIMIT,
     ACQUISITION_CHANNEL, REGISTERED_AT, _OP, _COMMIT_TS, _SEQ, _BATCH_ID, _SOURCE)
  SELECT CUSTOMER_ID, FULL_NAME, NIK_MASKED, EMAIL, PHONE, CITY, PROVINCE, BIRTH_DATE,
         GENDER, OCCUPATION, MONTHLY_INCOME, KYC_STATUS, RISK_SEGMENT, CREDIT_LIMIT,
         ACQUISITION_CHANNEL, REGISTERED_AT, 'I', REGISTERED_AT,
         DEMO_INDODANA.BRONZE.SEQ_CDC.NEXTVAL, :v_batch, 'core_lending'
  FROM T_CUST;
  v_c := SQLROWCOUNT;

  INSERT INTO DEMO_INDODANA.BRONZE.RAW_LEADS
    (LEAD_ID, CUSTOMER_ID, LEAD_SOURCE, CAMPAIGN_CODE, PRODUCT_INTEREST, PARTNER_ID,
     LEAD_STATUS, COST_PER_LEAD, CREATED_AT, _OP, _COMMIT_TS, _SEQ, _BATCH_ID, _SOURCE)
  SELECT LEAD_ID, CUSTOMER_ID, LEAD_SOURCE, CAMPAIGN_CODE, PRODUCT_INTEREST, PARTNER_ID,
         LEAD_STATUS, COST_PER_LEAD, CREATED_AT, 'I', CREATED_AT,
         DEMO_INDODANA.BRONZE.SEQ_CDC.NEXTVAL, :v_batch, 'cermati_marketplace'
  FROM T_LEAD;
  v_l := SQLROWCOUNT;

  INSERT INTO DEMO_INDODANA.BRONZE.RAW_LOAN_APPLICATIONS
    (APPLICATION_ID, CUSTOMER_ID, LEAD_ID, PRODUCT_TYPE, REQUESTED_AMOUNT, TENOR_MONTHS,
     BUREAU_SCORE, HAS_BUREAU_FILE, INTERNAL_SCORE, DECISION, DECLINE_REASON,
     APPROVED_AMOUNT, APPROVED_RATE_PA, APPLIED_AT, DECISION_AT,
     _OP, _COMMIT_TS, _SEQ, _BATCH_ID, _SOURCE)
  SELECT APPLICATION_ID, CUSTOMER_ID, LEAD_ID, PRODUCT_TYPE, REQUESTED_AMOUNT, TENOR_MONTHS,
         BUREAU_SCORE, HAS_BUREAU_FILE, INTERNAL_SCORE, DECISION, DECLINE_REASON,
         APPROVED_AMOUNT, APPROVED_RATE_PA, APPLIED_AT, DECISION_AT, 'I',
         COALESCE(DECISION_AT, APPLIED_AT),
         DEMO_INDODANA.BRONZE.SEQ_CDC.NEXTVAL, :v_batch, 'core_lending'
  FROM T_APP;
  v_a := SQLROWCOUNT;

  INSERT INTO DEMO_INDODANA.BRONZE.RAW_LOANS
    (LOAN_ID, APPLICATION_ID, CUSTOMER_ID, MERCHANT_ID, PARTNER_ID, PRODUCT_TYPE,
     PRINCIPAL_AMOUNT, INTEREST_RATE_PA, ADMIN_FEE, TENOR_MONTHS, LOAN_STATUS,
     OUTSTANDING_PRINCIPAL, DISBURSED_AT, FIRST_DUE_DATE, MATURITY_DATE,
     _OP, _COMMIT_TS, _SEQ, _BATCH_ID, _SOURCE)
  SELECT LOAN_ID, APPLICATION_ID, CUSTOMER_ID, MERCHANT_ID, PARTNER_ID, PRODUCT_TYPE,
         PRINCIPAL_AMOUNT, INTEREST_RATE_PA, ADMIN_FEE, TENOR_MONTHS, LOAN_STATUS,
         OUTSTANDING_PRINCIPAL, DISBURSED_AT, FIRST_DUE_DATE, MATURITY_DATE, 'I',
         DISBURSED_AT, DEMO_INDODANA.BRONZE.SEQ_CDC.NEXTVAL, :v_batch, 'core_lending'
  FROM T_LOAN_FINAL;
  v_ln := SQLROWCOUNT;

  INSERT INTO DEMO_INDODANA.BRONZE.RAW_INSTALLMENTS
    (INSTALLMENT_ID, LOAN_ID, INSTALLMENT_NO, DUE_DATE, PRINCIPAL_DUE, INTEREST_DUE,
     FEE_DUE, TOTAL_DUE, AMOUNT_PAID, INSTALLMENT_STATUS, PAID_AT,
     _OP, _COMMIT_TS, _SEQ, _BATCH_ID, _SOURCE)
  SELECT INSTALLMENT_ID, LOAN_ID, INSTALLMENT_NO, DUE_DATE, PRINCIPAL_DUE, INTEREST_DUE,
         FEE_DUE, TOTAL_DUE, AMOUNT_PAID, INSTALLMENT_STATUS, PAID_AT, 'I',
         COALESCE(PAID_AT, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ),
         DEMO_INDODANA.BRONZE.SEQ_CDC.NEXTVAL, :v_batch, 'lms'
  FROM T_INST_FINAL;
  v_i := SQLROWCOUNT;

  INSERT INTO DEMO_INDODANA.BRONZE.RAW_PAYMENTS
    (PAYMENT_ID, LOAN_ID, INSTALLMENT_ID, CUSTOMER_ID, PAID_AT, AMOUNT,
     PRINCIPAL_PORTION, INTEREST_PORTION, LATE_FEE, PAYMENT_CHANNEL, PAYMENT_TYPE,
     DAYS_LATE, _OP, _COMMIT_TS, _SEQ, _BATCH_ID, _SOURCE)
  SELECT PAYMENT_ID, LOAN_ID, INSTALLMENT_ID, CUSTOMER_ID, PAID_AT,
         TOTAL_DUE + LATE_FEE, PRINCIPAL_PORTION, INTEREST_PORTION, LATE_FEE,
         PAYMENT_CHANNEL, PAYMENT_TYPE, DAYS_LATE, 'I', PAID_AT,
         DEMO_INDODANA.BRONZE.SEQ_CDC.NEXTVAL, :v_batch, 'lms'
  FROM T_PAY;
  v_p := SQLROWCOUNT;

  INSERT INTO DEMO_INDODANA.BRONZE.RAW_COLLECTION_ACTIVITIES
    (ACTIVITY_ID, LOAN_ID, CUSTOMER_ID, COLLECTOR_ID, COLLECTION_BUCKET, CHANNEL,
     ACTION_CODE, OUTCOME, DPD_AT_CONTACT, PROMISE_AMOUNT, PROMISE_DATE, ACTIVITY_AT,
     _OP, _COMMIT_TS, _SEQ, _BATCH_ID, _SOURCE)
  SELECT c.ACTIVITY_ID, c.LOAN_ID, c.CUSTOMER_ID, c.COLLECTOR_ID,
         CASE WHEN c.DPD_AT_CONTACT <= 0  THEN 'X'
              WHEN c.DPD_AT_CONTACT <= 30 THEN '1-30'
              WHEN c.DPD_AT_CONTACT <= 60 THEN '31-60'
              WHEN c.DPD_AT_CONTACT <= 90 THEN '61-90'
              ELSE '90+' END,
         c.CHANNEL, c.ACTION_CODE, c.OUTCOME, c.DPD_AT_CONTACT,
         IFF(c.OUTCOME = 'PROMISE_TO_PAY', f.OUTSTANDING_PRINCIPAL, NULL),
         IFF(c.OUTCOME = 'PROMISE_TO_PAY', DATEADD(day, c.PROMISE_LAG, c.ACT_DATE), NULL),
         DATEADD(second, UNIFORM(28800, 64800, RANDOM()), c.ACT_DATE::TIMESTAMP_NTZ),
         'I',
         DATEADD(second, UNIFORM(28800, 64800, RANDOM()), c.ACT_DATE::TIMESTAMP_NTZ),
         DEMO_INDODANA.BRONZE.SEQ_CDC.NEXTVAL, :v_batch, 'collection_crm'
  FROM T_COLL c
  JOIN T_LOAN_FINAL f ON f.LOAN_ID = c.LOAN_ID;
  v_x := SQLROWCOUNT;

  v_rows := :v_c + :v_l + :v_a + :v_ln + :v_i + :v_p + :v_x;

  UPDATE DEMO_INDODANA.BRONZE.BATCH_LOG
     SET FINISHED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
         ROWS_LANDED = :v_rows,
         DETAIL = DETAIL || ' | customers=' || :v_c || ' leads=' || :v_l
                  || ' applications=' || :v_a || ' loans=' || :v_ln
                  || ' installments=' || :v_i || ' payments=' || :v_p
                  || ' collections=' || :v_x
   WHERE BATCH_ID = :v_batch;

  RETURN :v_batch || ' | rows=' || :v_rows
      || ' (cust ' || :v_c || ', lead ' || :v_l || ', app ' || :v_a
      || ', loan ' || :v_ln || ', inst ' || :v_i || ', pay ' || :v_p
      || ', coll ' || :v_x || ')';
END;
$$;
