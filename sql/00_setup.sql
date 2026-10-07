-- =====================================================================
-- INDODANA / CERMATI  —  Lending Lifecycle Lakehouse
-- 00_setup.sql : database, schemas, Bronze landing (append-only CDC)
-- =====================================================================
-- Bronze is a raw CDC landing zone. Every row is an immutable change
-- event emitted by a source system (core lending DB, marketplace, LMS).
-- Nothing is ever updated or deleted here — downstream dbt models
-- resolve the current state and the SCD2 history.
--
--   _OP         'I' insert | 'U' update | 'D' soft delete
--   _COMMIT_TS  when the change was committed in the SOURCE system
--   _SEQ        monotonic sequence, tie-breaker within the same _COMMIT_TS
--   _BATCH_ID   ingest batch that carried the row
--   _SOURCE     originating source system
-- =====================================================================

CREATE DATABASE IF NOT EXISTS DEMO_INDODANA
  COMMENT = 'Indodana/Cermati lending lifecycle demo — bronze/silver/gold via dbt on Snowflake';

USE DATABASE DEMO_INDODANA;

CREATE SCHEMA IF NOT EXISTS BRONZE COMMENT = 'Raw append-only CDC landing zone';
CREATE SCHEMA IF NOT EXISTS SILVER COMMENT = 'Cleaned, deduplicated, conformed (dbt)';
CREATE SCHEMA IF NOT EXISTS GOLD   COMMENT = 'Dimensional model + business marts (dbt)';
CREATE SCHEMA IF NOT EXISTS DBT    COMMENT = 'Deployed dbt project object + artifacts';

USE SCHEMA BRONZE;

-- ---------------------------------------------------------------------
-- Ingest batch control
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS BATCH_LOG (
    BATCH_ID        VARCHAR(40)   NOT NULL,
    BATCH_TYPE      VARCHAR(20)   NOT NULL,   -- SEED | INSERT | UPDATE | DELETE
    STARTED_AT      TIMESTAMP_NTZ NOT NULL,
    FINISHED_AT     TIMESTAMP_NTZ,
    ROWS_LANDED     NUMBER(38,0),
    DETAIL          VARCHAR(2000)
);

CREATE SEQUENCE IF NOT EXISTS SEQ_CDC START = 1 INCREMENT = 1;

-- ---------------------------------------------------------------------
-- 1. Customers  (source: core_lending.customers)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_CUSTOMERS (
    CUSTOMER_ID       VARCHAR(20),
    FULL_NAME         VARCHAR(120),
    NIK_MASKED        VARCHAR(20),
    EMAIL             VARCHAR(160),
    PHONE             VARCHAR(30),
    CITY              VARCHAR(60),
    PROVINCE          VARCHAR(60),
    BIRTH_DATE        DATE,
    GENDER            VARCHAR(10),
    OCCUPATION        VARCHAR(60),
    MONTHLY_INCOME    NUMBER(18,2),
    KYC_STATUS        VARCHAR(20),   -- PENDING | VERIFIED | REJECTED
    RISK_SEGMENT      VARCHAR(10),   -- A | B | C | D | E
    CREDIT_LIMIT      NUMBER(18,2),
    ACQUISITION_CHANNEL VARCHAR(40),
    REGISTERED_AT     TIMESTAMP_NTZ,
    _OP               VARCHAR(1),
    _COMMIT_TS        TIMESTAMP_NTZ,
    _SEQ              NUMBER(38,0),
    _BATCH_ID         VARCHAR(40),
    _SOURCE           VARCHAR(40),
    _INGESTED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
);

-- ---------------------------------------------------------------------
-- 2. Leads  (source: cermati_marketplace.leads)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_LEADS (
    LEAD_ID           VARCHAR(20),
    CUSTOMER_ID       VARCHAR(20),
    LEAD_SOURCE       VARCHAR(40),   -- ORGANIC_SEO | PAID_SEARCH | SOCIAL | AFFILIATE | EMAIL | DIRECT
    CAMPAIGN_CODE     VARCHAR(40),
    PRODUCT_INTEREST  VARCHAR(30),   -- PAYLATER | KTA | CREDIT_CARD | MULTIGUNA
    PARTNER_ID        VARCHAR(20),
    LEAD_STATUS       VARCHAR(20),   -- NEW | CONTACTED | QUALIFIED | APPLIED | DROPPED
    COST_PER_LEAD     NUMBER(18,2),
    CREATED_AT        TIMESTAMP_NTZ,
    _OP               VARCHAR(1),
    _COMMIT_TS        TIMESTAMP_NTZ,
    _SEQ              NUMBER(38,0),
    _BATCH_ID         VARCHAR(40),
    _SOURCE           VARCHAR(40),
    _INGESTED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
);

-- ---------------------------------------------------------------------
-- 3. Loan applications  (source: core_lending.loan_applications)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_LOAN_APPLICATIONS (
    APPLICATION_ID    VARCHAR(20),
    CUSTOMER_ID       VARCHAR(20),
    LEAD_ID           VARCHAR(20),
    PRODUCT_TYPE      VARCHAR(30),
    REQUESTED_AMOUNT  NUMBER(18,2),
    TENOR_MONTHS      NUMBER(5,0),
    BUREAU_SCORE      NUMBER(5,0),   -- 300..850, NULL = thin file
    HAS_BUREAU_FILE   BOOLEAN,
    INTERNAL_SCORE    NUMBER(5,0),
    DECISION          VARCHAR(20),   -- APPROVED | DECLINED | PENDING | CANCELLED
    DECLINE_REASON    VARCHAR(60),
    APPROVED_AMOUNT   NUMBER(18,2),
    APPROVED_RATE_PA  NUMBER(8,4),
    APPLIED_AT        TIMESTAMP_NTZ,
    DECISION_AT       TIMESTAMP_NTZ,
    _OP               VARCHAR(1),
    _COMMIT_TS        TIMESTAMP_NTZ,
    _SEQ              NUMBER(38,0),
    _BATCH_ID         VARCHAR(40),
    _SOURCE           VARCHAR(40),
    _INGESTED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
);

-- ---------------------------------------------------------------------
-- 4. Loans  (source: core_lending.loans)  — status mutates over time
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_LOANS (
    LOAN_ID               VARCHAR(20),
    APPLICATION_ID        VARCHAR(20),
    CUSTOMER_ID           VARCHAR(20),
    MERCHANT_ID           VARCHAR(20),
    PARTNER_ID            VARCHAR(20),
    PRODUCT_TYPE          VARCHAR(30),
    PRINCIPAL_AMOUNT      NUMBER(18,2),
    INTEREST_RATE_PA      NUMBER(8,4),
    ADMIN_FEE             NUMBER(18,2),
    TENOR_MONTHS          NUMBER(5,0),
    LOAN_STATUS           VARCHAR(20),  -- ACTIVE | PAID_OFF | DELINQUENT | WRITTEN_OFF | RESTRUCTURED
    OUTSTANDING_PRINCIPAL NUMBER(18,2),
    DISBURSED_AT          TIMESTAMP_NTZ,
    FIRST_DUE_DATE        DATE,
    MATURITY_DATE         DATE,
    _OP                   VARCHAR(1),
    _COMMIT_TS            TIMESTAMP_NTZ,
    _SEQ                  NUMBER(38,0),
    _BATCH_ID             VARCHAR(40),
    _SOURCE               VARCHAR(40),
    _INGESTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
);

-- ---------------------------------------------------------------------
-- 5. Installment schedule  (source: lms.installments)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_INSTALLMENTS (
    INSTALLMENT_ID    VARCHAR(30),
    LOAN_ID           VARCHAR(20),
    INSTALLMENT_NO    NUMBER(5,0),
    DUE_DATE          DATE,
    PRINCIPAL_DUE     NUMBER(18,2),
    INTEREST_DUE      NUMBER(18,2),
    FEE_DUE           NUMBER(18,2),
    TOTAL_DUE         NUMBER(18,2),
    AMOUNT_PAID       NUMBER(18,2),
    INSTALLMENT_STATUS VARCHAR(20),  -- UNPAID | PARTIAL | PAID | WAIVED
    PAID_AT           TIMESTAMP_NTZ,
    _OP               VARCHAR(1),
    _COMMIT_TS        TIMESTAMP_NTZ,
    _SEQ              NUMBER(38,0),
    _BATCH_ID         VARCHAR(40),
    _SOURCE           VARCHAR(40),
    _INGESTED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
);

-- ---------------------------------------------------------------------
-- 6. Payments  (source: lms.payments)  — pure append-only fact
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_PAYMENTS (
    PAYMENT_ID        VARCHAR(30),
    LOAN_ID           VARCHAR(20),
    INSTALLMENT_ID    VARCHAR(30),
    CUSTOMER_ID       VARCHAR(20),
    PAID_AT           TIMESTAMP_NTZ,
    AMOUNT            NUMBER(18,2),
    PRINCIPAL_PORTION NUMBER(18,2),
    INTEREST_PORTION  NUMBER(18,2),
    LATE_FEE          NUMBER(18,2),
    PAYMENT_CHANNEL   VARCHAR(40),  -- VA_BCA | VA_MANDIRI | GOPAY | OVO | ALFAMART | INDOMARET | AUTODEBIT
    PAYMENT_TYPE      VARCHAR(20),  -- SCHEDULED | EARLY | LATE | SETTLEMENT
    DAYS_LATE         NUMBER(10,0),
    _OP               VARCHAR(1),
    _COMMIT_TS        TIMESTAMP_NTZ,
    _SEQ              NUMBER(38,0),
    _BATCH_ID         VARCHAR(40),
    _SOURCE           VARCHAR(40),
    _INGESTED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
);

-- ---------------------------------------------------------------------
-- 7. Collection activities  (source: collection_crm.activities)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_COLLECTION_ACTIVITIES (
    ACTIVITY_ID       VARCHAR(30),
    LOAN_ID           VARCHAR(20),
    CUSTOMER_ID       VARCHAR(20),
    COLLECTOR_ID      VARCHAR(20),
    COLLECTION_BUCKET VARCHAR(20),  -- X | 1-30 | 31-60 | 61-90 | 90+
    CHANNEL           VARCHAR(20),  -- SMS | WHATSAPP | CALL | EMAIL | FIELD_VISIT
    ACTION_CODE       VARCHAR(30),
    OUTCOME           VARCHAR(30),  -- NO_ANSWER | PROMISE_TO_PAY | DISPUTE | PAID | REFUSED | WRONG_NUMBER
    DPD_AT_CONTACT    NUMBER(10,0),
    PROMISE_AMOUNT    NUMBER(18,2),
    PROMISE_DATE      DATE,
    ACTIVITY_AT       TIMESTAMP_NTZ,
    _OP               VARCHAR(1),
    _COMMIT_TS        TIMESTAMP_NTZ,
    _SEQ              NUMBER(38,0),
    _BATCH_ID         VARCHAR(40),
    _SOURCE           VARCHAR(40),
    _INGESTED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
);

-- ---------------------------------------------------------------------
-- 8. Partners  (source: cermati_marketplace.partners)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_PARTNERS (
    PARTNER_ID        VARCHAR(20),
    PARTNER_NAME      VARCHAR(80),
    PARTNER_TYPE      VARCHAR(30),  -- BANK | MULTIFINANCE | FINTECH | INSURANCE
    COMMISSION_RATE   NUMBER(8,4),
    PAYOUT_TERMS_DAYS NUMBER(5,0),
    IS_ACTIVE         BOOLEAN,
    _OP               VARCHAR(1),
    _COMMIT_TS        TIMESTAMP_NTZ,
    _SEQ              NUMBER(38,0),
    _BATCH_ID         VARCHAR(40),
    _SOURCE           VARCHAR(40),
    _INGESTED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
);

-- ---------------------------------------------------------------------
-- 9. Merchants  (source: core_lending.merchants) — BNPL acceptance
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_MERCHANTS (
    MERCHANT_ID       VARCHAR(20),
    MERCHANT_NAME     VARCHAR(80),
    MERCHANT_CATEGORY VARCHAR(40),
    MDR_RATE          NUMBER(8,4),
    CITY              VARCHAR(60),
    IS_ACTIVE         BOOLEAN,
    _OP               VARCHAR(1),
    _COMMIT_TS        TIMESTAMP_NTZ,
    _SEQ              NUMBER(38,0),
    _BATCH_ID         VARCHAR(40),
    _SOURCE           VARCHAR(40),
    _INGESTED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
);
