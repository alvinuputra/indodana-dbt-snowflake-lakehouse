-- =====================================================================
-- 01_generator.sql : synthetic source-system simulator
-- =====================================================================
-- The GEN schema holds reference pools and the stored procedures that
-- emit CDC events into BRONZE. It is NOT a dbt source — it stands in
-- for the operational systems (core lending DB, Cermati marketplace,
-- LMS, collection CRM) that would normally feed the landing zone.
-- =====================================================================

USE DATABASE DEMO_INDODANA;
CREATE SCHEMA IF NOT EXISTS GEN COMMENT = 'Synthetic source-system simulator (not a dbt source)';
USE SCHEMA GEN;

-- Bounded number spine; GENERATOR(ROWCOUNT) cannot take a bind variable,
-- so procedures select from this view and filter with <= :n instead.
CREATE OR REPLACE VIEW NUMBERS AS
SELECT ROW_NUMBER() OVER (ORDER BY SEQ8()) AS I
FROM TABLE(GENERATOR(ROWCOUNT => 50000));

CREATE SEQUENCE IF NOT EXISTS SEQ_CUSTOMER START = 100001 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_LEAD     START = 500001 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_APP      START = 700001 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_LOAN     START = 900001 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_PAYMENT  START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_ACTIVITY START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_BATCH    START = 1 INCREMENT = 1;

-- ---------------------------------------------------------------------
-- Reference pools
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE REF_CITY (CITY VARCHAR, PROVINCE VARCHAR, WEIGHT NUMBER);
INSERT INTO REF_CITY VALUES
  ('Jakarta Selatan','DKI Jakarta',14),('Jakarta Timur','DKI Jakarta',10),
  ('Jakarta Barat','DKI Jakarta',9),  ('Jakarta Pusat','DKI Jakarta',6),
  ('Jakarta Utara','DKI Jakarta',7),  ('Bekasi','Jawa Barat',9),
  ('Depok','Jawa Barat',7),           ('Tangerang','Banten',8),
  ('Bogor','Jawa Barat',6),           ('Bandung','Jawa Barat',9),
  ('Surabaya','Jawa Timur',8),        ('Semarang','Jawa Tengah',5),
  ('Medan','Sumatera Utara',5),       ('Makassar','Sulawesi Selatan',4),
  ('Palembang','Sumatera Selatan',3), ('Denpasar','Bali',3),
  ('Yogyakarta','DI Yogyakarta',3),   ('Malang','Jawa Timur',3),
  ('Balikpapan','Kalimantan Timur',2),('Pekanbaru','Riau',2);

CREATE OR REPLACE TABLE REF_OCCUPATION (OCCUPATION VARCHAR, INCOME_MULT NUMBER(6,3));
INSERT INTO REF_OCCUPATION VALUES
  ('Karyawan Swasta',1.00),('PNS / ASN',0.95),('Guru / Dosen',0.85),
  ('Wiraswasta',1.15),('Freelancer',0.80),('Profesional',1.60),
  ('TNI / Polri',1.00),('Karyawan BUMN',1.25),('Ojek Online',0.55),
  ('Pedagang',0.75),('Buruh Pabrik',0.60),('Perawat / Medis',1.05);

CREATE OR REPLACE TABLE REF_NAME_FIRST (NAME VARCHAR, GENDER VARCHAR);
INSERT INTO REF_NAME_FIRST VALUES
  ('Budi','M'),('Agus','M'),('Dedi','M'),('Rizki','M'),('Fajar','M'),
  ('Hendra','M'),('Iwan','M'),('Joko','M'),('Bambang','M'),('Yusuf','M'),
  ('Andi','M'),('Reza','M'),('Dimas','M'),('Taufik','M'),('Arif','M'),
  ('Siti','F'),('Dewi','F'),('Ratna','F'),('Ayu','F'),('Rina','F'),
  ('Lestari','F'),('Fitri','F'),('Indah','F'),('Maya','F'),('Nurul','F'),
  ('Wulan','F'),('Sari','F'),('Putri','F'),('Anisa','F'),('Yuni','F');

CREATE OR REPLACE TABLE REF_NAME_LAST (NAME VARCHAR);
INSERT INTO REF_NAME_LAST VALUES
  ('Santoso'),('Wijaya'),('Pratama'),('Nugroho'),('Hidayat'),('Saputra'),
  ('Kusuma'),('Hartono'),('Setiawan'),('Maulana'),('Permana'),('Gunawan'),
  ('Siregar'),('Nainggolan'),('Tanjung'),('Simanjuntak'),('Sihombing'),
  ('Rahmawati'),('Anggraini'),('Puspitasari'),('Handayani'),('Safitri'),
  ('Lubis'),('Harahap'),('Situmorang'),('Nasution'),('Siahaan'),('Purba');

CREATE OR REPLACE TABLE REF_DECLINE_REASON (REASON VARCHAR, WEIGHT NUMBER);
INSERT INTO REF_DECLINE_REASON VALUES
  ('LOW_BUREAU_SCORE',22),('INSUFFICIENT_INCOME',18),('HIGH_DBR',14),
  ('THIN_FILE',11),('ACTIVE_DELINQUENCY',10),('KYC_MISMATCH',8),
  ('FRAUD_SUSPECT',6),('BLACKLIST_MATCH',5),('AGE_OUT_OF_POLICY',4),
  ('DUPLICATE_APPLICATION',2);

-- ---------------------------------------------------------------------
-- Static dimensions land in Bronze once (slow-moving source tables)
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_LOAD_REFERENCE()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  v_batch VARCHAR;
  v_cnt   NUMBER;
BEGIN
  SELECT COUNT(*) INTO :v_cnt FROM DEMO_INDODANA.BRONZE.RAW_PARTNERS;
  IF (:v_cnt > 0) THEN
    RETURN 'Reference data already loaded — skipped.';
  END IF;

  v_batch := 'B-REF-' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS');

  INSERT INTO DEMO_INDODANA.BRONZE.RAW_PARTNERS
    (PARTNER_ID, PARTNER_NAME, PARTNER_TYPE, COMMISSION_RATE,
     PAYOUT_TERMS_DAYS, IS_ACTIVE, _OP, _COMMIT_TS, _SEQ, _BATCH_ID, _SOURCE)
  SELECT p.*, 'I', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
         DEMO_INDODANA.BRONZE.SEQ_CDC.NEXTVAL, :v_batch, 'cermati_marketplace'
  FROM (
    SELECT 'PTR001' AS a,'Bank BCA' AS b,'BANK' AS c,0.0225 AS d,30 AS e,TRUE AS f
    UNION ALL SELECT 'PTR002','Bank Mandiri','BANK',0.0210,30,TRUE
    UNION ALL SELECT 'PTR003','Bank BNI','BANK',0.0200,45,TRUE
    UNION ALL SELECT 'PTR004','Bank CIMB Niaga','BANK',0.0260,30,TRUE
    UNION ALL SELECT 'PTR005','Bank Danamon','BANK',0.0240,45,TRUE
    UNION ALL SELECT 'PTR006','Bank Permata','BANK',0.0235,30,TRUE
    UNION ALL SELECT 'PTR007','BRI Finance','MULTIFINANCE',0.0180,60,TRUE
    UNION ALL SELECT 'PTR008','Adira Finance','MULTIFINANCE',0.0195,60,TRUE
    UNION ALL SELECT 'PTR009','Indodana Direct','FINTECH',0.0000,0,TRUE
    UNION ALL SELECT 'PTR010','Sinarmas MSIG','INSURANCE',0.0400,30,FALSE
  ) p;

  INSERT INTO DEMO_INDODANA.BRONZE.RAW_MERCHANTS
    (MERCHANT_ID, MERCHANT_NAME, MERCHANT_CATEGORY, MDR_RATE, CITY,
     IS_ACTIVE, _OP, _COMMIT_TS, _SEQ, _BATCH_ID, _SOURCE)
  SELECT m.*, 'I', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
         DEMO_INDODANA.BRONZE.SEQ_CDC.NEXTVAL, :v_batch, 'core_lending'
  FROM (
    SELECT 'MCH001' AS a,'Erafone' AS b,'ELECTRONICS' AS c,0.0280 AS d,'Jakarta Selatan' AS e,TRUE AS f
    UNION ALL SELECT 'MCH002','Blibli','MARKETPLACE',0.0250,'Jakarta Barat',TRUE
    UNION ALL SELECT 'MCH003','Tokopedia','MARKETPLACE',0.0240,'Jakarta Selatan',TRUE
    UNION ALL SELECT 'MCH004','Informa','FURNITURE',0.0300,'Tangerang',TRUE
    UNION ALL SELECT 'MCH005','Ace Hardware','HOME_IMPROVEMENT',0.0290,'Bekasi',TRUE
    UNION ALL SELECT 'MCH006','Electronic City','ELECTRONICS',0.0285,'Surabaya',TRUE
    UNION ALL SELECT 'MCH007','Optik Melawai','HEALTH',0.0320,'Bandung',TRUE
    UNION ALL SELECT 'MCH008','Traveloka','TRAVEL',0.0260,'Jakarta Selatan',TRUE
    UNION ALL SELECT 'MCH009','RS Premier Jatinegara','HEALTHCARE',0.0220,'Jakarta Timur',TRUE
    UNION ALL SELECT 'MCH010','Hartono Elektronika','ELECTRONICS',0.0275,'Semarang',TRUE
    UNION ALL SELECT 'MCH011','Mitra10','HOME_IMPROVEMENT',0.0295,'Depok',TRUE
    UNION ALL SELECT 'MCH012','Gramedia','RETAIL',0.0310,'Yogyakarta',TRUE
    UNION ALL SELECT 'MCH013','Planet Sports','SPORTS',0.0330,'Medan',TRUE
    UNION ALL SELECT 'MCH014','Dealer Yamaha Mekar','AUTOMOTIVE',0.0190,'Makassar',TRUE
    UNION ALL SELECT 'MCH015','Klinik Gigi Dentia','HEALTHCARE',0.0350,'Denpasar',FALSE
  ) m;

  RETURN 'Loaded 10 partners and 15 merchants into Bronze.';
END;
$$;
