# Base de Données OLAP (Snowflake) - Stripe Payment Platform
## Documentation Technique du Star Schema

---

## 📋 Table des matières

1. [Vue d'ensemble](#1-vue-densemble)
2. [Dimensions](#2-dimensions)
3. [Tables de faits](#3-tables-de-faits)
4. [Tables de référence](#4-tables-de-référence)
5. [Aggregates analytiques](#5-aggregates-analytiques)
6. [Logs de conformité](#6-logs-de-conformité)
7. [Scripts DDL complets](#7-scripts-ddl-complets)
8. [Vues matérialisées](#8-vues-matérialisées)

---

## 1. Vue d'ensemble

### 1.1 Caractéristiques

| Propriété | Valeur |
|-----------|--------|
| **SGBD** | Snowflake (Cloud DWH) |
| **Modèle** | Star Schema (dénormalisé) |
| **Volume** | 1TB+ données historiques |
| **Refresh** | ETL quotidien + streaming |
| **Retention** | 7 ans (compliance) |
| **Queries/jour** | 50,000+ |
| **Utilisateurs** | 200+ analysts |

### 1.2 Architecture Star Schema

```
┌─────────────────────────────────────────────────────────────┐
│                    STAR SCHEMA ARCHITECTURE                  │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│                    ┌──────────────┐                          │
│               ┌───→│dim_customer  │                          │
│               │    └──────────────┘                          │
│               │                                               │
│               │    ┌──────────────┐                          │
│               ├───→│dim_merchant  │                          │
│               │    └──────────────┘                          │
│               │                                               │
│               │    ┌──────────────┐                          │
│   ┌──────────┴─┐  │dim_product   │◄──┐                      │
│   │    FACT    │◄─┤              │   │                      │
│   │TRANSACTIONS│  └──────────────┘   │                      │
│   └──────────┬─┘                     │                      │
│              │    ┌──────────────┐   │                      │
│              ├───→│dim_payment   │   │                      │
│              │    │_method       │   │                      │
│              │    └──────────────┘   │                      │
│              │                       │                      │
│              │    ┌──────────────┐   │                      │
│              ├───→│ dim_date     │   │                      │
│              │    └──────────────┘   │                      │
│              │                       │                      │
│              │    ┌──────────────┐   │                      │
│              ├───→│  dim_time    │   │                      │
│              │    └──────────────┘   │                      │
│              │                       │                      │
│              │    ┌──────────────┐   │                      │
│              └───→│ dim_location │   │                      │
│                   └──────────────┘   │                      │
│                                      │                      │
│                           AGGREGATES ┘                      │
│                           (Pre-calculated)                  │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

**Avantages Star Schema :**
- ✅ Requêtes simples et rapides (peu de JOIN s)
- ✅ Dénormalisé pour performance lecture
- ✅ Dimensions réutilisables
- ✅ Optimisé pour BI tools

---

## 2. Dimensions

### 2.1 Dimension `dim.customer`

**Description :** Dimension client avec SCD Type 2 (historisation).

```sql
CREATE OR REPLACE TABLE dim.customer (
    -- Surrogate key (auto-increment)
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Business key (de l'OLTP)
    customer_id VARCHAR(36) NOT NULL,
    
    -- Attributs client
    name VARCHAR(255),
    first_name VARCHAR(255),
    full_address TEXT,
    post_code VARCHAR(20),
    phone VARCHAR(50),
    email VARCHAR(255),
    
    -- Timestamps
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    
    -- SCD Type 2 (Slowly Changing Dimension)
    is_current BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('9999-12-31 23:59:59'),
    
    -- Audit
    etl_inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    etl_updated_at TIMESTAMP_NTZ
);

-- Indexes
CREATE INDEX idx_customer_id ON dim.customer(customer_id);
CREATE INDEX idx_customer_current ON dim.customer(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_customer_valid_dates ON dim.customer(valid_from, valid_to);

-- Unique constraint sur business key + current
CREATE UNIQUE INDEX idx_customer_unique_current 
    ON dim.customer(customer_id) 
    WHERE is_current = TRUE;

COMMENT ON TABLE dim.customer IS 'Customer dimension with SCD Type 2 for historical tracking';
COMMENT ON COLUMN dim.customer.customer_key IS 'Surrogate key (auto-increment)';
COMMENT ON COLUMN dim.customer.customer_id IS 'Business key from OLTP source';
COMMENT ON COLUMN dim.customer.is_current IS 'TRUE for current version, FALSE for historical';
```

**SCD Type 2 Example :**
```
customer_key | customer_id | email            | is_current | valid_from | valid_to
-------------|-------------|------------------|------------|------------|----------
1            | cus_123     | old@email.com    | FALSE      | 2025-01-01 | 2026-01-15
2            | cus_123     | new@email.com    | TRUE       | 2026-01-15 | 9999-12-31
```

### 2.2 Dimension `dim.merchant`

```sql
CREATE OR REPLACE TABLE dim.merchant (
    merchant_key INT IDENTITY(1,1) PRIMARY KEY,
    merchant_id VARCHAR(36) NOT NULL,
    legal_name VARCHAR(500),
    full_address TEXT,
    post_code VARCHAR(20),
    phone VARCHAR(50),
    email VARCHAR(255),
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    
    -- SCD Type 2
    is_current BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('9999-12-31'),
    
    etl_inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE INDEX idx_merchant_id ON dim.merchant(merchant_id);
CREATE UNIQUE INDEX idx_merchant_unique_current 
    ON dim.merchant(merchant_id) WHERE is_current = TRUE;
```

### 2.3 Dimension `dim.product`

```sql
CREATE OR REPLACE TABLE dim.product (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(36) NOT NULL,
    name VARCHAR(500),
    description TEXT,
    price DECIMAL(12,2),
    category VARCHAR(100),
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    
    -- SCD Type 2
    is_current BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('9999-12-31'),
    
    etl_inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE INDEX idx_product_id ON dim.product(product_id);
CREATE INDEX idx_product_category ON dim.product(category);
```

### 2.4 Dimension `dim.payment_method`

```sql
CREATE OR REPLACE TABLE dim.payment_method (
    payment_method_key INT IDENTITY(1,1) PRIMARY KEY,
    payment_method_id VARCHAR(36) NOT NULL,
    method_type VARCHAR(50), -- 'card', 'bank_transfer', 'wallet'
    card_brand VARCHAR(50),  -- 'visa', 'mastercard', 'amex'
    is_subscription BOOLEAN,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    
    -- SCD Type 2
    is_current BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('9999-12-31'),
    
    etl_inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE INDEX idx_payment_id ON dim.payment_method(payment_method_id);
CREATE INDEX idx_payment_type ON dim.payment_method(method_type);
```

### 2.5 Dimension `dim.date` (Date Dimension Table)

```sql
CREATE OR REPLACE TABLE dim.date (
    date_key INT PRIMARY KEY,  -- Format: YYYYMMDD (ex: 20260122)
    full_date DATE NOT NULL,
    
    -- Year components
    year INT,
    quarter INT,           -- 1-4
    month INT,             -- 1-12
    month_name VARCHAR(20), -- 'January', 'February'...
    week INT,              -- 1-53
    
    -- Day components
    day_of_month INT,      -- 1-31
    day_of_week INT,       -- 1-7 (1=Monday)
    day_name VARCHAR(20),  -- 'Monday', 'Tuesday'...
    
    -- Flags
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name VARCHAR(100),
    
    -- Fiscal (if different from calendar)
    fiscal_year INT,
    fiscal_quarter INT,
    fiscal_month INT
);

-- Generate date dimension (2020-2030)
INSERT INTO dim.date
WITH RECURSIVE date_range AS (
    SELECT TO_DATE('2020-01-01') AS dt
    UNION ALL
    SELECT DATEADD(day, 1, dt)
    FROM date_range
    WHERE dt < TO_DATE('2030-12-31')
)
SELECT
    TO_NUMBER(TO_CHAR(dt, 'YYYYMMDD')) AS date_key,
    dt AS full_date,
    YEAR(dt) AS year,
    QUARTER(dt) AS quarter,
    MONTH(dt) AS month,
    MONTHNAME(dt) AS month_name,
    WEEKOFYEAR(dt) AS week,
    DAYOFMONTH(dt) AS day_of_month,
    DAYOFWEEK(dt) AS day_of_week,
    DAYNAME(dt) AS day_name,
    CASE WHEN DAYOFWEEK(dt) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday,
    NULL AS holiday_name,
    YEAR(dt) AS fiscal_year,
    QUARTER(dt) AS fiscal_quarter,
    MONTH(dt) AS fiscal_month
FROM date_range;
```

### 2.6 Dimension `dim.time` (Time Dimension Table)

```sql
CREATE OR REPLACE TABLE dim.time (
    time_key INT PRIMARY KEY,  -- Format: HHMMSS (ex: 143025 = 14:30:25)
    hour INT,                  -- 0-23
    minute INT,                -- 0-59
    second INT,                -- 0-59
    time_of_day VARCHAR(20)    -- 'Morning', 'Afternoon', 'Evening', 'Night'
);

-- Generate time dimension (every second of the day)
INSERT INTO dim.time
WITH RECURSIVE time_range AS (
    SELECT 0 AS sec
    UNION ALL
    SELECT sec + 1
    FROM time_range
    WHERE sec < 86399  -- 24*60*60 - 1
)
SELECT
    TO_NUMBER(TO_CHAR(TO_TIME(sec), 'HH24MISS')) AS time_key,
    FLOOR(sec / 3600) AS hour,
    FLOOR((sec % 3600) / 60) AS minute,
    sec % 60 AS second,
    CASE
        WHEN FLOOR(sec / 3600) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN FLOOR(sec / 3600) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN FLOOR(sec / 3600) BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day
FROM time_range;
```

### 2.7 Dimension `dim.location`

```sql
CREATE OR REPLACE TABLE dim.location (
    location_key INT IDENTITY(1,1) PRIMARY KEY,
    location VARCHAR(255),      -- Original location string
    city VARCHAR(100),
    region VARCHAR(100),
    country VARCHAR(100),
    country_code VARCHAR(2),    -- ISO 3166-1 alpha-2
    
    -- Geocoding
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),
    
    etl_inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE INDEX idx_location_country ON dim.location(country);
CREATE INDEX idx_location_city ON dim.location(city);
```

---

## 3. Tables de faits

### 3.1 Fact `fact.transactions`

**Description :** Table de faits principale (grain = 1 transaction).

```sql
CREATE OR REPLACE TABLE fact.transactions (
    transaction_key INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Business key
    transaction_id VARCHAR(36) NOT NULL,
    
    -- Foreign keys vers dimensions (surrogate keys)
    customer_key INT NOT NULL,
    merchant_key INT NOT NULL,
    product_key INT,
    payment_method_key INT,
    date_key INT NOT NULL,
    time_key INT NOT NULL,
    location_key INT,
    
    -- Mesures (métriques)
    amount DECIMAL(12,2) NOT NULL,
    currency VARCHAR(3),
    
    -- Attributs dégénérés (degenerate dimensions)
    status VARCHAR(50),
    transaction_type VARCHAR(50),
    device_type VARCHAR(50),
    
    -- Flags (mesures binaires)
    is_successful BOOLEAN,
    is_refund BOOLEAN,
    
    -- Timestamps
    created_at TIMESTAMP_NTZ,
    
    -- Audit ETL
    etl_inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Foreign key constraints (optional in Snowflake for performance)
    -- CONSTRAINT fk_customer FOREIGN KEY (customer_key) REFERENCES dim.customer(customer_key),
    -- CONSTRAINT fk_merchant FOREIGN KEY (merchant_key) REFERENCES dim.merchant(merchant_key)
    -- etc...
);

-- Indexes (Snowflake auto-creates micro-partitions)
CREATE INDEX idx_txn_customer ON fact.transactions(customer_key);
CREATE INDEX idx_txn_merchant ON fact.transactions(merchant_key);
CREATE INDEX idx_txn_date ON fact.transactions(date_key);
CREATE INDEX idx_txn_created_at ON fact.transactions(created_at);

-- Cluster key pour performance (partition physique)
ALTER TABLE fact.transactions CLUSTER BY (date_key, customer_key);

COMMENT ON TABLE fact.transactions IS 'Fact table at transaction grain level';
COMMENT ON COLUMN fact.transactions.transaction_key IS 'Surrogate key (auto-increment)';
COMMENT ON COLUMN fact.transactions.amount IS 'Transaction amount (measure)';
```

**Exemple de requête Star Schema :**
```sql
-- Revenue par pays par mois
SELECT 
    d.year,
    d.month_name,
    l.country,
    COUNT(*) as transaction_count,
    SUM(f.amount) as total_revenue,
    AVG(f.amount) as avg_transaction
FROM fact.transactions f
JOIN dim.date d ON f.date_key = d.date_key
JOIN dim.location l ON f.location_key = l.location_key
WHERE d.year = 2026
  AND f.is_successful = TRUE
GROUP BY d.year, d.month_name, l.country
ORDER BY total_revenue DESC;
```

### 3.2 Fact `fact.fraud_scores`

```sql
CREATE OR REPLACE TABLE fact.fraud_scores (
    fraud_score_key INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Business key
    fraud_score_id VARCHAR(36) NOT NULL,
    
    -- FK vers fact.transactions
    transaction_key INT NOT NULL,
    
    -- FK vers dimensions
    customer_key INT NOT NULL,
    merchant_key INT NOT NULL,
    payment_method_key INT,
    date_key INT NOT NULL,
    time_key INT NOT NULL,
    
    -- Mesures
    fraud_score DECIMAL(5,4),        -- 0.0 - 1.0
    
    -- Attributs
    risk_level VARCHAR(50),          -- 'low', 'medium', 'high', 'critical'
    model_version VARCHAR(50),
    manual_review_required BOOLEAN,
    
    -- Timestamps
    scored_at TIMESTAMP_NTZ,
    
    -- Audit
    etl_inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE INDEX idx_fraud_txn ON fact.fraud_scores(transaction_key);
CREATE INDEX idx_fraud_date ON fact.fraud_scores(date_key);
CREATE INDEX idx_fraud_customer ON fact.fraud_scores(customer_key);

ALTER TABLE fact.fraud_scores CLUSTER BY (date_key);
```

---

## 4. Tables de référence

### 4.1 Reference `refd.countries`

```sql
CREATE OR REPLACE TABLE refd.countries (
    country_code VARCHAR(2) PRIMARY KEY,  -- ISO 3166-1 alpha-2
    country_name VARCHAR(100) NOT NULL,
    region VARCHAR(100),
    continent VARCHAR(50),
    currency_code VARCHAR(3),             -- ISO 4217
    phone_prefix VARCHAR(10),
    is_active BOOLEAN DEFAULT TRUE
);

-- Insert common countries
INSERT INTO refd.countries VALUES
('US', 'United States', 'North America', 'Americas', 'USD', '+1', TRUE),
('FR', 'France', 'Western Europe', 'Europe', 'EUR', '+33', TRUE),
('GB', 'United Kingdom', 'Northern Europe', 'Europe', 'GBP', '+44', TRUE),
('DE', 'Germany', 'Western Europe', 'Europe', 'EUR', '+49', TRUE),
('JP', 'Japan', 'East Asia', 'Asia', 'JPY', '+81', TRUE);
```

### 4.2 Reference `refd.currency_rates`

```sql
CREATE OR REPLACE TABLE refd.currency_rates (
    rate_id INT IDENTITY(1,1) PRIMARY KEY,
    from_currency VARCHAR(3) NOT NULL,
    to_currency VARCHAR(3) NOT NULL,
    exchange_rate DECIMAL(12,6) NOT NULL,
    rate_date DATE NOT NULL,
    source VARCHAR(50),  -- 'ECB', 'Federal Reserve', etc.
    
    UNIQUE (from_currency, to_currency, rate_date)
);

CREATE INDEX idx_currency_date ON refd.currency_rates(rate_date DESC);
```

### 4.3 Reference `refd.payment_method_types`

```sql
CREATE OR REPLACE TABLE refd.payment_method_types (
    method_type_code VARCHAR(50) PRIMARY KEY,
    method_type_name VARCHAR(100),
    typical_processing_time VARCHAR(50),
    average_fee_percentage DECIMAL(5,2),
    chargeback_risk VARCHAR(20),  -- 'low', 'medium', 'high'
    supports_refunds BOOLEAN,
    supports_recurring BOOLEAN,
    is_active BOOLEAN DEFAULT TRUE
);

INSERT INTO refd.payment_method_types VALUES
('card', 'Credit/Debit Card', '1-3 seconds', 2.90, 'medium', TRUE, TRUE, TRUE),
('bank_transfer', 'Bank Transfer', '1-3 days', 0.80, 'low', TRUE, TRUE, TRUE),
('wallet', 'Digital Wallet', '1-2 seconds', 3.40, 'low', TRUE, TRUE, TRUE);
```

---

## 5. Aggregates analytiques

### 5.1 Aggregate `agg.revenue_daily`

```sql
CREATE OR REPLACE TABLE agg.revenue_daily (
    date_key INT PRIMARY KEY,
    date DATE NOT NULL,
    
    -- Métriques transactions
    total_transactions INT,
    total_revenue DECIMAL(18,2),
    total_refunds DECIMAL(18,2),
    net_revenue DECIMAL(18,2),
    avg_transaction_amount DECIMAL(12,2),
    
    -- Métriques clients
    unique_customers INT,
    unique_merchants INT,
    
    -- Timestamp
    calculated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    FOREIGN KEY (date_key) REFERENCES dim.date(date_key)
);

-- Materialized view refresh
CREATE OR REPLACE VIEW vw_revenue_daily_refresh AS
SELECT 
    d.date_key,
    d.full_date as date,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN f.is_refund = FALSE THEN f.amount ELSE 0 END) as total_revenue,
    SUM(CASE WHEN f.is_refund = TRUE THEN f.amount ELSE 0 END) as total_refunds,
    SUM(CASE WHEN f.is_refund = FALSE THEN f.amount ELSE -f.amount END) as net_revenue,
    AVG(CASE WHEN f.is_refund = FALSE THEN f.amount END) as avg_transaction_amount,
    COUNT(DISTINCT f.customer_key) as unique_customers,
    COUNT(DISTINCT f.merchant_key) as unique_merchants,
    CURRENT_TIMESTAMP() as calculated_at
FROM fact.transactions f
JOIN dim.date d ON f.date_key = d.date_key
GROUP BY d.date_key, d.full_date;
```

### 5.2 Aggregate `agg.customer_segmentation`

```sql
CREATE OR REPLACE TABLE agg.customer_segmentation (
    customer_id VARCHAR(36) PRIMARY KEY,
    customer_key INT NOT NULL,
    
    -- Segment
    segment VARCHAR(50),  -- 'VIP', 'High Value', 'Standard', 'At Risk', 'New'
    
    -- RFM Scores
    rfm_score INT,        -- Combined RFM score (1-125)
    recency_score INT,    -- 1-5 (5 = most recent)
    frequency_score INT,  -- 1-5 (5 = most frequent)
    monetary_score INT,   -- 1-5 (5 = highest spend)
    
    -- Métriques
    lifetime_value DECIMAL(18,2),
    total_transactions INT,
    total_spent DECIMAL(18,2),
    avg_transaction_amount DECIMAL(12,2),
    
    -- Dates
    first_transaction_date DATE,
    last_transaction_date DATE,
    days_since_last_transaction INT,
    customer_tenure_days INT,
    
    -- Prédictions ML
    churn_probability DECIMAL(5,4),
    predicted_lifetime_value DECIMAL(18,2),
    
    -- Préférences
    preferred_payment_method VARCHAR(50),
    preferred_device VARCHAR(50),
    
    calculated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE INDEX idx_segment ON agg.customer_segmentation(segment);
CREATE INDEX idx_churn ON agg.customer_segmentation(churn_probability DESC);
```

### 5.3 Aggregate `agg.fraud_analysis_summary`

```sql
CREATE OR REPLACE TABLE agg.fraud_analysis_summary (
    date_key INT PRIMARY KEY,
    date DATE NOT NULL,
    
    -- Volumes
    total_transactions INT,
    flagged_transactions INT,
    confirmed_fraud INT,
    false_positives INT,
    
    -- Rates
    fraud_rate DECIMAL(5,4),
    avg_fraud_score DECIMAL(5,4),
    
    -- Risk breakdown
    high_risk_transactions INT,
    manual_reviews_required INT,
    manual_reviews_completed INT,
    
    -- Financial impact
    total_fraud_amount DECIMAL(18,2),
    prevented_fraud_amount DECIMAL(18,2),
    
    calculated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

---

## 6. Logs de conformité

### 6.1 Log `log.compliance_audit_log`

```sql
CREATE OR REPLACE TABLE log.compliance_audit_log (
    audit_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Event info
    event_type VARCHAR(50),   -- 'transaction', 'user_action', 'fraud_check'
    entity_type VARCHAR(50),  -- 'customer', 'merchant', 'transaction'
    entity_id VARCHAR(36),
    action VARCHAR(50),       -- 'create', 'update', 'delete', 'review'
    
    -- User
    performed_by VARCHAR(255),  -- user_id or 'system'
    
    -- Timestamp
    event_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Details (flexible JSON)
    event_details VARIANT,
    
    -- Network
    ip_address VARCHAR(45),
    user_agent VARCHAR(500),
    
    -- Status
    compliance_status VARCHAR(50),  -- 'compliant', 'flagged', 'under_review'
    reviewer_notes TEXT,
    resolved_at TIMESTAMP_NTZ
);

CREATE INDEX idx_audit_timestamp ON log.compliance_audit_log(event_timestamp DESC);
CREATE INDEX idx_audit_entity ON log.compliance_audit_log(entity_type, entity_id);
```

### 6.2 Log `log.data_retention_log`

```sql
CREATE OR REPLACE TABLE log.data_retention_log (
    retention_id INT IDENTITY(1,1) PRIMARY KEY,
    table_name VARCHAR(255),
    record_id VARCHAR(36),
    
    -- Retention policy
    retention_policy VARCHAR(50),  -- '1_year', '7_years', 'indefinite'
    
    -- Dates
    created_at TIMESTAMP_NTZ,
    scheduled_deletion_date DATE,
    
    -- Status
    deletion_status VARCHAR(50),  -- 'active', 'scheduled', 'deleted', 'retained'
    deletion_reason TEXT,
    deleted_at TIMESTAMP_NTZ,
    deleted_by VARCHAR(255)
);
```

---

## 7. Scripts DDL complets

```sql
-- ============================================
-- STRIPE OLAP DATABASE - COMPLETE DDL
-- Snowflake Data Warehouse
-- ============================================

-- Create database
CREATE DATABASE IF NOT EXISTS stripe_dwh;
USE DATABASE stripe_dwh;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS dim;     -- Dimensions
CREATE SCHEMA IF NOT EXISTS fact;    -- Facts
CREATE SCHEMA IF NOT EXISTS refd;    -- Reference data
CREATE SCHEMA IF NOT EXISTS agg;     -- Aggregates
CREATE SCHEMA IF NOT EXISTS log;     -- Compliance logs

-- ============================================
-- DIMENSIONS
-- ============================================

-- dim.customer
CREATE OR REPLACE TABLE dim.customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(36) NOT NULL,
    name VARCHAR(255),
    first_name VARCHAR(255),
    full_address TEXT,
    post_code VARCHAR(20),
    phone VARCHAR(50),
    email VARCHAR(255),
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    is_current BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('9999-12-31'),
    etl_inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- dim.merchant
CREATE OR REPLACE TABLE dim.merchant (
    merchant_key INT IDENTITY(1,1) PRIMARY KEY,
    merchant_id VARCHAR(36) NOT NULL,
    legal_name VARCHAR(500),
    full_address TEXT,
    post_code VARCHAR(20),
    phone VARCHAR(50),
    email VARCHAR(255),
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    is_current BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('9999-12-31'),
    etl_inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- dim.product
CREATE OR REPLACE TABLE dim.product (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(36) NOT NULL,
    name VARCHAR(500),
    description TEXT,
    price DECIMAL(12,2),
    category VARCHAR(100),
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    is_current BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('9999-12-31'),
    etl_inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- dim.payment_method
CREATE OR REPLACE TABLE dim.payment_method (
    payment_method_key INT IDENTITY(1,1) PRIMARY KEY,
    payment_method_id VARCHAR(36) NOT NULL,
    method_type VARCHAR(50),
    card_brand VARCHAR(50),
    is_subscription BOOLEAN,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    is_current BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('9999-12-31'),
    etl_inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- dim.date (generate 2020-2030)
CREATE OR REPLACE TABLE dim.date AS
WITH RECURSIVE date_range AS (
    SELECT TO_DATE('2020-01-01') AS dt
    UNION ALL
    SELECT DATEADD(day, 1, dt) FROM date_range WHERE dt < '2030-12-31'
)
SELECT
    TO_NUMBER(TO_CHAR(dt, 'YYYYMMDD')) AS date_key,
    dt AS full_date,
    YEAR(dt) AS year,
    QUARTER(dt) AS quarter,
    MONTH(dt) AS month,
    MONTHNAME(dt) AS month_name,
    WEEKOFYEAR(dt) AS week,
    DAYOFMONTH(dt) AS day_of_month,
    DAYOFWEEK(dt) AS day_of_week,
    DAYNAME(dt) AS day_name,
    IFF(DAYOFWEEK(dt) IN (6,7), TRUE, FALSE) AS is_weekend,
    FALSE AS is_holiday
FROM date_range;

-- dim.time
CREATE OR REPLACE TABLE dim.time AS
WITH RECURSIVE time_range AS (
    SELECT 0 AS sec
    UNION ALL
    SELECT sec + 1 FROM time_range WHERE sec < 86399
)
SELECT
    TO_NUMBER(LPAD(FLOOR(sec/3600)::STRING, 2, '0') || LPAD(FLOOR((sec%3600)/60)::STRING, 2, '0') || LPAD((sec%60)::STRING, 2, '0')) AS time_key,
    FLOOR(sec / 3600) AS hour,
    FLOOR((sec % 3600) / 60) AS minute,
    sec % 60 AS second,
    CASE
        WHEN FLOOR(sec/3600) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN FLOOR(sec/3600) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN FLOOR(sec/3600) BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day
FROM time_range;

-- dim.location
CREATE OR REPLACE TABLE dim.location (
    location_key INT IDENTITY(1,1) PRIMARY KEY,
    location VARCHAR(255),
    city VARCHAR(100),
    region VARCHAR(100),
    country VARCHAR(100),
    country_code VARCHAR(2),
    etl_inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- FACTS
-- ============================================

-- fact.transactions
CREATE OR REPLACE TABLE fact.transactions (
    transaction_key INT IDENTITY(1,1) PRIMARY KEY,
    transaction_id VARCHAR(36) NOT NULL,
    customer_key INT NOT NULL,
    merchant_key INT NOT NULL,
    product_key INT,
    payment_method_key INT,
    date_key INT NOT NULL,
    time_key INT NOT NULL,
    location_key INT,
    amount DECIMAL(12,2) NOT NULL,
    currency VARCHAR(3),
    status VARCHAR(50),
    transaction_type VARCHAR(50),
    device_type VARCHAR(50),
    is_successful BOOLEAN,
    is_refund BOOLEAN,
    created_at TIMESTAMP_NTZ,
    etl_inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

ALTER TABLE fact.transactions CLUSTER BY (date_key, customer_key);

-- fact.fraud_scores
CREATE OR REPLACE TABLE fact.fraud_scores (
    fraud_score_key INT IDENTITY(1,1) PRIMARY KEY,
    fraud_score_id VARCHAR(36) NOT NULL,
    transaction_key INT NOT NULL,
    customer_key INT NOT NULL,
    merchant_key INT NOT NULL,
    payment_method_key INT,
    date_key INT NOT NULL,
    time_key INT NOT NULL,
    fraud_score DECIMAL(5,4),
    risk_level VARCHAR(50),
    model_version VARCHAR(50),
    manual_review_required BOOLEAN,
    scored_at TIMESTAMP_NTZ,
    etl_inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- AGGREGATES
-- ============================================

CREATE OR REPLACE TABLE agg.revenue_daily (
    date_key INT PRIMARY KEY,
    date DATE,
    total_transactions INT,
    total_revenue DECIMAL(18,2),
    total_refunds DECIMAL(18,2),
    net_revenue DECIMAL(18,2),
    avg_transaction_amount DECIMAL(12,2),
    unique_customers INT,
    unique_merchants INT,
    calculated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- GRANTS
-- ============================================

-- Create roles
CREATE ROLE IF NOT EXISTS analyst_role;
CREATE ROLE IF NOT EXISTS engineer_role;
CREATE ROLE IF NOT EXISTS admin_role;

-- Analysts: Read-only
GRANT USAGE ON DATABASE stripe_dwh TO ROLE analyst_role;
GRANT USAGE ON ALL SCHEMAS IN DATABASE stripe_dwh TO ROLE analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA dim TO ROLE analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA fact TO ROLE analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA agg TO ROLE analyst_role;

-- Engineers: Read/Write
GRANT USAGE ON DATABASE stripe_dwh TO ROLE engineer_role;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE stripe_dwh TO ROLE engineer_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE stripe_dwh TO ROLE engineer_role;
```

---

## 8. Vues matérialisées

```sql
-- Vue: Top 100 clients par revenue
CREATE OR REPLACE VIEW vw_top_customers AS
SELECT 
    c.customer_id,
    c.name,
    c.email,
    COUNT(f.transaction_id) as total_transactions,
    SUM(f.amount) as total_revenue,
    AVG(f.amount) as avg_transaction,
    MAX(f.created_at) as last_transaction_date
FROM fact.transactions f
JOIN dim.customer c ON f.customer_key = c.customer_key
WHERE c.is_current = TRUE
  AND f.is_successful = TRUE
GROUP BY c.customer_id, c.name, c.email
ORDER BY total_revenue DESC
LIMIT 100;

-- Vue: Fraud rate par merchant
CREATE OR REPLACE VIEW vw_merchant_fraud_rate AS
SELECT 
    m.merchant_id,
    m.legal_name,
    COUNT(DISTINCT f.transaction_id) as total_transactions,
    COUNT(DISTINCT fs.fraud_score_id) as fraud_checks,
    AVG(fs.fraud_score) as avg_fraud_score,
    SUM(CASE WHEN fs.risk_level = 'critical' THEN 1 ELSE 0 END) as critical_risk_count
FROM fact.transactions f
JOIN dim.merchant m ON f.merchant_key = m.merchant_key
LEFT JOIN fact.fraud_scores fs ON f.transaction_key = fs.transaction_key
WHERE m.is_current = TRUE
GROUP BY m.merchant_id, m.legal_name;
```

---