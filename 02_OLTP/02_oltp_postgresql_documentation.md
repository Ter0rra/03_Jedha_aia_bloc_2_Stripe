# Base de Données OLTP (PostgreSQL) - Stripe Payment Platform
## Documentation Technique du Schéma Normalisé

---

## 📋 Table des matières

1. [Vue d'ensemble](#1-vue-densemble)
2. [Tables principales](#2-tables-principales)
3. [Relations et contraintes](#3-relations-et-contraintes)
4. [Indexes et optimisations](#4-indexes-et-optimisations)
5. [Scripts DDL complets](#5-scripts-ddl-complets)
6. [Procédures stockées](#6-procédures-stockées)

---

## 1. Vue d'ensemble

### 1.1 Caractéristiques

| Propriété | Valeur |
|-----------|--------|
| **SGBD** | PostgreSQL 15.x |
| **Modèle** | Normalisé (3NF) |
| **Volume** | 10M+ transactions/jour |
| **TPS** | 5,000 (peak: 15,000) |
| **Latence** | <30ms moyenne |
| **Disponibilité** | 99.99% SLA |

### 1.2 Schéma ER

```
Customer (1) ──< (n) Payment_methods
Customer (1) ──< (n) Transaction
Merchant (1) ──< (n) Transaction
Product (1) ──< (n) Transaction
Payment_methods (1) ──< (n) Transaction
Transaction (1) ── (1) Fraud
```

---

## 2. Tables principales

### 2.1 Table `customer`

```sql
CREATE TABLE customer (
    customer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    first_name VARCHAR(255),
    address_line_1 VARCHAR(500),
    address_line_2 VARCHAR(500),
    post_code VARCHAR(20),
    phone VARCHAR(50),
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT email_format CHECK (
        email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    )
);

CREATE INDEX idx_customer_email ON customer(email);
CREATE INDEX idx_customer_created_at ON customer(created_at DESC);
```

**Colonnes clés :**
- `customer_id` : UUID v4 (clé primaire)
- `email` : Unique, utilisé pour login
- `created_at` : Timestamp création compte

### 2.2 Table `merchant`

```sql
CREATE TABLE merchant (
    merchant_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    legal_name VARCHAR(500) NOT NULL,
    address_line_1 VARCHAR(500) NOT NULL,
    address_line_2 VARCHAR(500),
    post_code VARCHAR(20) NOT NULL,
    phone VARCHAR(50) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_merchant_email ON merchant(email);
```

### 2.3 Table `product`

```sql
CREATE TABLE product (
    product_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(500) NOT NULL,
    description TEXT,
    price DECIMAL(12,2) NOT NULL CHECK (price >= 0),
    category VARCHAR(100) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_product_category ON product(category);
CREATE INDEX idx_product_active ON product(is_active) WHERE is_active = TRUE;
```

### 2.4 Table `payment_methods`

```sql
CREATE TABLE payment_methods (
    payment_method_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL REFERENCES customer(customer_id) ON DELETE CASCADE,
    
    -- Type
    method_type VARCHAR(50) NOT NULL, -- 'card', 'bank_transfer', 'wallet'
    
    -- Card (PCI-DSS compliant - tokenized)
    card_brand VARCHAR(50),
    card_last4 VARCHAR(4),
    
    -- Bank
    bank_account_last4 VARCHAR(4),
    
    -- Status
    is_default BOOLEAN DEFAULT FALSE,
    is_subscription BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT method_type_valid CHECK (
        method_type IN ('card', 'bank_transfer', 'wallet')
    )
);

CREATE INDEX idx_payment_customer ON payment_methods(customer_id);
```

**⚠️ PCI-DSS Compliance :**
- Jamais stocker numéro carte complet
- Jamais stocker CVV
- Uniquement : brand, last4, token (vault externe)

### 2.5 Table `transaction`

```sql
CREATE TABLE transaction (
    transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Foreign keys
    customer_id UUID NOT NULL REFERENCES customer(customer_id),
    merchant_id UUID NOT NULL REFERENCES merchant(merchant_id),
    product_id UUID REFERENCES product(product_id),
    payment_method_id UUID REFERENCES payment_methods(payment_method_id),
    
    -- Details
    amount DECIMAL(12,2) NOT NULL CHECK (amount >= 0),
    currency VARCHAR(3) NOT NULL DEFAULT 'USD', -- 'USD', 'EUR', 'GBP'
    
    -- Status
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    transaction_type VARCHAR(50) NOT NULL DEFAULT 'purchase',
    
    -- Context
    location VARCHAR(255),
    device_type VARCHAR(50), -- 'web', 'mobile', 'api'
    
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT status_valid CHECK (
        status IN ('pending', 'processing', 'completed', 'failed', 'refunded', 'cancelled')
    ),
    CONSTRAINT transaction_type_valid CHECK (
        transaction_type IN ('purchase', 'refund', 'subscription', 'payout')
    ),
    CONSTRAINT currency_valid CHECK (
        currency IN ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD')
    )
);

-- Indexes principaux
CREATE INDEX idx_transaction_customer ON transaction(customer_id, created_at DESC);
CREATE INDEX idx_transaction_merchant ON transaction(merchant_id, created_at DESC);
CREATE INDEX idx_transaction_status ON transaction(status);
CREATE INDEX idx_transaction_created_at ON transaction(created_at DESC);
```

**Workflow Status :**
1. `pending` : Transaction initiée
2. `processing` : En cours de traitement
3. `completed` : Succès
4. `failed` : Échec (carte refusée, fraude, etc.)
5. `refunded` : Remboursée
6. `cancelled` : Annulée par client

### 2.6 Table `fraud`

```sql
CREATE TABLE fraud (
    fraud_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Relations (1:1 avec transaction)
    transaction_id UUID NOT NULL UNIQUE REFERENCES transaction(transaction_id),
    customer_id UUID NOT NULL REFERENCES customer(customer_id),
    merchant_id UUID NOT NULL REFERENCES merchant(merchant_id),
    payment_method_id UUID REFERENCES payment_methods(payment_method_id),
    
    -- Scoring
    fraud_probability DECIMAL(5,4) NOT NULL CHECK (fraud_probability BETWEEN 0 AND 1),
    risk_level VARCHAR(50) NOT NULL,
    
    -- Model
    model_version VARCHAR(50) NOT NULL,
    manual_review_required BOOLEAN DEFAULT FALSE,
    
    -- Timestamp
    scored_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT risk_level_valid CHECK (
        risk_level IN ('low', 'medium', 'high', 'critical')
    )
);

CREATE INDEX idx_fraud_transaction ON fraud(transaction_id);
CREATE INDEX idx_fraud_customer ON fraud(customer_id, scored_at DESC);
CREATE INDEX idx_fraud_risk ON fraud(risk_level) WHERE risk_level IN ('high', 'critical');
CREATE INDEX idx_fraud_review ON fraud(manual_review_required) WHERE manual_review_required = TRUE;
```

**Seuils de risque :**
- `low` (0.0-0.3) : Approuver automatiquement
- `medium` (0.3-0.6) : Approuver avec monitoring
- `high` (0.6-0.85) : Review manuelle requise
- `critical` (0.85-1.0) : Bloquer automatiquement

---

## 3. Relations et contraintes

### 3.1 Foreign Keys

```sql
-- Customer → Payment_methods (1:n)
ALTER TABLE payment_methods 
    ADD CONSTRAINT fk_payment_customer 
    FOREIGN KEY (customer_id) 
    REFERENCES customer(customer_id) 
    ON DELETE CASCADE;

-- Customer → Transaction (1:n)
ALTER TABLE transaction 
    ADD CONSTRAINT fk_transaction_customer 
    FOREIGN KEY (customer_id) 
    REFERENCES customer(customer_id);

-- Merchant → Transaction (1:n)
ALTER TABLE transaction 
    ADD CONSTRAINT fk_transaction_merchant 
    FOREIGN KEY (merchant_id) 
    REFERENCES merchant(merchant_id);

-- Product → Transaction (1:n)
ALTER TABLE transaction 
    ADD CONSTRAINT fk_transaction_product 
    FOREIGN KEY (product_id) 
    REFERENCES product(product_id) 
    ON DELETE SET NULL;

-- Payment_methods → Transaction (1:n)
ALTER TABLE transaction 
    ADD CONSTRAINT fk_transaction_payment 
    FOREIGN KEY (payment_method_id) 
    REFERENCES payment_methods(payment_method_id) 
    ON DELETE SET NULL;

-- Transaction → Fraud (1:1)
ALTER TABLE fraud 
    ADD CONSTRAINT fk_fraud_transaction 
    FOREIGN KEY (transaction_id) 
    REFERENCES transaction(transaction_id) 
    ON DELETE CASCADE;

-- Customer → Fraud (1:n)
ALTER TABLE fraud 
    ADD CONSTRAINT fk_fraud_customer 
    FOREIGN KEY (customer_id) 
    REFERENCES customer(customer_id);

-- Merchant → Fraud (1:n)
ALTER TABLE fraud 
    ADD CONSTRAINT fk_fraud_merchant 
    FOREIGN KEY (merchant_id) 
    REFERENCES merchant(merchant_id);

-- Payment_methods → Fraud (1:n)
ALTER TABLE fraud 
    ADD CONSTRAINT fk_fraud_payment 
    FOREIGN KEY (payment_method_id) 
    REFERENCES payment_methods(payment_method_id);
```

### 3.2 Constraints de validation

```sql
-- Email format
ALTER TABLE customer 
    ADD CONSTRAINT email_format 
    CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');

-- Montant positif
ALTER TABLE transaction 
    ADD CONSTRAINT amount_positive 
    CHECK (amount >= 0);

-- Fraud probability entre 0 et 1
ALTER TABLE fraud 
    ADD CONSTRAINT fraud_prob_range 
    CHECK (fraud_probability BETWEEN 0 AND 1);

-- Prix produit >= 0
ALTER TABLE product 
    ADD CONSTRAINT price_positive 
    CHECK (price >= 0);
```

---

## 4. Indexes et optimisations

### 4.1 Indexes par use case

**Use case 1 : Login client**
```sql
CREATE INDEX idx_customer_email_login 
    ON customer(LOWER(email)) 
    WHERE deleted_at IS NULL;
```

**Use case 2 : Dashboard client (transactions récentes)**
```sql
CREATE INDEX idx_transaction_customer_recent 
    ON transaction(customer_id, created_at DESC) 
    WHERE created_at > CURRENT_DATE - INTERVAL '90 days';
```

**Use case 3 : Queue fraud review**
```sql
CREATE INDEX idx_fraud_review_queue 
    ON fraud(scored_at DESC) 
    WHERE manual_review_required = TRUE;
```

**Use case 4 : Transactions échouées (retry)**
```sql
CREATE INDEX idx_transaction_failed 
    ON transaction(customer_id, created_at) 
    WHERE status = 'failed' 
      AND created_at > CURRENT_DATE - INTERVAL '7 days';
```

### 4.2 Indexes pour analytics

```sql
-- Revenue par merchant par jour
CREATE INDEX idx_transaction_merchant_daily 
    ON transaction(merchant_id, DATE(created_at), status) 
    INCLUDE (amount)
    WHERE status = 'completed';

-- Fraud rate par merchant
CREATE INDEX idx_fraud_merchant_stats 
    ON fraud(merchant_id, DATE(scored_at), is_fraud_confirmed);
```

---

## 5. Scripts DDL complets

### 5.1 Script de création

```sql
-- ============================================
-- STRIPE OLTP DATABASE - DDL COMPLET
-- PostgreSQL 15.x
-- ============================================

CREATE DATABASE stripe_oltp 
    WITH ENCODING='UTF8' 
    LC_COLLATE='en_US.UTF-8' 
    LC_CTYPE='en_US.UTF-8';

\c stripe_oltp

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================
-- TABLES
-- ============================================

-- Customer
CREATE TABLE customer (
    customer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    first_name VARCHAR(255),
    address_line_1 VARCHAR(500),
    address_line_2 VARCHAR(500),
    post_code VARCHAR(20),
    phone VARCHAR(50),
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
);

-- Merchant
CREATE TABLE merchant (
    merchant_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    legal_name VARCHAR(500) NOT NULL,
    address_line_1 VARCHAR(500) NOT NULL,
    address_line_2 VARCHAR(500),
    post_code VARCHAR(20) NOT NULL,
    phone VARCHAR(50) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Product
CREATE TABLE product (
    product_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(500) NOT NULL,
    description TEXT,
    price DECIMAL(12,2) NOT NULL CHECK (price >= 0),
    category VARCHAR(100) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Payment_methods
CREATE TABLE payment_methods (
    payment_method_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL REFERENCES customer(customer_id) ON DELETE CASCADE,
    method_type VARCHAR(50) NOT NULL,
    card_brand VARCHAR(50),
    card_last4 VARCHAR(4),
    bank_account_last4 VARCHAR(4),
    is_default BOOLEAN DEFAULT FALSE,
    is_subscription BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT method_type_valid CHECK (method_type IN ('card', 'bank_transfer', 'wallet'))
);

-- Transaction
CREATE TABLE transaction (
    transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL REFERENCES customer(customer_id),
    merchant_id UUID NOT NULL REFERENCES merchant(merchant_id),
    product_id UUID REFERENCES product(product_id),
    payment_method_id UUID REFERENCES payment_methods(payment_method_id),
    amount DECIMAL(12,2) NOT NULL CHECK (amount >= 0),
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    transaction_type VARCHAR(50) NOT NULL DEFAULT 'purchase',
    location VARCHAR(255),
    device_type VARCHAR(50),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT status_valid CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'refunded', 'cancelled')),
    CONSTRAINT transaction_type_valid CHECK (transaction_type IN ('purchase', 'refund', 'subscription', 'payout')),
    CONSTRAINT currency_valid CHECK (currency IN ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD'))
);

-- Fraud
CREATE TABLE fraud (
    fraud_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id UUID NOT NULL UNIQUE REFERENCES transaction(transaction_id) ON DELETE CASCADE,
    customer_id UUID NOT NULL REFERENCES customer(customer_id),
    merchant_id UUID NOT NULL REFERENCES merchant(merchant_id),
    payment_method_id UUID REFERENCES payment_methods(payment_method_id),
    fraud_probability DECIMAL(5,4) NOT NULL CHECK (fraud_probability BETWEEN 0 AND 1),
    risk_level VARCHAR(50) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    manual_review_required BOOLEAN DEFAULT FALSE,
    scored_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT risk_level_valid CHECK (risk_level IN ('low', 'medium', 'high', 'critical'))
);

-- ============================================
-- INDEXES
-- ============================================

-- Customer
CREATE INDEX idx_customer_email ON customer(email);
CREATE INDEX idx_customer_created_at ON customer(created_at DESC);

-- Merchant
CREATE INDEX idx_merchant_email ON merchant(email);

-- Product
CREATE INDEX idx_product_category ON product(category);
CREATE INDEX idx_product_active ON product(is_active) WHERE is_active = TRUE;

-- Payment_methods
CREATE INDEX idx_payment_customer ON payment_methods(customer_id);

-- Transaction
CREATE INDEX idx_transaction_customer ON transaction(customer_id, created_at DESC);
CREATE INDEX idx_transaction_merchant ON transaction(merchant_id, created_at DESC);
CREATE INDEX idx_transaction_status ON transaction(status);
CREATE INDEX idx_transaction_created_at ON transaction(created_at DESC);

-- Fraud
CREATE INDEX idx_fraud_transaction ON fraud(transaction_id);
CREATE INDEX idx_fraud_customer ON fraud(customer_id, scored_at DESC);
CREATE INDEX idx_fraud_risk ON fraud(risk_level) WHERE risk_level IN ('high', 'critical');
```

---

## 6. Procédures stockées

### 6.1 Fonction : Créer transaction

```sql
CREATE OR REPLACE FUNCTION create_transaction(
    p_customer_id UUID,
    p_merchant_id UUID,
    p_product_id UUID,
    p_payment_method_id UUID,
    p_amount DECIMAL,
    p_currency VARCHAR DEFAULT 'USD',
    p_device_type VARCHAR DEFAULT 'web'
)
RETURNS UUID AS $$
DECLARE
    v_transaction_id UUID;
BEGIN
    -- Insert transaction
    INSERT INTO transaction (
        customer_id, merchant_id, product_id, payment_method_id,
        amount, currency, device_type, status
    ) VALUES (
        p_customer_id, p_merchant_id, p_product_id, p_payment_method_id,
        p_amount, p_currency, p_device_type, 'pending'
    )
    RETURNING transaction_id INTO v_transaction_id;
    
    RETURN v_transaction_id;
END;
$$ LANGUAGE plpgsql;

-- Usage
SELECT create_transaction(
    'cus_abc123'::UUID,
    'merch_xyz789'::UUID,
    'prod_12345'::UUID,
    'pm_67890'::UUID,
    99.99,
    'USD'
);
```

### 6.2 Fonction : Profil client complet

```sql
CREATE OR REPLACE FUNCTION get_customer_profile(p_customer_id UUID)
RETURNS TABLE (
    customer_id UUID,
    name VARCHAR,
    email VARCHAR,
    total_transactions BIGINT,
    total_spent DECIMAL,
    avg_transaction DECIMAL,
    fraud_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.customer_id,
        c.name,
        c.email,
        COUNT(t.transaction_id) as total_transactions,
        COALESCE(SUM(t.amount), 0) as total_spent,
        COALESCE(AVG(t.amount), 0) as avg_transaction,
        COUNT(f.fraud_id) FILTER (WHERE f.is_fraud_confirmed = TRUE) as fraud_count
    FROM customer c
    LEFT JOIN transaction t ON c.customer_id = t.customer_id
    LEFT JOIN fraud f ON t.transaction_id = f.transaction_id
    WHERE c.customer_id = p_customer_id
    GROUP BY c.customer_id, c.name, c.email;
END;
$$ LANGUAGE plpgsql;
```

---