# Architecture du Pipeline de Données - PARTIE 2
## Document Technique : CDC, Transformations, Monitoring & Optimisation

---

## 📋 Table des matières (Partie 2)

7. [Change Data Capture (CDC)](#7-change-data-capture-cdc)
8. [Transformations de données](#8-transformations-de-données)
9. [Monitoring et observabilité](#9-monitoring-et-observabilité)
10. [Gestion des erreurs et retry](#10-gestion-des-erreurs-et-retry)
11. [Performance et optimisation](#11-performance-et-optimisation)
12. [Sécurité et conformité](#12-sécurité-et-conformité)

---

## 7. Change Data Capture (CDC)

### 7.1 Vue d'ensemble CDC

Le Change Data Capture permet de capturer les modifications de données en temps réel depuis les bases transactionnelles vers le data warehouse.

```
┌─────────────────────────────────────────────────────────────┐
│                    CDC ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐                                            │
│  │  PostgreSQL  │                                            │
│  │     OLTP     │                                            │
│  └──────┬───────┘                                            │
│         │ WAL (Write-Ahead Log)                             │
│         │                                                    │
│         ↓                                                    │
│  ┌──────────────┐                                            │
│  │  Debezium    │                                            │
│  │  Connector   │                                            │
│  │              │                                            │
│  │  - Reads WAL │                                            │
│  │  - Captures: │                                            │
│  │    • INSERT  │                                            │
│  │    • UPDATE  │                                            │
│  │    • DELETE  │                                            │
│  └──────┬───────┘                                            │
│         │ JSON Events                                        │
│         ↓                                                    │
│  ┌──────────────┐                                            │
│  │    Kafka     │                                            │
│  │              │                                            │
│  │  Topics:     │                                            │
│  │  - customers │                                            │
│  │  - txn       │                                            │
│  │  - fraud     │                                            │
│  └──────┬───────┘                                            │
│         │                                                    │
│         ↓                                                    │
│  ┌──────────────┐                                            │
│  │   Kafka      │                                            │
│  │   Connect    │                                            │
│  │   Snowflake  │                                            │
│  │   Sink       │                                            │
│  └──────┬───────┘                                            │
│         │                                                    │
│         ↓                                                    │
│  ┌──────────────┐                                            │
│  │  Snowflake   │                                            │
│  │     OLAP     │                                            │
│  └──────────────┘                                            │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### 7.2 Configuration Debezium pour PostgreSQL

#### Installation Debezium

```bash
# Docker Compose pour Debezium
version: '3.8'

services:
  debezium:
    image: debezium/connect:2.4
    container_name: debezium-connect
    ports:
      - "8083:8083"
    environment:
      GROUP_ID: debezium-cluster
      CONFIG_STORAGE_TOPIC: debezium_configs
      OFFSET_STORAGE_TOPIC: debezium_offsets
      STATUS_STORAGE_TOPIC: debezium_statuses
      BOOTSTRAP_SERVERS: kafka1:9092,kafka2:9092,kafka3:9092
      
      # Replication settings
      CONFIG_STORAGE_REPLICATION_FACTOR: 3
      OFFSET_STORAGE_REPLICATION_FACTOR: 3
      STATUS_STORAGE_REPLICATION_FACTOR: 3
      
      # Kafka settings
      KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      KEY_CONVERTER_SCHEMAS_ENABLE: "true"
      VALUE_CONVERTER_SCHEMAS_ENABLE: "true"
    
    depends_on:
      - kafka1
      - kafka2
      - kafka3
```

#### Configuration PostgreSQL pour CDC

```sql
-- 1. Enable logical replication
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;

-- Restart PostgreSQL
-- sudo systemctl restart postgresql

-- 2. Create replication user
CREATE USER debezium_user WITH REPLICATION LOGIN PASSWORD 'secure_password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
GRANT USAGE ON SCHEMA public TO debezium_user;

-- 3. Create publication for CDC
CREATE PUBLICATION stripe_cdc FOR ALL TABLES;

-- Verify configuration
SHOW wal_level;  -- Should return 'logical'
SELECT * FROM pg_replication_slots;
```

#### Debezium Connector Configuration

```json
{
  "name": "stripe-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    
    "database.hostname": "postgres.stripe.internal",
    "database.port": "5432",
    "database.user": "debezium_user",
    "database.password": "secure_password",
    "database.dbname": "stripe_oltp",
    "database.server.name": "stripe_postgres",
    
    "plugin.name": "pgoutput",
    "publication.name": "stripe_cdc",
    "slot.name": "debezium_slot",
    
    "table.include.list": "public.customer,public.merchant,public.transaction,public.payment_methods,public.product,public.fraud",
    
    "topic.prefix": "stripe.cdc",
    
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schemas.enable": "true",
    
    "heartbeat.interval.ms": "10000",
    "snapshot.mode": "initial",
    "snapshot.lock.timeout.ms": "10000",
    
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    
    "decimal.handling.mode": "precise",
    "time.precision.mode": "adaptive_time_microseconds",
    
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true"
  }
}
```

#### Déploiement du Connector

```bash
# Deploy connector via REST API
curl -X POST http://debezium:8083/connectors \
  -H "Content-Type: application/json" \
  -d @stripe-postgres-connector.json

# Check connector status
curl http://debezium:8083/connectors/stripe-postgres-connector/status

# List topics created
kafka-topics --bootstrap-server kafka1:9092 --list | grep stripe.cdc
```

### 7.3 Format des événements CDC

#### INSERT Event

```json
{
  "before": null,
  "after": {
    "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
    "customer_id": "cus_abc123",
    "merchant_id": "merch_xyz789",
    "amount": 99.99,
    "currency": "USD",
    "status": "completed",
    "created_at": "2026-01-22T14:30:25.123456Z"
  },
  "source": {
    "version": "2.4.0.Final",
    "connector": "postgresql",
    "name": "stripe_postgres",
    "ts_ms": 1706883025123,
    "snapshot": "false",
    "db": "stripe_oltp",
    "sequence": "[\"12345678\",\"12345679\"]",
    "schema": "public",
    "table": "transaction",
    "txId": 987654,
    "lsn": 12345679,
    "xmin": null
  },
  "op": "c",
  "ts_ms": 1706883025456,
  "transaction": null
}
```

#### UPDATE Event

```json
{
  "before": {
    "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "pending"
  },
  "after": {
    "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "completed"
  },
  "source": { ... },
  "op": "u",
  "ts_ms": 1706883125456
}
```

#### DELETE Event

```json
{
  "before": {
    "transaction_id": "550e8400-e29b-41d4-a716-446655440000"
  },
  "after": null,
  "source": { ... },
  "op": "d",
  "ts_ms": 1706883225456
}
```

### 7.4 MongoDB Change Streams

#### Configuration Change Streams

```javascript
// Node.js Change Stream Listener
const { MongoClient } = require('mongodb');
const { Kafka } = require('kafkajs');

// MongoDB Connection
const mongoClient = new MongoClient('mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=stripe-rs');
const db = mongoClient.db('stripe_events');

// Kafka Producer
const kafka = new Kafka({
  clientId: 'mongodb-cdc',
  brokers: ['kafka1:9092', 'kafka2:9092', 'kafka3:9092']
});
const producer = kafka.producer();

async function startChangeStream() {
  await mongoClient.connect();
  await producer.connect();
  
  // Watch user_interactions collection
  const collection = db.collection('user_interactions');
  const changeStream = collection.watch([], {
    fullDocument: 'updateLookup',
    fullDocumentBeforeChange: 'whenAvailable'
  });
  
  console.log('Change stream started for user_interactions');
  
  changeStream.on('change', async (change) => {
    console.log('Change detected:', change.operationType);
    
    // Format event
    const event = {
      operation: change.operationType,
      collection: 'user_interactions',
      documentKey: change.documentKey,
      fullDocument: change.fullDocument,
      fullDocumentBeforeChange: change.fullDocumentBeforeChange,
      updateDescription: change.updateDescription,
      timestamp: new Date(change.clusterTime.toNumber() * 1000),
      resumeToken: change._id
    };
    
    // Send to Kafka
    await producer.send({
      topic: 'stripe.cdc.user_interactions',
      messages: [{
        key: JSON.stringify(change.documentKey),
        value: JSON.stringify(event),
        timestamp: Date.now().toString()
      }]
    });
  });
  
  // Handle errors
  changeStream.on('error', (error) => {
    console.error('Change stream error:', error);
    // Implement retry logic
  });
}

// Resume from last position on restart
async function resumeChangeStream(resumeToken) {
  const changeStream = collection.watch([], {
    fullDocument: 'updateLookup',
    resumeAfter: resumeToken
  });
  // ... rest of logic
}

startChangeStream();
```

#### Change Stream pour toutes les collections

```javascript
// Watch all collections
const collections = [
  'user_interactions',
  'sessions',
  'fraud_features',
  'device_fingerprints',
  'behavior_patterns',
  'location_data',
  'anomaly_detections',
  'ml_predictions',
  'event_logs'
];

collections.forEach(collectionName => {
  const collection = db.collection(collectionName);
  const changeStream = collection.watch();
  
  changeStream.on('change', async (change) => {
    await producer.send({
      topic: `stripe.cdc.${collectionName}`,
      messages: [{
        key: JSON.stringify(change.documentKey),
        value: JSON.stringify(change)
      }]
    });
  });
});
```

### 7.5 Latence CDC et garanties

#### Métriques de latence

| Composant | Latence typique | Latence P95 | Latence P99 |
|-----------|----------------|-------------|-------------|
| **PostgreSQL WAL write** | <1ms | 2ms | 5ms |
| **Debezium read WAL** | 50-100ms | 200ms | 500ms |
| **Kafka produce** | 10-50ms | 100ms | 200ms |
| **Kafka consume** | 5-20ms | 50ms | 100ms |
| **Total end-to-end** | **<300ms** | **<1s** | **<2s** |

#### Garanties de livraison

```yaml
# Debezium guarantees
consistency: "at-least-once"
ordering: "per-table partition key"
durability: "WAL-based (PostgreSQL) or OpLog (MongoDB)"

# Configuration for exactly-once semantics
enable.idempotence: true
transactional.id: "debezium-txn-1"
acks: "all"
min.insync.replicas: 2
```

---

## 8. Transformations de données

### 8.1 DBT (Data Build Tool) Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DBT ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐                                            │
│  │  Raw Data    │                                            │
│  │  (Staging)   │                                            │
│  │              │                                            │
│  │  raw.cust... │                                            │
│  │  raw.txn...  │                                            │
│  └──────┬───────┘                                            │
│         │                                                    │
│         │ dbt source                                         │
│         ↓                                                    │
│  ┌──────────────┐                                            │
│  │  Staging     │                                            │
│  │  Models      │                                            │
│  │              │                                            │
│  │  stg_cust... │                                            │
│  │  stg_txn...  │                                            │
│  └──────┬───────┘                                            │
│         │                                                    │
│         │ dbt model                                          │
│         ↓                                                    │
│  ┌──────────────┐                                            │
│  │  Dimensions  │                                            │
│  │              │                                            │
│  │  dim.customer│                                            │
│  │  dim.merchant│                                            │
│  │  dim.product │                                            │
│  └──────┬───────┘                                            │
│         │                                                    │
│         │ dbt model                                          │
│         ↓                                                    │
│  ┌──────────────┐                                            │
│  │    Facts     │                                            │
│  │              │                                            │
│  │  fact.txn    │                                            │
│  │  fact.fraud  │                                            │
│  └──────┬───────┘                                            │
│         │                                                    │
│         │ dbt model                                          │
│         ↓                                                    │
│  ┌──────────────┐                                            │
│  │  Aggregates  │                                            │
│  │              │                                            │
│  │  agg.revenue │                                            │
│  │  agg.fraud   │                                            │
│  └──────────────┘                                            │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### 8.2 Structure DBT Project

```
stripe_dbt_project/
├── dbt_project.yml
├── profiles.yml
├── packages.yml
├── models/
│   ├── staging/
│   │   ├── _staging.yml
│   │   ├── stg_customers.sql
│   │   ├── stg_transactions.sql
│   │   ├── stg_merchants.sql
│   │   └── stg_products.sql
│   ├── intermediate/
│   │   ├── int_customer_metrics.sql
│   │   └── int_transaction_enriched.sql
│   ├── dimensions/
│   │   ├── dim_customer.sql
│   │   ├── dim_merchant.sql
│   │   ├── dim_product.sql
│   │   ├── dim_payment_method.sql
│   │   └── dim_location.sql
│   ├── facts/
│   │   ├── fact_transactions.sql
│   │   └── fact_fraud_scores.sql
│   └── aggregates/
│       ├── agg_revenue_daily.sql
│       ├── agg_customer_segmentation.sql
│       └── agg_fraud_analysis.sql
├── tests/
│   ├── assert_positive_amounts.sql
│   └── assert_valid_email.sql
├── macros/
│   ├── generate_schema_name.sql
│   └── surrogate_key.sql
└── snapshots/
    └── customers_snapshot.sql
```

### 8.3 Exemples de modèles DBT

#### Staging Model: stg_customers.sql

```sql
-- models/staging/stg_customers.sql

{{
  config(
    materialized='view',
    schema='staging'
  )
}}

WITH source AS (
    SELECT * FROM {{ source('stripe_raw', 'customers') }}
),

renamed AS (
    SELECT
        customer_id,
        name,
        first_name,
        CONCAT(address_line_1, ', ', COALESCE(address_line_2, '')) AS full_address,
        post_code,
        phone,
        LOWER(email) AS email,  -- Normalize email
        created_at,
        updated_at,
        is_active,
        kyc_verified,
        email_verified,
        deleted_at
    FROM source
    WHERE deleted_at IS NULL  -- Filter soft-deleted records
)

SELECT * FROM renamed
```

#### Dimension Model: dim_customer.sql (SCD Type 2)

```sql
-- models/dimensions/dim_customer.sql

{{
  config(
    materialized='incremental',
    unique_key='customer_id',
    schema='dim',
    on_schema_change='append_new_columns'
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

{% if is_incremental() %}
-- Get existing records
existing AS (
    SELECT * FROM {{ this }}
    WHERE is_current = TRUE
),

-- Detect changes (SCD Type 2)
changed_records AS (
    SELECT
        s.*,
        e.customer_key,
        e.valid_from AS existing_valid_from
    FROM source s
    LEFT JOIN existing e ON s.customer_id = e.customer_id
    WHERE
        e.customer_id IS NULL  -- New record
        OR s.email != e.email  -- Email changed
        OR s.full_address != e.full_address  -- Address changed
),

-- Close old records
closed_records AS (
    SELECT
        e.*,
        FALSE AS is_current,
        CURRENT_TIMESTAMP() AS valid_to
    FROM existing e
    JOIN changed_records c ON e.customer_id = c.customer_id
),
{% endif %}

-- New records with surrogate key
final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'CURRENT_TIMESTAMP()']) }} AS customer_key,
        customer_id,
        name,
        first_name,
        full_address,
        post_code,
        phone,
        email,
        created_at,
        updated_at,
        TRUE AS is_current,
        CURRENT_TIMESTAMP() AS valid_from,
        TO_TIMESTAMP('9999-12-31 23:59:59') AS valid_to,
        CURRENT_TIMESTAMP() AS etl_inserted_at
    FROM source
    
    {% if is_incremental() %}
    UNION ALL
    SELECT * FROM closed_records
    {% endif %}
)

SELECT * FROM final
```

#### Fact Model: fact_transactions.sql

```sql
-- models/facts/fact_transactions.sql

{{
  config(
    materialized='incremental',
    unique_key='transaction_id',
    schema='fact',
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['customer_key', 'merchant_key', 'date_key']
  )
}}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
    {% if is_incremental() %}
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),

customers AS (
    SELECT 
        customer_id,
        customer_key
    FROM {{ ref('dim_customer') }}
    WHERE is_current = TRUE
),

merchants AS (
    SELECT 
        merchant_id,
        merchant_key
    FROM {{ ref('dim_merchant') }}
    WHERE is_current = TRUE
),

products AS (
    SELECT 
        product_id,
        product_key
    FROM {{ ref('dim_product') }}
    WHERE is_current = TRUE
),

date_dim AS (
    SELECT 
        date_key,
        full_date
    FROM {{ ref('dim_date') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['t.transaction_id']) }} AS transaction_key,
        t.transaction_id,
        c.customer_key,
        m.merchant_key,
        p.product_key,
        d.date_key,
        TO_NUMBER(TO_CHAR(t.created_at, 'HH24MISS')) AS time_key,
        
        -- Measures
        t.amount,
        t.currency,
        
        -- Degenerate dimensions
        t.status,
        t.transaction_type,
        t.device_type,
        t.location,
        
        -- Flags
        CASE WHEN t.status = 'completed' THEN TRUE ELSE FALSE END AS is_successful,
        CASE WHEN t.transaction_type = 'refund' THEN TRUE ELSE FALSE END AS is_refund,
        
        -- Timestamps
        t.created_at,
        CURRENT_TIMESTAMP() AS etl_inserted_at
        
    FROM transactions t
    LEFT JOIN customers c ON t.customer_id = c.customer_id
    LEFT JOIN merchants m ON t.merchant_id = m.merchant_id
    LEFT JOIN products p ON t.product_id = p.product_id
    LEFT JOIN date_dim d ON DATE(t.created_at) = d.full_date
)

SELECT * FROM final
```

#### Aggregate Model: agg_revenue_daily.sql

```sql
-- models/aggregates/agg_revenue_daily.sql

{{
  config(
    materialized='incremental',
    unique_key='date_key',
    schema='agg'
  )
}}

WITH transactions AS (
    SELECT * FROM {{ ref('fact_transactions') }}
    {% if is_incremental() %}
    WHERE date_key > (SELECT MAX(date_key) FROM {{ this }})
    {% endif %}
),

daily_metrics AS (
    SELECT
        date_key,
        DATE(created_at) AS date,
        
        -- Transaction metrics
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN is_refund = FALSE THEN amount ELSE 0 END) AS total_revenue,
        SUM(CASE WHEN is_refund = TRUE THEN amount ELSE 0 END) AS total_refunds,
        SUM(CASE WHEN is_refund = FALSE THEN amount ELSE -amount END) AS net_revenue,
        AVG(CASE WHEN is_refund = FALSE THEN amount END) AS avg_transaction_amount,
        
        -- Customer metrics
        COUNT(DISTINCT customer_key) AS unique_customers,
        COUNT(DISTINCT merchant_key) AS unique_merchants,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM transactions
    GROUP BY date_key, DATE(created_at)
)

SELECT * FROM daily_metrics
```

### 8.4 DBT Tests et Data Quality

#### Schema Tests (_staging.yml)

```yaml
# models/staging/_staging.yml

version: 2

sources:
  - name: stripe_raw
    database: stripe_dwh
    schema: raw
    tables:
      - name: customers
        description: Raw customer data from OLTP
        columns:
          - name: customer_id
            tests:
              - unique
              - not_null
          - name: email
            tests:
              - unique
              - not_null
      
      - name: transactions
        description: Raw transaction data from OLTP
        freshness:
          warn_after: {count: 6, period: hour}
          error_after: {count: 12, period: hour}
        loaded_at_field: created_at
        columns:
          - name: transaction_id
            tests:
              - unique
              - not_null
          - name: amount
            tests:
              - not_null
              - dbt_utils.accepted_range:
                  min_value: 0
                  inclusive: true

models:
  - name: stg_customers
    description: Staged customer data with cleaned fields
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
      - name: email
        tests:
          - unique
          - not_null
          - dbt_utils.not_empty_string
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
```

#### Custom Tests (assert_positive_amounts.sql)

```sql
-- tests/assert_positive_amounts.sql

SELECT
    transaction_id,
    amount
FROM {{ ref('fact_transactions') }}
WHERE amount < 0
  AND is_refund = FALSE
```

### 8.5 DBT Orchestration dans Airflow

```python
# dags/dbt_transformation_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_daily_transformations',
    default_args=default_args,
    description='Daily DBT transformations for Stripe DWH',
    schedule_interval='0 2 * * *',  # 2 AM daily
    catchup=False,
    tags=['dbt', 'transformation', 'snowflake']
)

# DBT commands
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/dbt/stripe_dbt_project && dbt run --profiles-dir . --target prod',
    dag=dag
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/dbt/stripe_dbt_project && dbt test --profiles-dir . --target prod',
    dag=dag
)

dbt_snapshot = BashOperator(
    task_id='dbt_snapshot',
    bash_command='cd /opt/dbt/stripe_dbt_project && dbt snapshot --profiles-dir . --target prod',
    dag=dag
)

dbt_docs_generate = BashOperator(
    task_id='dbt_docs_generate',
    bash_command='cd /opt/dbt/stripe_dbt_project && dbt docs generate --profiles-dir . --target prod',
    dag=dag
)

# Dependencies
dbt_run >> dbt_test >> dbt_snapshot >> dbt_docs_generate
```

---

## 9. Monitoring et observabilité

### 9.1 Stack de monitoring

```
┌─────────────────────────────────────────────────────────────┐
│                  MONITORING ARCHITECTURE                     │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                   DATA SOURCES                        │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────┐ │   │
│  │  │PostgreSQL│ │  Kafka   │ │ Airflow  │ │Snowflake│ │   │
│  │  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬────┘ │   │
│  └───────┼────────────┼─────────────┼───────────┼───────┘   │
│          │            │             │           │            │
│          │ Metrics    │ Metrics     │ Logs      │ Metrics   │
│          ↓            ↓             ↓           ↓            │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              PROMETHEUS (Metrics Collector)            │  │
│  │  - Scrapes metrics every 15s                          │  │
│  │  - Retention: 15 days                                 │  │
│  │  - PromQL query engine                                │  │
│  └───────────────────┬───────────────────────────────────┘  │
│                      │                                       │
│                      ↓                                       │
│  ┌───────────────────────────────────────────────────────┐  │
│  │                    GRAFANA                             │  │
│  │  - Dashboards                                         │  │
│  │  - Alerting                                           │  │
│  │  - 24/7 monitoring                                    │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              ELK STACK (Logs)                          │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐            │  │
│  │  │Logstash  │→ │Elastic   │→ │  Kibana  │            │  │
│  │  │          │  │ search   │  │          │            │  │
│  │  └──────────┘  └──────────┘  └──────────┘            │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              DATADOG APM                               │  │
│  │  - Distributed tracing                                │  │
│  │  - Performance monitoring                             │  │
│  │  - Custom metrics                                     │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### 9.2 Métriques clés par composant

#### PostgreSQL OLTP Metrics

```yaml
# Prometheus postgresql_exporter

metrics:
  # Connection metrics
  - pg_stat_database_numbackends
  - pg_stat_database_xact_commit
  - pg_stat_database_xact_rollback
  
  # Performance
  - pg_stat_database_blks_read
  - pg_stat_database_blks_hit
  - pg_stat_database_tup_returned
  - pg_stat_database_tup_fetched
  - pg_stat_database_tup_inserted
  - pg_stat_database_tup_updated
  - pg_stat_database_tup_deleted
  
  # Replication lag
  - pg_replication_lag_bytes
  - pg_replication_lag_seconds
  
  # Table metrics
  - pg_stat_user_tables_seq_scan
  - pg_stat_user_tables_idx_scan
  - pg_stat_user_tables_n_tup_ins
  - pg_stat_user_tables_n_tup_upd
  - pg_stat_user_tables_n_tup_del

alerts:
  - name: HighDatabaseConnections
    expr: pg_stat_database_numbackends > 400
    severity: warning
    
  - name: ReplicationLag
    expr: pg_replication_lag_seconds > 60
    severity: critical
    
  - name: HighRollbackRate
    expr: rate(pg_stat_database_xact_rollback[5m]) > 0.1
    severity: warning
```

#### Kafka Metrics

```yaml
# Prometheus jmx_exporter for Kafka

metrics:
  # Broker metrics
  - kafka_server_brokertopicmetrics_messagesinpersec
  - kafka_server_brokertopicmetrics_bytesinpersec
  - kafka_server_brokertopicmetrics_bytesoutpersec
  
  # Consumer lag
  - kafka_consumer_group_lag
  - kafka_consumer_group_lag_seconds
  
  # Topic metrics
  - kafka_log_log_size
  - kafka_log_logendoffset
  
  # Network
  - kafka_network_requestmetrics_requestsperc
  - kafka_network_requestmetrics_totaltimerms

alerts:
  - name: HighConsumerLag
    expr: kafka_consumer_group_lag > 100000
    severity: warning
    
  - name: BrokerDown
    expr: up{job="kafka"} == 0
    severity: critical
    
  - name: UnderReplicatedPartitions
    expr: kafka_server_replicamanager_underreplicatedpartitions > 0
    severity: critical
```

#### Airflow Metrics

```yaml
# Prometheus airflow_exporter

metrics:
  - airflow_dag_run_duration_seconds
  - airflow_dag_run_success_total
  - airflow_dag_run_failed_total
  - airflow_task_instance_duration_seconds
  - airflow_scheduler_heartbeat
  - airflow_executor_open_slots
  - airflow_executor_running_tasks

alerts:
  - name: DAGFailure
    expr: increase(airflow_dag_run_failed_total[1h]) > 0
    severity: warning
    
  - name: SchedulerDown
    expr: time() - airflow_scheduler_heartbeat > 60
    severity: critical
    
  - name: LongRunningDAG
    expr: airflow_dag_run_duration_seconds > 3600
    severity: warning
```

#### Snowflake Metrics

```yaml
# Prometheus snowflake_exporter

metrics:
  - snowflake_warehouse_running_queries
  - snowflake_warehouse_queued_queries
  - snowflake_warehouse_credits_used_total
  - snowflake_table_row_count
  - snowflake_query_execution_time_seconds
  
alerts:
  - name: HighQueryQueueing
    expr: snowflake_warehouse_queued_queries > 10
    severity: warning
    
  - name: HighCreditsUsage
    expr: rate(snowflake_warehouse_credits_used_total[1h]) > 100
    severity: info
```

### 9.3 Grafana Dashboards

#### Dashboard 1: Data Pipeline Overview

```json
{
  "dashboard": {
    "title": "Stripe Data Pipeline - Overview",
    "panels": [
      {
        "title": "OLTP Transactions/sec",
        "targets": [{
          "expr": "rate(pg_stat_database_tup_inserted{datname='stripe_oltp'}[5m])"
        }],
        "type": "graph"
      },
      {
        "title": "Kafka Messages/sec",
        "targets": [{
          "expr": "sum(rate(kafka_server_brokertopicmetrics_messagesinpersec[5m]))"
        }],
        "type": "graph"
      },
      {
        "title": "CDC Lag (seconds)",
        "targets": [{
          "expr": "kafka_consumer_group_lag_seconds{group='debezium-stripe'}"
        }],
        "type": "gauge",
        "thresholds": [
          {"value": 0, "color": "green"},
          {"value": 10, "color": "yellow"},
          {"value": 60, "color": "red"}
        ]
      },
      {
        "title": "Airflow DAG Success Rate",
        "targets": [{
          "expr": "airflow_dag_run_success_total / (airflow_dag_run_success_total + airflow_dag_run_failed_total)"
        }],
        "type": "stat"
      },
      {
        "title": "Snowflake Query Latency P95",
        "targets": [{
          "expr": "histogram_quantile(0.95, snowflake_query_execution_time_seconds_bucket)"
        }],
        "type": "graph"
      }
    ]
  }
}
```

#### Dashboard 2: Data Quality Metrics

```json
{
  "dashboard": {
    "title": "Data Quality Monitoring",
    "panels": [
      {
        "title": "Null Values Rate",
        "targets": [{
          "expr": "dbt_test_null_rate"
        }],
        "type": "graph"
      },
      {
        "title": "Schema Violations",
        "targets": [{
          "expr": "sum(increase(dbt_test_failed_total[24h]))"
        }],
        "type": "stat",
        "alert": {
          "condition": "> 0",
          "message": "Schema validation failures detected"
        }
      },
      {
        "title": "Data Freshness (hours)",
        "targets": [{
          "expr": "(time() - max(fact_transactions_last_loaded_timestamp)) / 3600"
        }],
        "type": "gauge"
      }
    ]
  }
}
```

### 9.4 Alerting Configuration

#### Prometheus Alertmanager Rules

```yaml
# alertmanager.yml

route:
  receiver: 'team-data-engineering'
  group_by: ['alertname', 'severity']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 12h
  
  routes:
    - match:
        severity: critical
      receiver: 'pagerduty-critical'
      continue: true
    
    - match:
        severity: warning
      receiver: 'slack-warnings'

receivers:
  - name: 'team-data-engineering'
    email_configs:
      - to: 'data-eng@stripe.com'
        from: 'alerts@stripe.com'
        smarthost: 'smtp.stripe.com:587'
  
  - name: 'pagerduty-critical'
    pagerduty_configs:
      - service_key: 'xxx'
        description: '{{ .GroupLabels.alertname }}'
  
  - name: 'slack-warnings'
    slack_configs:
      - api_url: 'https://hooks.slack.com/services/xxx'
        channel: '#data-pipeline-alerts'
        title: '{{ .GroupLabels.alertname }}'
        text: '{{ range .Alerts }}{{ .Annotations.summary }}{{ end }}'
```

#### Alert Examples

```yaml
# prometheus_rules.yml

groups:
  - name: data_pipeline_alerts
    interval: 30s
    rules:
      # Critical alerts
      - alert: OLTPDatabaseDown
        expr: up{job="postgresql"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "OLTP PostgreSQL database is down"
          description: "Database {{ $labels.instance }} has been down for more than 1 minute"
      
      - alert: KafkaBrokerDown
        expr: up{job="kafka"} == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Kafka broker is down"
          description: "Broker {{ $labels.instance }} is unreachable"
      
      - alert: HighCDCLag
        expr: kafka_consumer_group_lag_seconds{group="debezium-stripe"} > 300
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "CDC lag exceeds 5 minutes"
          description: "Debezium consumer lag is {{ $value }}s for {{ $labels.topic }}"
      
      # Warning alerts
      - alert: SlowOLTPQueries
        expr: rate(pg_stat_statements_mean_time_ms[5m]) > 1000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Slow OLTP queries detected"
          description: "Average query time is {{ $value }}ms"
      
      - alert: HighAirflowDAGFailureRate
        expr: rate(airflow_dag_run_failed_total[1h]) > 0.1
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "High DAG failure rate"
          description: "{{ $value }} DAG failures per second"
      
      - alert: DataFreshnessIssue
        expr: (time() - fact_transactions_last_loaded_timestamp) > 14400
        for: 30m
        labels:
          severity: warning
        annotations:
          summary: "Data is stale"
          description: "fact_transactions hasn't been updated in 4+ hours"
```

---

## 10. Gestion des erreurs et retry

### 10.1 Stratégies de retry

#### Kafka Producer Retry Configuration

```python
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time

# Kafka Producer with retry
producer = KafkaProducer(
    bootstrap_servers=['kafka1:9092', 'kafka2:9092', 'kafka3:9092'],
    
    # Retry configuration
    retries=5,  # Retry up to 5 times
    retry_backoff_ms=100,  # Wait 100ms between retries
    max_in_flight_requests_per_connection=1,  # Ensure ordering
    
    # Idempotence (exactly-once semantics)
    enable_idempotence=True,
    
    # Acks
    acks='all',  # Wait for all replicas
    
    # Timeouts
    request_timeout_ms=30000,
    
    # Batching (for performance)
    batch_size=16384,
    linger_ms=10
)

# Manual retry with exponential backoff
def send_with_retry(topic, key, value, max_retries=5):
    for attempt in range(max_retries):
        try:
            future = producer.send(topic, key=key, value=value)
            record_metadata = future.get(timeout=10)
            
            print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
            return True
            
        except KafkaError as e:
            wait_time = 2 ** attempt  # Exponential backoff
            print(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
            time.sleep(wait_time)
    
    # All retries failed - send to DLQ
    send_to_dead_letter_queue(topic, key, value)
    return False
```

#### Airflow Task Retry Configuration

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta

def custom_failure_callback(context):
    """Called when task fails after all retries"""
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    exception = context['exception']
    
    # Send notification
    send_email(
        to='data-eng@stripe.com',
        subject=f'Airflow Task Failed: {dag_id}.{task_id}',
        html_content=f"""
        <h3>Task Failed</h3>
        <p><strong>DAG:</strong> {dag_id}</p>
        <p><strong>Task:</strong> {task_id}</p>
        <p><strong>Execution Date:</strong> {execution_date}</p>
        <p><strong>Exception:</strong> {exception}</p>
        """
    )

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    
    # Retry configuration
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    
    # Callbacks
    'on_failure_callback': custom_failure_callback,
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = DAG(
    'etl_with_retry',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
)

# Task with custom retry logic
def extract_data_with_retry(**context):
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            # Extract logic
            data = extract_from_database()
            return data
        except Exception as e:
            if attempt == max_attempts - 1:
                raise
            
            wait_time = 2 ** attempt * 60  # Exponential backoff
            print(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
            time.sleep(wait_time)

task = PythonOperator(
    task_id='extract_with_retry',
    python_callable=extract_data_with_retry,
    dag=dag
)
```

### 10.2 Dead Letter Queue (DLQ)

```python
# Dead Letter Queue for failed messages

def send_to_dead_letter_queue(original_topic, key, value, error=None):
    """Send failed message to DLQ for manual inspection"""
    dlq_topic = f'{original_topic}.dlq'
    
    dlq_message = {
        'original_topic': original_topic,
        'original_key': key,
        'original_value': value,
        'error': str(error),
        'timestamp': datetime.utcnow().isoformat(),
        'retries_exhausted': True
    }
    
    producer.send(
        dlq_topic,
        key=key,
        value=json.dumps(dlq_message).encode('utf-8')
    )
    
    print(f"Message sent to DLQ: {dlq_topic}")
    
    # Alert monitoring system
    increment_dlq_metric(original_topic)
```

### 10.3 Circuit Breaker Pattern

```python
from datetime import datetime, timedelta

class CircuitBreaker:
    def __init__(self, failure_threshold=5, timeout=60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func, *args, **kwargs):
        if self.state == 'OPEN':
            if datetime.now() - self.last_failure_time > timedelta(seconds=self.timeout):
                self.state = 'HALF_OPEN'
                print("Circuit breaker: HALF_OPEN (testing)")
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self.on_success()
            return result
        except Exception as e:
            self.on_failure()
            raise e
    
    def on_success(self):
        self.failure_count = 0
        self.state = 'CLOSED'
    
    def on_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'
            print(f"Circuit breaker: OPEN after {self.failure_count} failures")

# Usage
breaker = CircuitBreaker(failure_threshold=5, timeout=60)

def call_external_api():
    breaker.call(requests.get, 'https://api.example.com/data')
```

---

## 11. Performance et optimisation

### 11.1 Optimisations PostgreSQL OLTP

```sql
-- Partitioning table transaction par date
CREATE TABLE transaction_partitioned (
    transaction_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    amount DECIMAL(12,2),
    created_at TIMESTAMPTZ NOT NULL,
    -- ... autres colonnes
) PARTITION BY RANGE (created_at);

-- Créer partitions mensuelles
CREATE TABLE transaction_2026_01 PARTITION OF transaction_partitioned
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE transaction_2026_02 PARTITION OF transaction_partitioned
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

-- Index sur partitions
CREATE INDEX idx_txn_2026_01_customer ON transaction_2026_01(customer_id);
CREATE INDEX idx_txn_2026_02_customer ON transaction_2026_02(customer_id);

-- Vacuum auto plus agressif
ALTER TABLE transaction_partitioned SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.005
);

-- Connection pooling avec PgBouncer
# pgbouncer.ini
[databases]
stripe_oltp = host=postgres port=5432 dbname=stripe_oltp

[pgbouncer]
listen_addr = *
listen_port = 6432
auth_type = md5
auth_file = /etc/pgbouncer/userlist.txt
pool_mode = transaction
max_client_conn = 1000
default_pool_size = 25
reserve_pool_size = 5
reserve_pool_timeout = 3
```

### 11.2 Optimisations Kafka

```properties
# server.properties - Kafka Broker optimizations

# Storage
log.segment.bytes=1073741824  # 1GB segments
log.retention.hours=168       # 7 days
log.retention.check.interval.ms=300000

# Compression
compression.type=lz4

# Replication
num.replica.fetchers=4
replica.lag.time.max.ms=10000

# Performance
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# Producer
acks=all
min.insync.replicas=2
```

```python
# Kafka Consumer optimization
consumer = KafkaConsumer(
    'stripe.cdc.transactions',
    bootstrap_servers=['kafka1:9092', 'kafka2:9092'],
    
    # Batch processing
    max_poll_records=500,
    fetch_min_bytes=1048576,  # 1MB
    fetch_max_wait_ms=500,
    
    # Offset management
    enable_auto_commit=False,
    auto_offset_reset='earliest',
    
    # Deserialization
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Process in batches
batch = []
for message in consumer:
    batch.append(message.value)
    
    if len(batch) >= 500:
        process_batch(batch)
        consumer.commit()
        batch = []
```

### 11.3 Optimisations Snowflake

```sql
-- Clustering keys for performance
ALTER TABLE fact.transactions CLUSTER BY (date_key, customer_key);

-- Materialized views for frequent queries
CREATE MATERIALIZED VIEW mv_daily_revenue AS
SELECT
    date_key,
    SUM(amount) as total_revenue,
    COUNT(*) as transaction_count
FROM fact.transactions
WHERE is_successful = TRUE
GROUP BY date_key;

-- Auto-refresh every hour
ALTER MATERIALIZED VIEW mv_daily_revenue 
    SET AUTO_SUSPEND = TRUE;

-- Query result caching (enabled by default)
ALTER SESSION SET USE_CACHED_RESULT = TRUE;

-- Warehouse auto-suspend
ALTER WAREHOUSE compute_wh SET 
    AUTO_SUSPEND = 60  -- 1 minute
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Multi-cluster warehouse for concurrency
CREATE WAREHOUSE analytics_wh WITH
    WAREHOUSE_SIZE = 'LARGE'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 5
    SCALING_POLICY = 'STANDARD'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;
```

---

## 12. Sécurité et conformité

### 12.1 Encryption en transit et au repos

```yaml
# Encryption configuration

# PostgreSQL - SSL/TLS
ssl: on
ssl_cert_file: '/etc/ssl/certs/server.crt'
ssl_key_file: '/etc/ssl/private/server.key'
ssl_ca_file: '/etc/ssl/certs/ca.crt'
ssl_ciphers: 'HIGH:MEDIUM:+3DES:!aNULL'
ssl_prefer_server_ciphers: on

# Kafka - SSL/TLS
listeners: SSL://kafka1:9093
security.inter.broker.protocol: SSL
ssl.keystore.location: /var/private/ssl/kafka.keystore.jks
ssl.keystore.password: ${KEYSTORE_PASSWORD}
ssl.key.password: ${KEY_PASSWORD}
ssl.truststore.location: /var/private/ssl/kafka.truststore.jks
ssl.truststore.password: ${TRUSTSTORE_PASSWORD}

# Snowflake - Always encrypted
# Data at rest: AES-256
# Data in transit: TLS 1.2+
```

### 12.2 Accès et authentification

```sql
-- PostgreSQL RBAC
CREATE ROLE data_engineer WITH LOGIN PASSWORD 'xxx';
GRANT CONNECT ON DATABASE stripe_oltp TO data_engineer;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO data_engineer;

CREATE ROLE etl_service WITH LOGIN PASSWORD 'xxx';
GRANT CONNECT ON DATABASE stripe_oltp TO etl_service;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO etl_service;

-- Snowflake RBAC
CREATE ROLE data_analyst;
GRANT USAGE ON WAREHOUSE analytics_wh TO ROLE data_analyst;
GRANT USAGE ON DATABASE stripe_dwh TO ROLE data_analyst;
GRANT USAGE ON SCHEMA dim, fact, agg TO ROLE data_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA dim TO ROLE data_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA fact TO ROLE data_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA agg TO ROLE data_analyst;
```

### 12.3 Audit logging

```sql
-- PostgreSQL audit log
CREATE EXTENSION pgaudit;

ALTER SYSTEM SET pgaudit.log = 'all';
ALTER SYSTEM SET pgaudit.log_catalog = off;
ALTER SYSTEM SET pgaudit.log_parameter = on;

-- Snowflake audit queries
SELECT *
FROM snowflake.account_usage.query_history
WHERE user_name = 'SENSITIVE_USER'
  AND execution_status = 'SUCCESS'
  AND query_type = 'SELECT'
ORDER BY start_time DESC
LIMIT 100;
```

---