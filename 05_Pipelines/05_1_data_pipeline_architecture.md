# Architecture du Pipeline de Données - Stripe Payment Platform
## Document Technique : Orchestration ETL/ELT et Flux Temps Réel

---

## 📋 Table des matières

1. [Vue d'ensemble de l'architecture](#vue-densemble-de-larchitecture)
2. [Technologies et outils](#technologies-et-outils)
3. [Architecture ETL (OLTP → OLAP)](#architecture-etl-oltp--olap)
4. [Architecture ELT (NoSQL → OLAP)](#architecture-elt-nosql--olap)
5. [Streaming temps réel avec Kafka](#streaming-temps-réel-avec-kafka)
6. [Orchestration avec Airflow](#orchestration-avec-airflow)
7. [Change Data Capture (CDC)](#change-data-capture-cdc)
8. [Transformations de données](#transformations-de-données)
9. [Monitoring et observabilité](#monitoring-et-observabilité)
10. [Gestion des erreurs et retry](#gestion-des-erreurs-et-retry)
11. [Performance et optimisation](#performance-et-optimisation)
12. [Sécurité et conformité](#sécurité-et-conformité)

---

## 1. Vue d'ensemble de l'architecture

### 1.1 Architecture globale

```
┌────────────────────────────────────────────────────────────────────────────┐
│                        STRIPE DATA PIPELINE ARCHITECTURE                    │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────┐          ┌─────────────────┐          ┌──────────────┐   │
│  │ Data Sources│          │  Ingestion      │          │   Storage    │   │
│  └─────────────┘          └─────────────────┘          └──────────────┘   │
│                                                                              │
│  ┌──────────┐                                                               │
│  │   OLTP   │────┐                                                          │
│  │ Postgres │    │                                                          │
│  └──────────┘    │         ┌──────────────┐                                │
│                  ├────────→│   Debezium   │                                │
│  ┌──────────┐    │         │     CDC      │                                │
│  │  NoSQL   │────┘         └──────────────┘                                │
│  │ MongoDB  │                      ↓                                        │
│  └──────────┘              ┌──────────────┐             ┌──────────────┐   │
│                            │    Kafka     │────────────→│     OLAP     │   │
│  ┌──────────┐              │   Cluster    │             │  Snowflake   │   │
│  │   APIs   │─────────────→│              │             │     DWH      │   │
│  │ External │              └──────────────┘             └──────────────┘   │
│  └──────────┘                      ↓                            ↑          │
│                            ┌──────────────┐                     │          │
│                            │   Kafka      │                     │          │
│                            │   Connect    │                     │          │
│                            └──────────────┘                     │          │
│                                    ↓                             │          │
│                            ┌──────────────┐                     │          │
│                            │   Airflow    │─────────────────────┘          │
│                            │ Orchestrator │                                │
│                            └──────────────┘                                │
│                                    ↓                                        │
│                     ┌──────────────┴──────────────┐                        │
│                     ↓                              ↓                        │
│            ┌──────────────┐              ┌──────────────┐                  │
│            │  ETL Process │              │  ELT Process │                  │
│            │   (Batch)    │              │  (Streaming) │                  │
│            └──────────────┘              └──────────────┘                  │
│                     ↓                              ↓                        │
│            ┌──────────────┐              ┌──────────────┐                  │
│            │ Data Quality │              │   Real-time  │                  │
│            │   Checks     │              │  Analytics   │                  │
│            └──────────────┘              └──────────────┘                  │
│                     ↓                              ↓                        │
│                     └──────────────┬───────────────┘                        │
│                                    ↓                                        │
│                            ┌──────────────┐                                │
│                            │      BI      │                                │
│                            │    Tools     │                                │
│                            │ PowerBI/     │                                │
│                            │  Tableau     │                                │
│                            └──────────────┘                                │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────┐           │
│  │                   Monitoring & Observability                 │           │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │           │
│  │  │Prometheus│  │ Grafana  │  │  ELK     │  │ DataDog  │   │           │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │           │
│  └─────────────────────────────────────────────────────────────┘           │
│                                                                              │
└────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Flux de données principaux

#### Flux 1 : OLTP → OLAP (ETL Batch)
```
Postgres → Debezium CDC → Kafka → Airflow ETL → Staging → Transformation → Snowflake
```
- **Fréquence** : Horaire / Quotidien selon les tables
- **Volume** : 10M+ transactions/jour
- **Latence** : 15-60 minutes
- **Use case** : Rapports historiques, Analytics batch

#### Flux 2 : NoSQL → OLAP (ELT Streaming)
```
MongoDB → Change Streams → Kafka → Kafka Connect → Snowflake → DBT transformation
```
- **Fréquence** : Temps réel / Micro-batch (1-5 min)
- **Volume** : 100M+ événements/jour
- **Latence** : < 5 minutes
- **Use case** : Analytics temps réel, ML features

#### Flux 3 : OLTP → NoSQL (CDC Real-time)
```
Postgres → Debezium → Kafka → Consumer → Feature Engineering → MongoDB
```
- **Fréquence** : Temps réel continu
- **Volume** : 50M+ événements/jour
- **Latence** : < 1 seconde
- **Use case** : Détection fraude, ML inference

---

## 2. Technologies et outils

### 2.1 Stack technologique

| Composant | Technologie | Version | Rôle |
|-----------|-------------|---------|------|
| **Source OLTP** | PostgreSQL | 15.x | Base transactionnelle |
| **Source NoSQL** | MongoDB | 7.0+ | Données semi-structurées |
| **Target OLAP** | Snowflake | Enterprise | Data Warehouse |
| **Streaming Platform** | Apache Kafka | 3.6+ | Message broker |
| **CDC** | Debezium | 2.5+ | Change Data Capture |
| **Orchestrator** | Apache Airflow | 2.8+ | Workflow management |
| **Transform** | DBT (Data Build Tool) | 1.7+ | Data transformation |
| **Object Storage** | AWS S3 | - | Staging & archives |
| **Monitoring** | Prometheus + Grafana | Latest | Metrics & visualization |
| **Logging** | ELK Stack | 8.x | Centralized logging |
| **Container** | Kubernetes | 1.28+ | Orchestration |

### 2.2 Justification des choix

#### Apache Kafka
**Pourquoi :**
- Throughput élevé (millions de messages/sec)
- Durabilité avec réplication
- Support CDC natif via Kafka Connect
- Découplage producteurs/consommateurs
- Replay des événements pour recovery

**Configuration :**
```yaml
kafka:
  brokers: 6 brokers (3 par datacenter)
  replication_factor: 3
  min_insync_replicas: 2
  retention: 7 jours (168h)
  partitions: 24-48 par topic (selon volume)
  compression: snappy
```

#### Apache Airflow
**Pourquoi :**
- Orchestration complexe avec DAGs
- Gestion des dépendances
- Retry automatique et alerting
- Interface UI intuitive
- Scalabilité horizontale avec Celery/Kubernetes

**Architecture :**
```
┌────────────────────────────────────────┐
│         Airflow Architecture           │
├────────────────────────────────────────┤
│                                        │
│  ┌──────────────┐   ┌──────────────┐  │
│  │  Web Server  │   │  Scheduler   │  │
│  └──────────────┘   └──────────────┘  │
│          ↓                  ↓          │
│  ┌──────────────────────────────────┐ │
│  │      Metadata DB (Postgres)      │ │
│  └──────────────────────────────────┘ │
│          ↓                             │
│  ┌──────────────────────────────────┐ │
│  │    Executor (Kubernetes)         │ │
│  │  ┌────┐ ┌────┐ ┌────┐ ┌────┐    │ │
│  │  │Pod1│ │Pod2│ │Pod3│ │Pod4│    │ │
│  │  └────┘ └────┘ └────┘ └────┘    │ │
│  └──────────────────────────────────┘ │
│                                        │
└────────────────────────────────────────┘
```

#### Debezium
**Pourquoi :**
- CDC low-latency (< 100ms)
- Support PostgreSQL natif
- Intégration Kafka Connect
- Schema evolution management
- Exactly-once semantics

#### DBT (Data Build Tool)
**Pourquoi :**
- Transformations SQL versionnées
- Tests de qualité intégrés
- Documentation automatique
- Lineage tracking
- Incremental models

---

## 3. Architecture ETL (OLTP → OLAP)

### 3.1 Architecture détaillée

```
┌──────────────────────────────────────────────────────────────────────┐
│                         ETL Pipeline Architecture                     │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  STEP 1: EXTRACTION                                                   │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │  PostgreSQL OLTP                                             │     │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐       │     │
│  │  │customers │ │merchants │ │ products │ │transactions│      │     │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘       │     │
│  │       ↓             ↓             ↓             ↓            │     │
│  │  ┌────────────────────────────────────────────────┐         │     │
│  │  │         Debezium Source Connector              │         │     │
│  │  │  - Logical Replication Slot                    │         │     │
│  │  │  - WAL (Write-Ahead Log) reading               │         │     │
│  │  └────────────────────────────────────────────────┘         │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                              ↓                                        │
│  STEP 2: STREAMING                                                    │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │  Kafka Topics (Partitioned)                                  │     │
│  │  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐  │     │
│  │  │ customers-cdc  │ │ merchants-cdc  │ │transactions-cdc│  │     │
│  │  │  (24 parts)    │ │  (12 parts)    │ │  (48 parts)    │  │     │
│  │  └────────────────┘ └────────────────┘ └────────────────┘  │     │
│  │                                                              │     │
│  │  Format: Avro with Schema Registry                          │     │
│  │  Retention: 7 days                                           │     │
│  │  Replication: 3x                                             │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                              ↓                                        │
│  STEP 3: TRANSFORMATION                                               │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │  Airflow DAG: etl_oltp_to_olap                               │     │
│  │                                                               │     │
│  │  Task 1: extract_from_kafka                                  │     │
│  │    ├─→ Read Kafka topics                                     │     │
│  │    ├─→ Deserialize Avro                                      │     │
│  │    └─→ Write to S3 staging (Parquet)                         │     │
│  │                                                               │     │
│  │  Task 2: validate_data                                       │     │
│  │    ├─→ Schema validation                                     │     │
│  │    ├─→ Business rules checks                                 │     │
│  │    ├─→ Duplicate detection                                   │     │
│  │    └─→ Data quality metrics                                  │     │
│  │                                                               │     │
│  │  Task 3: transform_data                                      │     │
│  │    ├─→ Data cleansing                                        │     │
│  │    ├─→ Type conversions                                      │     │
│  │    ├─→ Business logic                                        │     │
│  │    ├─→ Surrogate key generation                              │     │
│  │    └─→ Dimension lookups                                     │     │
│  │                                                               │     │
│  │  Task 4: load_dimensions                                     │     │
│  │    ├─→ SCD Type 2 for dims                                   │     │
│  │    ├─→ Merge/Upsert logic                                    │     │
│  │    └─→ Integrity checks                                      │     │
│  │                                                               │     │
│  │  Task 5: load_facts                                          │     │
│  │    ├─→ Fact table inserts                                    │     │
│  │    ├─→ Batch optimization                                    │     │
│  │    └─→ Transaction wrapping                                  │     │
│  │                                                               │     │
│  │  Task 6: update_aggregates                                   │     │
│  │    ├─→ Refresh materialized views                            │     │
│  │    ├─→ Update summary tables                                 │     │
│  │    └─→ Recalculate metrics                                   │     │
│  │                                                               │     │
│  │  Task 7: data_quality_checks                                 │     │
│  │    ├─→ Row count validation                                  │     │
│  │    ├─→ Referential integrity                                 │     │
│  │    ├─→ Business metrics validation                           │     │
│  │    └─→ Alerting on failures                                  │     │
│  │                                                               │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                              ↓                                        │
│  STEP 4: LOADING                                                      │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │  Snowflake Data Warehouse                                    │     │
│  │  ┌──────────────────────────────────────────────────────┐   │     │
│  │  │  Database: STRIPE_DWH                                │   │     │
│  │  │                                                        │   │     │
│  │  │  Schema: DIM (Dimensions)                             │   │     │
│  │  │    - dim.customer                                     │   │     │
│  │  │    - dim.merchant                                     │   │     │
│  │  │    - dim.product                                      │   │     │
│  │  │    - dim.payment_method                               │   │     │
│  │  │    - dim.date                                         │   │     │
│  │  │    - dim.time                                         │   │     │
│  │  │                                                        │   │     │
│  │  │  Schema: FACT (Facts)                                 │   │     │
│  │  │    - fact.transactions                                │   │     │
│  │  │    - fact.fraud_scores                                │   │     │
│  │  │                                                        │   │     │
│  │  │  Schema: AGG (Aggregates)                             │   │     │
│  │  │    - agg.revenue_daily                                │   │     │
│  │  │    - agg.customer_segmentation                        │   │     │
│  │  │    - agg.fraud_analysis_summary                       │   │     │
│  │  └──────────────────────────────────────────────────────┘   │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                                                                        │
└──────────────────────────────────────────────────────────────────────┘
```

### 3.2 Code Airflow DAG - ETL Pipeline

```python
"""
Stripe OLTP to OLAP ETL Pipeline
DAG: etl_oltp_to_olap_daily
Schedule: Daily at 2:00 AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka-broker-1:9092,kafka-broker-2:9092"
S3_BUCKET = "stripe-data-staging"
SNOWFLAKE_CONN_ID = "snowflake_default"
KAFKA_TOPICS = [
    "stripe.public.customers",
    "stripe.public.merchants",
    "stripe.public.products",
    "stripe.public.payment_methods",
    "stripe.public.transaction",
    "stripe.public.fraud"
]

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email': ['data-alerts@stripe.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

dag = DAG(
    'etl_oltp_to_olap_daily',
    default_args=default_args,
    description='Daily ETL from PostgreSQL OLTP to Snowflake OLAP',
    schedule_interval='0 2 * * *',  # 2 AM daily
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'oltp', 'olap', 'daily']
)


# ============================================================================
# TASK 1: Extract from Kafka to S3 Staging
# ============================================================================

def extract_from_kafka(**context):
    """
    Extract CDC events from Kafka topics and write to S3 in Parquet format
    """
    from kafka import KafkaConsumer
    import pandas as pd
    import io
    
    execution_date = context['execution_date']
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    for topic in KAFKA_TOPICS:
        table_name = topic.split('.')[-1]
        
        # Create Kafka consumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=f'airflow-etl-{execution_date.date()}',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        records = []
        message_count = 0
        
        # Consume messages for the execution date
        for message in consumer:
            event_data = message.value
            
            # Filter by execution date
            event_timestamp = datetime.fromisoformat(
                event_data['payload']['source']['ts_ms']
            )
            
            if event_timestamp.date() == execution_date.date():
                # Extract the after state (new/updated record)
                after = event_data['payload'].get('after')
                if after:
                    records.append(after)
                    message_count += 1
        
        consumer.close()
        
        if records:
            # Convert to DataFrame
            df = pd.DataFrame(records)
            
            # Write to S3 as Parquet
            s3_key = f"staging/{execution_date.date()}/{table_name}.parquet"
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy')
            
            s3_hook.load_bytes(
                bytes_data=parquet_buffer.getvalue(),
                key=s3_key,
                bucket_name=S3_BUCKET,
                replace=True
            )
            
            print(f"✅ Extracted {message_count} records from {topic} to s3://{S3_BUCKET}/{s3_key}")
        else:
            print(f"⚠️  No records found for {topic} on {execution_date.date()}")
    
    return {"status": "success", "execution_date": str(execution_date.date())}


extract_task = PythonOperator(
    task_id='extract_from_kafka',
    python_callable=extract_from_kafka,
    provide_context=True,
    dag=dag
)


# ============================================================================
# TASK 2: Data Validation
# ============================================================================

def validate_staging_data(**context):
    """
    Validate data quality in S3 staging area
    """
    import pandas as pd
    from io import BytesIO
    
    execution_date = context['execution_date']
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    validation_results = {}
    
    for topic in KAFKA_TOPICS:
        table_name = topic.split('.')[-1]
        s3_key = f"staging/{execution_date.date()}/{table_name}.parquet"
        
        # Read from S3
        try:
            obj = s3_hook.get_key(key=s3_key, bucket_name=S3_BUCKET)
            parquet_bytes = obj.get()['Body'].read()
            df = pd.read_parquet(BytesIO(parquet_bytes))
            
            # Validation checks
            checks = {
                'row_count': len(df),
                'null_check': df.isnull().sum().to_dict(),
                'duplicate_check': df.duplicated().sum(),
                'schema_check': list(df.columns)
            }
            
            # Business rule validations
            if table_name == 'transaction':
                checks['negative_amount'] = (df['amount'] < 0).sum()
                checks['invalid_currency'] = (~df['currency'].isin(['USD', 'EUR', 'GBP'])).sum()
            
            validation_results[table_name] = checks
            
            # Alert if critical issues
            if checks['duplicate_check'] > 0:
                print(f"⚠️  WARNING: {checks['duplicate_check']} duplicates in {table_name}")
            
            if table_name == 'transaction' and checks.get('negative_amount', 0) > 0:
                raise ValueError(f"❌ CRITICAL: Negative amounts found in transactions: {checks['negative_amount']}")
            
            print(f"✅ Validation passed for {table_name}: {checks['row_count']} rows")
            
        except Exception as e:
            print(f"❌ Validation failed for {table_name}: {str(e)}")
            raise
    
    return validation_results


validate_task = PythonOperator(
    task_id='validate_staging_data',
    python_callable=validate_staging_data,
    provide_context=True,
    dag=dag
)


# ============================================================================
# TASK 3: Transform Data (DBT)
# ============================================================================

from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

transform_dbt = DbtCloudRunJobOperator(
    task_id='transform_with_dbt',
    dbt_cloud_conn_id='dbt_cloud_default',
    job_id=12345,  # DBT Cloud job ID
    check_interval=60,
    timeout=3600,
    dag=dag
)


# ============================================================================
# TASK 4: Load Dimensions (SCD Type 2)
# ============================================================================

load_dim_customer = SnowflakeOperator(
    task_id='load_dim_customer',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql="""
    -- SCD Type 2 for dim_customer
    MERGE INTO dim.customer AS target
    USING (
        SELECT 
            customer_id,
            name,
            first_name,
            full_address,
            post_code,
            phone,
            email,
            created_at,
            CURRENT_TIMESTAMP() as updated_at
        FROM staging.customers_stg
        WHERE execution_date = '{{ ds }}'
    ) AS source
    ON target.customer_id = source.customer_id 
       AND target.is_current = TRUE
    
    -- Expire old record if data changed
    WHEN MATCHED AND (
        target.name != source.name OR
        target.email != source.email OR
        target.phone != source.phone
    ) THEN UPDATE SET
        target.is_current = FALSE,
        target.valid_to = CURRENT_TIMESTAMP()
    
    -- Insert new version if data changed
    WHEN NOT MATCHED THEN INSERT (
        customer_id, name, first_name, full_address, post_code, 
        phone, email, created_at, updated_at, is_current, valid_from, valid_to
    ) VALUES (
        source.customer_id, source.name, source.first_name, source.full_address,
        source.post_code, source.phone, source.email, source.created_at,
        source.updated_at, TRUE, CURRENT_TIMESTAMP(), '9999-12-31'
    );
    
    -- Insert new version for changed records
    INSERT INTO dim.customer (
        customer_id, name, first_name, full_address, post_code, 
        phone, email, created_at, updated_at, is_current, valid_from, valid_to
    )
    SELECT 
        s.customer_id, s.name, s.first_name, s.full_address,
        s.post_code, s.phone, s.email, s.created_at,
        s.updated_at, TRUE, CURRENT_TIMESTAMP(), '9999-12-31'
    FROM staging.customers_stg s
    INNER JOIN dim.customer t 
        ON s.customer_id = t.customer_id 
        AND t.is_current = FALSE
        AND t.valid_to = CURRENT_TIMESTAMP();
    """,
    dag=dag
)


# Similar operators for other dimensions
load_dim_merchant = SnowflakeOperator(
    task_id='load_dim_merchant',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql='sql/load_dim_merchant.sql',
    dag=dag
)

load_dim_product = SnowflakeOperator(
    task_id='load_dim_product',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql='sql/load_dim_product.sql',
    dag=dag
)


# ============================================================================
# TASK 5: Load Facts
# ============================================================================

load_fact_transactions = SnowflakeOperator(
    task_id='load_fact_transactions',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql="""
    -- Load fact_transactions with surrogate keys
    INSERT INTO fact.transactions (
        transaction_id, customer_key, merchant_key, product_key,
        payment_method_key, date_key, time_key, location_key,
        amount, currency, status, transaction_type, device_type,
        is_successful, is_refund, created_at
    )
    SELECT 
        t.transaction_id,
        c.customer_key,
        m.merchant_key,
        p.product_key,
        pm.payment_method_key,
        d.date_key,
        tm.time_key,
        l.location_key,
        t.amount,
        t.currency,
        t.status,
        t.transaction_type,
        t.device_type,
        CASE WHEN t.status = 'completed' THEN TRUE ELSE FALSE END as is_successful,
        CASE WHEN t.transaction_type = 'refund' THEN TRUE ELSE FALSE END as is_refund,
        t.created_at
    FROM staging.transactions_stg t
    LEFT JOIN dim.customer c 
        ON t.customer_id = c.customer_id AND c.is_current = TRUE
    LEFT JOIN dim.merchant m 
        ON t.merchant_id = m.merchant_id AND m.is_current = TRUE
    LEFT JOIN dim.product p 
        ON t.product_id = p.product_id AND p.is_current = TRUE
    LEFT JOIN dim.payment_method pm 
        ON t.payment_method_id = pm.payment_method_id AND pm.is_current = TRUE
    LEFT JOIN dim.date d 
        ON DATE(t.created_at) = d.full_date
    LEFT JOIN dim.time tm 
        ON tm.time_key = TO_NUMBER(TO_CHAR(t.created_at, 'HH24MISS'))
    LEFT JOIN dim.location l 
        ON t.location = l.location
    WHERE t.execution_date = '{{ ds }}'
      AND t.transaction_id NOT IN (SELECT transaction_id FROM fact.transactions);
    """,
    dag=dag
)


# ============================================================================
# TASK 6: Update Aggregates
# ============================================================================

update_revenue_aggregates = SnowflakeOperator(
    task_id='update_revenue_aggregates',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql="""
    -- Refresh daily revenue aggregates
    MERGE INTO agg.revenue_daily AS target
    USING (
        SELECT 
            d.date_key,
            d.full_date as date,
            COUNT(*) as total_transactions,
            SUM(CASE WHEN t.is_refund = FALSE THEN t.amount ELSE 0 END) as total_revenue,
            SUM(CASE WHEN t.is_refund = TRUE THEN t.amount ELSE 0 END) as total_refunds,
            SUM(CASE WHEN t.is_refund = FALSE THEN t.amount ELSE -t.amount END) as net_revenue,
            AVG(CASE WHEN t.is_refund = FALSE THEN t.amount END) as avg_transaction_amount,
            COUNT(DISTINCT t.customer_key) as unique_customers,
            COUNT(DISTINCT t.merchant_key) as unique_merchants,
            CURRENT_TIMESTAMP() as calculated_at
        FROM fact.transactions t
        JOIN dim.date d ON t.date_key = d.date_key
        WHERE d.full_date = '{{ ds }}'
        GROUP BY d.date_key, d.full_date
    ) AS source
    ON target.date_key = source.date_key
    WHEN MATCHED THEN UPDATE SET
        target.total_transactions = source.total_transactions,
        target.total_revenue = source.total_revenue,
        target.total_refunds = source.total_refunds,
        target.net_revenue = source.net_revenue,
        target.avg_transaction_amount = source.avg_transaction_amount,
        target.unique_customers = source.unique_customers,
        target.unique_merchants = source.unique_merchants,
        target.calculated_at = source.calculated_at
    WHEN NOT MATCHED THEN INSERT (
        date_key, date, total_transactions, total_revenue, total_refunds,
        net_revenue, avg_transaction_amount, unique_customers, unique_merchants, calculated_at
    ) VALUES (
        source.date_key, source.date, source.total_transactions, source.total_revenue,
        source.total_refunds, source.net_revenue, source.avg_transaction_amount,
        source.unique_customers, source.unique_merchants, source.calculated_at
    );
    """,
    dag=dag
)


# ============================================================================
# TASK 7: Data Quality Checks
# ============================================================================

def data_quality_final_checks(**context):
    """
    Final data quality validation in Snowflake
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    execution_date = context['execution_date']
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    checks = []
    
    # Check 1: Row counts match
    query = f"""
    SELECT 
        'transactions' as table_name,
        COUNT(*) as row_count
    FROM fact.transactions
    WHERE DATE(created_at) = '{execution_date.date()}'
    """
    result = hook.get_first(query)
    checks.append({"check": "transaction_count", "value": result[1]})
    
    # Check 2: No orphan records
    query = """
    SELECT COUNT(*) 
    FROM fact.transactions t
    LEFT JOIN dim.customer c ON t.customer_key = c.customer_key
    WHERE c.customer_key IS NULL
      AND DATE(t.created_at) = '{{ ds }}'
    """
    result = hook.get_first(query)
    if result[0] > 0:
        raise ValueError(f"❌ Found {result[0]} orphan transactions without customers")
    
    # Check 3: Revenue totals
    query = """
    SELECT 
        SUM(amount) as fact_total,
        (SELECT total_revenue FROM agg.revenue_daily WHERE date = '{{ ds }}') as agg_total
    FROM fact.transactions
    WHERE DATE(created_at) = '{{ ds }}'
      AND is_refund = FALSE
    """
    result = hook.get_first(query)
    if abs(result[0] - result[1]) > 0.01:  # Allow 1 cent difference for rounding
        raise ValueError(f"❌ Revenue mismatch: Fact={result[0]}, Agg={result[1]}")
    
    print("✅ All data quality checks passed")
    return checks


quality_checks = PythonOperator(
    task_id='data_quality_final_checks',
    python_callable=data_quality_final_checks,
    provide_context=True,
    dag=dag
)


# ============================================================================
# Task Dependencies
# ============================================================================

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Linear flow
start >> extract_task >> validate_task >> transform_dbt

# Parallel dimension loading
with TaskGroup('load_dimensions', dag=dag) as load_dims:
    load_dim_customer
    load_dim_merchant
    load_dim_product

transform_dbt >> load_dims >> load_fact_transactions >> update_revenue_aggregates >> quality_checks >> end
```

### 3.3 Stratégies d'optimisation ETL

#### Incremental Loading
```sql
-- Exemple: Chargement incrémental avec high-water mark
CREATE OR REPLACE TABLE staging.watermarks (
    table_name VARCHAR(100) PRIMARY KEY,
    last_processed_timestamp TIMESTAMP_NTZ,
    last_processed_id BIGINT,
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Query avec water mark
SELECT *
FROM source_table
WHERE updated_at > (
    SELECT last_processed_timestamp 
    FROM staging.watermarks 
    WHERE table_name = 'source_table'
)
ORDER BY updated_at;
```

#### Parallel Processing
```python
# Configuration Airflow pour parallélisme
dag = DAG(
    'etl_oltp_to_olap',
    max_active_runs=1,  # Un seul run du DAG à la fois
    concurrency=16,      # Max 16 tasks en parallèle
    default_args={
        'pool': 'etl_pool',  # Pool dédié avec 20 slots
    }
)

# Utiliser Dynamic Task Mapping (Airflow 2.3+)
@task
def process_partition(partition_id: int):
    # Process one partition
    pass

process_partitions = process_partition.expand(
    partition_id=list(range(24))  # 24 partitions en parallèle
)
```

---

## 4. Architecture ELT (NoSQL → OLAP)

### 4.1 Architecture détaillée

```
┌──────────────────────────────────────────────────────────────────────┐
│                      ELT Pipeline Architecture                        │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  STEP 1: STREAMING EXTRACTION                                         │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │  MongoDB (NoSQL)                                             │     │
│  │  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐│     │
│  │  │  user_         │  │    sessions    │  │     fraud_     ││     │
│  │  │  interactions  │  │                │  │    features    ││     │
│  │  └────────────────┘  └────────────────┘  └────────────────┘│     │
│  │         ↓                    ↓                    ↓         │     │
│  │  ┌────────────────────────────────────────────────────────┐│     │
│  │  │         MongoDB Change Streams                         ││     │
│  │  │  - Resume token for fault tolerance                   ││     │
│  │  │  - Full document on update                            ││     │
│  │  └────────────────────────────────────────────────────────┘│     │
│  └─────────────────────────────────────────────────────────────┘     │
│                              ↓                                        │
│  STEP 2: KAFKA STREAMING                                              │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │  Kafka Topics                                                │     │
│  │  ┌─────────────────┐ ┌──────────────┐ ┌──────────────┐     │     │
│  │  │ mongo-user-     │ │ mongo-       │ │ mongo-fraud- │     │     │
│  │  │ interactions    │ │ sessions     │ │ features     │     │     │
│  │  └─────────────────┘ └──────────────┘ └──────────────┘     │     │
│  │                                                              │     │
│  │  Format: JSON (native MongoDB documents)                    │     │
│  │  Partitioning: By user_id / session_id                      │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                              ↓                                        │
│  STEP 3: LOADING (Raw)                                                │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │  Kafka Connect Snowflake Sink                               │     │
│  │    ├─→ Micro-batch every 60 seconds                         │     │
│  │    ├─→ Auto-schema evolution                                │     │
│  │    └─→ VARIANT data type for JSON                           │     │
│  │                                                               │     │
│  │  Snowflake Staging Tables (RAW)                             │     │
│  │    ├─→ raw.user_interactions_stream                         │     │
│  │    ├─→ raw.sessions_stream                                  │     │
│  │    └─→ raw.fraud_features_stream                            │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                              ↓                                        │
│  STEP 4: TRANSFORMATION (DBT)                                         │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │  DBT Models                                                  │     │
│  │                                                               │     │
│  │  Staging Models (Extract JSON)                              │     │
│  │    ├─→ stg_user_interactions                                │     │
│  │    ├─→ stg_sessions                                         │     │
│  │    └─→ stg_fraud_features                                   │     │
│  │                                                               │     │
│  │  Intermediate Models (Business Logic)                       │     │
│  │    ├─→ int_user_behavior_metrics                            │     │
│  │    ├─→ int_session_funnels                                  │     │
│  │    └─→ int_fraud_risk_scores                                │     │
│  │                                                               │     │
│  │  Mart Models (Analytics Ready)                              │     │
│  │    ├─→ mart_user_analytics                                  │     │
│  │    ├─→ mart_conversion_funnels                              │     │
│  │    └─→ mart_fraud_analytics                                 │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                              ↓                                        │
│  STEP 5: ANALYTICS LAYER                                              │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │  Snowflake Analytics Tables                                  │     │
│  │  ┌──────────────────────────────────────────────────────┐   │     │
│  │  │  Schema: ANALYTICS                                    │   │     │
│  │  │    - user_behavior_daily                              │   │     │
│  │  │    - conversion_metrics                               │   │     │
│  │  │    - fraud_detection_summary                          │   │     │
│  │  │    - ml_feature_store                                 │   │     │
│  │  └──────────────────────────────────────────────────────┘   │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                                                                        │
└──────────────────────────────────────────────────────────────────────┘
```

### 4.2 MongoDB Change Streams Implementation

```javascript
// change_stream_producer.js
// Publier les change streams MongoDB vers Kafka

const { MongoClient } = require('mongodb');
const { Kafka } = require('kafkajs');

const mongoUri = 'mongodb://mongo-cluster:27017/analytics_db';
const kafkaConfig = {
  clientId: 'mongodb-change-stream-producer',
  brokers: ['kafka-broker-1:9092', 'kafka-broker-2:9092']
};

const mongo = new MongoClient(mongoUri);
const kafka = new Kafka(kafkaConfig);
const producer = kafka.producer();

async function startChangeStream(collectionName, topicName) {
  await mongo.connect();
  await producer.connect();
  
  const db = mongo.db('analytics_db');
  const collection = db.collection(collectionName);
  
  // Pipeline pour filtrer les événements
  const pipeline = [
    {
      $match: {
        operationType: { $in: ['insert', 'update', 'replace'] }
      }
    }
  ];
  
  // Options avec resume token
  const options = {
    fullDocument: 'updateLookup',  // Include full document
    resumeAfter: await getLastResumeToken(collectionName)
  };
  
  const changeStream = collection.watch(pipeline, options);
  
  console.log(`📡 Watching collection: ${collectionName}`);
  
  changeStream.on('change', async (change) => {
    try {
      const message = {
        key: change.documentKey._id.toString(),
        value: JSON.stringify({
          operation: change.operationType,
          timestamp: change.clusterTime,
          document: change.fullDocument,
          resumeToken: change._id
        })
      };
      
      await producer.send({
        topic: topicName,
        messages: [message]
      });
      
      // Save resume token for recovery
      await saveResumeToken(collectionName, change._id);
      
      console.log(`✅ Published change from ${collectionName} to ${topicName}`);
    } catch (error) {
      console.error(`❌ Error publishing change:`, error);
    }
  });
  
  changeStream.on('error', async (error) => {
    console.error(`❌ Change stream error:`, error);
    // Implement retry logic
    await new Promise(resolve => setTimeout(resolve, 5000));
    startChangeStream(collectionName, topicName);
  });
}

// Start watching multiple collections
Promise.all([
  startChangeStream('user_interactions', 'mongo-user-interactions'),
  startChangeStream('sessions', 'mongo-sessions'),
  startChangeStream('fraud_features', 'mongo-fraud-features'),
  startChangeStream('ml_predictions', 'mongo-ml-predictions')
]).catch(console.error);
```

### 4.3 Kafka Connect Snowflake Sink Configuration

```json
{
  "name": "snowflake-sink-nosql",
  "config": {
    "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "tasks.max": "8",
    "topics": "mongo-user-interactions,mongo-sessions,mongo-fraud-features",
    
    "snowflake.url.name": "stripe.snowflakecomputing.com",
    "snowflake.user.name": "KAFKA_CONNECTOR_USER",
    "snowflake.private.key": "${file:/secrets/snowflake-key.pem}",
    "snowflake.database.name": "STRIPE_DWH",
    "snowflake.schema.name": "RAW",
    
    "buffer.count.records": "10000",
    "buffer.flush.time": "60",
    "buffer.size.bytes": "5000000",
    
    "snowflake.topic2table.map": "mongo-user-interactions:user_interactions_stream,mongo-sessions:sessions_stream,mongo-fraud-features:fraud_features_stream",
    
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "com.snowflake.kafka.connector.records.SnowflakeJsonConverter",
    
    "snowflake.metadata.createtime": "true",
    "snowflake.metadata.topic": "true",
    "snowflake.metadata.offset.and.partition": "true",
    
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",
    "errors.deadletterqueue.topic.name": "dlq-snowflake-sink",
    "errors.deadletterqueue.topic.replication.factor": "3"
  }
}
```

### 4.4 DBT Models pour transformation ELT

```sql
-- models/staging/stg_user_interactions.sql
-- Extract and flatten MongoDB JSON documents

WITH source AS (
    SELECT 
        RECORD_CONTENT as raw_data,
        RECORD_METADATA
    FROM {{ source('raw', 'user_interactions_stream') }}
    WHERE RECORD_METADATA:CreateTime::TIMESTAMP > 
          {{ var('lookback_hours', 24) }} * INTERVAL '1 HOUR'
),

flattened AS (
    SELECT
        raw_data:_id::STRING as interaction_id,
        raw_data:timestamp::TIMESTAMP as interaction_timestamp,
        raw_data:session_id::STRING as session_id,
        raw_data:user_id::STRING as user_id,
        raw_data:merchant_id::STRING as merchant_id,
        raw_data:event_type::STRING as event_type,
        raw_data:event_name::STRING as event_name,
        
        -- Page info
        raw_data:page.url::STRING as page_url,
        raw_data:page.referrer::STRING as page_referrer,
        raw_data:page.title::STRING as page_title,
        
        -- Device info
        raw_data:device.type::STRING as device_type,
        raw_data:device.os::STRING as device_os,
        raw_data:device.browser::STRING as device_browser,
        
        -- Geolocation
        raw_data:geolocation.country::STRING as country,
        raw_data:geolocation.city::STRING as city,
        raw_data:geolocation.latitude::FLOAT as latitude,
        raw_data:geolocation.longitude::FLOAT as longitude,
        
        -- Ecommerce context
        raw_data:ecommerce_context.cart_value::FLOAT as cart_value,
        raw_data:ecommerce_context.currency::STRING as currency,
        raw_data:ecommerce_context.items_count::INT as items_count,
        
        -- User properties
        raw_data:user_properties.customer_segment::STRING as customer_segment,
        raw_data:user_properties.lifetime_value::FLOAT as lifetime_value,
        
        -- Metadata
        RECORD_METADATA:CreateTime::TIMESTAMP as ingested_at,
        CURRENT_TIMESTAMP() as transformed_at
        
    FROM source
)

SELECT * FROM flattened
WHERE interaction_timestamp IS NOT NULL

{{ dbt_utils.deduplicate(
    relation=this,
    partition_by='interaction_id',
    order_by='ingested_at desc'
) }}
```

```sql
-- models/intermediate/int_user_behavior_metrics.sql
-- Calculate user behavior metrics

WITH user_interactions AS (
    SELECT * FROM {{ ref('stg_user_interactions') }}
),

session_data AS (
    SELECT * FROM {{ ref('stg_sessions') }}
),

user_metrics AS (
    SELECT
        user_id,
        DATE_TRUNC('day', interaction_timestamp) as activity_date,
        
        -- Engagement metrics
        COUNT(*) as interaction_count,
        COUNT(DISTINCT session_id) as session_count,
        COUNT(DISTINCT event_name) as unique_events,
        
        -- Time metrics
        MIN(interaction_timestamp) as first_interaction,
        MAX(interaction_timestamp) as last_interaction,
        DATEDIFF('second', MIN(interaction_timestamp), MAX(interaction_timestamp)) as session_duration_seconds,
        
        -- Page metrics
        COUNT(DISTINCT page_url) as unique_pages_viewed,
        COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) as page_views,
        COUNT(CASE WHEN event_type = 'click' THEN 1 END) as clicks,
        
        -- Device metrics
        MODE(device_type) as primary_device_type,
        COUNT(DISTINCT device_type) as devices_used,
        
        -- Ecommerce metrics
        AVG(cart_value) as avg_cart_value,
        MAX(cart_value) as max_cart_value,
        SUM(cart_value) as total_cart_value,
        
        -- Derived metrics
        DIV0(COUNT(*), COUNT(DISTINCT session_id)) as interactions_per_session,
        customer_segment
        
    FROM user_interactions
    GROUP BY 1, 2, customer_segment
)

SELECT * FROM user_metrics
```

```sql
-- models/marts/mart_conversion_funnels.sql
-- Conversion funnel analysis

WITH sessions AS (
    SELECT * FROM {{ ref('stg_sessions') }}
),

funnel_steps AS (
    SELECT
        session_id,
        user_id,
        merchant_id,
        session_start,
        device_type,
        country,
        
        -- Funnel progression
        MAX(CASE WHEN f.value:step = 'product_view' THEN 1 ELSE 0 END) as reached_product_view,
        MAX(CASE WHEN f.value:step = 'add_to_cart' THEN 1 ELSE 0 END) as reached_add_to_cart,
        MAX(CASE WHEN f.value:step = 'checkout_initiated' THEN 1 ELSE 0 END) as reached_checkout,
        MAX(CASE WHEN f.value:step = 'payment_info_entered' THEN 1 ELSE 0 END) as reached_payment,
        MAX(CASE WHEN f.value:step = 'purchase_completed' THEN 1 ELSE 0 END) as reached_purchase,
        
        -- Time to conversion
        CASE 
            WHEN MAX(CASE WHEN f.value:step = 'purchase_completed' THEN 1 ELSE 0 END) = 1
            THEN DATEDIFF('second', session_start, 
                 MAX(CASE WHEN f.value:step = 'purchase_completed' 
                     THEN f.value:timestamp::TIMESTAMP END))
        END as time_to_conversion_seconds,
        
        -- Revenue
        conversion_data.revenue,
        conversion_data.converted
        
    FROM sessions s,
    LATERAL FLATTEN(input => s.funnel_progress) f
    GROUP BY 1,2,3,4,5,6, conversion_data.revenue, conversion_data.converted
),

funnel_metrics AS (
    SELECT
        DATE_TRUNC('day', session_start) as funnel_date,
        device_type,
        country,
        
        COUNT(*) as total_sessions,
        SUM(reached_product_view) as step_1_product_view,
        SUM(reached_add_to_cart) as step_2_add_to_cart,
        SUM(reached_checkout) as step_3_checkout,
        SUM(reached_payment) as step_4_payment,
        SUM(reached_purchase) as step_5_purchase,
        
        -- Conversion rates
        DIV0(SUM(reached_add_to_cart), SUM(reached_product_view)) as cr_view_to_cart,
        DIV0(SUM(reached_checkout), SUM(reached_add_to_cart)) as cr_cart_to_checkout,
        DIV0(SUM(reached_payment), SUM(reached_checkout)) as cr_checkout_to_payment,
        DIV0(SUM(reached_purchase), SUM(reached_payment)) as cr_payment_to_purchase,
        DIV0(SUM(reached_purchase), COUNT(*)) as overall_conversion_rate,
        
        -- Revenue metrics
        SUM(revenue) as total_revenue,
        AVG(CASE WHEN converted THEN revenue END) as avg_order_value,
        AVG(time_to_conversion_seconds) as avg_time_to_conversion_seconds
        
    FROM funnel_steps
    GROUP BY 1, 2, 3
)

SELECT * FROM funnel_metrics
```

---

## 5. Streaming temps réel avec Kafka

### 5.1 Architecture Kafka Cluster

```
┌────────────────────────────────────────────────────────────────┐
│                   Kafka Cluster Architecture                    │
├────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Data Center 1 (US-East)          Data Center 2 (EU-West)      │
│  ┌─────────────────────┐          ┌─────────────────────┐      │
│  │  Broker 1           │          │  Broker 4           │      │
│  │  - Leader: P1, P4   │          │  - Leader: P7, P10  │      │
│  │  - Follower: P2,P5  │          │  - Follower: P8,P11 │      │
│  └─────────────────────┘          └─────────────────────┘      │
│                                                                  │
│  ┌─────────────────────┐          ┌─────────────────────┐      │
│  │  Broker 2           │          │  Broker 5           │      │
│  │  - Leader: P2, P5   │          │  - Leader: P8, P11  │      │
│  │  - Follower: P3,P6  │          │  - Follower: P9,P12 │      │
│  └─────────────────────┘          └─────────────────────┘      │
│                                                                  │
│  ┌─────────────────────┐          ┌─────────────────────┐      │
│  │  Broker 3           │          │  Broker 6           │      │
│  │  - Leader: P3, P6   │          │  - Leader: P9, P12  │      │
│  │  - Follower: P1,P4  │          │  - Follower: P7,P10 │      │
│  └─────────────────────┘          └─────────────────────┘      │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │             Zookeeper Ensemble (3 nodes)                  │ │
│  │  ┌──────────┐    ┌──────────┐    ┌──────────┐           │ │
│  │  │  ZK-1    │    │  ZK-2    │    │  ZK-3    │           │ │
│  │  │ (Leader) │    │(Follower)│    │(Follower)│           │ │
│  │  └──────────┘    └──────────┘    └──────────┘           │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │               Schema Registry (3 nodes)                   │ │
│  │  - Avro schema management                                 │ │
│  │  - Schema evolution & compatibility                       │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │              Kafka Connect Cluster (6 workers)            │ │
│  │  - Debezium Source Connectors (3)                         │ │
│  │  - Snowflake Sink Connectors (3)                          │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                  │
└────────────────────────────────────────────────────────────────┘
```

### 5.2 Configuration Topics Kafka

```properties
# Topic: stripe.public.transaction
# High-volume transactional data

num.partitions=48
replication.factor=3
min.insync.replicas=2
retention.ms=604800000  # 7 days
compression.type=snappy
cleanup.policy=delete

# Compaction disabled for append-only data
max.message.bytes=1048576  # 1MB max message size
```

```properties
# Topic: mongo-fraud-features
# ML feature store - requires longer retention

num.partitions=24
replication.factor=3
min.insync.replicas=2
retention.ms=2592000000  # 30 days
compression.type=lz4
cleanup.policy=delete

# Larger messages for nested JSON
max.message.bytes=5242880  # 5MB
```

```properties
# Topic: dlq-processing-errors
# Dead Letter Queue for failed messages

num.partitions=12
replication.factor=3
retention.ms=2592000000  # 30 days
compression.type=gzip
cleanup.policy=delete

# Preserve all errors for debugging
```

### 5.3 Producer Best Practices

```python
# fraud_detection_producer.py
# Stream fraud detection events to Kafka

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging

class FraudDetectionProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['kafka-1:9092', 'kafka-2:9092', 'kafka-3:9092'],
            
            # Serialization
            key_serializer=lambda k: k.encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            
            # Performance tuning
            batch_size=32768,  # 32KB batches
            linger_ms=10,      # Wait 10ms to batch messages
            compression_type='snappy',
            buffer_memory=67108864,  # 64MB buffer
            
            # Reliability
            acks='all',  # Wait for all replicas
            retries=3,
            max_in_flight_requests_per_connection=5,
            
            # Idempotence for exactly-once
            enable_idempotence=True,
            
            # Timeouts
            request_timeout_ms=30000,
            delivery_timeout_ms=120000
        )
        
        self.logger = logging.getLogger(__name__)
    
    def send_fraud_event(self, transaction_id, fraud_prediction):
        """
        Send fraud detection event to Kafka
        """
        message = {
            'transaction_id': transaction_id,
            'fraud_score': fraud_prediction['fraud_probability'],
            'risk_level': fraud_prediction['risk_level'],
            'model_version': fraud_prediction['model_version'],
            'timestamp': datetime.utcnow().isoformat()
        }
        
        try:
            # Async send with callback
            future = self.producer.send(
                topic='fraud-detection-events',
                key=transaction_id,
                value=message,
                partition=None,  # Use default partitioner
                headers=[
                    ('source', b'ml-inference-service'),
                    ('version', b'1.0')
                ]
            )
            
            # Register callback
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            
            return future
            
        except KafkaError as e:
            self.logger.error(f"Failed to send message: {e}")
            raise
    
    def _on_send_success(self, record_metadata):
        self.logger.info(
            f"✅ Message sent: topic={record_metadata.topic}, "
            f"partition={record_metadata.partition}, offset={record_metadata.offset}"
        )
    
    def _on_send_error(self, exception):
        self.logger.error(f"❌ Message send failed: {exception}")
    
    def flush_and_close(self):
        """
        Flush pending messages and close producer
        """
        self.producer.flush(timeout=30)
        self.producer.close(timeout=30)
```

### 5.4 Consumer Best Practices

```python
# fraud_analysis_consumer.py
# Consume and analyze fraud events in real-time

from kafka import KafkaConsumer
from kafka.structs import TopicPartition
import json
import logging

class FraudAnalysisConsumer:
    def __init__(self, consumer_group='fraud-analysis-group'):
        self.consumer = KafkaConsumer(
            'fraud-detection-events',
            bootstrap_servers=['kafka-1:9092', 'kafka-2:9092'],
            
            # Consumer group for load balancing
            group_id=consumer_group,
            client_id=f'{consumer_group}-instance-1',
            
            # Deserialization
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            
            # Offset management
            auto_offset_reset='earliest',  # Start from beginning if no offset
            enable_auto_commit=False,      # Manual commit for reliability
            
            # Performance
            max_poll_records=500,
            max_poll_interval_ms=300000,   # 5 minutes
            session_timeout_ms=45000,
            heartbeat_interval_ms=15000,
            
            # Fetch settings
            fetch_min_bytes=1024,
            fetch_max_wait_ms=500
        )
        
        self.logger = logging.getLogger(__name__)
        self.processed_count = 0
        
    def consume_and_analyze(self):
        """
        Consume fraud events and analyze patterns
        """
        try:
            for message in self.consumer:
                try:
                    # Process message
                    self._process_fraud_event(message)
                    
                    # Commit offset after successful processing
                    self.consumer.commit()
                    
                    self.processed_count += 1
                    
                    if self.processed_count % 1000 == 0:
                        self.logger.info(f"Processed {self.processed_count} messages")
                        
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")
                    # Send to DLQ
                    self._send_to_dlq(message, error=str(e))
                    # Still commit to avoid reprocessing
                    self.consumer.commit()
                    
        except KeyboardInterrupt:
            self.logger.info("Shutting down consumer...")
        finally:
            self.consumer.close()
    
    def _process_fraud_event(self, message):
        """
        Process individual fraud detection event
        """
        event = message.value
        transaction_id = event['transaction_id']
        fraud_score = event['fraud_score']
        
        # High-risk transaction handling
        if fraud_score > 0.75:
            self.logger.warning(
                f"⚠️  HIGH RISK: Transaction {transaction_id} "
                f"fraud_score={fraud_score}"
            )
            # Trigger immediate review
            self._trigger_manual_review(transaction_id)
            
        # Update real-time analytics
        self._update_fraud_metrics(event)
        
        # Store in MongoDB for ML retraining
        self._store_fraud_event(event)
    
    def _trigger_manual_review(self, transaction_id):
        """
        Trigger manual review workflow for high-risk transactions
        """
        # Implementation: Send to review queue, notify analysts, etc.
        pass
    
    def _update_fraud_metrics(self, event):
        """
        Update real-time fraud metrics
        """
        # Implementation: Increment counters, update dashboards, etc.
        pass
    
    def _store_fraud_event(self, event):
        """
        Store event in MongoDB for historical analysis
        """
        # Implementation: Insert into MongoDB
        pass
    
    def _send_to_dlq(self, message, error):
        """
        Send failed message to Dead Letter Queue
        """
        # Implementation: Produce to DLQ topic with error metadata
        pass
```

---

## 6. Orchestration avec Airflow

### 6.1 Architecture Airflow sur Kubernetes

```yaml
# airflow-kubernetes-deployment.yaml
# Airflow on Kubernetes with Celery Executor

apiVersion: v1
kind: Namespace
metadata:
  name: airflow
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-webserver
  namespace: airflow
spec:
  replicas: 2
  selector:
    matchLabels:
      app: airflow
      component: webserver
  template:
    metadata:
      labels:
        app: airflow
        component: webserver
    spec:
      containers:
      - name: webserver
        image: apache/airflow:2.8.1-python3.11
        command: ["airflow", "webserver"]
        ports:
        - containerPort: 8080
        env:
        - name: AIRFLOW__CORE__EXECUTOR
          value: "KubernetesExecutor"
        - name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
          valueFrom:
            secretKeyRef:
              name: airflow-secrets
              key: sql_alchemy_conn
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-scheduler
  namespace: airflow
spec:
  replicas: 2
  selector:
    matchLabels:
      app: airflow
      component: scheduler
  template:
    metadata:
      labels:
        app: airflow
        component: scheduler
    spec:
      containers:
      - name: scheduler
        image: apache/airflow:2.8.1-python3.11
        command: ["airflow", "scheduler"]
        env:
        - name: AIRFLOW__CORE__EXECUTOR
          value: "KubernetesExecutor"
        resources:
          requests:
            memory: "4Gi"
            cpu: "2000m"
          limits:
            memory: "8Gi"
            cpu: "4000m"
---
apiVersion: v1
kind: Service
metadata:
  name: airflow-webserver
  namespace: airflow
spec:
  type: LoadBalancer
  ports:
  - port: 80
    targetPort: 8080
  selector:
    app: airflow
    component: webserver
```

### 6.2 DAG Complexe - Pipeline Unifié

```python
"""
unified_data_pipeline.py
Master DAG orchestrating all data flows
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import BranchPythonOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': True,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'unified_data_pipeline_master',
    default_args=default_args,
    description='Master orchestrator for all data pipelines',
    schedule_interval='0 */4 * * *',  # Every 4 hours
    catchup=False,
    max_active_runs=1,
    tags=['orchestrator', 'master']
)


def check_data_freshness(**context):
    """
    Check if source data is fresh enough to proceed
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_oltp')
    
    # Check last transaction timestamp
    query = """
    SELECT MAX(created_at) as last_transaction
    FROM transaction
    """
    result = pg_hook.get_first(query)
    last_transaction = result[0]
    
    # If data is older than 10 minutes, skip this run
    time_diff = datetime.utcnow() - last_transaction
    if time_diff.total_seconds() > 600:
        return 'skip_pipeline'
    else:
        return 'proceed_with_pipeline'


check_freshness = BranchPythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    provide_context=True,
    dag=dag
)


# Trigger ETL Pipeline
trigger_etl = TriggerDagRunOperator(
    task_id='trigger_etl_pipeline',
    trigger_dag_id='etl_oltp_to_olap_daily',
    execution_date='{{ execution_date }}',
    wait_for_completion=True,
    poke_interval=60,
    dag=dag
)


# Wait for ELT streaming to catch up
wait_for_elt = ExternalTaskSensor(
    task_id='wait_for_elt_streaming',
    external_dag_id='elt_nosql_streaming',
    external_task_id='stream_to_snowflake',
    allowed_states=['success'],
    execution_delta=timedelta(minutes=5),
    timeout=1800,
    poke_interval=60,
    dag=dag
)


# Trigger aggregation refresh
trigger_aggregations = TriggerDagRunOperator(
    task_id='trigger_aggregate_refresh',
    trigger_dag_id='refresh_olap_aggregates',
    execution_date='{{ execution_date }}',
    wait_for_completion=True,
    dag=dag
)


# Trigger ML feature refresh
trigger_ml_features = TriggerDagRunOperator(
    task_id='trigger_ml_feature_refresh',
    trigger_dag_id='ml_feature_store_update',
    execution_date='{{ execution_date }}',
    wait_for_completion=False,  # Run async
    dag=dag
)


skip_pipeline = DummyOperator(
    task_id='skip_pipeline',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    trigger_rule='none_failed_min_one_success',
    dag=dag
)


# Dependencies
check_freshness >> [skip_pipeline, trigger_etl]
trigger_etl >> wait_for_elt >> trigger_aggregations
trigger_aggregations >> [trigger_ml_features, end]
skip_pipeline >> end
```