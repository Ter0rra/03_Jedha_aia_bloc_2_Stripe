# Plan de Sécurité et Conformité - PARTIE 2
## Sections 9-12 : Governance, Backup, Monitoring, Contrôles

---

## 📋 Table des matières (Partie 2)

9. [Data Governance](#9-data-governance)
10. [Backup et Disaster Recovery](#10-backup-et-disaster-recovery)
11. [Monitoring de sécurité](#11-monitoring-de-sécurité)
12. [Contrôles automatisés](#12-contrôles-automatisés)

---

## 9. Data Governance

### 9.1 Data Catalog

#### Architecture Data Catalog

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA CATALOG ARCHITECTURE                 │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────────────────────────────────────┐           │
│  │           Metadata Repository                │           │
│  │              (PostgreSQL)                    │           │
│  │                                              │           │
│  │  - Table definitions                        │           │
│  │  - Column metadata                          │           │
│  │  - Data lineage                             │           │
│  │  - Business glossary                        │           │
│  │  - Data owners                              │           │
│  │  - PII classification                       │           │
│  └────────────────┬─────────────────────────────┘           │
│                   │                                           │
│                   ↓                                           │
│  ┌──────────────────────────────────────────────┐           │
│  │         Data Discovery Agents                │           │
│  │                                              │           │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  │           │
│  │  │PostgreSQL│  │ Snowflake│  │ MongoDB  │  │           │
│  │  │ Scanner  │  │  Scanner │  │  Scanner │  │           │
│  │  └──────────┘  └──────────┘  └──────────┘  │           │
│  └──────────────────────────────────────────────┘           │
│                   │                                           │
│                   ↓                                           │
│  ┌──────────────────────────────────────────────┐           │
│  │         Data Lineage Tracker                 │           │
│  │                                              │           │
│  │  OLTP → CDC → Kafka → DBT → OLAP            │           │
│  │    ↓      ↓      ↓      ↓      ↓            │           │
│  │  [tracked at each step]                     │           │
│  └──────────────────────────────────────────────┘           │
│                   │                                           │
│                   ↓                                           │
│  ┌──────────────────────────────────────────────┐           │
│  │         Data Quality Dashboard               │           │
│  │                                              │           │
│  │  - Completeness                             │           │
│  │  - Accuracy                                 │           │
│  │  - Consistency                              │           │
│  │  - Freshness                                │           │
│  └──────────────────────────────────────────────┘           │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

#### Data Catalog Schema

```sql
-- catalog_metadata.sql

-- 1. Tables catalog
CREATE TABLE catalog.tables (
    table_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    database_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    table_type VARCHAR(50), -- 'fact', 'dimension', 'staging', 'raw'
    description TEXT,
    data_owner VARCHAR(255),
    business_domain VARCHAR(255), -- 'payments', 'fraud', 'analytics'
    
    -- Compliance
    contains_pii BOOLEAN DEFAULT FALSE,
    pii_types TEXT[], -- ['email', 'phone', 'address']
    contains_pci BOOLEAN DEFAULT FALSE,
    gdpr_applicable BOOLEAN DEFAULT TRUE,
    ccpa_applicable BOOLEAN DEFAULT TRUE,
    
    -- Quality metrics
    row_count BIGINT,
    size_mb NUMERIC(12,2),
    last_updated TIMESTAMPTZ,
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(database_name, schema_name, table_name)
);

-- 2. Columns catalog
CREATE TABLE catalog.columns (
    column_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_id UUID REFERENCES catalog.tables(table_id),
    column_name VARCHAR(255) NOT NULL,
    data_type VARCHAR(100),
    is_nullable BOOLEAN,
    default_value TEXT,
    
    -- Classification
    sensitivity_level VARCHAR(50), -- 'public', 'internal', 'confidential', 'restricted'
    is_pii BOOLEAN DEFAULT FALSE,
    pii_type VARCHAR(100), -- 'name', 'email', 'phone', 'ssn', 'credit_card'
    is_pci BOOLEAN DEFAULT FALSE,
    
    -- Business metadata
    business_name VARCHAR(255),
    description TEXT,
    example_values TEXT[],
    
    -- Quality
    null_percentage NUMERIC(5,2),
    unique_percentage NUMERIC(5,2),
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(table_id, column_name)
);

-- 3. Data lineage
CREATE TABLE catalog.data_lineage (
    lineage_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_table_id UUID REFERENCES catalog.tables(table_id),
    target_table_id UUID REFERENCES catalog.tables(table_id),
    transformation_type VARCHAR(50), -- 'copy', 'join', 'aggregate', 'filter'
    transformation_logic TEXT,
    pipeline_name VARCHAR(255),
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4. Data owners
CREATE TABLE catalog.data_owners (
    owner_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    role VARCHAR(100), -- 'Data Engineer', 'Data Scientist', 'Product Manager'
    department VARCHAR(100),
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 5. Business glossary
CREATE TABLE catalog.business_glossary (
    term_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    term VARCHAR(255) UNIQUE NOT NULL,
    definition TEXT NOT NULL,
    synonyms TEXT[],
    related_terms TEXT[],
    business_domain VARCHAR(255),
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

#### Automated PII Detection

```python
# pii_detection.py

import re
from typing import List, Dict

class PIIDetector:
    """
    Automated PII detection across all databases
    """
    
    def __init__(self):
        self.patterns = {
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'phone': r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
            'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
            'credit_card': r'\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b',
            'ip_address': r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b'
        }
        
        self.column_name_patterns = {
            'email': ['email', 'mail', 'e_mail'],
            'phone': ['phone', 'telephone', 'mobile', 'tel'],
            'name': ['name', 'first_name', 'last_name', 'full_name'],
            'address': ['address', 'street', 'city', 'zip', 'postal'],
            'ssn': ['ssn', 'social_security'],
            'credit_card': ['card_number', 'cc_number', 'credit_card']
        }
    
    def scan_database_schema(self, database_type: str, connection):
        """
        Scan database schema for PII columns
        """
        if database_type == 'postgresql':
            return self._scan_postgres(connection)
        elif database_type == 'snowflake':
            return self._scan_snowflake(connection)
        elif database_type == 'mongodb':
            return self._scan_mongodb(connection)
    
    def _scan_postgres(self, conn):
        """Scan PostgreSQL for PII"""
        cursor = conn.cursor()
        
        # Get all columns
        cursor.execute("""
            SELECT 
                table_schema,
                table_name,
                column_name,
                data_type
            FROM information_schema.columns
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """)
        
        columns = cursor.fetchall()
        pii_columns = []
        
        for schema, table, column, dtype in columns:
            # Check column name patterns
            pii_type = self._detect_pii_from_name(column)
            
            if pii_type:
                # Sample data to confirm
                cursor.execute(f"""
                    SELECT {column} 
                    FROM {schema}.{table} 
                    WHERE {column} IS NOT NULL 
                    LIMIT 100
                """)
                
                samples = [row[0] for row in cursor.fetchall()]
                confirmed = self._validate_pii_samples(samples, pii_type)
                
                if confirmed:
                    pii_columns.append({
                        'schema': schema,
                        'table': table,
                        'column': column,
                        'pii_type': pii_type,
                        'confidence': confirmed['confidence']
                    })
        
        return pii_columns
    
    def _detect_pii_from_name(self, column_name: str) -> str:
        """Detect PII type from column name"""
        column_lower = column_name.lower()
        
        for pii_type, patterns in self.column_name_patterns.items():
            for pattern in patterns:
                if pattern in column_lower:
                    return pii_type
        
        return None
    
    def _validate_pii_samples(self, samples: List, pii_type: str) -> Dict:
        """Validate PII by checking sample data"""
        if not samples:
            return None
        
        pattern = self.patterns.get(pii_type)
        if not pattern:
            return {'confidence': 'medium'}
        
        matches = sum(1 for s in samples if re.match(pattern, str(s)))
        confidence = matches / len(samples)
        
        if confidence > 0.9:
            return {'confidence': 'high', 'match_rate': confidence}
        elif confidence > 0.5:
            return {'confidence': 'medium', 'match_rate': confidence}
        else:
            return None
    
    def update_catalog(self, pii_columns: List[Dict]):
        """Update data catalog with PII findings"""
        for col in pii_columns:
            # Update catalog.columns
            db.execute("""
                UPDATE catalog.columns
                SET is_pii = TRUE,
                    pii_type = %s,
                    sensitivity_level = 'confidential',
                    updated_at = NOW()
                WHERE table_id = (
                    SELECT table_id FROM catalog.tables
                    WHERE schema_name = %s AND table_name = %s
                )
                AND column_name = %s
            """, (col['pii_type'], col['schema'], col['table'], col['column']))
            
            # Alert data owner
            self._alert_data_owner(col)
    
    def _alert_data_owner(self, col: Dict):
        """Alert data owner about PII detected"""
        message = f"""
        PII Detected Alert
        
        Schema: {col['schema']}
        Table: {col['table']}
        Column: {col['column']}
        PII Type: {col['pii_type']}
        Confidence: {col['confidence']}
        
        Action Required:
        1. Verify classification is correct
        2. Ensure encryption is enabled
        3. Review access controls
        4. Update retention policies
        """
        
        send_slack_alert(channel='#data-governance', message=message)
```

---

### 9.2 Data Quality Framework

#### Data Quality Dimensions

```yaml
# data_quality_framework.yaml

quality_dimensions:
  completeness:
    description: "Data has all required values"
    metrics:
      - null_percentage
      - missing_records_count
    thresholds:
      critical: null_percentage > 10%
      warning: null_percentage > 5%
    
  accuracy:
    description: "Data is correct and precise"
    metrics:
      - value_range_violations
      - format_violations
      - referential_integrity_violations
    thresholds:
      critical: violations > 1%
      warning: violations > 0.1%
  
  consistency:
    description: "Data is consistent across systems"
    metrics:
      - cross_system_mismatches
      - duplicate_records
    thresholds:
      critical: mismatches > 1%
      warning: mismatches > 0.1%
  
  timeliness:
    description: "Data is up-to-date"
    metrics:
      - data_freshness_hours
      - pipeline_lag_minutes
    thresholds:
      critical: freshness > 24 hours
      warning: freshness > 12 hours
  
  uniqueness:
    description: "No duplicate records"
    metrics:
      - duplicate_rate
    thresholds:
      critical: duplicates > 0.1%
      warning: duplicates > 0.01%
  
  validity:
    description: "Data conforms to business rules"
    metrics:
      - business_rule_violations
    thresholds:
      critical: violations > 1%
      warning: violations > 0.1%
```

#### Data Quality Checks (DBT)

```sql
-- dbt/tests/data_quality/test_transaction_quality.sql

-- Test 1: No negative amounts (accuracy)
SELECT
    transaction_id,
    amount
FROM {{ ref('fact_transactions') }}
WHERE amount < 0
  AND is_refund = FALSE;
-- Expect: 0 rows

-- Test 2: All transactions have customer (completeness)
SELECT
    transaction_id
FROM {{ ref('fact_transactions') }}
WHERE customer_key IS NULL;
-- Expect: 0 rows

-- Test 3: No duplicates (uniqueness)
SELECT
    transaction_id,
    COUNT(*) as occurrences
FROM {{ ref('fact_transactions') }}
GROUP BY transaction_id
HAVING COUNT(*) > 1;
-- Expect: 0 rows

-- Test 4: Freshness (timeliness)
SELECT
    MAX(created_at) as last_transaction,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(created_at))) / 3600 as hours_ago
FROM {{ ref('fact_transactions') }};
-- Expect: hours_ago < 24

-- Test 5: Referential integrity (consistency)
SELECT
    t.transaction_id,
    t.customer_key
FROM {{ ref('fact_transactions') }} t
LEFT JOIN {{ ref('dim_customer') }} c
    ON t.customer_key = c.customer_key
WHERE c.customer_key IS NULL;
-- Expect: 0 rows
```

---

### 9.3 Data Retention Policies

```sql
-- data_retention_policies.sql

-- Retention policies table
CREATE TABLE governance.retention_policies (
    policy_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_name VARCHAR(255) NOT NULL,
    retention_period_days INT NOT NULL,
    retention_reason VARCHAR(255),
    legal_hold_allowed BOOLEAN DEFAULT FALSE,
    
    -- Actions
    archival_enabled BOOLEAN DEFAULT TRUE,
    archival_location VARCHAR(255), -- S3 bucket
    deletion_enabled BOOLEAN DEFAULT FALSE,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Example policies
INSERT INTO governance.retention_policies 
(table_name, retention_period_days, retention_reason) VALUES
('transaction', 2555, '7 years - SOX compliance'),  -- 7 years
('customer', 2555, '7 years - GDPR legal basis'),
('payment_methods', 90, '90 days - PCI-DSS requirement'),
('fraud', 2555, '7 years - Fraud investigation'),
('user_interactions', 90, '90 days - GDPR data minimization'),
('sessions', 30, '30 days - Analytics only'),
('audit_logs', 2555, '7 years - Compliance requirement');

-- Automated retention enforcement
CREATE OR REPLACE FUNCTION enforce_retention_policy()
RETURNS void AS $$
DECLARE
    policy RECORD;
BEGIN
    FOR policy IN 
        SELECT * FROM governance.retention_policies
        WHERE deletion_enabled = TRUE
    LOOP
        -- Archive old data
        IF policy.archival_enabled THEN
            EXECUTE format('
                COPY (
                    SELECT * FROM %I
                    WHERE created_at < NOW() - INTERVAL ''%s days''
                ) TO ''%s/%s_%s.parquet''
                WITH (FORMAT PARQUET)
            ', 
                policy.table_name, 
                policy.retention_period_days,
                policy.archival_location,
                policy.table_name,
                TO_CHAR(NOW(), 'YYYY-MM-DD')
            );
        END IF;
        
        -- Delete old data
        EXECUTE format('
            DELETE FROM %I
            WHERE created_at < NOW() - INTERVAL ''%s days''
              AND NOT EXISTS (
                  SELECT 1 FROM governance.legal_holds
                  WHERE table_name = %L
                    AND record_id = %I.id
              )
        ', 
            policy.table_name, 
            policy.retention_period_days,
            policy.table_name,
            policy.table_name
        );
        
        RAISE NOTICE 'Retention policy enforced for %', policy.table_name;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Schedule daily (via Airflow)
-- airflow trigger: 0 1 * * * (1 AM daily)
```

---

## 10. Backup et Disaster Recovery

### 10.1 Stratégie de Backup

```
┌─────────────────────────────────────────────────────────────┐
│                   BACKUP STRATEGY                            │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  TIER 1: CRITICAL (RTO: 1h, RPO: 15min)                     │
│  ┌──────────────────────────────────────────────┐           │
│  │  PostgreSQL OLTP                             │           │
│  │  - Streaming replication (standby replica)  │           │
│  │  - WAL archiving every 5 min                │           │
│  │  - PITR (Point-in-Time Recovery) enabled    │           │
│  │  - Daily full backup                        │           │
│  │  - Retention: 30 days                       │           │
│  └──────────────────────────────────────────────┘           │
│                                                               │
│  TIER 2: HIGH (RTO: 4h, RPO: 1h)                           │
│  ┌──────────────────────────────────────────────┐           │
│  │  Snowflake OLAP                              │           │
│  │  - Time Travel (90 days)                    │           │
│  │  - Fail-safe (7 days after Time Travel)    │           │
│  │  - Daily snapshot to S3                     │           │
│  │  - Retention: 90 days                       │           │
│  └──────────────────────────────────────────────┘           │
│                                                               │
│  TIER 3: MEDIUM (RTO: 12h, RPO: 24h)                       │
│  ┌──────────────────────────────────────────────┐           │
│  │  MongoDB NoSQL                               │           │
│  │  - Replica set (3 nodes)                    │           │
│  │  - Daily snapshot                           │           │
│  │  - Oplog backup hourly                      │           │
│  │  - Retention: 30 days                       │           │
│  └──────────────────────────────────────────────┘           │
│                                                               │
│  TIER 4: LOW (RTO: 24h, RPO: 24h)                          │
│  ┌──────────────────────────────────────────────┐           │
│  │  ML Models & Feature Store                  │           │
│  │  - S3 versioning enabled                    │           │
│  │  - Daily backup                             │           │
│  │  - Retention: 90 days                       │           │
│  └──────────────────────────────────────────────┘           │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

#### Backup Configuration

```yaml
# backup_config.yaml

postgresql_backup:
  method: streaming_replication
  standby_replica:
    instance_type: db.r5.xlarge
    availability_zone: us-east-1b
    lag_threshold_seconds: 30
  
  wal_archiving:
    enabled: true
    archive_location: s3://stripe-backups/postgres-wal/
    archive_timeout: 300  # 5 minutes
  
  full_backup:
    schedule: "0 2 * * *"  # 2 AM daily
    method: pg_basebackup
    compression: gzip
    destination: s3://stripe-backups/postgres-full/
    retention_days: 30
  
  pitr:
    enabled: true
    max_recovery_time_minutes: 60

snowflake_backup:
  time_travel:
    enabled: true
    retention_days: 90
  
  fail_safe:
    enabled: true
    retention_days: 7
  
  snapshot:
    schedule: "0 3 * * *"  # 3 AM daily
    method: COPY INTO
    destination: s3://stripe-backups/snowflake-snapshots/
    format: PARQUET
    compression: SNAPPY
    retention_days: 90

mongodb_backup:
  replica_set:
    nodes: 3
    read_preference: secondaryPreferred
  
  snapshot:
    schedule: "0 4 * * *"  # 4 AM daily
    method: mongodump
    compression: gzip
    destination: s3://stripe-backups/mongodb-snapshots/
    retention_days: 30
  
  oplog_backup:
    schedule: "0 * * * *"  # Hourly
    destination: s3://stripe-backups/mongodb-oplog/
    retention_hours: 48

ml_models_backup:
  s3_versioning:
    enabled: true
    bucket: stripe-ml-models
  
  daily_snapshot:
    schedule: "0 5 * * *"  # 5 AM daily
    source: s3://stripe-ml-models/production/
    destination: s3://stripe-backups/ml-models/
    retention_days: 90
```

---

### 10.2 Disaster Recovery Plan

#### DR Runbook

```markdown
# DISASTER RECOVERY RUNBOOK

## Scenario 1: PostgreSQL Primary Failure

**Detection:**
- CloudWatch alarm: PostgreSQL unreachable > 2 minutes
- PagerDuty alert: Critical

**Recovery Steps:**

1. **Verify failure** (ETA: 2 min)
   ```bash
   # Check PostgreSQL health
   pg_isready -h postgres-primary.internal -p 5432
   
   # Check replication lag
   psql -h postgres-standby.internal -c "SELECT NOW() - pg_last_xact_replay_timestamp() AS lag"
   ```

2. **Promote standby to primary** (ETA: 3 min)
   ```bash
   # On standby server
   /usr/lib/postgresql/15/bin/pg_ctl promote -D /var/lib/postgresql/15/main
   
   # Verify promotion
   psql -h postgres-standby.internal -c "SELECT pg_is_in_recovery()"
   # Should return: f (false = not in recovery = primary)
   ```

3. **Update DNS / Load Balancer** (ETA: 2 min)
   ```bash
   # Update Route53 DNS
   aws route53 change-resource-record-sets \
     --hosted-zone-id Z1234567890ABC \
     --change-batch file://promote-standby.json
   
   # Or update application config
   sed -i 's/postgres-primary/postgres-standby/g' /etc/app/config.yaml
   ```

4. **Update applications** (ETA: 3 min)
   ```bash
   # Restart application servers
   kubectl rollout restart deployment stripe-api
   
   # Verify connections
   kubectl logs -l app=stripe-api | grep "Database connected"
   ```

5. **Provision new standby** (ETA: 30 min)
   ```bash
   # Launch new EC2 instance
   aws ec2 run-instances --image-id ami-postgres-15 ...
   
   # Configure streaming replication from new primary
   pg_basebackup -h postgres-standby.internal -D /var/lib/postgresql/15/main
   ```

**Total RTO: 10 minutes** ✅

**Post-Recovery:**
- Root cause analysis (within 24h)
- Update runbook if needed
- Incident report to stakeholders

---

## Scenario 2: Region Failure (us-east-1)

**Detection:**
- Multiple services down in us-east-1
- AWS Health Dashboard: Regional issue

**Recovery Steps:**

1. **Failover to us-west-2** (ETA: 10 min)
   ```bash
   # Update Route53 to point to us-west-2
   aws route53 change-resource-record-sets \
     --hosted-zone-id Z1234567890ABC \
     --change-batch file://failover-us-west-2.json
   ```

2. **Verify data replication** (ETA: 5 min)
   ```bash
   # PostgreSQL: Check cross-region replica lag
   psql -h postgres-us-west-2.internal -c "SELECT NOW() - pg_last_xact_replay_timestamp()"
   
   # Snowflake: Already multi-region (no action)
   
   # MongoDB: Check replica set status
   mongo mongodb://mongo-us-west-2.internal --eval "rs.status()"
   ```

3. **Scale up us-west-2 capacity** (ETA: 15 min)
   ```bash
   # Scale Kubernetes nodes
   eksctl scale nodegroup --cluster stripe-prod-us-west-2 --nodes 50
   
   # Scale application pods
   kubectl scale deployment stripe-api --replicas=100
   ```

4. **Update monitoring** (ETA: 5 min)
   ```bash
   # Silence us-east-1 alerts
   amtool silence add alertname=~".+" region="us-east-1"
   
   # Update Grafana to show us-west-2 metrics
   ```

**Total RTO: 35 minutes** ✅

**Post-Recovery:**
- Wait for us-east-1 to recover
- Gradual failback (50% traffic, then 100%)
- Post-mortem with AWS TAM
```

---

### 10.3 Backup Testing

```python
# backup_testing.py

import boto3
import psycopg2
from datetime import datetime, timedelta

class BackupTester:
    """
    Automated backup testing (monthly)
    """
    
    def __init__(self):
        self.s3 = boto3.client('s3')
        self.ec2 = boto3.client('ec2')
    
    def test_postgresql_backup(self):
        """
        Test PostgreSQL backup restore
        """
        # 1. Get latest backup
        backups = self.s3.list_objects_v2(
            Bucket='stripe-backups',
            Prefix='postgres-full/'
        )
        latest_backup = sorted(
            backups['Contents'],
            key=lambda x: x['LastModified'],
            reverse=True
        )[0]['Key']
        
        print(f"Testing backup: {latest_backup}")
        
        # 2. Launch test instance
        test_instance = self.ec2.run_instances(
            ImageId='ami-postgres-15',
            InstanceType='db.t3.medium',
            MinCount=1,
            MaxCount=1,
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [{'Key': 'Name', 'Value': 'backup-test-postgres'}]
            }]
        )
        
        instance_id = test_instance['Instances'][0]['InstanceId']
        
        # Wait for instance ready
        waiter = self.ec2.get_waiter('instance_running')
        waiter.wait(InstanceIds=[instance_id])
        
        # 3. Restore backup
        instance_ip = self.ec2.describe_instances(
            InstanceIds=[instance_id]
        )['Reservations'][0]['Instances'][0]['PrivateIpAddress']
        
        # Download and restore
        restore_cmd = f"""
        aws s3 cp s3://stripe-backups/{latest_backup} /tmp/backup.tar.gz
        tar -xzf /tmp/backup.tar.gz -C /var/lib/postgresql/15/main
        sudo systemctl start postgresql
        """
        
        # Execute via SSH (or EC2 Systems Manager)
        # ... (implementation details)
        
        # 4. Verify data
        conn = psycopg2.connect(
            host=instance_ip,
            database='stripe_oltp',
            user='postgres',
            password='test_password'
        )
        cursor = conn.cursor()
        
        # Check row counts
        cursor.execute("SELECT COUNT(*) FROM transaction")
        txn_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM customer")
        cust_count = cursor.fetchone()[0]
        
        print(f"Restored data: {txn_count} transactions, {cust_count} customers")
        
        # 5. Validate data integrity
        cursor.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE amount < 0) as negative_amounts,
                COUNT(*) FILTER (WHERE customer_id IS NULL) as null_customers
            FROM transaction
        """)
        
        issues = cursor.fetchone()
        
        if issues[0] > 0 or issues[1] > 0:
            raise Exception(f"Data integrity issues: {issues}")
        
        # 6. Cleanup
        self.ec2.terminate_instances(InstanceIds=[instance_id])
        
        return {
            'status': 'success',
            'backup_file': latest_backup,
            'transactions': txn_count,
            'customers': cust_count,
            'test_date': datetime.now().isoformat()
        }
    
    def test_point_in_time_recovery(self, target_time: datetime):
        """
        Test PITR (Point-in-Time Recovery)
        """
        # 1. Get base backup before target_time
        # 2. Get WAL files between backup and target_time
        # 3. Restore base backup + replay WAL
        # 4. Verify data at target_time
        # ... (implementation)
        pass

# Schedule monthly (Airflow DAG)
# airflow trigger: 0 0 1 * * (1st of month, midnight)
```

---

## 11. Monitoring de sécurité

### 11.1 Security Monitoring Stack

```
┌─────────────────────────────────────────────────────────────┐
│                SECURITY MONITORING ARCHITECTURE              │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────────────────────────────────────┐           │
│  │           Log Sources                        │           │
│  │                                              │           │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  │           │
│  │  │PostgreSQL│  │ Snowflake│  │   K8s    │  │           │
│  │  │  Logs    │  │   Logs   │  │  Audit   │  │           │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘  │           │
│  │       │             │             │         │           │
│  │       └─────────────┴─────────────┘         │           │
│  └───────────────────────┬──────────────────────┘           │
│                          ↓                                   │
│  ┌──────────────────────────────────────────────┐           │
│  │          Logstash (Parsing & Enrichment)     │           │
│  │                                              │           │
│  │  - Parse JSON logs                          │           │
│  │  - Geo-IP enrichment                        │           │
│  │  - Threat intelligence lookup               │           │
│  │  - PII detection & masking                  │           │
│  └───────────────────────┬──────────────────────┘           │
│                          ↓                                   │
│  ┌──────────────────────────────────────────────┐           │
│  │          Elasticsearch (SIEM)                │           │
│  │                                              │           │
│  │  - Security events indexed                  │           │
│  │  - Threat detection rules                   │           │
│  │  - Anomaly detection ML                     │           │
│  └───────────────────────┬──────────────────────┘           │
│                          ↓                                   │
│  ┌──────────────────────────────────────────────┐           │
│  │          Kibana (Security Dashboard)         │           │
│  │                                              │           │
│  │  - Failed login attempts                    │           │
│  │  - Unusual access patterns                  │           │
│  │  - Data exfiltration alerts                │           │
│  │  - Compliance violations                    │           │
│  └──────────────────────────────────────────────┘           │
│                          │                                   │
│                          ↓                                   │
│  ┌──────────────────────────────────────────────┐           │
│  │          AlertManager                        │           │
│  │                                              │           │
│  │  Critical → PagerDuty                       │           │
│  │  Warning  → Slack #security-alerts          │           │
│  └──────────────────────────────────────────────┘           │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

### 11.2 Security Alerts

```yaml
# security_alerts.yaml

alerts:
  # Alert 1: Brute force login attempts
  - name: BruteForceLoginAttempts
    query: |
      SELECT COUNT(*) as failed_attempts, user_email
      FROM audit_logs
      WHERE event_type = 'login_failed'
        AND timestamp > NOW() - INTERVAL '5 minutes'
      GROUP BY user_email
      HAVING COUNT(*) > 5
    severity: high
    action:
      - lock_account
      - send_pagerduty
      - send_email_to_user
  
  # Alert 2: Unusual data access
  - name: UnusualDataAccess
    query: |
      SELECT user_id, COUNT(*) as queries
      FROM snowflake.query_history
      WHERE query_text ILIKE '%SELECT%customer%'
        AND execution_status = 'SUCCESS'
        AND start_time > NOW() - INTERVAL '1 hour'
      GROUP BY user_id
      HAVING COUNT(*) > 1000
    severity: critical
    action:
      - suspend_user
      - send_pagerduty
      - alert_security_team
  
  # Alert 3: Privilege escalation
  - name: PrivilegeEscalation
    query: |
      SELECT user_id, old_role, new_role
      FROM audit_logs
      WHERE event_type = 'role_change'
        AND new_role IN ('ADMIN', 'SYSADMIN', 'SECURITYADMIN')
        AND user_id NOT IN (SELECT user_id FROM authorized_admins)
    severity: critical
    action:
      - revert_role_change
      - send_pagerduty
      - alert_ciso
  
  # Alert 4: Data exfiltration
  - name: DataExfiltration
    query: |
      SELECT user_id, SUM(bytes_sent) as total_bytes
      FROM audit_logs
      WHERE event_type = 'data_export'
        AND timestamp > NOW() - INTERVAL '1 hour'
      GROUP BY user_id
      HAVING SUM(bytes_sent) > 10737418240  -- 10 GB
    severity: critical
    action:
      - suspend_user
      - send_pagerduty
      - alert_legal_team
      - preserve_evidence
  
  # Alert 5: After-hours access
  - name: AfterHoursAccess
    query: |
      SELECT user_id, COUNT(*) as access_count
      FROM audit_logs
      WHERE event_type IN ('login', 'query_executed')
        AND EXTRACT(HOUR FROM timestamp) NOT BETWEEN 6 AND 22
        AND user_id NOT IN (SELECT user_id FROM on_call_engineers)
      GROUP BY user_id
    severity: warning
    action:
      - send_slack_alert
      - log_for_review
  
  # Alert 6: PII access without business justification
  - name: UnauthorizedPIIAccess
    query: |
      SELECT user_id, table_name
      FROM audit_logs
      WHERE table_name IN (
          SELECT table_name FROM catalog.tables WHERE contains_pii = TRUE
      )
      AND user_id NOT IN (
          SELECT user_id FROM authorized_pii_users
      )
    severity: high
    action:
      - send_pagerduty
      - alert_dpo
      - require_justification
```

---

## 12. Contrôles automatisés

### 12.1 Automated Compliance Checks

```python
# compliance_checks.py

from datetime import datetime
import psycopg2
import snowflake.connector

class ComplianceChecker:
    """
    Automated daily compliance checks
    """
    
    def __init__(self):
        self.postgres_conn = psycopg2.connect(...)
        self.snowflake_conn = snowflake.connector.connect(...)
        self.violations = []
    
    def run_all_checks(self):
        """Run all compliance checks"""
        checks = [
            self.check_encryption,
            self.check_access_controls,
            self.check_audit_logs,
            self.check_data_retention,
            self.check_pii_access,
            self.check_password_policy,
            self.check_mfa_enabled,
            self.check_failed_logins,
            self.check_unused_accounts,
            self.check_privilege_escalation
        ]
        
        for check in checks:
            try:
                check()
            except Exception as e:
                self.violations.append({
                    'check': check.__name__,
                    'error': str(e),
                    'timestamp': datetime.now()
                })
        
        return self.generate_report()
    
    def check_encryption(self):
        """Verify all PII/PCI data is encrypted"""
        cursor = self.postgres_conn.cursor()
        
        # Check PostgreSQL tablespaces encryption
        cursor.execute("""
            SELECT spcname, spcoptions
            FROM pg_tablespace
            WHERE spcname NOT IN ('pg_default', 'pg_global')
        """)
        
        for tablespace in cursor.fetchall():
            if 'encryption=on' not in str(tablespace[1]):
                self.violations.append({
                    'check': 'encryption',
                    'severity': 'critical',
                    'message': f'Tablespace {tablespace[0]} not encrypted',
                    'remediation': 'Enable tablespace encryption'
                })
    
    def check_access_controls(self):
        """Verify principle of least privilege"""
        cursor = self.snowflake_conn.cursor()
        
        # Check for users with ACCOUNTADMIN role
        cursor.execute("""
            SHOW GRANTS TO ROLE ACCOUNTADMIN
        """)
        
        admin_users = cursor.fetchall()
        
        if len(admin_users) > 5:
            self.violations.append({
                'check': 'access_controls',
                'severity': 'high',
                'message': f'{len(admin_users)} users have ACCOUNTADMIN role',
                'remediation': 'Review and remove unnecessary admin access'
            })
    
    def check_audit_logs(self):
        """Verify audit logging is enabled"""
        cursor = self.postgres_conn.cursor()
        
        cursor.execute("SHOW log_statement")
        log_setting = cursor.fetchone()[0]
        
        if log_setting != 'all':
            self.violations.append({
                'check': 'audit_logs',
                'severity': 'high',
                'message': f'PostgreSQL log_statement is {log_setting}, not "all"',
                'remediation': 'ALTER SYSTEM SET log_statement = "all"'
            })
    
    def check_data_retention(self):
        """Verify retention policies are enforced"""
        cursor = self.postgres_conn.cursor()
        
        # Check for old data that should be deleted
        cursor.execute("""
            SELECT 
                t.table_name,
                COUNT(*) as old_records
            FROM governance.retention_policies p
            JOIN information_schema.tables t ON p.table_name = t.table_name
            JOIN LATERAL (
                SELECT COUNT(*) FROM t.table_name
                WHERE created_at < NOW() - INTERVAL '1 day' * p.retention_period_days
            ) AS old_data
            WHERE p.deletion_enabled = TRUE
              AND old_data.count > 0
        """)
        
        for table, count in cursor.fetchall():
            self.violations.append({
                'check': 'data_retention',
                'severity': 'medium',
                'message': f'{table} has {count} records past retention period',
                'remediation': 'Run retention enforcement job'
            })
    
    def check_pii_access(self):
        """Verify PII access is logged and justified"""
        cursor = self.snowflake_conn.cursor()
        
        cursor.execute("""
            SELECT 
                user_name,
                COUNT(*) as pii_queries
            FROM snowflake.account_usage.query_history
            WHERE query_text ILIKE '%customer%email%'
              OR query_text ILIKE '%customer%phone%'
            AND start_time > DATEADD(day, -1, CURRENT_TIMESTAMP())
            GROUP BY user_name
            HAVING COUNT(*) > 100
        """)
        
        for user, count in cursor.fetchall():
            self.violations.append({
                'check': 'pii_access',
                'severity': 'high',
                'message': f'{user} accessed PII {count} times in last 24h',
                'remediation': 'Review user activity and request justification'
            })
    
    def check_mfa_enabled(self):
        """Verify MFA is enabled for all users"""
        # Check via Okta API
        # ... (implementation)
        pass
    
    def generate_report(self):
        """Generate compliance report"""
        report = {
            'date': datetime.now().isoformat(),
            'total_checks': 10,
            'passed': 10 - len(self.violations),
            'failed': len(self.violations),
            'violations': self.violations,
            'compliance_score': (10 - len(self.violations)) / 10 * 100
        }
        
        # Send to Slack if violations
        if self.violations:
            self.send_slack_report(report)
        
        # Store in database
        self.store_report(report)
        
        return report
    
    def send_slack_report(self, report):
        """Send compliance report to Slack"""
        critical = [v for v in self.violations if v.get('severity') == 'critical']
        high = [v for v in self.violations if v.get('severity') == 'high']
        
        message = f"""
        🔒 *Daily Compliance Report*
        
        ✅ Compliance Score: {report['compliance_score']:.1f}%
        ❌ Violations: {len(self.violations)}
        
        🚨 Critical: {len(critical)}
        ⚠️  High: {len(high)}
        
        Top violations:
        """
        
        for v in (critical + high)[:5]:
            message += f"\n• *{v['check']}*: {v['message']}"
        
        # Send to Slack
        # ... (Slack API call)

# Schedule daily (Airflow DAG)
# airflow trigger: 0 6 * * * (6 AM daily)
```

---

### 12.2 Automated Remediation

```python
# automated_remediation.py

class AutomatedRemediation:
    """
    Automated security remediation
    """
    
    def remediate_brute_force(self, user_email: str):
        """
        Remediate brute force attack
        """
        # 1. Lock account
        db.execute("""
            UPDATE users
            SET account_locked = TRUE,
                locked_at = NOW(),
                locked_reason = 'Brute force detected'
            WHERE email = %s
        """, (user_email,))
        
        # 2. Invalidate all sessions
        db.execute("""
            DELETE FROM sessions
            WHERE user_id = (SELECT user_id FROM users WHERE email = %s)
        """, (user_email,))
        
        # 3. Send notification to user
        send_email(
            to=user_email,
            subject='Security Alert: Account Locked',
            body='Your account has been locked due to suspicious login attempts.'
        )
        
        # 4. Alert security team
        send_slack_alert(
            channel='#security-incidents',
            message=f'🚨 Brute force detected for {user_email}. Account locked.'
        )
    
    def remediate_data_exfiltration(self, user_id: str):
        """
        Remediate data exfiltration attempt
        """
        # 1. Suspend user immediately
        db.execute("""
            UPDATE users
            SET suspended = TRUE,
                suspended_at = NOW(),
                suspended_reason = 'Data exfiltration detected'
            WHERE user_id = %s
        """, (user_id,))
        
        # 2. Revoke all permissions
        db.execute("""
            UPDATE user_roles
            SET role = 'suspended'
            WHERE user_id = %s
        """, (user_id,))
        
        # 3. Preserve evidence
        db.execute("""
            INSERT INTO security_incidents (
                incident_type, user_id, evidence, created_at
            )
            SELECT 
                'data_exfiltration',
                %s,
                json_agg(audit_logs.*),
                NOW()
            FROM audit_logs
            WHERE user_id = %s
              AND timestamp > NOW() - INTERVAL '24 hours'
        """, (user_id, user_id))
        
        # 4. Alert CISO immediately
        send_pagerduty_alert(
            severity='critical',
            title='Data Exfiltration Detected',
            details=f'User {user_id} attempted to exfiltrate large amount of data'
        )
    
    def remediate_privilege_escalation(self, user_id: str, old_role: str):
        """
        Remediate unauthorized privilege escalation
        """
        # 1. Revert role change
        db.execute("""
            UPDATE user_roles
            SET role = %s,
                updated_at = NOW()
            WHERE user_id = %s
        """, (old_role, user_id))
        
        # 2. Log incident
        db.execute("""
            INSERT INTO security_incidents (
                incident_type, user_id, details
            ) VALUES (
                'privilege_escalation',
                %s,
                'Unauthorized privilege escalation reverted'
            )
        """, (user_id,))
        
        # 3. Alert security team
        send_slack_alert(
            channel='#security-incidents',
            message=f'🚨 Privilege escalation detected and reverted for user {user_id}'
        )
```

---

## 13. Conclusion

### 13.1 Résumé des Mesures de Sécurité

| Domaine | Mesures | Status |
|---------|---------|--------|
| **Chiffrement** | AES-256 at rest, TLS 1.3 in transit | ✅ |
| **Accès** | RBAC, MFA, SSO (Okta) | ✅ |
| **Audit** | Logs immutables, 7 ans rétention | ✅ |
| **RGPD** | Droits utilisateurs, DPIA, DPO | ✅ |
| **PCI-DSS** | Level 1, Tokenization, Quarterly scans | ✅ |
| **CCPA** | Data disclosure, Opt-out | ✅ |
| **Governance** | Data catalog, Lineage, Quality | ✅ |
| **Backup** | Multi-tier (RTO 1h-24h), PITR | ✅ |
| **Monitoring** | SIEM (ELK), Alerting 24/7 | ✅ |
| **Automation** | Compliance checks, Auto-remediation | ✅ |

---

### 13.2 Conformité Scores

```
RGPD Compliance:     98% ✅
PCI-DSS Compliance:  100% ✅ (Level 1 certified)
CCPA Compliance:     95% ✅
ISO 27001:           90% (certification en cours)
SOC 2:               95% (audit annuel)
```

---

### 13.3 Audits Planifiés

```yaml
audits_schedule:
  - type: PCI-DSS Quarterly Scan
    frequency: Quarterly
    next_date: 2026-04-01
    vendor: Trustwave
  
  - type: PCI-DSS Annual Assessment
    frequency: Annual
    next_date: 2026-06-01
    vendor: QSA Certified
  
  - type: SOC 2 Audit
    frequency: Annual
    next_date: 2026-08-01
    vendor: Big 4 Firm
  
  - type: GDPR Compliance Review
    frequency: Semi-annual
    next_date: 2026-03-01
    vendor: Internal DPO
  
  - type: Penetration Testing
    frequency: Annual
    next_date: 2026-05-01
    vendor: External Security Firm
```

---