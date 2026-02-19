# Security & Compliance Plan - Sections Manquantes 2-5
## Complément au Document Principal

**Document** : 06_1_security_compliance_plan.md  
**Sections** : 2, 3, 4, 5 (manquantes dans le document original)  
**Date** : Janvier 2026

---

## 📋 Note

Les sections 2-5 étaient absentes du document original. Ce fichier les complète.

**Sections** :
- Section 2 : Framework de sécurité
- Section 3 : Chiffrement des données
- Section 4 : Contrôle d'accès et authentification
- Section 5 : Audit et logging

---

## 2. Framework de Sécurité

### 2.1 Architecture Sécurité Multi-Couches

```
┌─────────────────────────────────────────────────────────────┐
│              DEFENSE IN DEPTH (7 LAYERS)                     │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Layer 1: PHYSICAL SECURITY                                  │
│  └─ AWS Data Centers (SOC 1/2/3, ISO 27001)                 │
│                                                               │
│  Layer 2: NETWORK SECURITY                                   │
│  ├─ VPC Isolation                                            │
│  ├─ Security Groups (Firewall)                               │
│  ├─ Network ACLs                                             │
│  └─ WAF (Web Application Firewall)                           │
│                                                               │
│  Layer 3: HOST SECURITY                                      │
│  ├─ OS Hardening (CIS Benchmarks)                            │
│  ├─ Anti-malware (AWS GuardDuty)                             │
│  ├─ Patch Management (SSM)                                   │
│  └─ Host-based IDS                                           │
│                                                               │
│  Layer 4: APPLICATION SECURITY                               │
│  ├─ Input Validation                                         │
│  ├─ SQL Injection Prevention                                 │
│  ├─ XSS Protection                                           │
│  └─ CSRF Tokens                                              │
│                                                               │
│  Layer 5: DATA SECURITY                                      │
│  ├─ Encryption at Rest (AES-256)                             │
│  ├─ Encryption in Transit (TLS 1.3)                          │
│  ├─ Tokenization (PCI data)                                  │
│  └─ Data Masking                                             │
│                                                               │
│  Layer 6: IDENTITY & ACCESS                                  │
│  ├─ SSO (Okta)                                               │
│  ├─ MFA (Multi-Factor Auth)                                  │
│  ├─ RBAC (Role-Based Access Control)                         │
│  └─ Least Privilege                                          │
│                                                               │
│  Layer 7: MONITORING & RESPONSE                              │
│  ├─ SIEM (Elasticsearch)                                     │
│  ├─ Alerting (PagerDuty)                                     │
│  ├─ Incident Response                                        │
│  └─ Forensics                                                │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Security Standards & Certifications

| Standard | Status | Scope | Audit Frequency |
|----------|--------|-------|-----------------|
| **ISO 27001** | 90% compliant | Information Security Management | Annual |
| **SOC 2 Type II** | 95% compliant | Controls for Security, Availability | Annual |
| **PCI-DSS Level 1** | 100% certified | Payment Card Data Security | Quarterly scans + Annual assessment |
| **RGPD** | 98% compliant | Personal Data Protection | Semi-annual review |
| **CCPA** | 95% compliant | California Consumer Privacy | Annual review |
| **NIST Cybersecurity Framework** | Implemented | Risk Management | Continuous |

### 2.3 Security Policies

```yaml
security_policies:
  password_policy:
    min_length: 12
    complexity: "uppercase + lowercase + numbers + symbols"
    expiry_days: 90
    history: 5  # Cannot reuse last 5 passwords
    lockout_attempts: 5
    lockout_duration_minutes: 30
  
  access_review:
    frequency: quarterly
    approvers:
      - "Manager"
      - "Data Protection Officer"
    scope: "All access to PII/PCI data"
  
  data_classification:
    levels:
      - public: "No restrictions"
      - internal: "Company employees only"
      - confidential: "Restricted access, encryption required"
      - restricted: "PCI/PII data, strict access controls"
    
  incident_response:
    detection_target: "< 5 minutes"
    response_target: "< 30 minutes"
    containment_target: "< 2 hours"
    recovery_target: "< 24 hours"
    
  security_training:
    frequency: annual
    mandatory: true
    topics:
      - "RGPD awareness"
      - "Phishing detection"
      - "Data handling"
      - "Incident reporting"
```

---

## 3. Chiffrement des Données

### 3.1 Encryption at Rest

#### PostgreSQL (OLTP)

```yaml
postgresql_encryption:
  method: "Transparent Data Encryption (TDE)"
  algorithm: "AES-256-CBC"
  key_management: "AWS KMS"
  
  tablespaces:
    - name: "pg_default"
      encrypted: true
      key_rotation: 90_days
    
    - name: "customer_data"
      encrypted: true
      key_rotation: 30_days  # More frequent for PII
      
  column_level:
    - table: "payment_method"
      column: "card_number_encrypted"
      encryption: "pgcrypto AES-256"
      note: "Never store full card number, only last4 + token"
```

**Implementation** :

```sql
-- Enable pgcrypto extension
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Encrypt sensitive column
CREATE TABLE payment_method (
    payment_method_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    card_last4 VARCHAR(4) NOT NULL,
    card_token VARCHAR(255) NOT NULL,  -- From external vault
    card_encrypted BYTEA,  -- Encrypted with pgp_sym_encrypt
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert with encryption
INSERT INTO payment_method (payment_method_id, customer_id, card_last4, card_token, card_encrypted)
VALUES (
    gen_random_uuid(),
    'customer_123',
    '4242',
    'tok_visa_4242',
    pgp_sym_encrypt('sensitive_data', 'encryption_key_from_kms')
);

-- Decrypt (only authorized roles)
SELECT 
    payment_method_id,
    card_last4,
    pgp_sym_decrypt(card_encrypted, 'encryption_key_from_kms') AS card_data
FROM payment_method
WHERE customer_id = 'customer_123';
```

---

#### MongoDB (NoSQL)

```yaml
mongodb_encryption:
  method: "Encryption at Rest"
  algorithm: "AES-256-GCM"
  key_management: "AWS KMS"
  
  encryption_scope: "database"  # All collections encrypted
  
  field_level_encryption:
    - collection: "user_interactions"
      fields:
        - "details.payment_info"  # NEVER store, but if needed
      algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
```

---

#### Snowflake (OLAP)

```yaml
snowflake_encryption:
  method: "End-to-End Encryption"
  algorithm: "AES-256"
  key_management: "Tri-Secret Secure (Customer + Snowflake + Cloud Provider)"
  
  stages:
    - "Data in transit: TLS 1.3"
    - "Data at rest: AES-256"
    - "Internal transfers: Encrypted"
    - "Backups: Encrypted"
  
  key_rotation:
    master_key: "Automatically by Snowflake"
    table_keys: "Every 30 days"
    file_keys: "Per file"
```

---

#### AWS S3 (Object Storage)

```yaml
s3_encryption:
  default: "SSE-S3 (AES-256)"
  
  buckets:
    - name: "stripe-backups"
      encryption: "SSE-KMS"
      kms_key_id: "arn:aws:kms:us-east-1:123456:key/abc-123"
      versioning: enabled
      
    - name: "stripe-ml-models"
      encryption: "SSE-KMS"
      kms_key_id: "arn:aws:kms:us-east-1:123456:key/def-456"
      
    - name: "stripe-logs"
      encryption: "SSE-S3"
      lifecycle:
        transition_to_glacier: 90_days
        delete_after: 2555_days  # 7 years
```

---

### 3.2 Encryption in Transit

#### TLS Configuration

```yaml
tls_config:
  minimum_version: "TLS 1.3"
  cipher_suites:
    - "TLS_AES_256_GCM_SHA384"
    - "TLS_AES_128_GCM_SHA256"
    - "TLS_CHACHA20_POLY1305_SHA256"
  
  certificate_management:
    provider: "AWS Certificate Manager (ACM)"
    renewal: "Automatic"
    validity: 13_months
    
  applications:
    - service: "API Gateway"
      tls_version: "TLS 1.3"
      certificate: "*.stripe-api.com"
      
    - service: "PostgreSQL"
      tls_version: "TLS 1.2+"
      certificate: "PostgreSQL self-signed (internal)"
      client_auth: "required"
      
    - service: "Kafka"
      tls_version: "TLS 1.2+"
      certificate: "Kafka broker certificates"
      client_auth: "required"
```

---

### 3.3 Key Management (AWS KMS)

```yaml
kms_architecture:
  master_keys:
    - alias: "stripe/database-encryption"
      description: "Master key for database encryption"
      rotation: enabled
      rotation_period: 365_days
      
    - alias: "stripe/s3-encryption"
      description: "Master key for S3 bucket encryption"
      rotation: enabled
      
    - alias: "stripe/application-secrets"
      description: "Application secrets encryption"
      rotation: enabled
  
  key_policies:
    - principals:
        - "arn:aws:iam::123456:role/DataEngineer"
      actions:
        - "kms:Decrypt"
        - "kms:DescribeKey"
      resources: ["stripe/database-encryption"]
      
    - principals:
        - "arn:aws:iam::123456:role/SecurityAdmin"
      actions:
        - "kms:*"
      resources: ["*"]
  
  audit:
    cloudtrail: enabled
    log_group: "/aws/kms/stripe"
    alerts:
      - "Unauthorized access attempts"
      - "Key deletion attempts"
      - "Excessive decrypt operations"
```

---

## 4. Contrôle d'Accès et Authentification

### 4.1 Identity Provider (Okta SSO)

```yaml
okta_configuration:
  authentication:
    method: "SAML 2.0"
    mfa: "required"
    mfa_methods:
      - "Okta Verify (push)"
      - "SMS"
      - "Google Authenticator (TOTP)"
    
  password_policy:
    min_length: 12
    complexity: "high"
    expiry_days: 90
    lockout_attempts: 5
    
  session_management:
    idle_timeout: 30_minutes
    absolute_timeout: 8_hours
    remember_device: 30_days
```

---

### 4.2 Role-Based Access Control (RBAC)

#### PostgreSQL Roles

```sql
-- Role hierarchy
CREATE ROLE data_reader;
CREATE ROLE data_analyst;
CREATE ROLE data_engineer;
CREATE ROLE data_admin;

-- data_reader: Read-only access (BI users, analysts)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO data_reader;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO data_reader;

-- data_analyst: Read + limited write (temp tables)
GRANT data_reader TO data_analyst;
GRANT CREATE ON SCHEMA analytics TO data_analyst;

-- data_engineer: Full DDL/DML (pipeline maintenance)
GRANT data_analyst TO data_engineer;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO data_engineer;
GRANT CREATE ON DATABASE stripe_oltp TO data_engineer;

-- data_admin: Superuser (DBA only)
GRANT ALL PRIVILEGES ON DATABASE stripe_oltp TO data_admin;

-- PII access (restricted)
CREATE ROLE pii_reader;
GRANT SELECT ON customer, payment_method TO pii_reader;

-- Grant to specific users
GRANT data_reader TO analyst_john;
GRANT data_engineer TO engineer_jane;
GRANT pii_reader TO dpo_alice;  -- Data Protection Officer only
```

---

#### Snowflake Roles

```sql
-- Role hierarchy
CREATE ROLE data_viewer;
CREATE ROLE analyst;
CREATE ROLE engineer;
CREATE ROLE admin;

-- data_viewer: Read raw data
GRANT USAGE ON WAREHOUSE compute_xs TO data_viewer;
GRANT USAGE ON DATABASE stripe_dwh TO data_viewer;
GRANT USAGE ON SCHEMA raw TO data_viewer;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO data_viewer;

-- analyst: Read analytics views
GRANT ROLE data_viewer TO analyst;
GRANT USAGE ON SCHEMA analytics TO analyst;
GRANT SELECT ON ALL VIEWS IN SCHEMA analytics TO analyst;
GRANT USAGE ON WAREHOUSE compute_small TO analyst;

-- engineer: Full access for transformations
GRANT ROLE analyst TO engineer;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA analytics TO engineer;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA raw TO engineer;
GRANT USAGE ON WAREHOUSE compute_large TO engineer;

-- admin: Superuser
GRANT ALL PRIVILEGES ON DATABASE stripe_dwh TO admin;
GRANT ROLE engineer TO admin;

-- PII-restricted roles
CREATE ROLE pii_masked_reader;
CREATE ROLE pii_full_reader;

-- Assign users to roles
GRANT data_viewer TO USER john_analyst;
GRANT engineer TO USER jane_engineer;
GRANT pii_full_reader TO USER alice_dpo;
```

---

#### MongoDB Roles

```javascript
// Custom roles for MongoDB
db.createRole({
  role: "dataReader",
  privileges: [
    {
      resource: { db: "stripe_events", collection: "" },
      actions: ["find"]
    }
  ],
  roles: []
});

db.createRole({
  role: "dataEngineer",
  privileges: [
    {
      resource: { db: "stripe_events", collection: "" },
      actions: ["find", "insert", "update", "remove"]
    },
    {
      resource: { db: "stripe_events", collection: "" },
      actions: ["createIndex", "dropIndex"]
    }
  ],
  roles: ["dataReader"]
});

// Assign roles to users
db.createUser({
  user: "analyst_john",
  pwd: "secure_password",
  roles: ["dataReader"]
});

db.createUser({
  user: "engineer_jane",
  pwd: "secure_password",
  roles: ["dataEngineer"]
});
```

---

### 4.3 Principle of Least Privilege

```yaml
least_privilege_implementation:
  default: "deny_all"
  
  access_request_workflow:
    step_1: "Employee submits access request via ServiceNow"
    step_2: "Manager approves business justification"
    step_3: "Data Protection Officer reviews for PII/PCI"
    step_4: "Security team grants minimum required access"
    step_5: "Access auto-expires after 90 days"
    
  access_review:
    frequency: quarterly
    automated_checks:
      - "Unused accounts (no login 60 days) → disable"
      - "Excessive permissions → alert manager"
      - "Access beyond job role → flag for review"
```

---

### 4.4 Service Accounts & API Keys

```yaml
service_accounts:
  airflow_service:
    type: "AWS IAM Role"
    permissions:
      - "s3:GetObject on stripe-data/*"
      - "rds:Connect to stripe-oltp"
      - "secretsmanager:GetSecretValue"
    rotation: "Automatic via IAM"
    
  kafka_connect:
    type: "Kubernetes Service Account"
    permissions:
      - "Read/Write Kafka topics"
      - "Connect to PostgreSQL (readonly)"
    secret_management: "Sealed Secrets"
    
  ml_inference:
    type: "AWS IAM Role"
    permissions:
      - "sagemaker:InvokeEndpoint"
      - "s3:GetObject on ml-models/*"
    rotation: "90 days"

api_keys:
  management: "HashiCorp Vault"
  rotation: "30 days"
  revocation: "Immediate on suspicious activity"
  
  monitoring:
    - "API key usage patterns"
    - "Anomaly detection (unusual requests)"
    - "Rate limiting (1000 req/min per key)"
```

---

## 5. Audit et Logging

### 5.1 Logging Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  CENTRALIZED LOGGING                         │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Sources                                                     │
│  ├─ PostgreSQL audit logs                                    │
│  ├─ MongoDB audit logs                                       │
│  ├─ Snowflake query history                                  │
│  ├─ Kafka access logs                                        │
│  ├─ Airflow task logs                                        │
│  ├─ Kubernetes audit logs                                    │
│  ├─ Application logs (API)                                   │
│  └─ AWS CloudTrail                                           │
│         ↓                                                     │
│      Logstash (Parse & Enrich)                               │
│         ↓                                                     │
│      Elasticsearch (Index & Store)                           │
│         ↓                                                     │
│      Kibana (Visualize & Alert)                              │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

### 5.2 Audit Logs PostgreSQL

```sql
-- Enable audit logging (pgaudit extension)
CREATE EXTENSION IF NOT EXISTS pgaudit;

-- Configure audit settings
ALTER SYSTEM SET pgaudit.log = 'all';  -- Log all statements
ALTER SYSTEM SET pgaudit.log_catalog = 'off';  -- Don't log system catalog queries
ALTER SYSTEM SET pgaudit.log_parameter = 'on';  -- Log query parameters
ALTER SYSTEM SET pgaudit.log_relation = 'on';  -- Log table names
ALTER SYSTEM SET pgaudit.log_statement_once = 'off';  -- Log every statement

-- Log format
ALTER SYSTEM SET log_destination = 'jsonlog';
ALTER SYSTEM SET log_line_prefix = '%m [%p] %u@%d ';
ALTER SYSTEM SET log_connections = 'on';
ALTER SYSTEM SET log_disconnections = 'on';
ALTER SYSTEM SET log_duration = 'on';

-- Reload configuration
SELECT pg_reload_conf();

-- Example audit log entry
{
  "timestamp": "2026-01-22T14:30:15.234Z",
  "user": "engineer_jane",
  "database": "stripe_oltp",
  "statement": "SELECT * FROM customer WHERE email = 'user@example.com'",
  "duration_ms": 12,
  "rows_returned": 1,
  "client_ip": "10.0.1.50",
  "application_name": "DataGrip"
}
```

---

### 5.3 Immutable Audit Logs

```python
# audit_logger.py
import hashlib
import json

class ImmutableAuditLogger:
    """
    Blockchain-style audit logging
    Each log entry contains hash of previous entry
    """
    
    def __init__(self):
        self.db = get_postgres_connection()
        self._initialize_audit_table()
    
    def _initialize_audit_table(self):
        """Create audit log table with hash chain"""
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                log_id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ DEFAULT NOW(),
                event_type VARCHAR(100) NOT NULL,
                user_id VARCHAR(255),
                resource VARCHAR(255),
                action VARCHAR(50),
                details JSONB,
                previous_hash VARCHAR(64),
                current_hash VARCHAR(64) NOT NULL,
                CONSTRAINT unique_hash UNIQUE (current_hash)
            );
            
            -- Index for fast lookups
            CREATE INDEX idx_audit_timestamp ON audit_log(timestamp DESC);
            CREATE INDEX idx_audit_user ON audit_log(user_id);
            CREATE INDEX idx_audit_resource ON audit_log(resource);
        """)
    
    def log_event(self, event_type, user_id, resource, action, details=None):
        """
        Log an event with blockchain-style hash
        """
        # Get previous hash
        previous_entry = self.db.execute("""
            SELECT current_hash 
            FROM audit_log 
            ORDER BY log_id DESC 
            LIMIT 1
        """).fetchone()
        
        previous_hash = previous_entry[0] if previous_entry else "0" * 64
        
        # Create log entry
        log_entry = {
            'event_type': event_type,
            'user_id': user_id,
            'resource': resource,
            'action': action,
            'details': details,
            'previous_hash': previous_hash,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Calculate current hash
        log_string = json.dumps(log_entry, sort_keys=True)
        current_hash = hashlib.sha256(log_string.encode()).hexdigest()
        
        # Insert log entry
        self.db.execute("""
            INSERT INTO audit_log (
                event_type, user_id, resource, action, details,
                previous_hash, current_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            event_type, user_id, resource, action,
            json.dumps(details), previous_hash, current_hash
        ))
        
        return current_hash
    
    def verify_integrity(self):
        """
        Verify audit log integrity by checking hash chain
        """
        logs = self.db.execute("""
            SELECT log_id, event_type, user_id, resource, action, 
                   details, previous_hash, current_hash
            FROM audit_log
            ORDER BY log_id
        """).fetchall()
        
        for i, log in enumerate(logs):
            # Reconstruct hash
            log_entry = {
                'event_type': log[1],
                'user_id': log[2],
                'resource': log[3],
                'action': log[4],
                'details': log[5],
                'previous_hash': log[6],
                'timestamp': log[0]  # Simplified
            }
            
            expected_hash = hashlib.sha256(
                json.dumps(log_entry, sort_keys=True).encode()
            ).hexdigest()
            
            if expected_hash != log[7]:
                raise AuditIntegrityError(
                    f"Audit log tampered! Log ID {log[0]}"
                )
            
            # Verify chain
            if i > 0 and log[6] != logs[i-1][7]:
                raise AuditIntegrityError(
                    f"Broken chain at log ID {log[0]}"
                )
        
        return True

# Usage
audit = ImmutableAuditLogger()

# Log PII access
audit.log_event(
    event_type='pii_access',
    user_id='analyst_john',
    resource='customer.email',
    action='SELECT',
    details={'customer_id': 'cust_123', 'query': 'SELECT email FROM customer...'}
)

# Verify integrity daily
audit.verify_integrity()
```

---

### 5.4 Log Retention

```yaml
log_retention_policy:
  audit_logs:
    retention: 2555_days  # 7 years (legal requirement)
    storage: "S3 Glacier Deep Archive"
    immutable: true
    
  application_logs:
    retention: 365_days  # 1 year
    storage: "Elasticsearch → S3 Standard-IA"
    
  access_logs:
    retention: 90_days
    storage: "Elasticsearch"
    
  query_logs:
    retention: 30_days
    storage: "Elasticsearch"
    
  deletion_process:
    automated: true
    verification: "Manual approval for audit logs"
    documentation: "Required for all deletions"
```

---

**FIN DES SECTIONS 2-5**

Ces sections complètent le document `06_1_security_compliance_plan.md` qui commençait directement à la section 6.

Le document complet est maintenant structuré :
- Section 1 : Vue d'ensemble ✅
- Section 2 : Framework de sécurité ✅ (CE FICHIER)
- Section 3 : Chiffrement des données ✅ (CE FICHIER)
- Section 4 : Contrôle d'accès et authentification ✅ (CE FICHIER)
- Section 5 : Audit et logging ✅ (CE FICHIER)
- Section 6 : Conformité RGPD ✅ (Document original)
- Section 7 : Conformité PCI-DSS ✅ (Document original)
- Section 8 : Conformité CCPA ✅ (Document original)
- Sections 9-12 : Dans security_compliance_part2.md ✅
