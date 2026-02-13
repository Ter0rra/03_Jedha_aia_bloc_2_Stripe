# Stripe Payment Platform - Architecture Data
## Certification AIA - Projet Terorra

[![RGPD](https://img.shields.io/badge/RGPD-98%25-success.svg)](06_security_et_compliance/)
[![PCI-DSS](https://img.shields.io/badge/PCI--DSS-Level_1-success.svg)](06_security_et_compliance/)
[![Documentation](https://img.shields.io/badge/Docs-515_pages-blue.svg)](.)

---

## 🎯 En Bref

Architecture de données **production-ready** pour plateforme de paiement type Stripe.

**Capacité** : 10M transactions/jour  
**ML Fraud** : <100ms, AUC 0.93  
**Compliance** : RGPD 98% + PCI-DSS 100% + CCPA 95%  
**ROI** : 2,078% ($100M gains/an)

---

## 🏗️ Architecture Globale

### 4 Layers

```
1. STORAGE OPÉRATIONNEL
   PostgreSQL (OLTP) → 10M txn/jour, <30ms
   MongoDB (NoSQL)   → 100M events/jour

2. STREAMING & CDC
   Kafka → 48 topics, <1s latency
   Debezium + Change Streams

3. TRANSFORMATION
   Airflow + DBT → Orchestration & SQL transformations

4. ANALYTICS & ML
   Snowflake (OLAP)  → Star schema, 1TB+ data
   ML Platform       → 4 modèles en production
```

### 3 Niveaux de Détail

📁 **[01_Architecture/](01_Architecture/)** contient 3 versions :

| Version | Composants | Fichier | Usage |
|---------|-----------|---------|-------|
| **Simple** ⭐ | 10 | [Simple/](01_Architecture/Simple/) | Présentation slide principale |
| **Intermédiaire** | 30 | [Intermediaire/](01_Architecture/Intermediaire/) | Backup questions techniques |
| **Complète** | 60+ | [Details/](01_Architecture/Details/) | Documentation annexe |

---

## 📁 Structure du Projet

```
stripe-data-architecture/
│
├── 01_Architecture/                    # 3 niveaux d'architecture
│   ├── Simple/                         # 10 composants ⭐
│   │   ├── Archi_globale_simple.png
│   │   └── archi_global_simple.txt
│   ├── Intermediaire/                  # 30 composants
│   │   ├── Archi_globale_inter.png
│   │   └── architecture_intermediaire_gleek.txt
│   └── Details/                        # 60+ composants
│       ├── Full_architecture.png
│       └── architecture_globale_full.txt
│
├── 02_OLTP/                            # Base transactionnelle
│   ├── 02_oltp_postgresql_documentation.md    (40 pages)
│   ├── OLTP_STAR_SCHEMA_GLEEK.txt
│   └── stripe_OLTP_v2.png              # Schéma 6 tables
│
├── 03_OLAP/                            # Data Warehouse
│   ├── 03_olap_snowflake_documentation.md     (45 pages)
│   ├── OLAP_STAR_SCHEMA_DBDIAG.txt
│   └── stripe_OLAP_v2.png              # Star schema
│
├── 04_NoSQL/                           # Événements comportementaux
│   └── 04_nosql_data_model.md          (60 pages)
│
├── 05_Pipelines/                       # ETL/ELT & CDC
│   ├── 05_1_data_pipeline_architecture.md     (80 pages)
│   └── 05_2_data_pipeline_architecture.md     (50 pages)
│
├── 06_security_et_compliance/          # Sécurité & réglementation
│   ├── 06_1_security_compliance_plan.md       (60 pages)
│   └── 06_2_security_compliance_plan.md       (50 pages)
│
├── 07_ML_strategy/                     # MLOps complet
│   └── 07_ml_integration_strategy.md   (50 pages)
│
├── 08_queries_SQL_NoSQL/               # 50+ requêtes pratiques
│   └── 08_sql_nosql_queries.md         (40 pages)
│
└── README.md                           # Ce fichier
```

**Total** : **515 pages** de documentation technique 📚

---

## 🛠️ Stack Technologique

### Couche Storage (02_OLTP + 03_OLAP + 04_NoSQL)

| Composant | Technologie | Utilisation | Documentation |
|-----------|------------|-------------|---------------|
| **OLTP** | PostgreSQL 15 | Transactions ACID | [02_OLTP/](02_OLTP/) |
| **NoSQL** | MongoDB 6 | Événements comportementaux | [04_NoSQL/](04_NoSQL/) |
| **OLAP** | Snowflake | Analytics & BI | [03_OLAP/](03_OLAP/) |

### Couche Streaming & Transformation (05_Pipelines)

| Composant | Technologie | Utilisation | Documentation |
|-----------|------------|-------------|---------------|
| **Streaming** | Apache Kafka 3.x | Event streaming, 2M msg/sec | [05_Pipelines/](05_Pipelines/) |
| **CDC PostgreSQL** | Debezium 2.4 | Change Data Capture | [05_Pipelines/](05_Pipelines/) |
| **CDC MongoDB** | Change Streams | Change Data Capture | [05_Pipelines/](05_Pipelines/) |
| **Orchestration** | Apache Airflow 2.8 | Workflow orchestration | [05_Pipelines/](05_Pipelines/) |
| **Transformation** | DBT 1.7 | SQL transformations testées | [05_Pipelines/](05_Pipelines/) |

### Couche ML & Analytics (07_ML_strategy)

| Composant | Technologie | Utilisation | Documentation |
|-----------|------------|-------------|---------------|
| **Feature Store** | Feast 0.35 | Features online/offline | [07_ML_strategy/](07_ML_strategy/) |
| **Model Registry** | MLflow 2.9 | Versioning, A/B test | [07_ML_strategy/](07_ML_strategy/) |
| **Training** | Kubeflow 1.8 | ML pipelines | [07_ML_strategy/](07_ML_strategy/) |
| **Serving** | AWS SageMaker | Inference <100ms | [07_ML_strategy/](07_ML_strategy/) |
| **Monitoring** | Evidently AI | Drift detection | [07_ML_strategy/](07_ML_strategy/) |

### Couche Sécurité (06_security_et_compliance)

| Composant | Technologie | Utilisation | Documentation |
|-----------|------------|-------------|---------------|
| **Encryption** | AES-256 + TLS 1.3 | At rest + In transit | [06_security_et_compliance/](06_security_et_compliance/) |
| **Access Control** | Okta SSO | MFA + RBAC | [06_security_et_compliance/](06_security_et_compliance/) |
| **Secrets** | HashiCorp Vault | Secret management | [06_security_et_compliance/](06_security_et_compliance/) |
| **SIEM** | ELK Stack | Security monitoring | [06_security_et_compliance/](06_security_et_compliance/) |
| **Monitoring** | Prometheus + Grafana | Metrics 24/7 | [06_security_et_compliance/](06_security_et_compliance/) |

---

## 🤖 Machine Learning (07_ML_strategy)

### 4 Modèles en Production

| Modèle | Algorithm | Metrics | Impact Business |
|--------|-----------|---------|-----------------|
| **1. Fraud Detection** | XGBoost | AUC 0.93, <100ms | -40% fraude ($15M/an) |
| **2. Churn Prediction** | LightGBM | AUC 0.87 | +12% rétention ($25M/an) |
| **3. Customer LTV** | XGBoost Regressor | R² 0.82 | Optimisation marketing |
| **4. Segmentation** | K-Means | 8 clusters | +15% conversion |

📄 **Détails complets** : [07_ML_strategy/07_ml_integration_strategy.md](07_ML_strategy/07_ml_integration_strategy.md)

---

## 🔒 Sécurité & Conformité (06_security_et_compliance)

### Compliance Scores

```
✅ RGPD:          98%  (DPO désigné, DPIA complétée)
✅ PCI-DSS L1:   100%  (QSA certified)
✅ CCPA:          95%  (Data disclosure portal)
✅ ISO 27001:     90%  (certification en cours)
✅ SOC 2 Type II: 95%  (audit annuel)
```

### Security Measures

- 🔐 **Encryption** : AES-256 at rest + TLS 1.3 in transit
- 🔑 **Access Control** : RBAC + MFA obligatoire + SSO (Okta)
- 📝 **Audit Logs** : Immutables, 7 ans rétention, blockchain hash
- 🤖 **Automation** : PII detection daily, 10 compliance checks daily
- 🚨 **Alerting** : 6 types d'alertes sécurité + auto-remediation

📄 **Détails complets** : 
- [06_1_security_compliance_plan.md](06_security_et_compliance/06_1_security_compliance_plan.md)
- [06_2_security_compliance_plan.md](06_security_et_compliance/06_2_security_compliance_plan.md)

---

## 📊 Performance & Métriques

### Latences Production

```
OLTP (PostgreSQL):      <30ms  (P95)
ML Fraud Detection:     <100ms (P95)
Kafka CDC:              <1s    (end-to-end)
Snowflake Queries:      <5s    (complex analytics)
Availability:           99.99% (52 min/an max)
```

### Throughput

```
PostgreSQL:  10,000+ TPS
MongoDB:     100M+ events/jour
Kafka:       2M+ messages/sec
Snowflake:   50,000+ queries/jour
```

---

## 💰 ROI Business

### Gains Annuels

```
Fraude -40%:              $15M
Conversion +5%:           $50M
Rétention +12%:           $25M
Optimisation infra -30%:  $10M
─────────────────────────────
TOTAL GAINS:             $100M/an
```

### Investissement

```
Infrastructure (2 ans):   $138K/an × 2 = $276K
Équipe dev (10 eng × 2y): 10 × $200K × 2 = $4M
Misc (tools, audits):     $300K
─────────────────────────────
TOTAL INVEST:            $4.6M
```

### ROI

```
ROI:         ($100M - $4.6M) / $4.6M = 2,078%
Payback:     16 jours
```

---

## 🚀 Navigation Documentation

### Par Thématique

| Thématique | Dossier | Pages | Description |
|------------|---------|-------|-------------|
| **Architecture Globale** | [01_Architecture/](01_Architecture/) | - | 3 niveaux (simple, inter, full) |
| **Base Transactionnelle** | [02_OLTP/](02_OLTP/) | 40 | PostgreSQL 6 tables normalisées |
| **Data Warehouse** | [03_OLAP/](03_OLAP/) | 45 | Snowflake star schema |
| **NoSQL Événements** | [04_NoSQL/](04_NoSQL/) | 60 | MongoDB 9 collections |
| **Pipelines Data** | [05_Pipelines/](05_Pipelines/) | 130 | Kafka, Airflow, DBT, CDC |
| **Sécurité & Compliance** | [06_security_et_compliance/](06_security_et_compliance/) | 110 | RGPD, PCI-DSS, CCPA |
| **Machine Learning** | [07_ML_strategy/](07_ML_strategy/) | 50 | MLOps complet |
| **Requêtes Pratiques** | [08_queries_SQL_NoSQL/](08_queries_SQL_NoSQL/) | 40 | 50+ queries exemples |

### Par Ordre de Lecture Recommandé

1. 📊 **Commencer par** : [01_Architecture/Simple/](01_Architecture/Simple/) (vue globale)
2. 💾 **Puis bases de données** : [02_OLTP/](02_OLTP/) → [03_OLAP/](03_OLAP/) → [04_NoSQL/](04_NoSQL/)
3. 🔄 **Ensuite pipelines** : [05_Pipelines/](05_Pipelines/)
4. 🤖 **Machine Learning** : [07_ML_strategy/](07_ML_strategy/)
5. 🔒 **Sécurité** : [06_security_et_compliance/](06_security_et_compliance/)
6. 📝 **Pratique** : [08_queries_SQL_NoSQL/](08_queries_SQL_NoSQL/)

---

## 🎯 Quick Start

### 1. Explorer l'Architecture

```bash
# Voir architecture simple (recommandé pour commencer)
cat 01_Architecture/Simple/archi_global_simple.txt

# Ou ouvrir le PNG
open 01_Architecture/Simple/Archi_globale_simple.png
```

### 2. Comprendre les Bases de Données

```bash
# OLTP PostgreSQL
cat 02_OLTP/02_oltp_postgresql_documentation.md

# OLAP Snowflake
cat 03_OLAP/03_olap_snowflake_documentation.md

# NoSQL MongoDB
cat 04_NoSQL/04_nosql_data_model.md
```

### 3. Étudier les Pipelines

```bash
# Pipelines Part 1 (ETL/ELT, Kafka, Airflow)
cat 05_Pipelines/05_1_data_pipeline_architecture.md

# Pipelines Part 2 (CDC, DBT, Monitoring)
cat 05_Pipelines/05_2_data_pipeline_architecture.md
```

### 4. Machine Learning

```bash
# MLOps complet
cat 07_ML_strategy/07_ml_integration_strategy.md
```

### 5. Sécurité & Compliance

```bash
# Security Part 1 (Encryption, RBAC, Audit, RGPD, PCI-DSS, CCPA)
cat 06_security_et_compliance/06_1_security_compliance_plan.md

# Security Part 2 (Governance, DR, Monitoring, Automated Controls)
cat 06_security_et_compliance/06_2_security_compliance_plan.md
```

### 6. Requêtes Pratiques

```bash
# 50+ exemples SQL/NoSQL
cat 08_queries_SQL_NoSQL/08_sql_nosql_queries.md
```

---

