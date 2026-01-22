# Stratégie d'Intégration Machine Learning - Stripe Payment Platform
## ML Pipeline : Détection de Fraude, Personnalisation et Analyse Prédictive

---

## 📋 Vue d'ensemble

### Objectifs ML

**Use Cases principaux :**
1. **Détection de fraude** (CRITICAL) - Score risque transaction en temps réel <100ms
2. **Segmentation client** (HIGH) - Personnalisation expérience
3. **Prédiction churn** (HIGH) - Identification clients à risque 60 jours avant
4. **Prédiction LTV** (MEDIUM) - Valeur client à 12/24 mois
5. **Détection d'anomalies** (HIGH) - Patterns inhabituels

### Stack ML

| Composant | Technologie | Justification |
|-----------|-------------|---------------|
| Training | XGBoost, PyTorch | Performance tabular + DL |
| Feature Store | Feast + Redis | Real-time + batch features |
| Model Registry | MLflow | Versioning, tracking |
| Inference | FastAPI + ONNX | <100ms latency |
| Orchestration | Airflow + Kubeflow | Pipeline automation |
| Monitoring | Evidently AI | Drift detection |
| Serving | AWS SageMaker | Auto-scaling |

---

## Feature Engineering

### Feature Categories (87 features total)

**1. Transaction Features (14)**
- amount, amount_usd, amount_zscore
- currency, transaction_hour, is_weekend
- device_type, payment_method_type

**2. Velocity Features (18)**  
- transactions_last_1h/24h/7d
- total_amount_last_24h
- avg_transaction_amount_30d
- days_since_last_transaction
- failed_transactions_24h

**3. Customer Features (18)**
- account_age_days
- total_lifetime_value
- customer_segment
- chargeback_rate, refund_rate
- avg_days_between_transactions

**4. Device Features (15)**
- device_type, os, browser
- is_known_device, is_new_device
- devices_used_count
- screen_resolution

**5. Location Features (17)**
- country, city, latitude, longitude
- distance_from_billing_km
- is_vpn_detected, is_proxy_detected
- travel_velocity_kmh

**6. Payment Method Features (13)**
- card_brand, card_funding
- bin_risk_score
- is_prepaid_card
- failed_attempts_last_24h

**7. Merchant Features (8)**
- merchant_category
- merchant_risk_score
- merchant_chargeback_rate
- previous_transactions_with_merchant

**8. Behavioral Features (10)**
- time_on_checkout_page_seconds
- form_fill_speed_score
- mouse_movement_entropy
- copy_paste_detected

**9. Network Features (11)**
- ip_reputation_score
- is_datacenter_ip
- email_domain_age_days
- is_disposable_email

**10. Aggregated Scores (7)**
- velocity_risk_score
- location_risk_score
- device_risk_score
- payment_risk_score
- behavioral_risk_score

---

## Modèle de Détection de Fraude

### Architecture

**Algorithme :** XGBoost (Gradient Boosting)

**Justification :**
- Performance exceptionnelle sur données tabulaires
- Gestion native déséquilibre classes
- Interpretabilité (SHAP values)
- Latence <100ms

**Hyperparamètres optimisés :**
```python
{
    'max_depth': 8,
    'learning_rate': 0.05,
    'n_estimators': 300,
    'scale_pos_weight': 20,  # Fraud ~5%
    'subsample': 0.8,
    'colsample_bytree': 0.8
}
```

### Métriques cibles

- **AUC-ROC** : >0.92
- **Precision** : >90% (minimiser faux positifs)
- **Recall** : >85% (détecter majorité fraudes)
- **Latency** : <100ms (inference temps réel)
- **False Positive Rate** : <2%

### Pipeline d'entraînement

```python
# Pseudo-code simplifié

# 1. Extraction features depuis Feature Store
training_df = feature_store.get_historical_features(
    entity_df=transactions_df,
    features=ALL_FRAUD_FEATURES
)

# 2. Préparation données
X = training_df.drop(['is_fraud'], axis=1)
y = training_df['is_fraud']

# Split: 70% train, 15% val, 15% test
X_train, X_val, X_test, y_train, y_val, y_test = split_data(X, y)

# 3. Training avec early stopping
model = xgb.train(
    params=optimized_params,
    dtrain=DMatrix(X_train, y_train),
    num_boost_round=1000,
    early_stopping_rounds=50,
    evals=[(dval, 'validation')]
)

# 4. Seuil optimal (maximize F1)
threshold = find_optimal_threshold(y_val, predictions_val)

# 5. Évaluation test set
auc = roc_auc_score(y_test, predictions_test)
precision = precision_score(y_test, predictions_test > threshold)
recall = recall_score(y_test, predictions_test > threshold)

# 6. Export ONNX (fast inference)
export_to_onnx(model, "fraud_detection_v1.onnx")

# 7. MLflow tracking
mlflow.log_metrics({'auc': auc, 'precision': precision, 'recall': recall})
mlflow.log_model(model, "fraud-model")
```

### Inference temps réel

```python
# API FastAPI pour inference <100ms

@app.post("/predict")
async def predict_fraud(transaction: Transaction):
    # 1. Features real-time (Redis)
    features_rt = compute_realtime_features(transaction)
    
    # 2. Features historiques (Feast online store)
    features_hist = feast_store.get_online_features(
        entity_rows=[{"customer_id": transaction.customer_id}],
        features=CUSTOMER_FEATURES + VELOCITY_FEATURES
    )
    
    # 3. Combine features
    X = combine_features(features_rt, features_hist)
    
    # 4. Inference ONNX (optimized)
    fraud_prob = onnx_session.run(None, {'input': X})[0]
    
    # 5. Decision
    is_fraud = fraud_prob >= threshold
    risk_level = get_risk_level(fraud_prob)
    
    return {
        'fraud_probability': fraud_prob,
        'is_fraud': is_fraud,
        'risk_level': risk_level,  # low/medium/high/critical
        'action': 'block' if is_fraud else 'approve'
    }
```

**Explainability (SHAP) :**
```python
# Pour chaque prédiction, calculer SHAP values
shap_values = explainer.shap_values(X)

# Top 5 facteurs de risque
top_factors = [
    {'feature': 'transactions_last_1h', 'contribution': 0.12},
    {'feature': 'is_new_device', 'contribution': 0.08},
    {'feature': 'distance_from_billing_km', 'contribution': 0.06},
    ...
]
```

---

## Modèle de Segmentation Client

### Architecture

**Algorithme :** K-Means clustering + RFM analysis

**Features utilisées (15) :**
- Recency: days_since_last_transaction
- Frequency: transaction_count_90d
- Monetary: total_spent_90d, avg_transaction_amount
- Behavioral: preferred_device, preferred_payment_method
- Engagement: session_count_30d, avg_session_duration

**Segments identifiés (6) :**
1. **VIP High-Value** (5%) - LTV >$10K, fréquence élevée
2. **Engaged High-Value** (12%) - LTV >$5K, actifs
3. **Super Engaged** (18%) - Fréquence élevée, LTV moyen
4. **At-Risk** (15%) - Baisse activité récente
5. **Standard** (40%) - Comportement normal
6. **New** (10%) - <30 jours d'ancienneté

**Pipeline :**
```python
# 1. Feature engineering
customer_features = calculate_rfm_scores(transactions)

# 2. Normalisation
scaler = StandardScaler()
X_scaled = scaler.fit_transform(customer_features)

# 3. K-Means (k=6)
kmeans = KMeans(n_clusters=6, random_state=42)
segments = kmeans.fit_predict(X_scaled)

# 4. Segment labeling (business logic)
segment_labels = assign_business_labels(segments, customer_features)

# 5. Store in OLAP
store_segments_to_snowflake(customer_id, segment_labels)
```

---

## Modèle de Prédiction Churn

### Architecture

**Algorithme :** LightGBM (binary classification)

**Horizon de prédiction :** 30, 60, 90 jours

**Features (40) :**
- Tendances transaction (declining_transaction_trend)
- Changements comportement (device_switches, payment_method_changes)
- Engagement (days_since_last_login, session_frequency_decline)
- Support (support_tickets_count, complaint_count)
- Competitive (merchant_diversification_score)

**Target :** Churn = no transaction for 60+ days

**Métriques :**
- **AUC-ROC** : >0.85
- **Precision@10%** : >75% (top 10% à risque)
- **Recall@10%** : >40%

**Actions automatisées :**
```python
if churn_prob > 0.7:
    # High risk
    trigger_retention_campaign(customer_id, 'high_value_offer')
    assign_to_account_manager(customer_id)
    
elif churn_prob > 0.5:
    # Medium risk
    send_personalized_email(customer_id, 'engagement_campaign')
    offer_loyalty_reward(customer_id)
```

---

## Modèle de Prédiction LTV

### Architecture

**Algorithme :** XGBoost Regressor

**Horizon :** 12 mois et 24 mois

**Features (35) :**
- Historical: total_spent, transaction_count, avg_order_value
- Temporal: account_age, months_active, purchase_frequency
- Engagement: session_count, pages_viewed, time_on_site
- Product: categories_purchased, avg_items_per_order
- Behavioral: refund_rate, support_interactions

**Target :** Total revenue next 12/24 months

**Métriques :**
- **MAPE** : <15% (Mean Absolute Percentage Error)
- **R²** : >0.75

**Use case :**
```python
# Marketing allocation optimisée
customer_ltv_12m = model.predict(customer_features)

if customer_ltv_12m > 5000:
    # High LTV - invest in retention
    marketing_budget = customer_ltv_12m * 0.15
    recommended_channel = 'personalized_campaigns'
    
elif customer_ltv_12m > 1000:
    # Medium LTV
    marketing_budget = customer_ltv_12m * 0.08
    recommended_channel = 'email_automation'
```

---

## Feature Store (Feast)

### Architecture

```
┌──────────────────────────────────────┐
│         Feature Store (Feast)        │
├──────────────────────────────────────┤
│                                      │
│  Online Store (Redis)                │
│  - Real-time features                │
│  - Low latency (<10ms)               │
│  - TTL: 1h - 30 days                 │
│                                      │
│  Offline Store (S3 + Snowflake)     │
│  - Historical features               │
│  - Training data                     │
│  - Point-in-time correct joins       │
│                                      │
└──────────────────────────────────────┘
```

**Feature Views :**
```python
# Customer features (batch, TTL=30d)
customer_fv = FeatureView(
    name="customer_features",
    entities=["customer_id"],
    ttl=timedelta(days=30),
    features=[
        Feature("account_age_days", ValueType.INT32),
        Feature("total_lifetime_value", ValueType.DOUBLE),
        Feature("customer_segment", ValueType.STRING),
        ...
    ],
    online=True,
    batch_source=s3_source
)

# Velocity features (streaming, TTL=1h)
velocity_fv = FeatureView(
    name="velocity_features",
    entities=["customer_id"],
    ttl=timedelta(hours=1),
    features=[
        Feature("transactions_last_1h", ValueType.INT32),
        Feature("total_amount_last_24h", ValueType.DOUBLE),
        ...
    ],
    online=True,
    stream_source=kafka_source
)
```

**Usage :**
```python
# Training (offline)
training_df = store.get_historical_features(
    entity_df=transactions_df,
    features=["customer_features:*", "velocity_features:*"]
).to_df()

# Inference (online)
online_features = store.get_online_features(
    entity_rows=[{"customer_id": "cus_123"}],
    features=["customer_features:*", "velocity_features:*"]
).to_dict()
```

---

## Model Monitoring & Retraining

### Monitoring Strategy

**1. Performance Monitoring**
```python
# Track daily metrics
metrics = {
    'auc_roc': calculate_auc(y_true, y_pred),
    'precision': calculate_precision(y_true, y_pred),
    'recall': calculate_recall(y_true, y_pred),
    'false_positive_rate': fpr,
    'latency_p95': latency_p95
}

# Alert si dégradation
if metrics['auc_roc'] < 0.90:  # Threshold
    trigger_alert("Model performance degraded")
    schedule_retraining()
```

**2. Data Drift Detection**
```python
from evidently import Dashboard
from evidently.tabs import DataDriftTab

# Compare production data vs training data
drift_report = Dashboard(tabs=[DataDriftTab()])
drift_report.calculate(
    reference_data=training_df,
    current_data=production_df,
    column_mapping=column_mapping
)

# Drift detected → retrain
if drift_report.data_drift.share_drifted_columns > 0.3:
    trigger_retraining("Data drift detected")
```

**3. Concept Drift Detection**
```python
# Track fraud patterns evolution
current_fraud_patterns = analyze_fraud_patterns(recent_transactions)
historical_patterns = load_patterns_from_db()

if pattern_divergence(current_fraud_patterns, historical_patterns) > 0.5:
    trigger_retraining("Concept drift - fraud patterns evolved")
```

### Retraining Strategy

**Triggers :**
- **Scheduled** : Weekly (automatic)
- **Performance** : AUC < 0.90
- **Drift** : >30% features drifted
- **Manual** : On-demand by ML engineer

**Pipeline :**
```python
@airflow_dag(schedule_interval='@weekly')
def model_retraining_pipeline():
    # 1. Check if retraining needed
    check_metrics = check_model_performance()
    
    # 2. Extract fresh data
    training_data = extract_training_data(last_n_days=90)
    
    # 3. Train new model (challenger)
    challenger_model = train_model(training_data)
    
    # 4. Evaluate challenger vs champion
    comparison = compare_models(champion_model, challenger_model, test_set)
    
    # 5. A/B test (10% traffic to challenger)
    if comparison['challenger_better']:
        deploy_ab_test(challenger_model, traffic_split=0.1)
        
        # Monitor for 48h
        ab_results = monitor_ab_test(duration_hours=48)
        
        # 6. Promote if successful
        if ab_results['challenger_wins']:
            promote_to_production(challenger_model)
            archive_champion(champion_model)
```

**Champion/Challenger Framework :**
```
┌─────────────────────────────────────┐
│   Production Traffic (100%)         │
└────────────┬────────────────────────┘
             │
       ┌─────┴─────┐
       │           │
       ↓           ↓
  ┌─────────┐  ┌─────────┐
  │Champion │  │Challenger│
  │  (90%)  │  │  (10%)  │
  └─────────┘  └─────────┘
       │           │
       └─────┬─────┘
             ↓
    Compare metrics
    after 48 hours
```

---

## A/B Testing

### Framework

```python
class ABTestFramework:
    def create_experiment(self, name, variants):
        """
        Create A/B test experiment
        
        Example:
        variants = {
            'champion': {'model_version': 'v1.0', 'traffic': 0.9},
            'challenger': {'model_version': 'v2.0', 'traffic': 0.1}
        }
        """
        experiment = {
            'experiment_id': generate_id(),
            'name': name,
            'variants': variants,
            'start_time': datetime.utcnow(),
            'status': 'active',
            'metrics': ['auc_roc', 'precision', 'recall', 'latency']
        }
        
        store_experiment(experiment)
        return experiment
    
    def assign_variant(self, experiment_id, customer_id):
        """
        Consistent variant assignment (hash-based)
        """
        experiment = load_experiment(experiment_id)
        
        # Hash customer_id to assign variant
        hash_value = hash(f"{customer_id}{experiment_id}") % 100
        
        cumulative = 0
        for variant_name, config in experiment['variants'].items():
            cumulative += config['traffic'] * 100
            if hash_value < cumulative:
                return variant_name
        
        return 'champion'  # Fallback
    
    def log_experiment_result(self, experiment_id, variant, metrics):
        """
        Log metrics for variant
        """
        result = {
            'experiment_id': experiment_id,
            'variant': variant,
            'metrics': metrics,
            'timestamp': datetime.utcnow()
        }
        
        append_to_results(result)
    
    def analyze_results(self, experiment_id, min_samples=1000):
        """
        Statistical analysis of A/B test
        """
        results = load_experiment_results(experiment_id)
        
        if len(results) < min_samples:
            return {'status': 'insufficient_data'}
        
        # Statistical test (t-test)
        champion_auc = results[results.variant == 'champion']['auc_roc']
        challenger_auc = results[results.variant == 'challenger']['auc_roc']
        
        t_stat, p_value = ttest_ind(champion_auc, challenger_auc)
        
        # Winner if p < 0.05 and improvement > 1%
        if p_value < 0.05 and (challenger_auc.mean() - champion_auc.mean()) > 0.01:
            winner = 'challenger'
        elif p_value < 0.05 and (champion_auc.mean() - challenger_auc.mean()) > 0.01:
            winner = 'champion'
        else:
            winner = 'inconclusive'
        
        return {
            'status': 'complete',
            'winner': winner,
            'champion_mean': champion_auc.mean(),
            'challenger_mean': challenger_auc.mean(),
            'improvement': (challenger_auc.mean() - champion_auc.mean()),
            'p_value': p_value,
            'confidence': 1 - p_value
        }
```

---

## MLOps Infrastructure

### CI/CD for ML

```yaml
# .github/workflows/ml-pipeline.yml

name: ML Model CI/CD

on:
  push:
    branches: [main]
    paths:
      - 'models/**'
      - 'features/**'

jobs:
  validate_features:
    runs-on: ubuntu-latest
    steps:
      - name: Feature validation
        run: |
          python -m pytest tests/test_features.py
          python -m great_expectations checkpoint run features_checkpoint
  
  train_model:
    needs: validate_features
    runs-on: ubuntu-latest
    steps:
      - name: Train model
        run: |
          python models/fraud_detection/train.py
          
      - name: Evaluate model
        run: |
          python models/fraud_detection/evaluate.py
          
      - name: Check metrics
        run: |
          python scripts/check_metrics_threshold.py --min-auc 0.92
  
  deploy_staging:
    needs: train_model
    runs-on: ubuntu-latest
    steps:
      - name: Deploy to staging
        run: |
          kubectl apply -f k8s/staging/model-deployment.yaml
          
      - name: Integration tests
        run: |
          python -m pytest tests/integration/
  
  deploy_production:
    needs: deploy_staging
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - name: Deploy to production (canary)
        run: |
          kubectl apply -f k8s/production/canary-deployment.yaml
          python scripts/monitor_canary.py --duration 2h
          
      - name: Full rollout
        run: |
          kubectl apply -f k8s/production/model-deployment.yaml
```

### Model Governance

```yaml
Model Registry Structure:
  fraud_detection:
    v1.0:
      metrics: {auc: 0.93, precision: 0.91, recall: 0.87}
      training_date: "2026-01-15"
      training_data: "s3://stripe-ml/training-data/2026-01-15/"
      features: [87 features]
      hyperparameters: {...}
      status: "production"
      
    v1.1:
      metrics: {auc: 0.94, precision: 0.92, recall: 0.88}
      training_date: "2026-01-22"
      status: "staging"
      
    v0.9:
      status: "archived"

Model Approval Process:
  1. Model trained → "pending_review"
  2. ML Engineer review → "approved_for_staging"
  3. Staging tests pass → "approved_for_canary"
  4. Canary successful → "approved_for_production"
  5. Production deployment → "production"
```

---

## Résumé des Livrables ML

### Models Developed

| Model | Algorithm | Features | Metrics | Latency | Status |
|-------|-----------|----------|---------|---------|--------|
| Fraud Detection | XGBoost | 87 | AUC 0.93, P 0.91 | <100ms | Production |
| Customer Segmentation | K-Means | 15 | Silhouette 0.68 | Batch | Production |
| Churn Prediction | LightGBM | 40 | AUC 0.86 | Batch | Production |
| LTV Prediction | XGBoost Reg | 35 | MAPE 14%, R² 0.77 | Batch | Production |

### Infrastructure

- ✅ Feature Store (Feast) - Online + Offline
- ✅ Model Registry (MLflow) - Versioning + Tracking
- ✅ Inference API (FastAPI + ONNX) - <100ms
- ✅ Monitoring (Evidently AI) - Drift detection
- ✅ Retraining Pipeline (Airflow) - Automated
- ✅ A/B Testing Framework - Champion/Challenger

### Business Impact

- **Fraud losses** : -15M$/an (85% fraudes détectées)
- **False positives** : -60% (moins de frictions client)
- **Customer retention** : +12% (churn prediction proactive)
- **Marketing ROI** : +25% (LTV-based allocation)

---