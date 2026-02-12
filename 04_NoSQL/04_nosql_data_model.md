# Modèle de Données NoSQL - Stripe Payment Platform
## Architecture MongoDB pour données non structurées et semi-structurées

---

## 📋 Table des matières

1. [Vue d'ensemble](#vue-densemble)
2. [Architecture des collections](#architecture-des-collections)
3. [Schémas détaillés](#schémas-détaillés)
4. [Stratégies d'indexation](#stratégies-dindexation)
5. [Patterns de relations](#patterns-de-relations)
6. [Sharding et partitionnement](#sharding-et-partitionnement)
7. [Intégration avec OLTP/OLAP](#intégration-avec-oltpolap)
8. [Cas d'usage métier](#cas-dusage-métier)

---

## 1. Vue d'ensemble

### 1.1 Objectifs du système NoSQL

Le système NoSQL MongoDB de Stripe est conçu pour :

- **Flexibilité** : Stocker des données semi-structurées et non structurées
- **Performance** : Requêtes rapides sur des volumes massifs de logs
- **Évolutivité** : Scaling horizontal avec sharding
- **ML Integration** : Support natif des pipelines d'apprentissage automatique
- **Real-time analytics** : Agrégations et analyses en temps réel

### 1.2 Choix technologique : MongoDB

**Justifications :**
- Support natif JSON/BSON
- Schéma flexible avec validation optionnelle
- Indexation avancée (compound, geospatial, text)
- Aggregation pipeline puissant
- Time-series collections (MongoDB 5.0+)
- Change streams pour synchronisation temps réel
- Transactions multi-documents (ACID)

### 1.3 Architecture globale

```
┌──────────────────────────────────────────────────────────────┐
│                    MongoDB Cluster (NoSQL)                   │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐  ┌───────────────┐ ┌──────────────┐        │
│  │   Logs DB    │  │  Analytics    │ │   ML Data    │        │
│  │              │  │      DB       │ │      DB      │        │
│  │ • system_logs│  │ • user_       │ │ • fraud_     │        │
│  │ • api_logs   │  │   interactions│ │   features   │        │
│  │ • error_logs │  │ • sessions    │ │ • model_     │        │
│  │              │  │ • clickstream │ │   predictions│        │
│  └──────────────┘  └───────────────┘ └──────────────┘        │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐                          │
│  │ Customer DB  │  │   Events DB  │                          │
│  │              │  │              │                          │
│  │ • feedback   │  │ • webhook_   │                          │
│  │ • surveys    │  │   events     │                          │
│  │ • reviews    │  │ • audit_trail│                          │
│  └──────────────┘  └──────────────┘                          │
│                                                              │
└──────────────────────────────────────────────────────────────┘
         ↕                    ↕                    ↕
    ETL/ELT              Real-time             Change Data
    Pipeline              Kafka               Capture (CDC)
         ↕                    ↕                    ↕
   ┌──────────┐        ┌──────────┐        ┌──────────┐
   │   OLTP   │        │   OLAP   │        │    ML    │
   │ Postgres │        │Snowflake │        │ Pipeline │
   └──────────┘        └──────────┘        └──────────┘
```

---

## 2. Architecture des collections

### 2.1 Collections principales

| Collection | Type | Volume estimé | Retention | Sharding Key |
|-----------|------|---------------|-----------|--------------|
| `system_logs` | Time-series | 1B+ docs/mois | 90 jours | timestamp |
| `api_logs` | Time-series | 500M+ docs/mois | 30 jours | timestamp |
| `error_logs` | Document | 10M+ docs/mois | 1 an | timestamp |
| `user_interactions` | Time-series | 2B+ docs/mois | 6 mois | user_id + timestamp |
| `sessions` | Document | 100M+ sessions/mois | 3 mois | session_id |
| `fraud_features` | Document | 50M+ docs/mois | 2 ans | transaction_id |
| `ml_predictions` | Document | 50M+ docs/mois | 1 an | model_version + timestamp |
| `customer_feedback` | Document | 5M+ docs/mois | Permanent | customer_id |
| `webhook_events` | Document | 200M+ docs/mois | 6 mois | event_type + timestamp |

### 2.2 Hiérarchie des databases

```
stripe_nosql_cluster/
├── logs_db/
│   ├── system_logs (time-series)
│   ├── api_logs (time-series)
│   └── error_logs
│
├── analytics_db/
│   ├── user_interactions (time-series)
│   ├── sessions
│   └── clickstream (time-series)
│
├── ml_db/
│   ├── fraud_features
│   ├── ml_predictions
│   └── feature_store
│
├── customer_db/
│   ├── feedback
│   ├── surveys
│   └── reviews
│
└── events_db/
    ├── webhook_events
    └── audit_trail
```

---

## 3. Schémas détaillés

### 3.1 Collection : `system_logs` (Time-Series)

**Description :** Logs système pour monitoring et debugging

**Configuration Time-Series :**
```javascript
db.createCollection("system_logs", {
  timeseries: {
    timeField: "timestamp",
    metaField: "metadata",
    granularity: "seconds"
  },
  expireAfterSeconds: 7776000  // 90 jours
})
```

**Schéma JSON :**
```json
{
  "_id": ObjectId("507f1f77bcf86cd799439011"),
  "timestamp": ISODate("2026-01-21T14:32:45.123Z"),
  "metadata": {
    "service": "payment-processor",
    "environment": "production",
    "region": "us-east-1",
    "host": "stripe-app-node-42",
    "pod_id": "pod-xyz-789"
  },
  "level": "INFO",
  "message": "Transaction processed successfully",
  "log_type": "application",
  "context": {
    "transaction_id": "txn_1234567890",
    "merchant_id": "mch_abc123",
    "customer_id": "cus_def456",
    "amount": 150.00,
    "currency": "USD",
    "processing_time_ms": 245
  },
  "trace_id": "trace-abc-def-ghi",
  "span_id": "span-123-456",
  "tags": ["payment", "success", "high-value"]
}
```

**Validation Schema :**
```javascript
db.runCommand({
  collMod: "system_logs",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["timestamp", "metadata", "level", "message"],
      properties: {
        timestamp: { bsonType: "date" },
        metadata: {
          bsonType: "object",
          required: ["service", "environment"],
          properties: {
            service: { bsonType: "string" },
            environment: { enum: ["production", "staging", "development"] }
          }
        },
        level: { enum: ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] },
        message: { bsonType: "string", minLength: 1 }
      }
    }
  },
  validationLevel: "moderate"
})
```

---

### 3.2 Collection : `api_logs` (Time-Series)

**Description :** Logs API pour analytics et monitoring des endpoints

**Schéma JSON :**
```json
{
  "_id": ObjectId("507f1f77bcf86cd799439012"),
  "timestamp": ISODate("2026-01-21T14:32:45.500Z"),
  "metadata": {
    "api_version": "v1",
    "gateway": "api-gateway-01",
    "region": "eu-west-1"
  },
  "request": {
    "method": "POST",
    "endpoint": "/v1/charges",
    "path": "/v1/charges",
    "query_params": {},
    "headers": {
      "user-agent": "Stripe-iOS/21.0.0",
      "content-type": "application/json"
    },
    "body_size_bytes": 512,
    "ip_address": "198.51.100.42",
    "geolocation": {
      "country": "FR",
      "city": "Paris",
      "latitude": 48.8566,
      "longitude": 2.3522
    }
  },
  "response": {
    "status_code": 200,
    "body_size_bytes": 1024,
    "duration_ms": 187
  },
  "authentication": {
    "api_key_id": "pk_live_abc123",
    "merchant_id": "mch_xyz789",
    "scope": ["charges.create"]
  },
  "rate_limiting": {
    "limit": 100,
    "remaining": 87,
    "reset_at": ISODate("2026-01-21T15:00:00.000Z")
  },
  "business_context": {
    "transaction_id": "txn_1234567890",
    "customer_id": "cus_abc123",
    "amount": 150.00,
    "currency": "USD"
  }
}
```

---

### 3.3 Collection : `user_interactions` (Time-Series)

**Description :** Clickstream et interactions utilisateur pour analytics

**Schéma JSON :**
```json
{
  "_id": ObjectId("507f1f77bcf86cd799439013"),
  "timestamp": ISODate("2026-01-21T14:35:22.789Z"),
  "metadata": {
    "platform": "web",
    "app_version": "2.15.3"
  },
  "session_id": "sess_abc123def456",
  "user_id": "cus_xyz789",
  "merchant_id": "mch_merchant123",
  
  "event_type": "page_view",
  "event_name": "checkout_initiated",
  
  "page": {
    "url": "https://example.com/checkout",
    "referrer": "https://example.com/cart",
    "title": "Checkout - Example Store"
  },
  
  "device": {
    "type": "mobile",
    "os": "iOS",
    "os_version": "17.2",
    "browser": "Safari",
    "browser_version": "17.2",
    "screen_resolution": "1170x2532",
    "viewport": "390x844"
  },
  
  "geolocation": {
    "country": "FR",
    "region": "Île-de-France",
    "city": "Paris",
    "postal_code": "75001",
    "latitude": 48.8566,
    "longitude": 2.3522,
    "timezone": "Europe/Paris"
  },
  
  "interaction_details": {
    "element_type": "button",
    "element_id": "checkout-btn",
    "element_text": "Proceed to Payment",
    "element_position": { "x": 180, "y": 450 }
  },
  
  "ecommerce_context": {
    "cart_value": 299.99,
    "currency": "EUR",
    "items_count": 3,
    "product_ids": ["prod_123", "prod_456", "prod_789"]
  },
  
  "user_properties": {
    "is_authenticated": true,
    "account_age_days": 245,
    "lifetime_value": 1450.00,
    "previous_purchases": 8,
    "customer_segment": "high_value"
  },
  
  "ab_tests": {
    "checkout_flow_v2": "variant_b",
    "payment_button_color": "blue"
  }
}
```

---

### 3.4 Collection : `sessions`

**Description :** Sessions utilisateur agrégées pour analytics comportementales

**Schéma JSON :**
```json
{
  "_id": ObjectId("507f1f77bcf86cd799439014"),
  "session_id": "sess_abc123def456",
  "user_id": "cus_xyz789",
  "merchant_id": "mch_merchant123",
  
  "session_start": ISODate("2026-01-21T14:30:00.000Z"),
  "session_end": ISODate("2026-01-21T14:45:30.000Z"),
  "session_duration_seconds": 930,
  
  "device_info": {
    "type": "mobile",
    "os": "iOS",
    "browser": "Safari",
    "is_mobile": true
  },
  
  "geolocation": {
    "country": "FR",
    "city": "Paris"
  },
  
  "entry_point": {
    "url": "https://example.com/products",
    "referrer": "https://google.com/search",
    "utm_source": "google",
    "utm_medium": "cpc",
    "utm_campaign": "winter_sale_2026"
  },
  
  "exit_point": {
    "url": "https://example.com/checkout/success",
    "exit_type": "conversion"
  },
  
  "engagement_metrics": {
    "page_views": 12,
    "unique_pages": 8,
    "clicks": 24,
    "scroll_depth_avg": 0.78,
    "time_on_site_seconds": 930,
    "bounce": false
  },
  
  "conversion_data": {
    "converted": true,
    "transaction_id": "txn_1234567890",
    "revenue": 299.99,
    "currency": "EUR",
    "items_purchased": 3,
    "time_to_conversion_seconds": 850
  },
  
  "funnel_progress": [
    { "step": "product_view", "timestamp": ISODate("2026-01-21T14:31:00.000Z") },
    { "step": "add_to_cart", "timestamp": ISODate("2026-01-21T14:33:15.000Z") },
    { "step": "checkout_initiated", "timestamp": ISODate("2026-01-21T14:35:22.000Z") },
    { "step": "payment_info_entered", "timestamp": ISODate("2026-01-21T14:40:10.000Z") },
    { "step": "purchase_completed", "timestamp": ISODate("2026-01-21T14:44:30.000Z") }
  ],
  
  "technical_metrics": {
    "page_load_avg_ms": 1250,
    "errors_encountered": 0,
    "api_calls": 15
  }
}
```

---

### 3.5 Collection : `fraud_features`

**Description :** Features extraites pour les modèles ML de détection de fraude

**Schéma JSON :**
```json
{
  "_id": ObjectId("507f1f77bcf86cd799439015"),
  "transaction_id": "txn_1234567890",
  "customer_id": "cus_abc123",
  "merchant_id": "mch_xyz789",
  "payment_method_id": "pm_card_visa_1234",
  
  "extracted_at": ISODate("2026-01-21T14:32:46.000Z"),
  "feature_version": "v3.2.1",
  
  "transaction_features": {
    "amount": 1850.00,
    "currency": "USD",
    "amount_usd": 1850.00,
    "is_high_value": true,
    "is_international": true,
    "transaction_hour": 14,
    "transaction_day_of_week": 3,
    "is_weekend": false,
    "is_night_transaction": false
  },
  
  "velocity_features": {
    "transactions_last_1h": 1,
    "transactions_last_24h": 3,
    "transactions_last_7d": 12,
    "total_amount_last_24h": 2450.00,
    "avg_transaction_amount_30d": 185.50,
    "days_since_last_transaction": 2,
    "transaction_frequency_score": 0.72
  },
  
  "customer_features": {
    "account_age_days": 245,
    "total_lifetime_transactions": 48,
    "total_lifetime_value": 8920.00,
    "avg_transaction_amount": 185.83,
    "failed_transactions_count": 2,
    "chargeback_count": 0,
    "refund_count": 1,
    "customer_segment": "high_value",
    "is_first_transaction": false,
    "days_since_account_creation": 245
  },
  
  "device_features": {
    "device_type": "mobile",
    "os": "iOS",
    "browser": "Safari",
    "is_known_device": true,
    "device_fingerprint": "fp_abc123def456",
    "devices_used_count": 3,
    "is_new_device": false
  },
  
  "location_features": {
    "country": "FR",
    "city": "Paris",
    "is_known_location": true,
    "distance_from_billing_km": 2.5,
    "distance_from_last_transaction_km": 3.2,
    "is_high_risk_country": false,
    "countries_used_count": 2,
    "ip_address_risk_score": 0.15,
    "is_vpn_detected": false,
    "is_proxy_detected": false
  },
  
  "payment_method_features": {
    "method_type": "card",
    "card_brand": "Visa",
    "card_funding": "credit",
    "card_country": "FR",
    "bin_risk_score": 0.22,
    "is_prepaid_card": false,
    "card_age_days": 180,
    "failed_attempts_last_24h": 0
  },
  
  "merchant_features": {
    "merchant_category": "retail",
    "merchant_risk_score": 0.18,
    "merchant_chargeback_rate": 0.008,
    "is_new_merchant_for_customer": false
  },
  
  "behavioral_features": {
    "time_on_checkout_page_seconds": 120,
    "form_fill_speed_score": 0.65,
    "mouse_movement_entropy": 0.82,
    "keyboard_typing_pattern_score": 0.71,
    "session_duration_before_purchase_seconds": 850
  },
  
  "network_features": {
    "ip_address": "198.51.100.42",
    "ip_reputation_score": 0.88,
    "isp": "Orange France",
    "is_datacenter_ip": false,
    "email_domain": "gmail.com",
    "email_domain_age_days": 7300,
    "is_disposable_email": false
  },
  
  "aggregated_scores": {
    "velocity_risk_score": 0.18,
    "location_risk_score": 0.12,
    "device_risk_score": 0.08,
    "payment_risk_score": 0.15,
    "behavioral_risk_score": 0.22,
    "overall_risk_score": 0.15
  }
}
```

---

### 3.6 Collection : `ml_predictions`

**Description :** Prédictions des modèles ML pour audit et monitoring

**Schéma JSON :**
```json
{
  "_id": ObjectId("507f1f77bcf86cd799439016"),
  "transaction_id": "txn_1234567890",
  "customer_id": "cus_abc123",
  "merchant_id": "mch_xyz789",
  
  "predicted_at": ISODate("2026-01-21T14:32:46.500Z"),
  "processing_time_ms": 45,
  
  "model_info": {
    "model_name": "fraud_detection_v3",
    "model_version": "v3.2.1",
    "model_type": "gradient_boosting",
    "framework": "xgboost",
    "trained_at": ISODate("2026-01-15T00:00:00.000Z"),
    "training_data_size": 10000000,
    "feature_count": 87
  },
  
  "prediction": {
    "is_fraud": false,
    "fraud_probability": 0.15,
    "risk_level": "low",
    "confidence": 0.92
  },
  
  "prediction_details": {
    "fraud_score": 0.15,
    "threshold_used": 0.75,
    "decision": "approve",
    "manual_review_required": false
  },
  
  "feature_importance": [
    { "feature": "velocity_transactions_24h", "importance": 0.18 },
    { "feature": "location_distance_km", "importance": 0.14 },
    { "feature": "device_is_new", "importance": 0.12 },
    { "feature": "amount_zscore", "importance": 0.11 },
    { "feature": "customer_lifetime_value", "importance": 0.09 }
  ],
  
  "ensemble_predictions": {
    "model_1_fraud_prob": 0.12,
    "model_2_fraud_prob": 0.18,
    "model_3_fraud_prob": 0.14,
    "ensemble_method": "weighted_average",
    "weights": [0.4, 0.35, 0.25]
  },
  
  "shap_values": {
    "base_value": 0.08,
    "contributions": {
      "velocity_transactions_24h": -0.05,
      "location_distance_km": 0.03,
      "device_is_new": 0.02,
      "amount_zscore": 0.04,
      "customer_lifetime_value": -0.08
    }
  },
  
  "metadata": {
    "inference_server": "ml-inference-pod-12",
    "gpu_used": false,
    "batch_size": 1,
    "features_used": 87,
    "missing_features": []
  },
  
  "ground_truth": {
    "label_available": false,
    "is_fraud_actual": null,
    "labeled_at": null,
    "labeler": null
  }
}
```

---

### 3.7 Collection : `customer_feedback`

**Description :** Feedbacks clients (avis, surveys, NPS)

**Schéma JSON :**
```json
{
  "_id": ObjectId("507f1f77bcf86cd799439017"),
  "feedback_id": "fb_abc123def456",
  "customer_id": "cus_xyz789",
  "merchant_id": "mch_merchant123",
  "transaction_id": "txn_1234567890",
  
  "created_at": ISODate("2026-01-21T16:30:00.000Z"),
  "updated_at": ISODate("2026-01-21T16:30:00.000Z"),
  
  "feedback_type": "review",
  "channel": "email",
  
  "rating": {
    "overall": 4.5,
    "ease_of_use": 5.0,
    "speed": 4.0,
    "customer_support": 4.5,
    "value_for_money": 4.0
  },
  
  "nps": {
    "score": 9,
    "category": "promoter",
    "likelihood_to_recommend": 9
  },
  
  "text_feedback": {
    "title": "Great payment experience!",
    "comment": "The checkout process was smooth and fast. I appreciated the multiple payment options.",
    "language": "en",
    "word_count": 16,
    "sentiment": {
      "polarity": 0.85,
      "subjectivity": 0.65,
      "label": "positive"
    }
  },
  
  "topics_detected": ["checkout", "payment_options", "speed"],
  
  "context": {
    "transaction_amount": 299.99,
    "transaction_currency": "EUR",
    "payment_method": "card",
    "transaction_date": ISODate("2026-01-21T14:44:30.000Z"),
    "days_since_transaction": 0
  },
  
  "customer_info": {
    "account_age_days": 245,
    "total_transactions": 48,
    "previous_feedback_count": 2,
    "customer_segment": "high_value"
  },
  
  "response": {
    "responded": true,
    "responded_at": ISODate("2026-01-22T09:15:00.000Z"),
    "response_time_hours": 16.75,
    "responder": "support_agent_42",
    "response_text": "Thank you for your positive feedback! We're glad you had a great experience."
  },
  
  "internal_notes": {
    "flagged_for_review": false,
    "priority": "normal",
    "assigned_to": null,
    "status": "resolved"
  }
}
```

---

### 3.8 Collection : `webhook_events`

**Description :** Événements webhook pour intégrations externes

**Schéma JSON :**
```json
{
  "_id": ObjectId("507f1f77bcf86cd799439018"),
  "event_id": "evt_abc123def456",
  "event_type": "charge.succeeded",
  "created_at": ISODate("2026-01-21T14:32:47.000Z"),
  
  "api_version": "2024-11-20",
  "idempotency_key": "idem_xyz789",
  
  "data": {
    "object": "charge",
    "id": "ch_1234567890",
    "amount": 15000,
    "currency": "usd",
    "customer": "cus_abc123",
    "description": "Payment for order #12345",
    "metadata": {
      "order_id": "order_12345",
      "customer_email": "customer@example.com"
    },
    "status": "succeeded"
  },
  
  "related_objects": {
    "transaction_id": "txn_1234567890",
    "merchant_id": "mch_xyz789",
    "payment_intent_id": "pi_abc123def456"
  },
  
  "webhook_deliveries": [
    {
      "webhook_endpoint_id": "we_endpoint_1",
      "url": "https://merchant.example.com/webhook",
      "attempted_at": ISODate("2026-01-21T14:32:47.100Z"),
      "http_status": 200,
      "response_time_ms": 250,
      "success": true,
      "attempt_number": 1,
      "headers_sent": {
        "Stripe-Signature": "t=1737467567,v1=abc123def456..."
      },
      "response_body": "{\"received\": true}"
    }
  ],
  
  "retry_info": {
    "max_attempts": 3,
    "current_attempt": 1,
    "next_retry_at": null,
    "final_status": "delivered"
  },
  
  "security": {
    "signature_valid": true,
    "ip_whitelist_passed": true,
    "ssl_verified": true
  }
}
```

---

## 4. Stratégies d'indexation

### 4.1 Indexes par collection

#### `system_logs`
```javascript
// Time-series index automatique
db.system_logs.createIndex({ "metadata.service": 1, "timestamp": -1 })
db.system_logs.createIndex({ "level": 1, "timestamp": -1 })
db.system_logs.createIndex({ "context.transaction_id": 1 })
db.system_logs.createIndex({ "metadata.environment": 1, "level": 1, "timestamp": -1 })
db.system_logs.createIndex({ "tags": 1, "timestamp": -1 })
```

#### `api_logs`
```javascript
db.api_logs.createIndex({ "request.endpoint": 1, "timestamp": -1 })
db.api_logs.createIndex({ "response.status_code": 1, "timestamp": -1 })
db.api_logs.createIndex({ "authentication.merchant_id": 1, "timestamp": -1 })
db.api_logs.createIndex({ "response.duration_ms": 1 })  // Pour monitoring performance
db.api_logs.createIndex({ "business_context.transaction_id": 1 })
```

#### `user_interactions`
```javascript
db.user_interactions.createIndex({ "user_id": 1, "timestamp": -1 })
db.user_interactions.createIndex({ "session_id": 1, "timestamp": 1 })
db.user_interactions.createIndex({ "merchant_id": 1, "event_type": 1, "timestamp": -1 })
db.user_interactions.createIndex({ "event_name": 1, "timestamp": -1 })
// Geospatial index
db.user_interactions.createIndex({ "geolocation": "2dsphere" })
```

#### `sessions`
```javascript
db.sessions.createIndex({ "session_id": 1 }, { unique: true })
db.sessions.createIndex({ "user_id": 1, "session_start": -1 })
db.sessions.createIndex({ "merchant_id": 1, "session_start": -1 })
db.sessions.createIndex({ "conversion_data.converted": 1, "session_end": -1 })
db.sessions.createIndex({ "entry_point.utm_campaign": 1 })
db.sessions.createIndex({ "device_info.type": 1, "geolocation.country": 1 })
```

#### `fraud_features`
```javascript
db.fraud_features.createIndex({ "transaction_id": 1 }, { unique: true })
db.fraud_features.createIndex({ "customer_id": 1, "extracted_at": -1 })
db.fraud_features.createIndex({ "merchant_id": 1, "extracted_at": -1 })
db.fraud_features.createIndex({ "aggregated_scores.overall_risk_score": -1 })
db.fraud_features.createIndex({ "feature_version": 1, "extracted_at": -1 })
```

#### `ml_predictions`
```javascript
db.ml_predictions.createIndex({ "transaction_id": 1 })
db.ml_predictions.createIndex({ "model_info.model_version": 1, "predicted_at": -1 })
db.ml_predictions.createIndex({ "prediction.risk_level": 1, "predicted_at": -1 })
db.ml_predictions.createIndex({ "prediction.fraud_probability": -1 })
db.ml_predictions.createIndex({ "ground_truth.is_fraud_actual": 1 })  // Pour model evaluation
```

#### `customer_feedback`
```javascript
db.customer_feedback.createIndex({ "feedback_id": 1 }, { unique: true })
db.customer_feedback.createIndex({ "customer_id": 1, "created_at": -1 })
db.customer_feedback.createIndex({ "merchant_id": 1, "rating.overall": -1 })
db.customer_feedback.createIndex({ "nps.category": 1, "created_at": -1 })
// Text index pour recherche full-text
db.customer_feedback.createIndex({ 
  "text_feedback.title": "text", 
  "text_feedback.comment": "text" 
})
db.customer_feedback.createIndex({ "text_feedback.sentiment.label": 1 })
```

#### `webhook_events`
```javascript
db.webhook_events.createIndex({ "event_id": 1 }, { unique: true })
db.webhook_events.createIndex({ "event_type": 1, "created_at": -1 })
db.webhook_events.createIndex({ "related_objects.transaction_id": 1 })
db.webhook_events.createIndex({ "related_objects.merchant_id": 1, "created_at": -1 })
db.webhook_events.createIndex({ "retry_info.final_status": 1, "created_at": -1 })
```

### 4.2 Stratégie globale d'indexation

**Principes :**
1. **Compound indexes** : Ordre = Equality → Sort → Range
2. **Covering indexes** : Inclure les champs fréquemment projetés
3. **TTL indexes** : Auto-suppression des anciens documents
4. **Partial indexes** : Index conditionnels pour économiser l'espace
5. **Wildcard indexes** : Pour champs dynamiques

**Exemple d'index partial :**
```javascript
// Index uniquement les logs ERROR et CRITICAL
db.system_logs.createIndex(
  { "level": 1, "timestamp": -1 },
  { 
    partialFilterExpression: { 
      level: { $in: ["ERROR", "CRITICAL"] } 
    } 
  }
)
```

**Exemple de TTL index :**
```javascript
// Auto-suppression après 90 jours
db.api_logs.createIndex(
  { "timestamp": 1 },
  { expireAfterSeconds: 7776000 }  // 90 days
)
```

---

## 5. Patterns de relations

### 5.1 Embedding vs Referencing

**Décision framework :**

| Critère | Embedding | Referencing |
|---------|-----------|-------------|
| Taille des sous-documents | < 100 items | > 100 items |
| Fréquence de lecture | Toujours ensemble | Parfois séparé |
| Fréquence de mise à jour | Rare | Fréquent |
| Croissance | Bornée | Illimitée |
| Cardinalité | 1-to-few | 1-to-many, many-to-many |

### 5.2 Patterns appliqués

#### Pattern 1 : **Extended Reference** (Hybrid)
Utilisé dans `user_interactions` → OLTP Customer

```javascript
// Document user_interactions avec référence enrichie
{
  "user_id": "cus_xyz789",  // Référence OLTP
  "user_properties": {       // Données dénormalisées (snapshot)
    "is_authenticated": true,
    "account_age_days": 245,
    "customer_segment": "high_value"
  }
}
```

**Avantages :**
- Évite les joins coûteux
- Préserve l'historique (snapshot au moment de l'événement)
- Performance optimale pour analytics

#### Pattern 2 : **Subset Pattern**
Utilisé dans `sessions` avec `funnel_progress`

```javascript
{
  "session_id": "sess_abc123",
  "funnel_progress": [  // Embedded array limité
    { "step": "product_view", "timestamp": "..." },
    { "step": "add_to_cart", "timestamp": "..." }
    // Maximum 10-15 steps
  ]
}
```

#### Pattern 3 : **Bucket Pattern**
Utilisé pour regrouper les logs par période

```javascript
// Bucket de 1 heure de logs
{
  "_id": ObjectId("..."),
  "bucket_id": "logs_2026-01-21_14",
  "start_time": ISODate("2026-01-21T14:00:00Z"),
  "end_time": ISODate("2026-01-21T15:00:00Z"),
  "count": 5234,
  "logs": [
    { "timestamp": "...", "message": "..." },
    // ... jusqu'à 1000 logs max
  ],
  "summary": {
    "error_count": 12,
    "warning_count": 45,
    "info_count": 5177
  }
}
```

#### Pattern 4 : **Computed Pattern**
Utilisé dans `fraud_features` avec scores agrégés

```javascript
{
  "transaction_id": "txn_123",
  "aggregated_scores": {  // Computed à l'insertion
    "velocity_risk_score": 0.18,
    "location_risk_score": 0.12,
    "overall_risk_score": 0.15  // Moyenne pondérée
  }
}
```

### 5.3 Relations avec OLTP/OLAP

**Principe général :** Référence par ID business, pas par clé technique

```javascript
// ✅ CORRECT - Business ID
{
  "transaction_id": "txn_1234567890",  // UUID de la transaction OLTP
  "customer_id": "cus_abc123"          // UUID du customer OLTP
}

// ❌ INCORRECT - Clé technique
{
  "transaction_key": 42,  // Surrogate key OLAP
  "customer_key": 1337
}
```

---

## 6. Sharding et partitionnement

### 6.1 Stratégie de sharding

**Objectif :** Distribuer 2B+ documents/mois sur cluster

```
┌────────────────────────────────────────────────────────┐
│              MongoDB Sharded Cluster                    │
├────────────────────────────────────────────────────────┤
│                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   Shard 1    │  │   Shard 2    │  │   Shard 3    │ │
│  │   (US-East)  │  │   (US-West)  │  │    (EU)      │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
│          ↑                  ↑                  ↑        │
│          └──────────────────┴──────────────────┘        │
│                            │                            │
│                    ┌───────────────┐                    │
│                    │  Config Server│                    │
│                    │   (Metadata)  │                    │
│                    └───────────────┘                    │
│                            ↑                            │
│                    ┌───────────────┐                    │
│                    │    mongos     │                    │
│                    │    (Router)   │                    │
│                    └───────────────┘                    │
└────────────────────────────────────────────────────────┘
```

### 6.2 Shard Keys par collection

#### `system_logs` : Hashed timestamp
```javascript
sh.shardCollection("logs_db.system_logs", { "timestamp": "hashed" })
```
**Justification :**
- Distribution uniforme sur time-series
- Évite les hot spots
- Bon pour write-heavy workload

#### `user_interactions` : Compound (user_id + timestamp)
```javascript
sh.shardCollection("analytics_db.user_interactions", { 
  "user_id": 1, 
  "timestamp": 1 
})
```
**Justification :**
- Isolation des données par utilisateur
- Requêtes par user efficaces
- Ordre chronologique préservé

#### `fraud_features` : Hashed transaction_id
```javascript
sh.shardCollection("ml_db.fraud_features", { 
  "transaction_id": "hashed" 
})
```
**Justification :**
- Distribution uniforme
- Accès direct par transaction ID
- Pas de hot shard pour transactions populaires

#### `sessions` : Ranged par session_start
```javascript
sh.shardCollection("analytics_db.sessions", { 
  "session_start": 1 
})
```
**Justification :**
- Requêtes temporelles fréquentes
- Archives anciennes sessions sur shard dédié
- Facilite le TTL et archiving

### 6.3 Zone Sharding (Geo-distribution)

```javascript
// Définir les zones géographiques
sh.addShardToZone("shard01", "US")
sh.addShardToZone("shard02", "EU")
sh.addShardToZone("shard03", "ASIA")

// Configurer les ranges par zone
sh.updateZoneKeyRange(
  "analytics_db.user_interactions",
  { "geolocation.country": "US", "timestamp": MinKey },
  { "geolocation.country": "US", "timestamp": MaxKey },
  "US"
)

sh.updateZoneKeyRange(
  "analytics_db.user_interactions",
  { "geolocation.country": "FR", "timestamp": MinKey },
  { "geolocation.country": "GB", "timestamp": MaxKey },
  "EU"
)
```

**Avantages :**
- Latence réduite (données locales)
- Conformité GDPR (données EU en EU)
- Résilience régionale

---

## 7. Intégration avec OLTP/OLAP

### 7.1 Architecture d'intégration

```
┌─────────────────────────────────────────────────────────┐
│                    Integration Layer                     │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  OLTP (Postgres) ──┐                                     │
│                    │                                     │
│                    ├──→ Kafka Topics ──→ MongoDB (NoSQL)│
│                    │                                     │
│  User Actions   ───┘                     │               │
│                                          ↓               │
│                                    Change Streams        │
│                                          ↓               │
│  MongoDB (NoSQL) ───→ ELT Pipeline ──→ OLAP (Snowflake) │
│                                                           │
└─────────────────────────────────────────────────────────┘
```

### 7.2 Change Data Capture (CDC)

**From OLTP to NoSQL :**

```javascript
// Debezium connector pour Postgres → Kafka → MongoDB
{
  "name": "postgres-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres-primary",
    "database.port": "5432",
    "database.user": "cdc_user",
    "database.password": "***",
    "database.dbname": "stripe_oltp",
    "database.server.name": "stripe",
    "table.include.list": "public.transaction,public.fraud",
    "transforms": "route",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "([^.]+)\\.([^.]+)\\.([^.]+)",
    "transforms.route.replacement": "$3"
  }
}
```

**Consumer Kafka → MongoDB :**

```python
from kafka import KafkaConsumer
from pymongo import MongoClient
import json

consumer = KafkaConsumer(
    'stripe.public.transaction',
    bootstrap_servers=['kafka:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

mongo_client = MongoClient('mongodb://mongo-cluster:27017')
db = mongo_client['ml_db']
fraud_features_collection = db['fraud_features']

for message in consumer:
    transaction_data = message.value
    
    # Enrichir avec features ML
    features = extract_fraud_features(transaction_data)
    
    # Insérer dans MongoDB
    fraud_features_collection.insert_one(features)
```

### 7.3 MongoDB Change Streams → OLAP

```javascript
// Pipeline de change stream pour synchronisation
const pipeline = [
  {
    $match: {
      'operationType': { $in: ['insert', 'update'] },
      'fullDocument.prediction.fraud_probability': { $gt: 0.7 }
    }
  },
  {
    $project: {
      'fullDocument.transaction_id': 1,
      'fullDocument.prediction': 1,
      'fullDocument.predicted_at': 1
    }
  }
];

const changeStream = db.collection('ml_predictions').watch(pipeline);

changeStream.on('change', (change) => {
  // Publier vers Kafka pour ingestion dans OLAP
  publishToKafka('high-risk-transactions', change.fullDocument);
});
```

### 7.4 Requêtes cross-system

**Exemple : Analyse de fraude combinant OLTP + NoSQL**

```javascript
// Étape 1 : Récupérer transactions suspectes de OLTP
const suspiciousTransactions = await postgresClient.query(`
  SELECT 
    t.transaction_id,
    t.customer_id,
    t.amount,
    f.fraud_score
  FROM transaction t
  JOIN fraud f ON t.transaction_id = f.transaction_id
  WHERE f.risk_level IN ('high', 'critical')
    AND t.created_at > NOW() - INTERVAL '7 days'
`);

// Étape 2 : Enrichir avec features détaillées de MongoDB
const enrichedData = await Promise.all(
  suspiciousTransactions.rows.map(async (txn) => {
    const features = await mongoClient
      .db('ml_db')
      .collection('fraud_features')
      .findOne({ transaction_id: txn.transaction_id });
    
    const prediction = await mongoClient
      .db('ml_db')
      .collection('ml_predictions')
      .findOne({ transaction_id: txn.transaction_id });
    
    return {
      ...txn,
      detailed_features: features,
      ml_prediction: prediction
    };
  })
);

// Étape 3 : Agréger et analyser
const analysis = enrichedData.map(data => ({
  transaction_id: data.transaction_id,
  customer_segment: data.detailed_features.customer_features.customer_segment,
  risk_factors: identifyRiskFactors(data.detailed_features),
  model_confidence: data.ml_prediction.prediction.confidence
}));
```

---

## 8. Cas d'usage métier

### 8.1 Détection de fraude en temps réel

**Pipeline complet :**

```javascript
// 1. Transaction arrive dans OLTP
// 2. Kafka event déclenché
// 3. Feature extraction en temps réel

async function processFraudDetection(transactionEvent) {
  const txn = transactionEvent.transaction;
  
  // Extraire features depuis MongoDB + OLTP
  const features = {
    transaction_id: txn.transaction_id,
    
    // Features transactionnelles (OLTP)
    transaction_features: {
      amount: txn.amount,
      currency: txn.currency,
      is_high_value: txn.amount > 1000
    },
    
    // Features de vélocité (MongoDB aggregation)
    velocity_features: await db.collection('user_interactions').aggregate([
      {
        $match: {
          user_id: txn.customer_id,
          timestamp: { $gte: new Date(Date.now() - 24*60*60*1000) }
        }
      },
      {
        $group: {
          _id: '$user_id',
          transactions_last_24h: { $sum: 1 },
          total_amount_last_24h: { $sum: '$ecommerce_context.cart_value' }
        }
      }
    ]).toArray(),
    
    // Features comportementales (MongoDB)
    behavioral_features: await db.collection('sessions').findOne(
      { user_id: txn.customer_id },
      { sort: { session_start: -1 }, limit: 1 }
    )
  };
  
  // Sauvegarder features
  await db.collection('fraud_features').insertOne(features);
  
  // Appeler modèle ML
  const prediction = await mlModel.predict(features);
  
  // Sauvegarder prédiction
  await db.collection('ml_predictions').insertOne(prediction);
  
  // Si fraude détectée, déclencher alerte
  if (prediction.fraud_probability > 0.75) {
    await sendFraudAlert(txn, prediction);
  }
  
  return prediction;
}
```

### 8.2 Analyse comportementale client

```javascript
// Requête : Identifier patterns de navigation avant achat

const purchasePatterns = await db.collection('sessions').aggregate([
  // Filtrer sessions avec conversion
  {
    $match: {
      'conversion_data.converted': true,
      'session_start': { $gte: new Date('2026-01-01') }
    }
  },
  
  // Dérouler les étapes du funnel
  { $unwind: '$funnel_progress' },
  
  // Grouper par étape et calculer métriques
  {
    $group: {
      _id: {
        step: '$funnel_progress.step',
        device: '$device_info.type'
      },
      count: { $sum: 1 },
      avg_time_spent: { 
        $avg: { 
          $divide: [
            { $subtract: ['$session_end', '$funnel_progress.timestamp'] },
            1000
          ]
        }
      },
      avg_conversion_value: { $avg: '$conversion_data.revenue' }
    }
  },
  
  // Trier par fréquence
  { $sort: { count: -1 } },
  
  // Formater résultat
  {
    $project: {
      _id: 0,
      step: '$_id.step',
      device: '$_id.device',
      frequency: '$count',
      avg_time_spent_seconds: { $round: ['$avg_time_spent', 2] },
      avg_conversion_value: { $round: ['$avg_conversion_value', 2] }
    }
  }
]).toArray();

console.log(purchasePatterns);
/* Résultat:
[
  {
    step: 'product_view',
    device: 'mobile',
    frequency: 45000,
    avg_time_spent_seconds: 120.5,
    avg_conversion_value: 89.99
  },
  ...
]
*/
```

### 8.3 Analytics en temps réel pour dashboard

```javascript
// Agrégation en temps réel : Métriques des dernières 24h

const realtimeMetrics = await db.collection('api_logs').aggregate([
  // Filtrer dernières 24h
  {
    $match: {
      timestamp: { $gte: new Date(Date.now() - 24*60*60*1000) }
    }
  },
  
  // Grouper par heure et endpoint
  {
    $group: {
      _id: {
        hour: { $dateToString: { format: '%Y-%m-%d %H:00', date: '$timestamp' } },
        endpoint: '$request.endpoint',
        status_category: {
          $switch: {
            branches: [
              { case: { $lt: ['$response.status_code', 400] }, then: 'success' },
              { case: { $lt: ['$response.status_code', 500] }, then: 'client_error' },
              { case: { $gte: ['$response.status_code', 500] }, then: 'server_error' }
            ]
          }
        }
      },
      request_count: { $sum: 1 },
      avg_response_time: { $avg: '$response.duration_ms' },
      p95_response_time: { $percentile: { input: '$response.duration_ms', p: [0.95], method: 'approximate' } },
      total_bytes_transferred: { $sum: { $add: ['$request.body_size_bytes', '$response.body_size_bytes'] } }
    }
  },
  
  // Calculer taux d'erreur
  {
    $group: {
      _id: { hour: '$_id.hour', endpoint: '$_id.endpoint' },
      success_count: {
        $sum: { $cond: [{ $eq: ['$_id.status_category', 'success'] }, '$request_count', 0] }
      },
      error_count: {
        $sum: { $cond: [{ $ne: ['$_id.status_category', 'success'] }, '$request_count', 0] }
      },
      avg_response_time: { $avg: '$avg_response_time' }
    }
  },
  
  // Calculer taux d'erreur en pourcentage
  {
    $project: {
      endpoint: '$_id.endpoint',
      hour: '$_id.hour',
      total_requests: { $add: ['$success_count', '$error_count'] },
      success_count: 1,
      error_count: 1,
      error_rate: {
        $multiply: [
          { $divide: ['$error_count', { $add: ['$success_count', '$error_count'] }] },
          100
        ]
      },
      avg_response_time_ms: { $round: ['$avg_response_time', 2] }
    }
  },
  
  { $sort: { hour: -1, error_rate: -1 } }
]).toArray();
```

### 8.4 Segmentation ML pour personnalisation

```javascript
// Utiliser features historiques pour segmentation client

const customerSegments = await db.collection('user_interactions').aggregate([
  // Calculer métriques par client (30 derniers jours)
  {
    $match: {
      timestamp: { $gte: new Date(Date.now() - 30*24*60*60*1000) }
    }
  },
  
  {
    $group: {
      _id: '$user_id',
      session_count: { $sum: 1 },
      avg_session_duration: { $avg: { $subtract: ['$session_end', '$timestamp'] } },
      total_interactions: { $sum: '$interaction_details.clicks' },
      countries_visited: { $addToSet: '$geolocation.country' },
      devices_used: { $addToSet: '$device.type' },
      cart_values: { $push: '$ecommerce_context.cart_value' }
    }
  },
  
  // Calculer métriques agrégées
  {
    $project: {
      user_id: '$_id',
      session_count: 1,
      avg_session_duration_minutes: { $divide: ['$avg_session_duration', 60000] },
      total_interactions: 1,
      countries_count: { $size: '$countries_visited' },
      devices_count: { $size: '$devices_used' },
      avg_cart_value: { $avg: '$cart_values' },
      max_cart_value: { $max: '$cart_values' }
    }
  },
  
  // Segmentation basée sur comportement
  {
    $addFields: {
      engagement_score: {
        $add: [
          { $multiply: ['$session_count', 0.3] },
          { $multiply: ['$total_interactions', 0.5] },
          { $multiply: ['$avg_session_duration_minutes', 0.2] }
        ]
      },
      value_score: {
        $cond: {
          if: { $gte: ['$avg_cart_value', 200] },
          then: 'high',
          else: { $cond: { if: { $gte: ['$avg_cart_value', 50] }, then: 'medium', else: 'low' } }
        }
      },
      mobility_score: {
        $cond: {
          if: { $gte: ['$countries_count', 3] },
          then: 'global',
          else: 'local'
        }
      }
    }
  },
  
  // Attribuer segment final
  {
    $addFields: {
      segment: {
        $switch: {
          branches: [
            { 
              case: { $and: [{ $gte: ['$engagement_score', 50] }, { $eq: ['$value_score', 'high'] }] },
              then: 'vip_high_value'
            },
            {
              case: { $and: [{ $gte: ['$engagement_score', 30] }, { $eq: ['$value_score', 'high'] }] },
              then: 'engaged_high_value'
            },
            {
              case: { $gte: ['$engagement_score', 50] },
              then: 'super_engaged'
            },
            {
              case: { $eq: ['$value_score', 'high'] },
              then: 'high_value_low_engagement'
            }
          ],
          default: 'standard'
        }
      }
    }
  },
  
  // Grouper par segment pour analytics
  {
    $group: {
      _id: '$segment',
      user_count: { $sum: 1 },
      avg_engagement_score: { $avg: '$engagement_score' },
      avg_cart_value: { $avg: '$avg_cart_value' },
      total_sessions: { $sum: '$session_count' }
    }
  }
]).toArray();
```

---

## 📊 Résumé des choix techniques

| Aspect | Solution | Justification |
|--------|----------|---------------|
| **SGBD** | MongoDB 7.0+ | Flexibilité schéma, Time-series, Aggregation pipeline |
| **Collections** | 9 principales | Séparation par domaine métier |
| **Indexation** | 40+ indexes | Optimisation requêtes fréquentes |
| **Sharding** | Hashed + Ranged | Distribution uniforme + requêtes temporelles |
| **CDC** | Debezium + Kafka | Synchronisation temps réel OLTP ↔ NoSQL |
| **Change Streams** | MongoDB native | Push vers OLAP/ML pipelines |
| **Retention** | TTL indexes | Auto-archivage selon politique |
| **Sécurité** | RBAC + Encryption | Conformité PCI-DSS/GDPR |

---

## 🔍 Points d'attention

### Performance
- **Monitoring des slow queries** : Activer le profiler MongoDB
- **Index utilization** : Vérifier avec `explain()` régulièrement
- **Shard balancing** : Surveiller la distribution des chunks

### Scalabilité
- **Hot shards** : Utiliser hashed sharding pour distributions uniformes
- **Connection pooling** : Configurer correctement les drivers
- **Read preferences** : Utiliser secondaries pour analytics

### Sécurité
- **Encryption at rest** : Activer WiredTiger encryption
- **Encryption in transit** : TLS 1.3 obligatoire
- **Audit logging** : Tracker tous les accès aux données sensibles

### Conformité
- **RGPD** : Zone sharding EU pour données européennes
- **Right to be forgotten** : Procédures de suppression
- **Data retention** : TTL automatique selon réglementation

---

## 📈 Métriques de succès

| Métrique | Target | Mesure |
|----------|--------|--------|
| **Write throughput** | 50K ops/sec | `db.serverStatus().opcounters` |
| **Read latency p95** | < 50ms | Monitoring APM |
| **Index hit ratio** | > 95% | `$indexStats` aggregation |
| **Shard distribution** | ±10% variance | `sh.status()` |
| **Change stream lag** | < 1 sec | Custom metrics |

---