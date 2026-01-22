# Requêtes SQL et NoSQL - Stripe Payment Platform
## Requêtes Pratiques pour OLTP, OLAP et NoSQL

---

## 📋 Table des matières

1. [Requêtes OLTP (PostgreSQL)](#requêtes-oltp-postgresql)
2. [Requêtes OLAP (Snowflake)](#requêtes-olap-snowflake)
3. [Requêtes NoSQL (MongoDB)](#requêtes-nosql-mongodb)
4. [Requêtes Cross-System](#requêtes-cross-system)
5. [Requêtes pour ML](#requêtes-pour-ml)
6. [Requêtes de Monitoring](#requêtes-de-monitoring)
7. [Requêtes d'Audit et Conformité](#requêtes-daudit-et-conformité)

---

## 1. Requêtes OLTP (PostgreSQL)

### 1.1 Transactions en temps réel

#### Récupérer les transactions d'un client
```sql
-- Historique complet des transactions d'un client
SELECT 
    t.transaction_id,
    t.created_at,
    t.amount,
    t.currency,
    t.status,
    t.transaction_type,
    m.merchant_name,
    p.product_name,
    pm.card_last4,
    pm.card_brand
FROM transaction t
JOIN merchant m ON t.merchant_id = m.merchant_id
JOIN product p ON t.product_id = p.product_id
JOIN payment_methods pm ON t.payment_method_id = pm.payment_method_id
WHERE t.customer_id = 'cus_abc123'
ORDER BY t.created_at DESC
LIMIT 50;
```

#### Transactions récentes avec montant élevé
```sql
-- Alertes pour transactions > P95 du client
WITH customer_stats AS (
    SELECT 
        customer_id,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount) as p95_amount,
        AVG(amount) as avg_amount
    FROM transaction
    WHERE created_at > NOW() - INTERVAL '90 days'
    GROUP BY customer_id
)
SELECT 
    t.transaction_id,
    t.customer_id,
    t.amount,
    t.currency,
    t.created_at,
    t.status,
    cs.avg_amount,
    cs.p95_amount,
    (t.amount / cs.avg_amount) as amount_ratio
FROM transaction t
JOIN customer_stats cs ON t.customer_id = cs.customer_id
WHERE t.amount > cs.p95_amount
  AND t.created_at > NOW() - INTERVAL '24 hours'
ORDER BY amount_ratio DESC;
```

#### Transactions échouées à investiguer
```sql
-- Transactions échouées avec contexte
SELECT 
    t.transaction_id,
    t.customer_id,
    c.email,
    c.name,
    t.amount,
    t.currency,
    t.status,
    t.failure_code,
    t.failure_message,
    t.created_at,
    pm.method_type,
    pm.card_brand,
    COUNT(*) OVER (PARTITION BY t.customer_id, DATE(t.created_at)) as daily_failures
FROM transaction t
JOIN customer c ON t.customer_id = c.customer_id
LEFT JOIN payment_methods pm ON t.payment_method_id = pm.payment_method_id
WHERE t.status = 'failed'
  AND t.created_at > NOW() - INTERVAL '7 days'
ORDER BY t.created_at DESC;
```

### 1.2 Analyses de fraude

#### Transactions suspectes (patterns multiples)
```sql
-- Détection de patterns de fraude
WITH suspicious_patterns AS (
    -- Pattern 1: Velocity élevée
    SELECT DISTINCT customer_id, 'high_velocity' as pattern
    FROM transaction
    WHERE created_at > NOW() - INTERVAL '1 hour'
    GROUP BY customer_id
    HAVING COUNT(*) >= 5
    
    UNION
    
    -- Pattern 2: Montants ronds inhabituels
    SELECT DISTINCT customer_id, 'round_amounts' as pattern
    FROM transaction
    WHERE amount IN (100, 200, 500, 1000, 5000)
      AND created_at > NOW() - INTERVAL '24 hours'
    GROUP BY customer_id
    HAVING COUNT(*) >= 3
    
    UNION
    
    -- Pattern 3: Nouveau device + montant élevé
    SELECT DISTINCT t.customer_id, 'new_device_high_amount' as pattern
    FROM transaction t
    WHERE t.is_new_device = true
      AND t.amount > 1000
      AND t.created_at > NOW() - INTERVAL '24 hours'
)
SELECT 
    c.customer_id,
    c.email,
    c.name,
    ARRAY_AGG(DISTINCT sp.pattern) as fraud_patterns,
    COUNT(DISTINCT sp.pattern) as pattern_count,
    SUM(t.amount) as total_amount_24h,
    COUNT(t.transaction_id) as transaction_count_24h
FROM customer c
JOIN suspicious_patterns sp ON c.customer_id = sp.customer_id
JOIN transaction t ON c.customer_id = t.customer_id 
    AND t.created_at > NOW() - INTERVAL '24 hours'
GROUP BY c.customer_id, c.email, c.name
HAVING COUNT(DISTINCT sp.pattern) >= 2  -- Au moins 2 patterns
ORDER BY pattern_count DESC, total_amount_24h DESC;
```

#### Chargebacks à analyser
```sql
-- Analyse des chargebacks avec contexte
SELECT 
    f.fraud_id,
    f.transaction_id,
    t.customer_id,
    c.name as customer_name,
    t.amount,
    t.currency,
    t.created_at as transaction_date,
    f.scored_at as fraud_check_date,
    f.fraud_probability,
    f.risk_level,
    f.is_fraud_confirmed,
    f.chargeback_amount,
    f.chargeback_reason,
    EXTRACT(DAY FROM f.scored_at - t.created_at) as days_to_chargeback,
    -- Historique du client
    (SELECT COUNT(*) 
     FROM transaction t2 
     WHERE t2.customer_id = t.customer_id 
       AND t2.created_at < t.created_at) as previous_transactions,
    (SELECT COUNT(*) 
     FROM fraud f2 
     JOIN transaction t2 ON f2.transaction_id = t2.transaction_id
     WHERE t2.customer_id = t.customer_id 
       AND f2.is_fraud_confirmed = true
       AND f2.scored_at < f.scored_at) as previous_chargebacks
FROM fraud f
JOIN transaction t ON f.transaction_id = t.transaction_id
JOIN customer c ON t.customer_id = c.customer_id
WHERE f.chargeback_amount IS NOT NULL
  AND f.scored_at > NOW() - INTERVAL '30 days'
ORDER BY f.chargeback_amount DESC;
```

### 1.3 Gestion des clients

#### Profil client complet
```sql
-- Vue 360° d'un client
WITH customer_metrics AS (
    SELECT 
        customer_id,
        COUNT(*) as total_transactions,
        SUM(amount) as lifetime_value,
        AVG(amount) as avg_transaction_amount,
        MIN(created_at) as first_transaction,
        MAX(created_at) as last_transaction,
        COUNT(DISTINCT merchant_id) as unique_merchants,
        COUNT(DISTINCT product_id) as unique_products,
        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_count
    FROM transaction
    WHERE customer_id = 'cus_abc123'
    GROUP BY customer_id
),
fraud_metrics AS (
    SELECT 
        t.customer_id,
        COUNT(*) as fraud_checks,
        COUNT(CASE WHEN f.is_fraud_confirmed THEN 1 END) as confirmed_frauds,
        AVG(f.fraud_probability) as avg_fraud_score
    FROM fraud f
    JOIN transaction t ON f.transaction_id = t.transaction_id
    WHERE t.customer_id = 'cus_abc123'
    GROUP BY t.customer_id
)
SELECT 
    c.customer_id,
    c.name,
    c.first_name,
    c.email,
    c.phone,
    c.full_address,
    c.created_at as account_created,
    EXTRACT(DAY FROM NOW() - c.created_at) as account_age_days,
    cm.total_transactions,
    cm.lifetime_value,
    cm.avg_transaction_amount,
    cm.first_transaction,
    cm.last_transaction,
    EXTRACT(DAY FROM NOW() - cm.last_transaction) as days_since_last_transaction,
    cm.unique_merchants,
    cm.unique_products,
    cm.failed_count,
    ROUND(cm.failed_count::numeric / cm.total_transactions * 100, 2) as failure_rate_pct,
    COALESCE(fm.fraud_checks, 0) as fraud_checks,
    COALESCE(fm.confirmed_frauds, 0) as confirmed_frauds,
    COALESCE(fm.avg_fraud_score, 0) as avg_fraud_score,
    -- Segment
    CASE 
        WHEN cm.lifetime_value > 10000 THEN 'VIP'
        WHEN cm.lifetime_value > 5000 THEN 'High Value'
        WHEN cm.lifetime_value > 1000 THEN 'Standard'
        ELSE 'New'
    END as customer_segment
FROM customer c
LEFT JOIN customer_metrics cm ON c.customer_id = cm.customer_id
LEFT JOIN fraud_metrics fm ON c.customer_id = fm.customer_id
WHERE c.customer_id = 'cus_abc123';
```

#### Top clients par LTV
```sql
-- Top 100 clients par valeur
WITH customer_ltv AS (
    SELECT 
        t.customer_id,
        COUNT(DISTINCT DATE(t.created_at)) as active_days,
        COUNT(*) as total_transactions,
        SUM(t.amount) as total_spent,
        AVG(t.amount) as avg_order_value,
        MIN(t.created_at) as first_purchase,
        MAX(t.created_at) as last_purchase,
        COUNT(DISTINCT t.merchant_id) as merchants_count
    FROM transaction t
    WHERE t.status = 'completed'
    GROUP BY t.customer_id
)
SELECT 
    c.customer_id,
    c.name,
    c.email,
    ltv.total_spent,
    ltv.total_transactions,
    ltv.avg_order_value,
    ltv.active_days,
    ROUND(ltv.total_spent / ltv.active_days, 2) as spend_per_active_day,
    ltv.first_purchase,
    ltv.last_purchase,
    EXTRACT(DAY FROM NOW() - ltv.last_purchase) as recency_days,
    ltv.merchants_count,
    -- RFM Score
    NTILE(5) OVER (ORDER BY EXTRACT(DAY FROM NOW() - ltv.last_purchase)) as recency_score,
    NTILE(5) OVER (ORDER BY ltv.total_transactions DESC) as frequency_score,
    NTILE(5) OVER (ORDER BY ltv.total_spent DESC) as monetary_score
FROM customer c
JOIN customer_ltv ltv ON c.customer_id = ltv.customer_id
WHERE ltv.total_spent > 1000
ORDER BY ltv.total_spent DESC
LIMIT 100;
```

### 1.4 Performance des marchands

#### Merchants avec taux d'échec élevé
```sql
-- Analyse des merchants problématiques
WITH merchant_metrics AS (
    SELECT 
        merchant_id,
        COUNT(*) as total_transactions,
        COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful,
        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
        SUM(amount) as total_volume,
        AVG(amount) as avg_transaction_amount,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM transaction
    WHERE created_at > NOW() - INTERVAL '30 days'
    GROUP BY merchant_id
    HAVING COUNT(*) >= 100  -- Au moins 100 transactions
)
SELECT 
    m.merchant_id,
    m.merchant_name,
    m.merchant_category,
    m.merchant_country,
    mm.total_transactions,
    mm.successful,
    mm.failed,
    ROUND(mm.failed::numeric / mm.total_transactions * 100, 2) as failure_rate_pct,
    mm.total_volume,
    mm.avg_transaction_amount,
    mm.unique_customers,
    ROUND(mm.total_volume / mm.unique_customers, 2) as revenue_per_customer
FROM merchant m
JOIN merchant_metrics mm ON m.merchant_id = mm.merchant_id
WHERE mm.failed::numeric / mm.total_transactions > 0.1  -- >10% échec
ORDER BY failure_rate_pct DESC;
```

---

## 2. Requêtes OLAP (Snowflake)

### 2.1 Rapports Business

#### Revenue Dashboard - Métriques quotidiennes
```sql
-- KPIs quotidiens pour dashboard
SELECT 
    d.full_date as date,
    d.year,
    d.month_name,
    d.day_of_week_name,
    
    -- Volume
    COUNT(DISTINCT f.transaction_id) as total_transactions,
    COUNT(DISTINCT f.customer_key) as unique_customers,
    COUNT(DISTINCT f.merchant_key) as unique_merchants,
    
    -- Revenue
    SUM(CASE WHEN f.is_refund = FALSE THEN f.amount ELSE 0 END) as gross_revenue,
    SUM(CASE WHEN f.is_refund = TRUE THEN f.amount ELSE 0 END) as refunds,
    SUM(CASE WHEN f.is_refund = FALSE THEN f.amount ELSE -f.amount END) as net_revenue,
    
    -- Average metrics
    AVG(CASE WHEN f.is_refund = FALSE THEN f.amount END) as avg_transaction_value,
    
    -- Success rates
    ROUND(
        COUNT(CASE WHEN f.is_successful = TRUE THEN 1 END)::FLOAT / 
        COUNT(*)::FLOAT * 100, 
        2
    ) as success_rate_pct,
    
    -- Device breakdown
    COUNT(CASE WHEN f.device_type = 'mobile' THEN 1 END) as mobile_transactions,
    COUNT(CASE WHEN f.device_type = 'desktop' THEN 1 END) as desktop_transactions,
    
    -- Growth vs yesterday
    LAG(SUM(CASE WHEN f.is_refund = FALSE THEN f.amount ELSE -f.amount END)) 
        OVER (ORDER BY d.full_date) as prev_day_revenue,
    ROUND(
        (SUM(CASE WHEN f.is_refund = FALSE THEN f.amount ELSE -f.amount END) - 
         LAG(SUM(CASE WHEN f.is_refund = FALSE THEN f.amount ELSE -f.amount END)) 
             OVER (ORDER BY d.full_date)) / 
        NULLIF(LAG(SUM(CASE WHEN f.is_refund = FALSE THEN f.amount ELSE -f.amount END)) 
               OVER (ORDER BY d.full_date), 0) * 100,
        2
    ) as revenue_growth_pct

FROM fact.transactions f
JOIN dim.date d ON f.date_key = d.date_key
WHERE d.full_date >= CURRENT_DATE - 30
GROUP BY d.full_date, d.year, d.month_name, d.day_of_week_name
ORDER BY d.full_date DESC;
```

#### Analyse Cohort - Rétention client
```sql
-- Cohort analysis par mois d'inscription
WITH customer_cohorts AS (
    SELECT 
        c.customer_key,
        DATE_TRUNC('month', c.created_at) as cohort_month,
        MIN(f.created_at) as first_transaction_date
    FROM dim.customer c
    JOIN fact.transactions f ON c.customer_key = f.customer_key
    WHERE c.is_current = TRUE
    GROUP BY c.customer_key, cohort_month
),
cohort_activity AS (
    SELECT 
        cc.cohort_month,
        DATE_TRUNC('month', f.created_at) as activity_month,
        MONTHS_BETWEEN(DATE_TRUNC('month', f.created_at), cc.cohort_month) as month_number,
        COUNT(DISTINCT f.customer_key) as active_customers,
        SUM(f.amount) as revenue
    FROM customer_cohorts cc
    JOIN fact.transactions f ON cc.customer_key = f.customer_key
    WHERE f.is_refund = FALSE
    GROUP BY cc.cohort_month, activity_month, month_number
),
cohort_size AS (
    SELECT 
        cohort_month,
        COUNT(DISTINCT customer_key) as cohort_size
    FROM customer_cohorts
    GROUP BY cohort_month
)
SELECT 
    ca.cohort_month,
    cs.cohort_size,
    ca.month_number,
    ca.active_customers,
    ROUND(ca.active_customers::FLOAT / cs.cohort_size::FLOAT * 100, 2) as retention_rate_pct,
    ca.revenue,
    ROUND(ca.revenue / ca.active_customers, 2) as revenue_per_active_customer
FROM cohort_activity ca
JOIN cohort_size cs ON ca.cohort_month = cs.cohort_month
WHERE ca.cohort_month >= '2025-01-01'
  AND ca.month_number <= 12  -- Première année
ORDER BY ca.cohort_month, ca.month_number;
```

#### Top produits par catégorie
```sql
-- Analyse des produits les plus performants
WITH product_metrics AS (
    SELECT 
        p.product_key,
        p.product_name,
        p.product_category,
        COUNT(DISTINCT f.transaction_id) as transaction_count,
        COUNT(DISTINCT f.customer_key) as unique_buyers,
        SUM(f.amount) as total_revenue,
        AVG(f.amount) as avg_price,
        COUNT(CASE WHEN f.is_refund = TRUE THEN 1 END) as refund_count,
        ROW_NUMBER() OVER (
            PARTITION BY p.product_category 
            ORDER BY SUM(f.amount) DESC
        ) as rank_in_category
    FROM fact.transactions f
    JOIN dim.product p ON f.product_key = p.product_key
    JOIN dim.date d ON f.date_key = d.date_key
    WHERE d.full_date >= CURRENT_DATE - 90
      AND p.is_current = TRUE
      AND f.is_refund = FALSE
    GROUP BY p.product_key, p.product_name, p.product_category
)
SELECT 
    product_category,
    product_name,
    transaction_count,
    unique_buyers,
    total_revenue,
    avg_price,
    refund_count,
    ROUND(refund_count::FLOAT / transaction_count::FLOAT * 100, 2) as refund_rate_pct,
    ROUND(total_revenue / unique_buyers, 2) as revenue_per_buyer,
    rank_in_category
FROM product_metrics
WHERE rank_in_category <= 10  -- Top 10 par catégorie
ORDER BY product_category, rank_in_category;
```

### 2.2 Analyses Géographiques

#### Revenue par région avec croissance
```sql
-- Performance géographique avec tendances
WITH monthly_geo_revenue AS (
    SELECT 
        l.country,
        l.region,
        d.year,
        d.month,
        SUM(f.amount) as revenue,
        COUNT(DISTINCT f.customer_key) as unique_customers,
        COUNT(DISTINCT f.transaction_id) as transactions
    FROM fact.transactions f
    JOIN dim.location l ON f.location_key = l.location_key
    JOIN dim.date d ON f.date_key = d.date_key
    WHERE f.is_refund = FALSE
      AND d.full_date >= CURRENT_DATE - 365
    GROUP BY l.country, l.region, d.year, d.month
)
SELECT 
    country,
    region,
    year,
    month,
    revenue,
    unique_customers,
    transactions,
    ROUND(revenue / transactions, 2) as avg_transaction_value,
    
    -- Croissance month-over-month
    LAG(revenue) OVER (
        PARTITION BY country, region 
        ORDER BY year, month
    ) as prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (
            PARTITION BY country, region 
            ORDER BY year, month
        )) / NULLIF(LAG(revenue) OVER (
            PARTITION BY country, region 
            ORDER BY year, month
        ), 0) * 100,
        2
    ) as mom_growth_pct,
    
    -- Part de marché (% du total global)
    ROUND(
        revenue / SUM(revenue) OVER (PARTITION BY year, month) * 100,
        2
    ) as market_share_pct

FROM monthly_geo_revenue
WHERE year = 2026
ORDER BY year DESC, month DESC, revenue DESC;
```

### 2.3 Segmentation Client

#### Customer Lifetime Value Analysis
```sql
-- Segmentation par LTV avec prédictions
WITH customer_ltv AS (
    SELECT 
        c.customer_key,
        c.name,
        c.email,
        c.customer_segment,
        DATE(c.created_at) as signup_date,
        DATEDIFF('day', c.created_at, CURRENT_DATE) as customer_age_days,
        
        -- Historical metrics
        COUNT(DISTINCT f.transaction_id) as total_transactions,
        SUM(f.amount) as total_spent,
        AVG(f.amount) as avg_order_value,
        MAX(f.created_at) as last_purchase_date,
        DATEDIFF('day', MAX(f.created_at), CURRENT_DATE) as days_since_last_purchase,
        
        -- Calculate LTV
        SUM(f.amount) as actual_ltv,
        
        -- Predict future LTV (simple linear extrapolation)
        CASE 
            WHEN DATEDIFF('day', c.created_at, CURRENT_DATE) > 0 THEN
                SUM(f.amount) / DATEDIFF('day', c.created_at, CURRENT_DATE) * 365
            ELSE 0
        END as predicted_annual_ltv
        
    FROM dim.customer c
    LEFT JOIN fact.transactions f ON c.customer_key = f.customer_key
        AND f.is_refund = FALSE
    WHERE c.is_current = TRUE
    GROUP BY 
        c.customer_key, c.name, c.email, c.customer_segment, 
        c.created_at
)
SELECT 
    customer_segment,
    COUNT(*) as customer_count,
    
    -- LTV statistics
    ROUND(AVG(actual_ltv), 2) as avg_ltv,
    ROUND(MEDIAN(actual_ltv), 2) as median_ltv,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY actual_ltv), 2) as p75_ltv,
    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY actual_ltv), 2) as p90_ltv,
    ROUND(MAX(actual_ltv), 2) as max_ltv,
    
    -- Behavioral metrics
    ROUND(AVG(total_transactions), 1) as avg_transactions,
    ROUND(AVG(avg_order_value), 2) as avg_order_value,
    ROUND(AVG(days_since_last_purchase), 1) as avg_days_since_purchase,
    
    -- Predicted future value
    ROUND(AVG(predicted_annual_ltv), 2) as avg_predicted_annual_ltv,
    ROUND(SUM(predicted_annual_ltv), 2) as total_predicted_annual_revenue

FROM customer_ltv
GROUP BY customer_segment
ORDER BY avg_ltv DESC;
```

### 2.4 Fraude Analytics

#### Fraude trends et patterns
```sql
-- Analyse des tendances de fraude
WITH fraud_daily AS (
    SELECT 
        d.full_date,
        d.day_of_week_name,
        COUNT(DISTINCT fs.transaction_id) as total_fraud_checks,
        COUNT(DISTINCT CASE 
            WHEN fs.risk_level IN ('high', 'critical') 
            THEN fs.transaction_id 
        END) as high_risk_transactions,
        COUNT(DISTINCT CASE 
            WHEN fs.is_fraud_confirmed = TRUE 
            THEN fs.transaction_id 
        END) as confirmed_frauds,
        SUM(CASE 
            WHEN fs.is_fraud_confirmed = TRUE 
            THEN fs.fraud_amount 
            ELSE 0 
        END) as fraud_losses,
        AVG(fs.fraud_score) as avg_fraud_score,
        
        -- False positive/negative analysis
        COUNT(CASE 
            WHEN fs.risk_level IN ('high', 'critical') 
                AND fs.is_fraud_confirmed = FALSE 
            THEN 1 
        END) as false_positives,
        COUNT(CASE 
            WHEN fs.risk_level IN ('low', 'medium') 
                AND fs.is_fraud_confirmed = TRUE 
            THEN 1 
        END) as false_negatives
        
    FROM fact.fraud_scores fs
    JOIN dim.date d ON fs.date_key = d.date_key
    WHERE d.full_date >= CURRENT_DATE - 90
    GROUP BY d.full_date, d.day_of_week_name
)
SELECT 
    full_date,
    day_of_week_name,
    total_fraud_checks,
    high_risk_transactions,
    confirmed_frauds,
    fraud_losses,
    ROUND(avg_fraud_score, 4) as avg_fraud_score,
    
    -- Rates
    ROUND(
        confirmed_frauds::FLOAT / NULLIF(total_fraud_checks, 0) * 100, 
        2
    ) as fraud_rate_pct,
    ROUND(
        high_risk_transactions::FLOAT / NULLIF(total_fraud_checks, 0) * 100, 
        2
    ) as high_risk_rate_pct,
    
    -- Model performance
    ROUND(
        false_positives::FLOAT / NULLIF(high_risk_transactions, 0) * 100, 
        2
    ) as false_positive_rate_pct,
    ROUND(
        false_negatives::FLOAT / NULLIF(confirmed_frauds, 0) * 100, 
        2
    ) as false_negative_rate_pct,
    
    -- 7-day moving average
    ROUND(
        AVG(fraud_losses) OVER (
            ORDER BY full_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 
        2
    ) as fraud_losses_7d_ma

FROM fraud_daily
ORDER BY full_date DESC;
```

---

## 3. Requêtes NoSQL (MongoDB)

### 3.1 Analyse comportementale

#### Sessions utilisateur avec funnel
```javascript
// Analyse du funnel de conversion
db.sessions.aggregate([
    {
        $match: {
            session_start: {
                $gte: ISODate("2026-01-15T00:00:00Z"),
                $lt: ISODate("2026-01-22T00:00:00Z")
            }
        }
    },
    {
        $addFields: {
            funnel_steps_reached: {
                $size: "$funnel_progress"
            },
            max_step: {
                $max: "$funnel_progress.step"
            }
        }
    },
    {
        $group: {
            _id: "$device_type",
            total_sessions: { $sum: 1 },
            
            // Funnel metrics
            reached_product_view: {
                $sum: {
                    $cond: [
                        { $in: ["product_view", "$funnel_progress.step"] },
                        1, 0
                    ]
                }
            },
            reached_add_to_cart: {
                $sum: {
                    $cond: [
                        { $in: ["add_to_cart", "$funnel_progress.step"] },
                        1, 0
                    ]
                }
            },
            reached_checkout: {
                $sum: {
                    $cond: [
                        { $in: ["checkout_initiated", "$funnel_progress.step"] },
                        1, 0
                    ]
                }
            },
            reached_purchase: {
                $sum: {
                    $cond: [
                        { $in: ["purchase_completed", "$funnel_progress.step"] },
                        1, 0
                    ]
                }
            },
            
            // Engagement metrics
            avg_session_duration: { 
                $avg: "$session_duration_minutes" 
            },
            avg_pages_viewed: { 
                $avg: "$total_pages_viewed" 
            },
            
            // Conversion metrics
            total_converted: {
                $sum: { $cond: ["$conversion_data.converted", 1, 0] }
            },
            total_revenue: {
                $sum: "$conversion_data.revenue"
            }
        }
    },
    {
        $project: {
            device_type: "$_id",
            total_sessions: 1,
            
            // Conversion rates
            product_view_rate: {
                $multiply: [
                    { $divide: ["$reached_product_view", "$total_sessions"] },
                    100
                ]
            },
            add_to_cart_rate: {
                $multiply: [
                    { $divide: ["$reached_add_to_cart", "$total_sessions"] },
                    100
                ]
            },
            checkout_rate: {
                $multiply: [
                    { $divide: ["$reached_checkout", "$total_sessions"] },
                    100
                ]
            },
            purchase_rate: {
                $multiply: [
                    { $divide: ["$reached_purchase", "$total_sessions"] },
                    100
                ]
            },
            
            overall_conversion_rate: {
                $multiply: [
                    { $divide: ["$total_converted", "$total_sessions"] },
                    100
                ]
            },
            
            avg_session_duration: { $round: ["$avg_session_duration", 2] },
            avg_pages_viewed: { $round: ["$avg_pages_viewed", 1] },
            total_revenue: { $round: ["$total_revenue", 2] },
            revenue_per_session: {
                $round: [
                    { $divide: ["$total_revenue", "$total_sessions"] },
                    2
                ]
            }
        }
    },
    {
        $sort: { overall_conversion_rate: -1 }
    }
])
```

#### User interactions - Patterns inhabituels
```javascript
// Détection d'anomalies comportementales
db.user_interactions.aggregate([
    {
        $match: {
            timestamp: {
                $gte: ISODate("2026-01-21T00:00:00Z")
            }
        }
    },
    {
        $group: {
            _id: {
                user_id: "$user_id",
                session_id: "$session_id"
            },
            interaction_count: { $sum: 1 },
            unique_events: { $addToSet: "$event_name" },
            countries: { $addToSet: "$geolocation.country" },
            devices: { $addToSet: "$device.type" },
            avg_time_between_clicks: {
                $avg: {
                    $subtract: [
                        "$timestamp",
                        { $ifNull: ["$previous_timestamp", "$timestamp"] }
                    ]
                }
            },
            suspicious_behaviors: {
                $push: {
                    $cond: [
                        {
                            $or: [
                                { $eq: ["$behavioral_signals.copy_paste", true] },
                                { $lt: ["$behavioral_signals.time_on_page", 2] },
                                { $gt: ["$behavioral_signals.click_frequency", 10] }
                            ]
                        },
                        {
                            event: "$event_name",
                            reason: {
                                $cond: [
                                    { $eq: ["$behavioral_signals.copy_paste", true] },
                                    "copy_paste_detected",
                                    {
                                        $cond: [
                                            { $lt: ["$behavioral_signals.time_on_page", 2] },
                                            "too_fast",
                                            "high_frequency_clicks"
                                        ]
                                    }
                                ]
                            }
                        },
                        null
                    ]
                }
            }
        }
    },
    {
        $match: {
            $or: [
                { interaction_count: { $gt: 100 } },  // Trop d'interactions
                { "countries.1": { $exists: true } },  // Multiple pays
                { "devices.2": { $exists: true } },    // Multiple devices
                { "suspicious_behaviors.0": { $exists: true } }  // Comportements suspects
            ]
        }
    },
    {
        $project: {
            user_id: "$_id.user_id",
            session_id: "$_id.session_id",
            interaction_count: 1,
            unique_events_count: { $size: "$unique_events" },
            countries_count: { $size: "$countries" },
            devices_count: { $size: "$devices" },
            suspicious_behaviors: {
                $filter: {
                    input: "$suspicious_behaviors",
                    as: "behavior",
                    cond: { $ne: ["$$behavior", null] }
                }
            },
            anomaly_score: {
                $add: [
                    { $cond: [{ $gt: ["$interaction_count", 100] }, 0.3, 0] },
                    { $cond: [{ $gt: [{ $size: "$countries" }, 1] }, 0.4, 0] },
                    { $cond: [{ $gt: [{ $size: "$devices" }, 2] }, 0.3, 0] }
                ]
            }
        }
    },
    {
        $match: {
            anomaly_score: { $gte: 0.5 }  // Score >= 0.5
        }
    },
    {
        $sort: { anomaly_score: -1 }
    },
    {
        $limit: 100
    }
])
```

### 3.2 ML Features

#### Features de fraude en temps réel
```javascript
// Récupération des features ML pour un client
db.fraud_features.findOne(
    { customer_id: "cus_abc123" },
    {
        projection: {
            customer_id: 1,
            timestamp: 1,
            
            // Velocity features
            "velocity_features.transactions_last_1h": 1,
            "velocity_features.transactions_last_24h": 1,
            "velocity_features.total_amount_last_24h": 1,
            "velocity_features.avg_amount_7d": 1,
            
            // Location features
            "location_features.countries_used_7d": 1,
            "location_features.distance_from_last_transaction_km": 1,
            "location_features.is_vpn_detected": 1,
            
            // Device features
            "device_features.devices_used_count": 1,
            "device_features.is_new_device": 1,
            
            // Behavioral features
            "behavioral_features.form_fill_speed_score": 1,
            "behavioral_features.mouse_movement_entropy": 1,
            
            // Network features
            "network_features.ip_reputation_score": 1,
            "network_features.is_datacenter_ip": 1,
            
            // Aggregated scores
            "aggregated_scores.velocity_risk_score": 1,
            "aggregated_scores.location_risk_score": 1,
            "aggregated_scores.device_risk_score": 1,
            "aggregated_scores.overall_risk_score": 1
        }
    }
)
```

#### Top features de fraude par importance
```javascript
// Analyse de l'importance des features
db.fraud_features.aggregate([
    {
        $match: {
            timestamp: {
                $gte: ISODate("2026-01-15T00:00:00Z")
            },
            linked_transaction_id: { $exists: true }
        }
    },
    // Lookup pour savoir si fraude confirmée
    {
        $lookup: {
            from: "ml_predictions",
            let: { txn_id: "$linked_transaction_id" },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: ["$transaction_id", "$$txn_id"] },
                                { $eq: ["$model_name", "fraud_detection"] }
                            ]
                        }
                    }
                }
            ],
            as: "prediction"
        }
    },
    {
        $unwind: {
            path: "$prediction",
            preserveNullAndEmptyArrays: true
        }
    },
    {
        $group: {
            _id: null,
            
            // Moyennes par feature pour fraudes vs légitimes
            avg_velocity_risk_fraud: {
                $avg: {
                    $cond: [
                        { $gte: ["$prediction.fraud_probability", 0.75] },
                        "$aggregated_scores.velocity_risk_score",
                        null
                    ]
                }
            },
            avg_velocity_risk_legit: {
                $avg: {
                    $cond: [
                        { $lt: ["$prediction.fraud_probability", 0.75] },
                        "$aggregated_scores.velocity_risk_score",
                        null
                    ]
                }
            },
            
            avg_location_risk_fraud: {
                $avg: {
                    $cond: [
                        { $gte: ["$prediction.fraud_probability", 0.75] },
                        "$aggregated_scores.location_risk_score",
                        null
                    ]
                }
            },
            avg_location_risk_legit: {
                $avg: {
                    $cond: [
                        { $lt: ["$prediction.fraud_probability", 0.75] },
                        "$aggregated_scores.location_risk_score",
                        null
                    ]
                }
            },
            
            avg_device_risk_fraud: {
                $avg: {
                    $cond: [
                        { $gte: ["$prediction.fraud_probability", 0.75] },
                        "$aggregated_scores.device_risk_score",
                        null
                    ]
                }
            },
            avg_device_risk_legit: {
                $avg: {
                    $cond: [
                        { $lt: ["$prediction.fraud_probability", 0.75] },
                        "$aggregated_scores.device_risk_score",
                        null
                    ]
                }
            },
            
            total_fraud: {
                $sum: {
                    $cond: [
                        { $gte: ["$prediction.fraud_probability", 0.75] },
                        1, 0
                    ]
                }
            },
            total_legit: {
                $sum: {
                    $cond: [
                        { $lt: ["$prediction.fraud_probability", 0.75] },
                        1, 0
                    ]
                }
            }
        }
    },
    {
        $project: {
            velocity_risk_separation: {
                $subtract: [
                    "$avg_velocity_risk_fraud",
                    "$avg_velocity_risk_legit"
                ]
            },
            location_risk_separation: {
                $subtract: [
                    "$avg_location_risk_fraud",
                    "$avg_location_risk_legit"
                ]
            },
            device_risk_separation: {
                $subtract: [
                    "$avg_device_risk_fraud",
                    "$avg_device_risk_legit"
                ]
            },
            total_fraud: 1,
            total_legit: 1
        }
    }
])
```

### 3.3 Logs & Monitoring

#### Erreurs système récentes
```javascript
// Top erreurs dans les dernières 24h
db.error_logs.aggregate([
    {
        $match: {
            timestamp: {
                $gte: new Date(Date.now() - 24 * 60 * 60 * 1000)
            },
            severity: { $in: ["error", "critical"] }
        }
    },
    {
        $group: {
            _id: {
                error_type: "$error_type",
                error_message: "$error_message",
                service_name: "$service_name"
            },
            count: { $sum: 1 },
            first_occurrence: { $min: "$timestamp" },
            last_occurrence: { $max: "$timestamp" },
            affected_users: { $addToSet: "$user_id" },
            stack_traces: { $push: "$stack_trace" }
        }
    },
    {
        $project: {
            error_type: "$_id.error_type",
            error_message: "$_id.error_message",
            service_name: "$_id.service_name",
            count: 1,
            first_occurrence: 1,
            last_occurrence: 1,
            affected_users_count: { $size: "$affected_users" },
            duration_minutes: {
                $divide: [
                    { $subtract: ["$last_occurrence", "$first_occurrence"] },
                    60000
                ]
            },
            sample_stack_trace: { $arrayElemAt: ["$stack_traces", 0] }
        }
    },
    {
        $sort: { count: -1 }
    },
    {
        $limit: 20
    }
])
```

---

## 4. Requêtes Cross-System

### 4.1 Enrichissement OLTP → NoSQL → OLAP

```sql
-- Snowflake: Transaction enrichie avec données NoSQL
WITH transaction_base AS (
    SELECT 
        f.transaction_id,
        f.customer_key,
        c.customer_id,
        f.amount,
        f.created_at
    FROM fact.transactions f
    JOIN dim.customer c ON f.customer_key = c.customer_key
    WHERE f.created_at >= CURRENT_DATE - 7
),
-- Données NoSQL importées via pipeline ELT
nosql_features AS (
    SELECT 
        customer_id,
        velocity_risk_score,
        location_risk_score,
        device_risk_score
    FROM analytics.fraud_features_enriched
    WHERE event_date >= CURRENT_DATE - 7
)
SELECT 
    t.transaction_id,
    t.customer_id,
    t.amount,
    t.created_at,
    nf.velocity_risk_score,
    nf.location_risk_score,
    nf.device_risk_score,
    (nf.velocity_risk_score + nf.location_risk_score + nf.device_risk_score) / 3 
        as avg_risk_score
FROM transaction_base t
LEFT JOIN nosql_features nf ON t.customer_id = nf.customer_id
ORDER BY avg_risk_score DESC
LIMIT 100;
```

---

## 5. Requêtes pour ML

### 5.1 Extraction training data

```sql
-- Snowflake: Dataset pour training modèle de fraude
WITH fraud_labels AS (
    SELECT 
        transaction_id,
        CASE 
            WHEN chargeback_amount IS NOT NULL THEN 1
            WHEN is_fraud_confirmed = TRUE THEN 1
            ELSE 0
        END as is_fraud
    FROM fact.fraud_scores
),
features AS (
    SELECT 
        t.transaction_id,
        -- Transaction features
        t.amount,
        t.currency,
        EXTRACT(HOUR FROM t.created_at) as transaction_hour,
        t.device_type,
        
        -- Customer features
        c.account_age_days,
        c.total_lifetime_transactions,
        c.chargeback_rate,
        
        -- Additional features from analytics
        af.velocity_risk_score,
        af.location_risk_score,
        af.device_risk_score
        
    FROM fact.transactions t
    JOIN dim.customer c ON t.customer_key = c.customer_key
    LEFT JOIN analytics.fraud_features_enriched af 
        ON t.transaction_id = af.transaction_id
    WHERE t.created_at >= CURRENT_DATE - 90
)
SELECT 
    f.*,
    COALESCE(fl.is_fraud, 0) as label
FROM features f
LEFT JOIN fraud_labels fl ON f.transaction_id = fl.transaction_id
WHERE f.transaction_id IS NOT NULL;
```

---

## 6. Requêtes de Monitoring

### 6.1 Data Quality Checks

```sql
-- Snowflake: Checks qualité quotidiens
WITH data_quality_checks AS (
    SELECT 
        'fact.transactions' as table_name,
        'null_customer_key' as check_name,
        COUNT(*) as failed_rows,
        CURRENT_DATE as check_date
    FROM fact.transactions
    WHERE customer_key IS NULL
      AND date_key = (SELECT MAX(date_key) FROM fact.transactions)
    
    UNION ALL
    
    SELECT 
        'fact.transactions',
        'negative_amounts',
        COUNT(*),
        CURRENT_DATE
    FROM fact.transactions
    WHERE amount < 0
      AND is_refund = FALSE
      AND date_key = (SELECT MAX(date_key) FROM fact.transactions)
    
    UNION ALL
    
    SELECT 
        'fact.transactions',
        'orphan_transactions',
        COUNT(*),
        CURRENT_DATE
    FROM fact.transactions t
    LEFT JOIN dim.customer c ON t.customer_key = c.customer_key
    WHERE c.customer_key IS NULL
      AND t.date_key = (SELECT MAX(date_key) FROM fact.transactions)
)
SELECT 
    table_name,
    check_name,
    failed_rows,
    check_date,
    CASE WHEN failed_rows > 0 THEN 'FAILED' ELSE 'PASSED' END as status
FROM data_quality_checks
ORDER BY failed_rows DESC;
```

---

## 7. Requêtes d'Audit et Conformité

### 7.1 RGPD - Data Subject Access Request

```sql
-- PostgreSQL: Extraction complète données d'un client (RGPD)
-- Customer Info
SELECT 
    'customer_profile' as data_type,
    json_build_object(
        'customer_id', customer_id,
        'name', name,
        'email', email,
        'phone', phone,
        'address', full_address,
        'created_at', created_at
    ) as data
FROM customer
WHERE customer_id = 'cus_abc123'

UNION ALL

-- Transactions
SELECT 
    'transactions' as data_type,
    json_agg(json_build_object(
        'transaction_id', transaction_id,
        'amount', amount,
        'currency', currency,
        'merchant', merchant_id,
        'created_at', created_at,
        'status', status
    )) as data
FROM transaction
WHERE customer_id = 'cus_abc123'

UNION ALL

-- Payment Methods (tokenized only)
SELECT 
    'payment_methods' as data_type,
    json_agg(json_build_object(
        'payment_method_id', payment_method_id,
        'card_last4', card_last4,
        'card_brand', card_brand,
        'created_at', created_at
    )) as data
FROM payment_methods
WHERE customer_id = 'cus_abc123';
```

---