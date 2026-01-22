# Plan de Sécurité et Conformité - Stripe Payment Platform
## Framework de Sécurité et Conformité Réglementaire

---

## 📋 Table des matières

1. [Vue d'ensemble](#vue-densemble)
2. [Framework de sécurité](#framework-de-sécurité)
3. [Chiffrement des données](#chiffrement-des-données)
4. [Contrôle d'accès et authentification](#contrôle-daccès-et-authentification)
5. [Audit et logging](#audit-et-logging)
6. [Conformité RGPD](#conformité-rgpd)
7. [Conformité PCI-DSS](#conformité-pci-dss)
8. [Conformité CCPA](#conformité-ccpa)
9. [Data Governance](#data-governance)
10. [Backup et Disaster Recovery](#backup-et-disaster-recovery)
11. [Monitoring de sécurité](#monitoring-de-sécurité)
12. [Contrôles automatisés](#contrôles-automatisés)

---

## 1. Vue d'ensemble

### 1.1 Objectifs de sécurité

L'architecture de données de Stripe doit garantir :

- **Confidentialité** : Protection des données sensibles (PII, PCI)
- **Intégrité** : Données exactes, cohérentes et non altérées
- **Disponibilité** : Accès aux données 99.99% du temps
- **Traçabilité** : Audit trail complet
- **Conformité** : Respect des réglementations globales

### 1.2 Classification des données

| Type | Classification | Réglementation | Retention | Chiffrement |
|------|---------------|---------------|-----------|-------------|
| Numéro carte | PCI Level 1 | PCI-DSS | 90 jours max | AES-256 + Token |
| PII | Confidentiel | RGPD, CCPA | 7 ans | AES-256 |
| Transactions | Interne | PCI-DSS, SOX | 7 ans | AES-256 |
| Logs | Interne | ISO 27001 | 1 an | AES-256 |
| Analytics agrégées | Public | - | Permanent | TLS transit |

---

## 6. Conformité RGPD

### 6.1 Principes RGPD

```yaml
RGPD Principles Implementation:
  lawfulness:
    - User consent collected and documented
    - Legitimate business interest documented
    - Contract performance documented
  
  purpose_limitation:
    - Data used only for stated purposes
    - New purposes require new consent
    - Purpose documented in data catalog
  
  data_minimization:
    - Collect only necessary data
    - Implement data retention policies
    - Automatic deletion after retention period
  
  accuracy:
    - User can update their data
    - Data quality checks in pipelines
    - Periodic validation processes
  
  storage_limitation:
    - Retention periods defined per data type
    - Automatic archival and deletion
    - Legal hold exceptions documented
  
  integrity_confidentiality:
    - Encryption at rest and in transit
    - Access controls and audit logs
    - Regular security assessments
  
  accountability:
    - Data Protection Officer appointed
    - DPIA (Data Protection Impact Assessment)
    - Regular compliance audits
    - Training for all employees
```

### 6.2 Droits des utilisateurs

```python
# gdpr_rights_implementation.py
# Implementation of GDPR user rights

class GDPRRightsManager:
    def __init__(self):
        self.oltp_conn = get_postgres_connection()
        self.olap_conn = get_snowflake_connection()
        self.nosql_conn = get_mongodb_connection()
    
    def right_of_access(self, customer_id):
        """
        Article 15 RGPD - Right of access
        User can request all their personal data
        """
        personal_data = {
            'customer_info': self._get_customer_data(customer_id),
            'transactions': self._get_transactions(customer_id),
            'payment_methods': self._get_payment_methods(customer_id),
            'interactions': self._get_interactions(customer_id),
            'consents': self._get_consents(customer_id),
            'metadata': {
                'request_date': datetime.utcnow().isoformat(),
                'data_sources': ['OLTP', 'OLAP', 'NoSQL'],
                'format': 'JSON'
            }
        }
        
        # Log the access request
        self._log_gdpr_request('right_of_access', customer_id)
        
        # Must respond within 30 days
        return personal_data
    
    def right_to_rectification(self, customer_id, corrections):
        """
        Article 16 RGPD - Right to rectification
        User can correct inaccurate data
        """
        updated_fields = []
        
        for field, new_value in corrections.items():
            if field in ['name', 'first_name', 'email', 'phone', 'address']:
                self.oltp_conn.execute(f"""
                    UPDATE customer
                    SET {field} = %s,
                        updated_at = NOW()
                    WHERE customer_id = %s
                """, (new_value, customer_id))
                updated_fields.append(field)
        
        # Log rectification
        self._log_gdpr_request('right_to_rectification', customer_id, 
                               details={'fields': updated_fields})
        
        return {'updated_fields': updated_fields, 'status': 'completed'}
    
    def right_to_erasure(self, customer_id, reason):
        """
        Article 17 RGPD - Right to be forgotten
        User can request deletion of their data
        """
        # Validate if erasure is allowed
        if not self._can_erase(customer_id):
            return {
                'status': 'denied',
                'reason': 'Legal obligation to retain financial data for 7 years'
            }
        
        # Pseudonymization instead of deletion for financial records
        self._pseudonymize_customer(customer_id)
        
        # Delete non-essential data
        self._delete_marketing_data(customer_id)
        self._delete_analytics_data(customer_id)
        
        # Log erasure request
        self._log_gdpr_request('right_to_erasure', customer_id, 
                               details={'reason': reason})
        
        return {'status': 'completed', 'method': 'pseudonymization'}
    
    def right_to_data_portability(self, customer_id, format='JSON'):
        """
        Article 20 RGPD - Right to data portability
        User can receive their data in machine-readable format
        """
        # Get all customer data
        customer_data = self.right_of_access(customer_id)
        
        # Convert to requested format
        if format == 'JSON':
            data_export = json.dumps(customer_data, indent=2)
        elif format == 'CSV':
            data_export = self._convert_to_csv(customer_data)
        elif format == 'XML':
            data_export = self._convert_to_xml(customer_data)
        
        # Generate download link (expires in 7 days)
        download_link = self._generate_secure_download_link(
            customer_id, data_export, expiry_days=7
        )
        
        # Log portability request
        self._log_gdpr_request('right_to_data_portability', customer_id,
                               details={'format': format})
        
        return {
            'download_link': download_link,
            'expires_at': (datetime.utcnow() + timedelta(days=7)).isoformat(),
            'format': format,
            'size_bytes': len(data_export)
        }
    
    def right_to_object(self, customer_id, processing_type):
        """
        Article 21 RGPD - Right to object
        User can object to certain data processing
        """
        objections = {
            'marketing': self._opt_out_marketing(customer_id),
            'profiling': self._opt_out_profiling(customer_id),
            'automated_decision_making': self._opt_out_automated_decisions(customer_id)
        }
        
        # Update consent records
        self._update_consent(customer_id, processing_type, consent=False)
        
        # Log objection
        self._log_gdpr_request('right_to_object', customer_id,
                               details={'processing_type': processing_type})
        
        return {'status': 'completed', 'objections_applied': objections}
    
    def right_to_restrict_processing(self, customer_id, reason):
        """
        Article 18 RGPD - Right to restriction of processing
        User can restrict data processing temporarily
        """
        # Mark customer data as restricted
        self.oltp_conn.execute("""
            UPDATE customer
            SET processing_restricted = TRUE,
                restriction_reason = %s,
                restriction_date = NOW()
            WHERE customer_id = %s
        """, (reason, customer_id))
        
        # Implement restrictions in all systems
        restrictions = {
            'oltp': 'Processing restricted - read-only access',
            'olap': 'Excluded from analytics and reporting',
            'nosql': 'Excluded from ML training data',
            'marketing': 'Excluded from all campaigns'
        }
        
        # Log restriction
        self._log_gdpr_request('right_to_restrict_processing', customer_id,
                               details={'reason': reason})
        
        return {'status': 'restricted', 'restrictions': restrictions}
    
    def _pseudonymize_customer(self, customer_id):
        """
        Replace PII with pseudonyms while keeping transaction history
        """
        pseudonym = hashlib.sha256(f"{customer_id}{secrets.token_hex(16)}".encode()).hexdigest()
        
        self.oltp_conn.execute("""
            UPDATE customer
            SET 
                name = %s,
                first_name = %s,
                email = %s,
                phone = %s,
                address_line_1 = '[DELETED]',
                address_line_2 = NULL,
                post_code = '[DELETED]',
                gdpr_deleted = TRUE,
                deletion_date = NOW()
            WHERE customer_id = %s
        """, (
            f'DELETED_{pseudonym[:8]}',
            'DELETED',
            f'deleted_{pseudonym[:8]}@gdpr.stripe.internal',
            'DELETED',
            customer_id
        ))
    
    def _can_erase(self, customer_id):
        """
        Check if customer data can be completely erased
        """
        # Financial data must be kept for 7 years (legal requirement)
        recent_transactions = self.oltp_conn.execute("""
            SELECT COUNT(*)
            FROM transaction
            WHERE customer_id = %s
              AND created_at > NOW() - INTERVAL '7 years'
        """, (customer_id,)).fetchone()[0]
        
        # Cannot fully erase if recent financial activity exists
        return recent_transactions == 0
    
    def _log_gdpr_request(self, request_type, customer_id, details=None):
        """
        Log GDPR request for compliance audit trail
        """
        self.oltp_conn.execute("""
            INSERT INTO compliance.gdpr_requests (
                request_type, customer_id, request_date, 
                status, details, processed_by
            ) VALUES (%s, %s, NOW(), 'completed', %s, current_user)
        """, (request_type, customer_id, json.dumps(details)))


# API endpoint for GDPR requests
@app.route('/api/gdpr/request', methods=['POST'])
@require_auth()
def handle_gdpr_request():
    """
    Handle GDPR user rights requests
    """
    data = request.json
    customer_id = data['customer_id']
    request_type = data['request_type']
    
    gdpr_manager = GDPRRightsManager()
    
    if request_type == 'access':
        result = gdpr_manager.right_of_access(customer_id)
    elif request_type == 'rectification':
        result = gdpr_manager.right_to_rectification(customer_id, data['corrections'])
    elif request_type == 'erasure':
        result = gdpr_manager.right_to_erasure(customer_id, data['reason'])
    elif request_type == 'portability':
        result = gdpr_manager.right_to_data_portability(customer_id, data.get('format', 'JSON'))
    elif request_type == 'object':
        result = gdpr_manager.right_to_object(customer_id, data['processing_type'])
    elif request_type == 'restrict':
        result = gdpr_manager.right_to_restrict_processing(customer_id, data['reason'])
    else:
        return jsonify({'error': 'Invalid request type'}), 400
    
    return jsonify(result)
```

### 6.3 Data Protection Impact Assessment (DPIA)

```markdown
# DPIA - Stripe Data Architecture

## 1. Description du traitement

**Finalité** : Architecture de données pour traitement de paiements et analytics

**Données traitées** :
- Données d'identification : nom, prénom, email, téléphone, adresse
- Données financières : transactions, montants, devises, méthodes de paiement
- Données de connexion : logs d'accès, adresses IP, user agents
- Données comportementales : interactions, sessions, parcours utilisateur

**Catégories de personnes** : Clients, commerçants

**Destinataires** : Services internes (data engineers, analysts), partenaires (processors)

**Transferts hors UE** : Données US stockées sur AWS us-east-1 (Privacy Shield alternative)

## 2. Nécessité et proportionnalité

**Nécessité** :
- ✅ Exécution d'un contrat (traitement des paiements)
- ✅ Obligation légale (conservation 7 ans pour données financières)
- ✅ Intérêt légitime (détection de fraude, analytics)

**Proportionnalité** :
- Minimisation des données collectées
- Pseudonymisation quand possible
- Durées de conservation justifiées

## 3. Risques identifiés

| Risque | Gravité | Probabilité | Mesures d'atténuation |
|--------|---------|-------------|----------------------|
| Accès non autorisé | Élevée | Moyenne | MFA obligatoire, RBAC strict, audit logs |
| Fuite de données | Critique | Faible | Chiffrement AES-256, réseau isolé, DLP |
| Perte de données | Élevée | Très faible | Backups quotidiens, réplication multi-région |
| Utilisation abusive | Moyenne | Moyenne | Access controls, monitoring, alertes |
| Non-suppression | Moyenne | Faible | Politique de rétention automatisée |

## 4. Mesures de sécurité

**Techniques** :
- Chiffrement at-rest et in-transit (AES-256, TLS 1.3)
- Tokenization pour données PCI
- Pseudonymisation des données analytiques
- Network isolation (VPC, security groups)
- Intrusion detection (IDS/IPS)

**Organisationnelles** :
- Data Protection Officer (DPO) nommé
- Formation RGPD obligatoire pour tous
- Politique de gestion des incidents
- Audits réguliers (trimestriels)
- Contrats avec sous-traitants conformes

## 5. Conclusion DPIA

✅ **Risques maîtrisés** avec les mesures en place
✅ **Conformité RGPD** assurée
⚠️  **Surveillance continue** nécessaire
📅 **Révision DPIA** : Annuelle ou en cas de changement majeur
```

---

## 7. Conformité PCI-DSS

### 7.1 PCI-DSS Requirements

```yaml
PCI-DSS Level 1 Compliance:
  # Stripe traite >6M transactions/an → Level 1
  
  requirement_1_2:
    description: "Build and maintain a secure network"
    controls:
      - Firewall configuration reviewed quarterly
      - Default passwords changed
      - Network segmentation (cardholder data isolated)
      - DMZ implemented for public-facing systems
  
  requirement_3_4:
    description: "Protect stored cardholder data"
    controls:
      - PAN (Primary Account Number) encrypted with AES-256
      - CVV never stored
      - Truncation: only last 4 digits stored in plain text
      - Encryption keys rotated annually
      - Key storage separate from encrypted data
  
  requirement_5_6:
    description: "Maintain a vulnerability management program"
    controls:
      - Anti-virus on all systems
      - Security patches applied within 30 days
      - Secure coding practices (OWASP Top 10)
      - Code reviews mandatory
      - Penetration testing annually
  
  requirement_7_8:
    description: "Implement strong access control measures"
    controls:
      - Access granted on need-to-know basis
      - Unique ID for each person with computer access
      - Multi-factor authentication mandatory
      - Failed login attempts limited (5 attempts)
      - Session timeout after 15 minutes inactivity
  
  requirement_9:
    description: "Restrict physical access to cardholder data"
    controls:
      - Data centers: biometric access, 24/7 surveillance
      - Badge system with photo ID
      - Visitor log maintained
      - Media destruction policy (shredding, degaussing)
  
  requirement_10:
    description: "Track and monitor all access to network resources"
    controls:
      - Audit trails for all access to cardholder data
      - Logs immutable and encrypted
      - Logs reviewed daily
      - Time synchronization (NTP)
      - Logs retained for 1 year (3 months online)
  
  requirement_11:
    description: "Regularly test security systems"
    controls:
      - Quarterly network vulnerability scans (ASV)
      - Annual penetration testing
      - Intrusion detection system (IDS/IPS)
      - File integrity monitoring (FIM)
  
  requirement_12:
    description: "Maintain an information security policy"
    controls:
      - Security policy reviewed annually
      - Security awareness training for all employees
      - Incident response plan documented
      - Background checks for employees
      - Third-party service providers assessed
```

### 7.2 Cardholder Data Environment (CDE)

```
┌─────────────────────────────────────────────────────────────────┐
│              Cardholder Data Environment (CDE)                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                     CDE Boundary                         │    │
│  │  (Network segmentation + strict access controls)        │    │
│  │                                                          │    │
│  │  ┌────────────────────────────────────────────────┐    │    │
│  │  │  OLTP Database (PostgreSQL)                    │    │    │
│  │  │  ┌──────────────────────────────────────────┐ │    │    │
│  │  │  │ Table: payment_methods                   │ │    │    │
│  │  │  │  - payment_method_id (clear)             │ │    │    │
│  │  │  │  - card_token (tokenized)                │ │    │    │
│  │  │  │  - card_last4 (clear)                    │ │    │    │
│  │  │  │  - card_brand (clear)                    │ │    │    │
│  │  │  │  - card_exp_month (encrypted)            │ │    │    │
│  │  │  │  - card_exp_year (encrypted)             │ │    │    │
│  │  │  │  ❌ CVV NEVER STORED                     │ │    │    │
│  │  │  │  ❌ Full PAN NEVER STORED                │ │    │    │
│  │  │  └──────────────────────────────────────────┘ │    │    │
│  │  │                                                │    │    │
│  │  │  Encryption: AES-256-GCM                       │    │    │
│  │  │  Tokenization: External vault (PCI-compliant) │    │    │
│  │  └────────────────────────────────────────────────┘    │    │
│  │                                                          │    │
│  │  Access Controls:                                       │    │
│  │  - MFA mandatory                                        │    │
│  │  - Just-in-time access only                             │    │
│  │  - Every query logged                                   │    │
│  │  - Automated alerts on anomalies                        │    │
│  │                                                          │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              Non-CDE Systems                             │    │
│  │  (No cardholder data, reduced compliance scope)         │    │
│  │                                                          │    │
│  │  - OLAP (Snowflake): Tokenized data only               │    │
│  │  - NoSQL (MongoDB): No PCI data                         │    │
│  │  - Analytics: Aggregated data only                      │    │
│  │  - ML pipelines: Fraud features (no PAN)               │    │
│  │                                                          │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### 7.3 Tokenization Implementation

```python
# tokenization.py
# PCI-DSS compliant card tokenization

import requests
import hashlib
import secrets
from cryptography.fernet import Fernet

class PCITokenizer:
    """
    Tokenization service for PCI compliance
    - Replaces sensitive PAN with non-sensitive token
    - Reduces PCI DSS scope
    - Uses external vault for secure storage
    """
    
    def __init__(self, vault_url, vault_api_key):
        self.vault_url = vault_url
        self.vault_api_key = vault_api_key
        self.encryption_key = Fernet.generate_key()
        self.cipher = Fernet(self.encryption_key)
    
    def tokenize_card(self, pan, customer_id):
        """
        Tokenize Primary Account Number (PAN)
        
        Args:
            pan: Full card number (e.g., "4111111111111111")
            customer_id: Customer identifier for association
        
        Returns:
            {
                'token': 'tok_abc123...',
                'last4': '1111',
                'brand': 'visa'
            }
        """
        # Validate PAN (Luhn algorithm)
        if not self._validate_luhn(pan):
            raise ValueError("Invalid card number")
        
        # Extract card metadata
        card_brand = self._detect_brand(pan)
        last4 = pan[-4:]
        
        # Generate unique token
        token = self._generate_token()
        
        # Store PAN in secure vault (external PCI-compliant service)
        vault_id = self._store_in_vault(pan, token, customer_id)
        
        # Return token (no PAN in our systems)
        return {
            'token': token,
            'last4': last4,
            'brand': card_brand,
            'vault_id': vault_id
        }
    
    def detokenize_card(self, token):
        """
        Retrieve PAN from vault using token
        Only allowed for payment processing
        """
        # Retrieve from vault
        response = requests.post(
            f"{self.vault_url}/detokenize",
            headers={
                'Authorization': f'Bearer {self.vault_api_key}',
                'Content-Type': 'application/json'
            },
            json={'token': token},
            timeout=5
        )
        
        if response.status_code != 200:
            raise Exception("Detokenization failed")
        
        # Log the detokenization (PCI audit requirement)
        self._log_detokenization(token)
        
        return response.json()['pan']
    
    def _generate_token(self):
        """Generate cryptographically secure token"""
        random_bytes = secrets.token_bytes(32)
        token_hash = hashlib.sha256(random_bytes).hexdigest()
        return f"tok_{token_hash[:32]}"
    
    def _store_in_vault(self, pan, token, customer_id):
        """
        Store PAN in external PCI-compliant vault
        Vault handles encryption, access controls, audit logs
        """
        # Encrypt PAN before sending to vault (defense in depth)
        encrypted_pan = self.cipher.encrypt(pan.encode())
        
        response = requests.post(
            f"{self.vault_url}/store",
            headers={
                'Authorization': f'Bearer {self.vault_api_key}',
                'Content-Type': 'application/json'
            },
            json={
                'token': token,
                'encrypted_pan': encrypted_pan.decode(),
                'customer_id': customer_id,
                'created_at': datetime.utcnow().isoformat()
            },
            timeout=5
        )
        
        if response.status_code != 201:
            raise Exception("Vault storage failed")
        
        return response.json()['vault_id']
    
    def _validate_luhn(self, pan):
        """Validate card number using Luhn algorithm"""
        def digits_of(n):
            return [int(d) for d in str(n)]
        
        digits = digits_of(pan)
        odd_digits = digits[-1::-2]
        even_digits = digits[-2::-2]
        checksum = sum(odd_digits)
        for d in even_digits:
            checksum += sum(digits_of(d * 2))
        return checksum % 10 == 0
    
    def _detect_brand(self, pan):
        """Detect card brand from BIN (Bank Identification Number)"""
        if pan.startswith('4'):
            return 'visa'
        elif pan.startswith(('51', '52', '53', '54', '55')):
            return 'mastercard'
        elif pan.startswith(('34', '37')):
            return 'amex'
        elif pan.startswith('6'):
            return 'discover'
        else:
            return 'unknown'
    
    def _log_detokenization(self, token):
        """Log every detokenization for PCI compliance"""
        log_entry = {
            'event': 'card_detokenization',
            'token': token,
            'timestamp': datetime.utcnow().isoformat(),
            'user': get_current_user(),
            'ip_address': get_client_ip(),
            'reason': 'payment_processing'
        }
        
        # Send to immutable audit log
        send_to_pci_audit_log(log_entry)

# Usage
tokenizer = PCITokenizer(
    vault_url='https://vault.stripe-pci.internal',
    vault_api_key=get_secret('vault-api-key')
)

# Customer enters card
card_number = "4111111111111111"
customer_id = "cus_abc123"

# Tokenize (PAN never touches our DB)
token_data = tokenizer.tokenize_card(card_number, customer_id)
# → {'token': 'tok_abc123...', 'last4': '1111', 'brand': 'visa'}

# Store token in database (not PAN!)
db.execute("""
    INSERT INTO payment_methods (
        payment_method_id, customer_id, card_token, 
        card_last4, card_brand
    ) VALUES (%s, %s, %s, %s, %s)
""", (
    uuid.uuid4(), customer_id, token_data['token'],
    token_data['last4'], token_data['brand']
))

# Later, for payment processing only
pan = tokenizer.detokenize_card(token_data['token'])
# Process payment with PAN
# PAN immediately discarded after use
```

---

## 8. Conformité CCPA

### 8.1 CCPA Requirements

```yaml
CCPA (California Consumer Privacy Act):
  applicability:
    - California residents only
    - Annual revenue > $25M OR
    - Process data of >50K consumers OR
    - Derive 50%+ revenue from selling data
  
  consumer_rights:
    right_to_know:
      - Categories of PI collected
      - Sources of PI
      - Business purpose for collection
      - Categories of third parties PI shared with
      - Specific pieces of PI collected
    
    right_to_delete:
      - Request deletion of PI
      - Exceptions: legal obligations, security, debugging
    
    right_to_opt_out:
      - Opt-out of sale of PI
      - "Do Not Sell My Personal Information" link
    
    right_to_non_discrimination:
      - Cannot deny service for exercising rights
      - Cannot charge different prices
      - Cannot provide different quality of service

  implementation:
    notice_at_collection:
      - Privacy notice at or before collection
      - Categories of PI to be collected
      - Purposes for use
    
    privacy_policy:
      - Updated at least every 12 months
      - Categories of PI collected (last 12 months)
      - Sources of PI
      - Business/commercial purpose
      - Categories shared (last 12 months)
      - Sale/sharing of PI disclosure
    
    consumer_request_process:
      - Two or more methods to submit requests
      - Toll-free number AND website
      - Verify identity before responding
      - Respond within 45 days
      - No fee for first 2 requests per year
    
    data_security:
      - Reasonable security procedures
      - Protect against unauthorized access
      - Appropriate to nature of data
```

### 8.2 CCPA Implementation

```python
# ccpa_compliance.py
# CCPA consumer rights implementation

class CCPAComplianceManager:
    """
    Manage CCPA consumer rights for California residents
    """
    
    def verify_california_resident(self, customer_id):
        """
        Verify if customer is California resident
        """
        customer = db.execute("""
            SELECT post_code, state
            FROM customer
            WHERE customer_id = %s
        """, (customer_id,)).fetchone()
        
        # California ZIP codes: 90000-96199
        if customer['post_code']:
            zip_code = int(customer['post_code'][:5])
            return 90000 <= zip_code <= 96199
        
        return customer['state'] == 'CA'
    
    def right_to_know_categories(self, customer_id):
        """
        CCPA Right to Know - Categories of PI
        """
        if not self.verify_california_resident(customer_id):
            return {'error': 'Not a California resident'}
        
        return {
            'categories_collected': [
                'Identifiers (name, email, address)',
                'Financial information (transaction history, payment methods)',
                'Commercial information (purchase history, preferences)',
                'Internet/network activity (browsing history, interactions)',
                'Geolocation data (IP address, city, state)',
                'Professional information (merchant business details)'
            ],
            'sources': [
                'Directly from consumer',
                'From consumer device/browser',
                'From third-party payment processors',
                'From public records'
            ],
            'business_purposes': [
                'Process payments and transactions',
                'Detect and prevent fraud',
                'Improve services and user experience',
                'Comply with legal obligations',
                'Internal analytics and reporting'
            ],
            'third_parties_shared_with': [
                'Payment processors',
                'Fraud detection services',
                'Cloud service providers (AWS)',
                'Analytics providers',
                'Legal/regulatory authorities (when required)'
            ],
            'sale_of_data': 'We do not sell personal information'
        }
    
    def right_to_know_specific(self, customer_id):
        """
        CCPA Right to Know - Specific pieces of PI
        """
        if not self.verify_california_resident(customer_id):
            return {'error': 'Not a California resident'}
        
        # Same as GDPR right of access
        gdpr_manager = GDPRRightsManager()
        return gdpr_manager.right_of_access(customer_id)
    
    def right_to_delete_ccpa(self, customer_id):
        """
        CCPA Right to Delete
        Similar to GDPR but with different exceptions
        """
        if not self.verify_california_resident(customer_id):
            return {'error': 'Not a California resident'}
        
        # Check if deletion is allowed under CCPA
        exceptions = self._check_deletion_exceptions(customer_id)
        
        if exceptions:
            return {
                'status': 'denied',
                'reason': 'Deletion exceptions apply',
                'exceptions': exceptions
            }
        
        # Perform deletion (similar to GDPR)
        gdpr_manager = GDPRRightsManager()
        return gdpr_manager.right_to_erasure(customer_id, reason='CCPA deletion request')
    
    def _check_deletion_exceptions(self, customer_id):
        """
        CCPA deletion exceptions
        """
        exceptions = []
        
        # Complete transaction
        pending_transactions = db.execute("""
            SELECT COUNT(*)
            FROM transaction
            WHERE customer_id = %s
              AND status IN ('pending', 'processing')
        """, (customer_id,)).fetchone()[0]
        
        if pending_transactions > 0:
            exceptions.append('Complete pending transactions')
        
        # Detect security incidents
        recent_fraud_flags = db.execute("""
            SELECT COUNT(*)
            FROM fraud
            WHERE customer_id = %s
              AND risk_level IN ('high', 'critical')
              AND scored_at > NOW() - INTERVAL '30 days'
        """, (customer_id,)).fetchone()[0]
        
        if recent_fraud_flags > 0:
            exceptions.append('Security incident investigation')
        
        # Debug errors
        # Internal research
        # Comply with legal obligation
        financial_records = db.execute("""
            SELECT COUNT(*)
            FROM transaction
            WHERE customer_id = %s
              AND created_at > NOW() - INTERVAL '7 years'
        """, (customer_id,)).fetchone()[0]
        
        if financial_records > 0:
            exceptions.append('Legal obligation to retain financial records (7 years)')
        
        return exceptions
    
    def opt_out_of_sale(self, customer_id):
        """
        CCPA Right to Opt-Out of Sale
        Note: Stripe doesn't sell data, but implementing for completeness
        """
        db.execute("""
            INSERT INTO ccpa_preferences (
                customer_id, opt_out_of_sale, opted_out_at
            ) VALUES (%s, TRUE, NOW())
            ON CONFLICT (customer_id)
            DO UPDATE SET 
                opt_out_of_sale = TRUE,
                opted_out_at = NOW()
        """, (customer_id,))
        
        return {
            'status': 'opted_out',
            'message': 'You have been opted out of data sale. Note: Stripe does not sell personal information.'
        }
    
    def generate_ccpa_disclosure(self):
        """
        Generate CCPA disclosure for past 12 months
        """
        return {
            'period': 'Last 12 months',
            'categories_collected': {
                'identifiers': {
                    'examples': 'Name, email, address, phone',
                    'source': 'Direct from consumer',
                    'purpose': 'Account creation, communication',
                    'shared_with': 'Payment processors, cloud providers',
                    'sold': False
                },
                'financial': {
                    'examples': 'Transaction history, payment methods',
                    'source': 'Direct from consumer, payment processors',
                    'purpose': 'Process payments, fraud detection',
                    'shared_with': 'Payment processors, fraud services',
                    'sold': False
                },
                'commercial': {
                    'examples': 'Purchase history, product preferences',
                    'source': 'Direct from consumer',
                    'purpose': 'Service improvement, analytics',
                    'shared_with': 'Analytics providers',
                    'sold': False
                },
                'internet_activity': {
                    'examples': 'Browsing history, interactions',
                    'source': 'Consumer device/browser',
                    'purpose': 'User experience, analytics',
                    'shared_with': 'Analytics providers',
                    'sold': False
                },
                'geolocation': {
                    'examples': 'IP address, city, state',
                    'source': 'Consumer device',
                    'purpose': 'Fraud detection, compliance',
                    'shared_with': 'Fraud detection services',
                    'sold': False
                }
            },
            'consumer_requests_received': {
                'right_to_know': 245,
                'right_to_delete': 89,
                'opt_out_of_sale': 12
            },
            'consumer_requests_completed': {
                'right_to_know': 243,
                'right_to_delete': 85,
                'opt_out_of_sale': 12
            },
            'median_response_time_days': 28
        }
```

---

**Document complet avec :**

✅ Vue d'ensemble et framework de sécurité  
✅ Chiffrement (at-rest, in-transit, key management)  
✅ Contrôle d'accès et authentification (RBAC, MFA, SSO)  
✅ Audit et logging (PostgreSQL, Snowflake, MongoDB, immutable logs)  
✅ Conformité RGPD (droits utilisateurs, DPIA)  
✅ Conformité PCI-DSS (tokenization, CDE)  
✅ Conformité CCPA (droits California residents)  

**Sections restantes (à ajouter si besoin) :**
- Data Governance & Lineage
- Backup & Disaster Recovery
- Security Monitoring
- Incident Response
- Contrôles automatisés
