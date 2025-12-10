#!/usr/bin/env bash
#
# test_security.sh
#
# Demonstrate Kafka security configuration concepts.
# Shows TLS/SSL, SASL authentication, and credential management patterns.
#
# Note: This is educational - actual security setup requires
#       proper certificates and broker configuration.
#
# Usage:
#   ./test_security.sh
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../../common" && pwd)"

# Source common utilities
source "$COMMON_DIR/env_loader.sh"
source "$COMMON_DIR/logging.sh"

log_section() {
  echo ""
  echo "════════════════════════════════════════════════════════════"
  echo "  $1"
  echo "════════════════════════════════════════════════════════════"
}

log_concept() {
  echo ""
  echo "────────────────────────────────────────────────────────────"
  echo "  $1"
  echo "────────────────────────────────────────────────────────────"
}

demonstrate_security_protocols() {
  log_concept "Kafka Security Protocols"
  
  cat << 'EOF'
Kafka Security Protocol Options:
══════════════════════════════════════════════════════════

1. PLAINTEXT (No Security)
───────────────────────────────────────────────────────
Configuration:
  security.protocol=PLAINTEXT
  bootstrap.servers=localhost:9092

Use Case: Local development only
Security: None - all data in clear text
⚠️ NEVER use in production!

2. SSL (TLS Encryption)
───────────────────────────────────────────────────────
Configuration:
  security.protocol=SSL
  bootstrap.servers=broker:9093
  ssl.truststore.location=/path/to/truststore.jks
  ssl.truststore.password=changeit

Use Case: Encryption without authentication
Security: Data encrypted in transit
Protects: Man-in-the-middle attacks, eavesdropping

3. SASL_PLAINTEXT (Authentication, No Encryption)
───────────────────────────────────────────────────────
Configuration:
  security.protocol=SASL_PLAINTEXT
  sasl.mechanism=PLAIN
  sasl.jaas.config=...username/password...

Use Case: Internal networks with authentication
Security: Username/password auth, but NO encryption
⚠️ Credentials sent in clear text!

4. SASL_SSL (Authentication + Encryption) ✓ RECOMMENDED
───────────────────────────────────────────────────────
Configuration:
  security.protocol=SASL_SSL
  sasl.mechanism=SCRAM-SHA-512
  sasl.jaas.config=...
  ssl.truststore.location=/path/to/truststore.jks

Use Case: Production deployments
Security: Both encryption AND authentication
Protects: All common attack vectors

Protocol Comparison:
───────────────────────────────────────────────────────
Protocol         Encryption  Auth    Use Case
─────────────────────────────────────────────────────
PLAINTEXT        ✗           ✗       Dev only
SSL              ✓           ✗       Basic encryption
SASL_PLAINTEXT   ✗           ✓       Internal auth
SASL_SSL         ✓           ✓       Production ✓
EOF
}

demonstrate_sasl_mechanisms() {
  log_concept "SASL Authentication Mechanisms"
  
  cat << 'EOF'
SASL Mechanism Options:
══════════════════════════════════════════════════════════

1. SASL/PLAIN
───────────────────────────────────────────────────────
Simplest username/password authentication

Configuration:
  sasl.mechanism=PLAIN
  sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="producer-user" \
    password="secret-password";

Pros:
  ✓ Simple to set up
  ✓ Easy to understand
  ✓ Works with most systems

Cons:
  ✗ Password stored in config (use with SSL!)
  ✗ No built-in password hashing
  ✗ Credential rotation requires restart

Use Case: Development, simple setups with SSL

2. SASL/SCRAM (Recommended for Production)
───────────────────────────────────────────────────────
Salted Challenge Response Authentication Mechanism

Configuration:
  sasl.mechanism=SCRAM-SHA-512
  sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="producer-user" \
    password="secret-password";

Pros:
  ✓ Password never sent over network
  ✓ Salted and hashed credentials
  ✓ More secure than PLAIN
  ✓ Built-in to Kafka

Cons:
  ✗ Slightly more complex setup
  ✗ Requires broker configuration

Use Case: Production (recommended)

Setup SCRAM users (one-time):
  kafka-configs.sh --bootstrap-server localhost:9092 \
    --alter --add-config 'SCRAM-SHA-512=[password=secret]' \
    --entity-type users --entity-name producer-user

3. SASL/GSSAPI (Kerberos)
───────────────────────────────────────────────────────
Enterprise SSO integration with Kerberos

Configuration:
  sasl.mechanism=GSSAPI
  sasl.kerberos.service.name=kafka
  sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required \
    useKeyTab=true \
    storeKey=true \
    keyTab="/path/to/keytab" \
    principal="kafka-producer@REALM";

Pros:
  ✓ Enterprise SSO integration
  ✓ Centralized user management
  ✓ Ticket-based auth (no passwords)

Cons:
  ✗ Complex setup and management
  ✗ Requires Kerberos infrastructure
  ✗ Troubleshooting can be difficult

Use Case: Large enterprises with existing Kerberos

4. SASL/OAUTHBEARER
───────────────────────────────────────────────────────
OAuth 2.0 token-based authentication

Configuration:
  sasl.mechanism=OAUTHBEARER
  sasl.login.callback.handler.class=org.example.OAuthAuthenticateLoginCallbackHandler
  sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;

Pros:
  ✓ Modern token-based auth
  ✓ Works with OAuth providers
  ✓ Short-lived tokens
  ✓ Good for cloud-native apps

Cons:
  ✗ Requires OAuth infrastructure
  ✗ Custom callback handler needed
  ✗ More moving parts

Use Case: Cloud-native, microservices

5. AWS MSK IAM (AWS-Specific)
───────────────────────────────────────────────────────
AWS IAM role-based authentication for MSK

Configuration:
  security.protocol=SASL_SSL
  sasl.mechanism=AWS_MSK_IAM
  sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
  sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler

Pros:
  ✓ No credential management (uses IAM roles)
  ✓ Fine-grained IAM policies
  ✓ Audit trail via CloudTrail
  ✓ Automatic credential rotation

Cons:
  ✗ AWS-specific (vendor lock-in)
  ✗ Requires AWS MSK library

Use Case: AWS MSK clusters (recommended on AWS)

Recommendation Matrix:
───────────────────────────────────────────────────────
Environment          Mechanism           Rationale
─────────────────────────────────────────────────────
Development          PLAIN + SSL         Simple
Production (general) SCRAM-SHA-512       Secure, standard
Enterprise          GSSAPI (Kerberos)   SSO integration
Cloud-native        OAUTHBEARER         Token-based
AWS MSK             AWS_MSK_IAM         IAM integration
EOF
}

demonstrate_credential_management() {
  log_concept "Credential Management Best Practices"
  
  cat << 'EOF'
Credential Management Patterns:
══════════════════════════════════════════════════════════

❌ ANTI-PATTERNS (Never Do This):
───────────────────────────────────────────────────────

1. Hardcoded in Code
  # DON'T!
  username = "admin"
  password = "secret123"

2. Plain Text Config Files
  # DON'T!
  cat > producer.properties << EOF
  sasl.jaas.config=... username="admin" password="secret" ...
  EOF

3. Committed to Git
  # DON'T!
  git add config/credentials.properties
  git commit -m "Added Kafka credentials"

4. Logged to Console
  # DON'T!
  echo "Connecting with username: $USERNAME password: $PASSWORD"

5. Shared Across Environments
  # DON'T!
  # Same credentials for dev, staging, and production

✓ BEST PRACTICES:
───────────────────────────────────────────────────────

1. Environment Variables
   ──────────────────────────────────────────────────
   export KAFKA_USERNAME="producer-user"
   export KAFKA_PASSWORD="$(get_from_secret_manager)"
   
   Use in config:
   sasl.jaas.config=... username="${KAFKA_USERNAME}" ...

2. Secret Managers
   ──────────────────────────────────────────────────
   AWS Secrets Manager:
     aws secretsmanager get-secret-value \
       --secret-id kafka/prod/credentials \
       --query SecretString --output text
   
   HashiCorp Vault:
     vault kv get -field=password secret/kafka/prod
   
   Kubernetes Secrets:
     kubectl get secret kafka-creds \
       -o jsonpath='{.data.password}' | base64 -d

3. IAM Roles (Cloud Providers)
   ──────────────────────────────────────────────────
   AWS MSK with IAM:
     # No credentials needed!
     # EC2 instance role provides authentication
     
   GCP with Workload Identity:
     # Service account provides authentication

4. Certificate-Based (mTLS)
   ──────────────────────────────────────────────────
   ssl.keystore.location=/path/to/keystore.jks
   ssl.keystore.password=${KEYSTORE_PASSWORD}
   ssl.key.password=${KEY_PASSWORD}
   
   Certificates stored securely, passwords from secrets

5. Credential Rotation Strategy
   ──────────────────────────────────────────────────
   Step 1: Generate new credentials
   Step 2: Add to secret manager (keep old)
   Step 3: Update applications (dual-credential period)
   Step 4: Verify all apps using new credentials
   Step 5: Remove old credentials
   Step 6: Monitor for auth failures

Rotation Schedule:
───────────────────────────────────────────────────────
Credential Type              Rotation Frequency
─────────────────────────────────────────────────────
Passwords (SASL/PLAIN)       Every 90 days
Passwords (SASL/SCRAM)       Every 90 days
Certificates (SSL)           Before expiration (annually)
API Tokens                   Every 30 days
Service Account Keys         Every 90-180 days
IAM Roles                    No rotation needed

Access Control:
───────────────────────────────────────────────────────
✓ Use least-privilege principle
✓ Separate credentials per environment
✓ Separate credentials per service/team
✓ Audit credential access
✓ Monitor for unauthorized use
✓ Revoke unused credentials
EOF
}

demonstrate_tls_configuration() {
  log_concept "TLS/SSL Configuration"
  
  cat << 'EOF'
TLS/SSL Configuration Guide:
══════════════════════════════════════════════════════════

Basic TLS (Server Authentication):
───────────────────────────────────────────────────────
Client verifies broker's certificate

Configuration:
  security.protocol=SSL
  ssl.truststore.location=/path/to/kafka.client.truststore.jks
  ssl.truststore.password=changeit
  ssl.endpoint.identification.algorithm=https

Components:
  Truststore: Contains CA certificates
  Purpose: Verify broker's identity
  Required: Yes for SSL

Mutual TLS (Client + Server Authentication):
───────────────────────────────────────────────────────
Both client and broker verify each other

Configuration:
  security.protocol=SSL
  
  # Truststore (verify broker)
  ssl.truststore.location=/path/to/truststore.jks
  ssl.truststore.password=changeit
  
  # Keystore (client certificate)
  ssl.keystore.location=/path/to/keystore.jks
  ssl.keystore.password=changeit
  ssl.key.password=changeit
  
  ssl.endpoint.identification.algorithm=https

Components:
  Truststore: CA certificates
  Keystore: Client's certificate + private key
  Purpose: Mutual authentication
  Required: When broker requires client certs

TLS Version and Ciphers:
───────────────────────────────────────────────────────
ssl.enabled.protocols=TLSv1.2,TLSv1.3
ssl.protocol=TLSv1.3

Recommended Ciphers:
  ssl.cipher.suites=
    TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
    TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
    TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256

⚠️ Disable weak protocols:
  ✗ SSLv3
  ✗ TLSv1.0
  ✗ TLSv1.1

Certificate Management:
───────────────────────────────────────────────────────
Creating Truststore:
  # Import CA certificate
  keytool -import \
    -trustcacerts \
    -alias ca-cert \
    -file ca-cert.pem \
    -keystore kafka.client.truststore.jks \
    -storepass changeit

Creating Keystore (for mTLS):
  # Import client certificate and key
  openssl pkcs12 -export \
    -in client-cert.pem \
    -inkey client-key.pem \
    -out client.p12 \
    -name client
  
  keytool -importkeystore \
    -srckeystore client.p12 \
    -srcstoretype PKCS12 \
    -destkeystore kafka.client.keystore.jks \
    -deststoretype JKS

Verify Certificate:
  keytool -list -v \
    -keystore kafka.client.truststore.jks \
    -storepass changeit

Test TLS Connection:
  openssl s_client -connect broker:9093 \
    -CAfile ca-cert.pem \
    -showcerts

Common TLS Errors:
───────────────────────────────────────────────────────
Error: "Certificate doesn't match hostname"
  Fix: Set ssl.endpoint.identification.algorithm=""
       (or fix hostname in certificate)

Error: "unable to find valid certification path"
  Fix: Import CA certificate into truststore

Error: "No appropriate protocol"
  Fix: Check ssl.enabled.protocols match broker

Error: "Certificate expired"
  Fix: Renew certificate and update keystores
EOF
}

demonstrate_acl_configuration() {
  log_concept "ACL (Access Control Lists)"
  
  cat << 'EOF'
Kafka ACL Configuration:
══════════════════════════════════════════════════════════

ACL Components:
───────────────────────────────────────────────────────
Principal: Who (user or service)
Resource:  What (topic, group, cluster)
Operation: Action (read, write, describe, etc.)
Host:      From where (IP address or *)

Common Producer ACLs:
───────────────────────────────────────────────────────

1. Write to Topic:
   kafka-acls.sh --bootstrap-server localhost:9092 \
     --add --allow-principal User:producer-service \
     --operation Write --topic user-events

2. Describe Topic (for metadata):
   kafka-acls.sh --bootstrap-server localhost:9092 \
     --add --allow-principal User:producer-service \
     --operation Describe --topic user-events

3. Create Topic (if auto-create enabled):
   kafka-acls.sh --bootstrap-server localhost:9092 \
     --add --allow-principal User:producer-service \
     --operation Create --cluster

4. Idempotent Producer (additional perms):
   kafka-acls.sh --bootstrap-server localhost:9092 \
     --add --allow-principal User:producer-service \
     --operation IdempotentWrite --cluster

Complete Producer ACL Set:
───────────────────────────────────────────────────────
# Grant Write permission on topic
kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:my-producer \
  --operation Write \
  --topic my-topic

# Grant Describe permission (for metadata)
kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:my-producer \
  --operation Describe \
  --topic my-topic

# Grant IdempotentWrite for idempotent producers
kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:my-producer \
  --operation IdempotentWrite \
  --cluster

List ACLs:
───────────────────────────────────────────────────────
# All ACLs
kafka-acls.sh --bootstrap-server localhost:9092 --list

# ACLs for specific principal
kafka-acls.sh --bootstrap-server localhost:9092 \
  --list --principal User:my-producer

# ACLs for specific topic
kafka-acls.sh --bootstrap-server localhost:9092 \
  --list --topic my-topic

Remove ACLs:
───────────────────────────────────────────────────────
kafka-acls.sh --bootstrap-server localhost:9092 \
  --remove --allow-principal User:my-producer \
  --operation Write --topic my-topic

Best Practices:
───────────────────────────────────────────────────────
✓ Use least-privilege (only permissions needed)
✓ Create service accounts per application
✓ Use specific topic names (not wildcard *)
✓ Document ACL assignments
✓ Review ACLs regularly
✓ Audit ACL changes
✓ Test ACLs in staging first

✗ Don't grant cluster-level Write
✗ Don't use single "admin" user for all services
✗ Don't grant wildcard topic access
✗ Don't forget IdempotentWrite for idempotent producers
EOF
}

main() {
  log_section "Kafka Security Configuration Guide"
  
  log_info "This is an educational guide to Kafka security"
  log_info "Actual implementation requires proper certificates and broker setup"
  
  # Demonstrate concepts
  demonstrate_security_protocols
  demonstrate_sasl_mechanisms
  demonstrate_credential_management
  demonstrate_tls_configuration
  demonstrate_acl_configuration
  
  log_section "Security Guide Complete"
  
  echo ""
  echo "Key Takeaways:"
  echo "  1. Always use SASL_SSL in production"
  echo "  2. Prefer SCRAM-SHA-512 for authentication"
  echo "  3. Never hardcode credentials"
  echo "  4. Use secret managers for credential storage"
  echo "  5. Implement regular credential rotation"
  echo "  6. Use ACLs with least-privilege principle"
  echo "  7. Test security configuration in staging"
  echo ""
  echo "Security Checklist:"
  echo "  □ Security protocol configured (SASL_SSL)"
  echo "  □ SASL mechanism selected (SCRAM recommended)"
  echo "  □ Credentials in secret manager"
  echo "  □ TLS certificates valid and not expiring soon"
  echo "  □ ACLs configured with least privilege"
  echo "  □ Credential rotation process documented"
  echo "  □ Security tested in staging"
  echo "  □ Monitoring configured for auth failures"
  echo ""
}

main "$@"
