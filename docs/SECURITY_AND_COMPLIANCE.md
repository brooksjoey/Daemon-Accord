# Security and Compliance

Daemon Accord is a **policy-driven automation platform** designed for compliance-safe web automation and data collection.

---

## Overview

Daemon Accord provides comprehensive policy controls and audit logging to ensure all automation activities are:

- **Compliant** with legal and ethical guidelines
- **Traceable** through complete audit logs
- **Controlled** through domain-level policies
- **Authorized** based on customer authorization levels

This document describes the compliance and security features that make Daemon Accord suitable for enterprise deployment.

---

## Policy Controls

### Domain Allowlist/Denylist

Control which domains can be accessed through the platform.

**Allowlist Mode:**
- Only explicitly allowed domains can be accessed
- All other domains are automatically denied
- Use for strict compliance requirements

**Denylist Mode:**
- Explicitly blocked domains are denied
- All other domains are allowed
- Use for blocking known problematic domains

**Configuration:**
```python
# Example: Deny access to a specific domain
domain_policy = DomainPolicy(
    domain="blocked-site.com",
    allowed=False,
    denied=True,
)
```

### Rate Limiting

Prevent excessive requests to target domains.

**Per-Domain Rate Limits:**
- `rate_limit_per_minute`: Maximum requests per minute
- `rate_limit_per_hour`: Maximum requests per hour

**Enforcement:**
- Rate limits are enforced at job submission time
- Jobs exceeding rate limits are rejected with `403 Forbidden`
- Rate limit counters are stored in Redis with automatic expiration

**Example:**
```python
domain_policy = DomainPolicy(
    domain="example.com",
    rate_limit_per_minute=10,
    rate_limit_per_hour=100,
)
```

### Concurrency Limits

Control how many simultaneous jobs can run for a specific domain.

**Purpose:**
- Prevent resource exhaustion
- Ensure fair usage across domains
- Maintain system stability

**Configuration:**
```python
domain_policy = DomainPolicy(
    domain="example.com",
    max_concurrent_jobs=5,  # Maximum 5 concurrent jobs
)
```

### Strategy Restrictions

Control which execution strategies can be used for specific domains.

**Execution Strategies:**
- `vanilla`: Standard browser automation (lowest risk)
- `stealth`: Enhanced evasion techniques (moderate risk)
- `assault`: Maximum evasion capabilities (highest risk, enterprise only)

**Domain-Level Restrictions:**
```python
domain_policy = DomainPolicy(
    domain="example.com",
    allowed_strategies="vanilla,stealth",  # Assault not allowed
)
```

---

## Authorization Modes

### Public Mode

**Use Case:** Public pages, no special authorization required

**Restrictions:**
- Only `vanilla` strategy allowed
- Subject to all domain policies
- Full audit logging

**Example:**
```bash
curl -X POST "http://localhost:8080/api/v1/jobs?domain=example.com&url=https://example.com&authorization_mode=public" \
  -H "Content-Type: application/json" \
  -d '{"selector": "h1"}'
```

### Customer-Authorized Mode

**Use Case:** Customer has explicit authorization from target domain

**Restrictions:**
- `vanilla` and `stealth` strategies allowed
- `assault` strategy requires enterprise tier
- Subject to all domain policies
- Full audit logging

**Example:**
```bash
curl -X POST "http://localhost:8080/api/v1/jobs?domain=example.com&url=https://example.com&authorization_mode=customer-authorized&strategy=stealth" \
  -H "Content-Type: application/json" \
  -d '{"selector": "h1"}'
```

### Internal Mode (Enterprise)

**Use Case:** Internal use, enterprise customers with full access

**Restrictions:**
- All strategies allowed (`vanilla`, `stealth`, `assault`, `custom`)
- Subject to all domain policies
- Full audit logging
- Requires enterprise tier access

**Example:**
```bash
curl -X POST "http://localhost:8080/api/v1/jobs?domain=example.com&url=https://example.com&authorization_mode=internal&strategy=assault" \
  -H "Content-Type: application/json" \
  -d '{"selector": "h1"}'
```

---

## Policy Enforcement

### At Job Submission

**When:** Before job is created and queued

**Checks:**
1. Domain allowlist/denylist
2. Rate limits
3. Concurrency limits
4. Strategy restrictions based on authorization mode
5. Domain-level strategy restrictions

**Result:**
- **Allowed:** Job is created and queued
- **Denied:** Job is rejected with `403 Forbidden` and reason

**Example Response (Denied):**
```json
{
  "detail": "Policy violation: Domain example.com is on denylist"
}
```

### At Execution Time

**When:** Before job execution begins

**Checks:**
1. Re-validate all policies (policies may have changed)
2. Final authorization check
3. Hard stop if policy violation detected

**Result:**
- **Allowed:** Job proceeds with execution
- **Denied:** Job is immediately cancelled, status set to `cancelled`

**Purpose:**
- Ensures policies are enforced even if they change between submission and execution
- Provides defense-in-depth security

---

## Audit Logging

### What is Logged

Every policy decision and enforcement action is logged with:

- **Job Information:**
  - Job ID
  - Domain
  - URL
  - Strategy requested
  - Authorization mode

- **Policy Decision:**
  - Action taken (`allow`, `deny`, `rate_limit`, etc.)
  - Whether job was allowed
  - Reason for decision
  - Policy ID (if applicable)

- **Enforcement Details:**
  - Rate limit applied
  - Concurrency limit applied
  - Strategy restricted

- **Request Context:**
  - User/API key ID
  - IP address
  - Timestamp
  - Additional context (JSON)

### Audit Log Schema

```python
class AuditLog:
    id: str                    # Unique audit log ID
    job_id: str               # Related job ID
    domain: str               # Target domain
    policy_id: Optional[str]  # Domain policy ID
    authorization_mode: AuthorizationMode
    strategy: str
    action: PolicyAction       # allow, deny, rate_limit, etc.
    allowed: bool
    reason: Optional[str]
    rate_limit_applied: bool
    concurrency_limit_applied: bool
    strategy_restricted: bool
    timestamp: datetime
    user_id: Optional[str]
    ip_address: Optional[str]
    context: Optional[str]     # JSON context
```

### Querying Audit Logs

**Get audit logs for a job:**
```sql
SELECT * FROM audit_logs WHERE job_id = 'job-uuid';
```

**Get all denied jobs:**
```sql
SELECT * FROM audit_logs WHERE allowed = false;
```

**Get rate limit violations:**
```sql
SELECT * FROM audit_logs WHERE rate_limit_applied = true;
```

---

## Compliance Features

### 1. Policy-Driven Access Control

- All access is controlled by explicit policies
- No "gray area" access - everything is either allowed or denied
- Policies are auditable and traceable

### 2. Complete Audit Trail

- Every policy decision is logged
- All enforcement actions are recorded
- Full request context is preserved
- Immutable audit logs (append-only)

### 3. Defense in Depth

- Policy checks at job submission
- Policy checks at execution time
- Hard stops on policy violations
- No bypass mechanisms

### 4. Authorization-Based Access

- Clear authorization modes (public, customer-authorized, internal)
- Strategy restrictions based on authorization level
- Enterprise features reserved for enterprise customers

### 5. Rate Limiting and Throttling

- Prevents abuse and excessive requests
- Protects target domains from overload
- Configurable per-domain limits

### 6. Concurrency Control

- Prevents resource exhaustion
- Ensures fair usage
- Maintains system stability

---

## Enterprise Features

### Production Core Strategies

Available to all tiers (positioned by tier):

- **Vanilla:** Standard automation (Starter, Professional, Enterprise)
- **Stealth:** Enhanced evasion (Professional, Enterprise)
- **Assault:** Maximum evasion (Enterprise)

**Status:** Production-ready, validated, tested

### Enterprise Advanced Features

Enterprise tier customers have access to advanced execution strategies:

- **Ultimate Stealth:** Maximum stealth with human behavior simulation and complete artifact cleanup
- **Custom Executors:** Custom execution logic with JavaScript injection and specialized evasion techniques

**Access Control:**
- Enterprise tier required
- Must use `authorization_mode=internal`
- Subject to all domain policies
- Full audit logging

**Status:** Enterprise advanced features - functional, tested, positioned for specialized enterprise use cases

**Use Cases:**
- Competitive intelligence (with proper authorization)
- Security research
- Compliance monitoring
- Authorized data collection
- Specialized evasion requirements

**Note:** See `docs/EXECUTION_STRATEGIES.md` for complete strategy documentation.

---

## Best Practices

### 1. Configure Domain Policies

**Before Production:**
- Set up allowlist/denylist for your use cases
- Configure rate limits based on target domain requirements
- Set concurrency limits to prevent overload
- Define strategy restrictions per domain

### 2. Use Appropriate Authorization Modes

- **Public pages:** Use `public` mode
- **Customer-authorized access:** Use `customer-authorized` mode
- **Enterprise use cases:** Use `internal` mode (requires enterprise tier)

### 3. Monitor Audit Logs

- Regularly review audit logs for policy violations
- Monitor rate limit hits
- Track strategy restrictions
- Review denied jobs

### 4. Review and Update Policies

- Regularly review domain policies
- Update rate limits based on usage patterns
- Adjust concurrency limits as needed
- Update strategy restrictions based on requirements

### 5. Secure API Keys

- Use API keys for authentication
- Rotate keys regularly
- Monitor API key usage in audit logs
- Restrict API key permissions

---

## Compliance Statements

### Legal Compliance

Daemon Accord is designed to support legal and compliant web automation:

- **Public Data:** Access to publicly available data
- **Authorized Access:** Customer-authorized access to specific domains
- **Internal Use:** Enterprise internal use cases

**Important:** Users are responsible for ensuring their use of Daemon Accord complies with:
- Terms of Service of target websites
- Applicable laws and regulations
- Data protection requirements
- Intellectual property rights

### Ethical Use

Daemon Accord includes policy controls to promote ethical use:

- Rate limiting prevents abuse
- Concurrency limits ensure fair usage
- Audit logging provides transparency
- Policy enforcement ensures compliance

### Data Protection

- All audit logs are stored securely
- User information is protected
- IP addresses are logged for security (can be anonymized)
- Audit logs can be exported for compliance reporting

---

## API Reference

### Create Job with Authorization

```bash
POST /api/v1/jobs?domain=example.com&url=https://example.com&authorization_mode=public
```

**Parameters:**
- `authorization_mode`: `public`, `customer-authorized`, or `internal`
- `strategy`: `vanilla`, `stealth`, `assault` (restricted by authorization mode)

**Response (Policy Violation):**
```json
{
  "detail": "Policy violation: Strategy 'stealth' requires customer authorization. Public mode only allows 'vanilla'."
}
```

---

## Summary

Daemon Accord provides comprehensive policy controls and audit logging to ensure:

✅ **Compliance:** All access is policy-driven and auditable  
✅ **Security:** Defense-in-depth policy enforcement  
✅ **Transparency:** Complete audit trail of all decisions  
✅ **Control:** Domain-level policies for rate limits, concurrency, and strategies  
✅ **Authorization:** Clear authorization modes with appropriate restrictions  

**Result:** A policy-driven automation platform suitable for enterprise deployment, not a "gray area" scraping tool.

---

**Last Updated:** 2024-01-01  
**Version:** 1.0.0

