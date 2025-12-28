# Workflow Templates

**First-class workflow templates for common, compliance-safe use cases.**

---

## Overview

Daemon Accord provides three production-ready workflow templates that solve real business problems:

1. **Page Change Detection** - Monitor public pages for changes and alert
2. **Job Posting Monitor** - Extract structured job posting data and alert on new postings
3. **Uptime/UX Smoke Check** - Verify page loads correctly with required elements

All workflows are:
- ✅ **Legal/Compliance-Safe** - Only access public pages
- ✅ **Production-Ready** - Tested and validated
- ✅ **Webhook-Enabled** - Optional notifications
- ✅ **Result-Persistent** - Stored in database

---

## Available Workflows

### List All Workflows

```bash
curl http://localhost:8080/api/v1/workflows
```

**Response:**
```json
{
  "page_change_detection": {
    "name": "page_change_detection",
    "display_name": "Page Change Detection",
    "description": "Monitor public pages for changes and alert when content differs from baseline",
    "input_schema": {...},
    "output_schema": {...}
  },
  "job_posting_monitor": {...},
  "uptime_smoke_check": {...}
}
```

### Get Workflow Details

```bash
curl http://localhost:8080/api/v1/workflows/page_change_detection
```

---

## Workflow 1: Page Change Detection

**Use Case:** Monitor public pages (documentation, terms of service, pricing pages) for changes.

### Input Schema

```json
{
  "url": "https://example.com/page",
  "domain": "example.com",
  "selectors": ["h1", ".content", "#main"],
  "baseline_content": "optional-hash-from-previous-run",
  "alert_on_change": true,
  "webhook_url": "https://your-app.com/webhook",
  "strategy": "vanilla"
}
```

### Execution Steps

1. Navigate to target URL
2. Extract content from specified selectors
3. Calculate content hash
4. Compare with baseline (if provided)
5. Generate diff summary if changed
6. Send webhook alert if changes detected

### Output Schema

```json
{
  "changed": true,
  "baseline_hash": "abc123...",
  "current_hash": "def456...",
  "diff_summary": "Content hash changed from abc123 to def456",
  "extracted_content": {...},
  "alert_sent": true
}
```

### Example: Monitor Documentation Page

```bash
curl -X POST "http://localhost:8080/api/v1/workflows/page_change_detection/run" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://httpbin.org/html",
    "domain": "httpbin.org",
    "selectors": ["h1", "p"],
    "alert_on_change": true,
    "webhook_url": "https://your-app.com/webhook"
  }'
```

**Response:**
```json
{
  "workflow_name": "page_change_detection",
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "created_at": "2024-12-24T10:00:00Z"
}
```

### Use Cases

- **Legal Compliance:** Monitor terms of service changes
- **Competitive Intelligence:** Track competitor pricing pages
- **Content Management:** Detect unauthorized content changes
- **Documentation:** Monitor documentation updates

---

## Workflow 2: Job Posting Monitor

**Use Case:** Monitor job board pages and extract structured job posting data.

### Input Schema

```json
{
  "url": "https://jobs.example.com/listings",
  "domain": "jobs.example.com",
  "extract_fields": {
    "title": "h2.job-title",
    "company": ".company-name",
    "location": ".location",
    "description": ".job-description"
  },
  "alert_on_new": true,
  "filter_keywords": ["python", "remote"],
  "webhook_url": "https://your-app.com/webhook",
  "strategy": "stealth"
}
```

### Execution Steps

1. Navigate to job board URL
2. Extract structured data using field mappings
3. Filter postings by keywords (if provided)
4. Compare with previous run (if baseline exists)
5. Identify new postings
6. Send webhook alert if new postings found

### Output Schema

```json
{
  "postings": [
    {
      "title": "Senior Python Developer",
      "company": "Tech Corp",
      "location": "Remote",
      "description": "..."
    }
  ],
  "posting_count": 10,
  "new_postings": 3,
  "alert_sent": true
}
```

### Example: Monitor Job Board

```bash
curl -X POST "http://localhost:8080/api/v1/workflows/job_posting_monitor/run" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://jsonplaceholder.typicode.com/",
    "domain": "jsonplaceholder.typicode.com",
    "extract_fields": {
      "title": "h1",
      "content": "p"
    },
    "filter_keywords": ["api", "json"],
    "webhook_url": "https://your-app.com/webhook"
  }'
```

### Use Cases

- **Recruitment:** Monitor job boards for relevant positions
- **Market Research:** Track hiring trends
- **Competitive Intelligence:** Monitor competitor job postings
- **Talent Acquisition:** Automated job discovery

---

## Workflow 3: Uptime/UX Smoke Check

**Use Case:** Verify pages load correctly, required elements present, and capture screenshots.

### Input Schema

```json
{
  "url": "https://example.com",
  "domain": "example.com",
  "required_selectors": [".header", "#main-content", ".footer"],
  "screenshot": true,
  "verify_load_time": true,
  "max_load_time_ms": 5000,
  "webhook_url": "https://your-app.com/webhook",
  "strategy": "vanilla"
}
```

### Execution Steps

1. Navigate to target URL and measure load time
2. Verify all required selectors are present
3. Capture screenshot (if enabled)
4. Determine overall status (pass/fail)
5. Send webhook alert if check fails

### Output Schema

```json
{
  "page_loaded": true,
  "load_time_ms": 1234.5,
  "selectors_found": {
    ".header": true,
    "#main-content": true,
    ".footer": true
  },
  "all_selectors_present": true,
  "screenshot_path": "/artifacts/screenshot-123.png",
  "status": "pass",
  "alert_sent": false
}
```

### Example: Smoke Check

```bash
curl -X POST "http://localhost:8080/api/v1/workflows/uptime_smoke_check/run" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://example.com",
    "domain": "example.com",
    "required_selectors": ["h1", "body"],
    "screenshot": true,
    "max_load_time_ms": 3000
  }'
```

### Use Cases

- **Uptime Monitoring:** Verify critical pages are accessible
- **Quality Assurance:** Automated smoke tests
- **Performance Monitoring:** Track page load times
- **Visual Regression:** Screenshot-based testing

---

## Workflow Execution Flow

```
1. Client → POST /api/v1/workflows/{name}/run
2. Control Plane → Validates input against schema
3. Control Plane → Creates underlying job
4. Execution Engine → Executes job
5. Control Plane → Processes workflow result
6. Control Plane → Stores result in database
7. Control Plane → Sends webhook (if configured)
8. Client → GET /api/v1/jobs/{job_id} to get result
```

---

## Webhook Notifications

All workflows support optional webhook notifications.

### Webhook Payload Format

**Page Change Detection:**
```json
{
  "workflow": "page_change_detection",
  "changed": true,
  "current_hash": "def456...",
  "baseline_hash": "abc123...",
  "diff_summary": "Content changed"
}
```

**Job Posting Monitor:**
```json
{
  "workflow": "job_posting_monitor",
  "posting_count": 10,
  "new_postings": 3,
  "postings": [...]
}
```

**Uptime Smoke Check:**
```json
{
  "workflow": "uptime_smoke_check",
  "status": "fail",
  "load_time_ms": 6000,
  "selectors_found": {...},
  "all_selectors_present": false
}
```

### Webhook Configuration

Include `webhook_url` in workflow input:

```json
{
  "url": "https://example.com",
  "domain": "example.com",
  "webhook_url": "https://your-app.com/webhook",
  ...
}
```

---

## Getting Workflow Results

After submitting a workflow, get results via the job status endpoint:

```bash
# Get job status (includes workflow result)
curl http://localhost:8080/api/v1/jobs/{job_id}
```

**Response includes workflow output:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "result": {
    "changed": true,
    "current_hash": "def456...",
    "baseline_hash": "abc123...",
    "alert_sent": true
  },
  ...
}
```

---

## Best Practices

### 1. Use Appropriate Strategies

- **Vanilla:** For public pages, documentation sites
- **Stealth:** For job boards, content sites
- **Assault:** For sites with advanced bot detection

### 2. Set Baseline for Change Detection

First run establishes baseline:
```bash
# First run - get baseline
curl -X POST ".../page_change_detection/run" -d '{...}'
# Save baseline_hash from result

# Subsequent runs - compare with baseline
curl -X POST ".../page_change_detection/run" -d '{
  ...,
  "baseline_content": "saved-hash-from-first-run"
}'
```

### 3. Configure Webhooks

Always configure webhooks for alerts:
```json
{
  "webhook_url": "https://your-app.com/webhook",
  "alert_on_change": true
}
```

### 4. Monitor Job Status

Poll job status until completion:
```python
import httpx
import asyncio

async def wait_for_workflow(job_id: str):
    async with httpx.AsyncClient() as client:
        while True:
            response = await client.get(f"http://localhost:8080/api/v1/jobs/{job_id}")
            status = response.json()
            
            if status["status"] in ["completed", "failed"]:
                return status
            
            await asyncio.sleep(2)
```

---

## Compliance & Legal

### ✅ Safe Use Cases

- **Public Pages:** Documentation, terms of service, public APIs
- **Job Boards:** Public job listings
- **Status Pages:** Public status/uptime pages
- **News Sites:** Public news articles

### ❌ Not For

- **Authenticated Pages:** Requires login
- **Rate-Limited APIs:** May violate terms
- **Private Data:** Any non-public content
- **Scraping at Scale:** Respect robots.txt and rate limits

### Recommendations

1. **Respect robots.txt:** Check before monitoring
2. **Rate Limiting:** Don't overwhelm target sites
3. **Terms of Service:** Review and comply
4. **Public Only:** Never access authenticated content

---

## Error Handling

### Validation Errors (400)

```json
{
  "detail": "Missing required field: selectors"
}
```

### Workflow Not Found (404)

```json
{
  "detail": "Workflow 'invalid_workflow' not found"
}
```

### Execution Errors (500)

Check job status for detailed error:
```bash
curl http://localhost:8080/api/v1/jobs/{job_id}
```

---

## Integration Examples

### Python Client

```python
import httpx
import asyncio

async def monitor_page_changes(url: str, selectors: list):
    async with httpx.AsyncClient() as client:
        # Submit workflow
        response = await client.post(
            "http://localhost:8080/api/v1/workflows/page_change_detection/run",
            json={
                "url": url,
                "domain": "example.com",
                "selectors": selectors,
                "webhook_url": "https://your-app.com/webhook"
            }
        )
        job = response.json()
        job_id = job["job_id"]
        
        # Wait for completion
        while True:
            status = await client.get(f"http://localhost:8080/api/v1/jobs/{job_id}")
            data = status.json()
            
            if data["status"] == "completed":
                result = data["result"]
                if result.get("changed"):
                    print(f"Page changed! {result['diff_summary']}")
                return result
            elif data["status"] == "failed":
                print(f"Workflow failed: {data.get('error')}")
                return None
            
            await asyncio.sleep(2)
```

### Scheduled Monitoring

```bash
# Use cron or scheduler to run periodically
# Example: Check every hour
0 * * * * curl -X POST "http://localhost:8080/api/v1/workflows/page_change_detection/run" \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com", "domain": "example.com", "selectors": ["h1"]}'
```

---

## Testing

### Unit Tests

```bash
pytest tests/unit/test_workflows.py
```

### Integration Tests

```bash
pytest tests/integration/test_workflows.py
```

Tests validate:
- Input schema validation
- Workflow execution
- Result processing
- Webhook delivery

---

## Summary

**Three Production-Ready Workflows:**

1. ✅ **Page Change Detection** - Monitor and alert on changes
2. ✅ **Job Posting Monitor** - Extract and alert on new postings
3. ✅ **Uptime Smoke Check** - Verify page loads and elements

**All workflows:**
- Legal/compliance-safe
- Webhook-enabled
- Result-persistent
- Production-tested

---

**Last Updated:** 2024-12-24  
**API Version:** 1.0.0

