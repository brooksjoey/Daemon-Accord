## Local “golden demo” runbook (start → prove → stop)

This is the shortest repeatable proof path aligned to the repo’s current deployment and ports.

### Start (uses the same compose stack CI uses)
```bash
docker-compose -f 05-Deploy-Monitoring-Infra/src/deploy/docker-compose.full.yml up -d
sleep 30
```

### Prove services are up
```bash
curl -f http://localhost:8082/health
curl -f http://localhost:8100/health
```

### Prove end-to-end job flow (enqueue → execute → store → query)
```bash
pip install httpx
python scripts/test_e2e_flow.py
```

### Optional: validate executor strategies (“advanced” = stealth variants)
```bash
python scripts/validate_all_executors.py
```

### Stop
```bash
docker-compose -f 05-Deploy-Monitoring-Infra/src/deploy/docker-compose.full.yml down
```

