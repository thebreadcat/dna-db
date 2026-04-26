# Deployment and Monitoring Baseline

This baseline provides a local packaging pattern for DNA-DB runtime + observability tooling.

## Included Assets

- `docker-compose.observability.yml` - runtime + Prometheus + Grafana compose stack
- `config/dnadb.config.toml.example` - runtime config template with monitoring keys
- `docs/prometheus.yml` - Prometheus scrape config for DNA-DB metrics endpoint
- `scripts/ops_check.py` - read-only verification command for packaging + endpoint health

## Local Bring-Up (planned runtime image)

```bash
docker compose -f docker-compose.observability.yml up -d
```

## Admin Check

```bash
python3 scripts/ops_check.py --json
```

Environment overrides:

- `DNADB_HEALTH_URL` (default `http://127.0.0.1:8080/healthz`)
- `DNADB_METRICS_URL` (default `http://127.0.0.1:9090/-/healthy`)

## Notes

- This is a packaging/monitoring scaffold; it assumes the runtime server image and endpoints exist.
- Health and metrics checks are intentionally lightweight and safe for CI smoke jobs.
