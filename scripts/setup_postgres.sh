#!/usr/bin/env bash
# Configure PostgreSQL for logical replication (idempotent).
# Prerequisites: docker compose up -d

set -euo pipefail

echo "==> Configuring wal_level=logical"
docker compose exec -T postgres psql -U postgres -d postgres \
  -c "ALTER SYSTEM SET wal_level = logical;"
docker compose restart postgres

echo "==> Waiting for restart"
for i in $(seq 1 30); do
  docker compose exec -T postgres pg_isready -U postgres -d postgres 2>/dev/null && break
  sleep 1
done

echo "==> Creating publication and granting replication"
docker compose exec -T postgres psql -U postgres -d postgres \
  -c "CREATE PUBLICATION pgx_pub FOR TABLE users;" \
  -c "ALTER USER postgres REPLICATION;"

echo "==> Setup complete"
