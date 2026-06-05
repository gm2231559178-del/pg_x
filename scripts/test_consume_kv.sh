#!/usr/bin/env bash
# Test pgx consume command with KV sink (Redis).
# Prerequisites: docker compose up -d, cargo build --release
# Tests: RabbitMQ source → GraphQL composition → Redis KV sink

set -euo pipefail

PGURL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/postgres}"
PGX="${PGX_BINARY:-./target/release/pgx}"
AMQP_URL="${AMQP_URL:-amqp://guest:guest@localhost:5672/%2F}"

cleanup() {
  local pid=$1
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# Wait for Redis to be ready
for i in $(seq 1 15); do
  if redis-cli -h localhost -p 6379 ping 2>/dev/null | grep -q PONG; then
    break
  fi
  sleep 1
done

echo "==> consume-kv: setting up schema directory"
mkdir -p ~/.pgx/schema ~/.pgx/queries
cp -r examples/graphql/pgx/schema/* ~/.pgx/schema/
cp -r examples/graphql/pgx/queries/* ~/.pgx/queries/

echo "==> consume-kv: declaring RabbitMQ exchange 'pgx'"
curl -u guest:guest -X PUT http://localhost:15672/api/exchanges/%2F/pgx \
  -H "content-type: application/json" \
  -d '{"type":"topic","durable":true}' 2>/dev/null || true

echo "==> consume-kv: starting pgx consume with rabbitmq source and kv sink"
$PGX -U "$PGURL" consume \
  --source rabbitmq \
  --amqp-url "$AMQP_URL" \
  --queue pgx-events \
  --exchange pgx \
  --routing-key pgx.notify \
  --sink kv \
  --kv-url "redis://localhost:6379" \
  --key-field "mat_no" \
  --key-prefix "pgx:" \
  --ttl 3600 \
  --query-mode contract > /tmp/pgx_consume_kv.log 2>&1 &
CONSUME_PID=$!
sleep 3

echo "==> consume-kv: publishing ContractMessage to RabbitMQ"
curl -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F/pgx/publish \
  -H "content-type: application/json" \
  -d '{
    "properties": {},
    "routing_key": "pgx.notify",
    "payload": "{\"meta\":{\"event_type\":\"MaterialFull\",\"schema_version\":\"1\"},\"data\":{\"mat_no\":\"M001\"}}",
    "payload_encoding": "string"
  }' 2>/dev/null | python3 -c "import sys; d=__import__('json').load(sys.stdin); assert d.get('routed'), f'publish failed: {d}'"

sleep 3

echo "==> consume-kv: verifying document in Redis"
VALUE=$(redis-cli -h localhost -p 6379 GET "pgx:M001")
if echo "$VALUE" | grep -q '"mat_no":"M001"' && \
   echo "$VALUE" | grep -q '"sizes"' && \
   echo "$VALUE" | grep -q '"colorways"'; then
  echo "==> consume-kv: document verified in Redis"
else
  cleanup $CONSUME_PID
  echo "==> consume-kv: FAIL — document not found or incomplete in Redis"
  echo "Redis GET result: $VALUE"
  echo "Consume log:"
  cat /tmp/pgx_consume_kv.log
  exit 1
fi

# Verify TTL was set
TTL=$(redis-cli -h localhost -p 6379 TTL "pgx:M001")
if [ "$TTL" -gt 0 ] 2>/dev/null; then
  echo "==> consume-kv: TTL verified ($TTL seconds remaining)"
else
  cleanup $CONSUME_PID
  echo "==> consume-kv: FAIL — TTL not set or expired"
  exit 1
fi

echo "==> consume-kv: stopping"
cleanup $CONSUME_PID
rm -f /tmp/pgx_consume_kv.log

echo "==> consume-kv: PASS"
