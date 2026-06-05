#!/usr/bin/env bash
# Test pgx consume command end-to-end.
# Prerequisites: docker compose up -d, cargo build --release
# Tests: RabbitMQ source → GraphQL composition → stdout sink

set -euo pipefail

PGURL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/postgres}"
PGX="${PGX_BINARY:-./target/release/pgx}"
AMQP_URL="${AMQP_URL:-amqp://guest:guest@localhost:5672/%2F}"

cleanup() {
  local pid=$1
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# Capture output
OUTFILE=$(mktemp)
echo "Output file: $OUTFILE"

echo "==> consume: setting up schema directory"
mkdir -p ~/.pgx/schema ~/.pgx/queries
cp -r examples/graphql/pgx/schema/* ~/.pgx/schema/
cp -r examples/graphql/pgx/queries/* ~/.pgx/queries/
cp examples/graphql/pgx/config.toml ~/.pgx/config.toml

echo "==> consume: declaring RabbitMQ exchange 'pgx'"
curl -u guest:guest -X PUT http://localhost:15672/api/exchanges/%2F/pgx \
  -H "content-type: application/json" \
  -d '{"type":"topic","durable":true}' 2>/dev/null || true

echo "==> consume: starting pgx consume with rabbitmq source and stdout sink"
$PGX -U "$PGURL" consume \
  --source rabbitmq \
  --amqp-url "$AMQP_URL" \
  --queue pgx-events \
  --exchange pgx \
  --routing-key pgx.notify \
  --sink stdout \
  --query-mode contract > "$OUTFILE" 2>&1 &
CONSUME_PID=$!
sleep 3

echo "==> consume: publishing ContractMessage to RabbitMQ"
curl -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F/pgx/publish \
  -H "content-type: application/json" \
  -d '{
    "properties": {},
    "routing_key": "pgx.notify",
    "payload": "{\"meta\":{\"event_type\":\"MaterialFull\",\"schema_version\":\"1\"},\"data\":{\"mat_no\":\"M001\"}}",
    "payload_encoding": "string"
  }' 2>/dev/null | python3 -c "import sys; d=__import__('json').load(sys.stdin); assert d.get('routed'), f'publish failed: {d}'"

sleep 3

echo "==> consume: stopping"
cleanup $CONSUME_PID

# Verify the output contained expected GraphQL-composed fields
OUTPUT=$(cat "$OUTFILE")
echo "=== consume output ==="
echo "$OUTPUT"
echo "=== end output ==="

if echo "$OUTPUT" | grep -q '"mat_no": "M001"' && \
   echo "$OUTPUT" | grep -q '"sizes"' && \
   echo "$OUTPUT" | grep -q '"colorways"'; then
  rm "$OUTFILE"
  echo "==> consume: PASS"
else
  rm "$OUTFILE"
  echo "==> consume: FAIL — output missing expected fields"
  exit 1
fi
