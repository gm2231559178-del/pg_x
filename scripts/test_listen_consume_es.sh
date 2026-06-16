#!/usr/bin/env bash
# Test pgx listen → rabbitmq → consume → elasticsearch end-to-end.
# Prerequisites: docker compose up -d, cargo build --release
#
# Flow tested:
#   1. pgx listen subscribes to PostgreSQL NOTIFY channel "materials"
#   2. On INSERT, the NOTIFY trigger fires → pgx listen forwards to RabbitMQ
#   3. pgx consume reads from RabbitMQ, runs GraphQL composition, indexes into ES
#   4. Verify the composed document appears in Elasticsearch

set -euo pipefail

PGURL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/postgres}"
PGX="${PGX_BINARY:-./target/release/pgx}"
AMQP_URL="${AMQP_URL:-amqp://guest:guest@localhost:5672/%2F}"
ES_URL="${ES_URL:-http://localhost:9200}"

cleanup() {
  local pid=$1
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

echo "==> Setting up ~/.pgx schema and queries"
mkdir -p ~/.pgx/schema ~/.pgx/queries
cp -r examples/graphql/pgx/schema/* ~/.pgx/schema/
cp -r examples/graphql/pgx/queries/* ~/.pgx/queries/

echo "==> Setting up ~/.pgx/config.toml"
cat > ~/.pgx/config.toml << 'CONFIG'
default = "local"

[connections.local]
url = "postgres://postgres:postgres@localhost:5432/postgres"

[connections.local.listen]
channels = ["materials"]

[connections.local.listen.sink]
type = "rabbitmq"
amqp_url = "amqp://guest:guest@localhost:5672/%2F"
exchange = "pgx"
routing_key = "pgx.notify"
mode = "contract"

[connections.local.consume]
source = { type = "rabbitmq", amqp_url = "amqp://guest:guest@localhost:5672/%2F", queue = "pgx-events", exchange = "pgx", routing_key = "pgx.notify" }
sink = { type = "elasticsearch", url = "http://localhost:9200", index = "materials", id_field = "mat_no" }
query_mode = "contract"
max_depth = 8
on_error = "lenient"

[resolvers.material]
sql = "SELECT mat_no, name, status FROM materials WHERE mat_no = $1"
param = "mat_no"

[resolvers.sizes]
sql = "SELECT size_code, mat_no, name FROM sizes WHERE mat_no = ANY($1)"
param = "mat_no"
batch_by = "mat_no"

[resolvers.colorways]
sql = "SELECT colorway_code, mat_no, name, hex FROM colorways WHERE mat_no = ANY($1)"
param = "mat_no"
batch_by = "mat_no"

[resolvers.features]
sql = "SELECT id, mat_no, feature_name, description FROM material_features WHERE mat_no = ANY($1)"
param = "mat_no"
batch_by = "mat_no"

[resolvers.attribute_entries]
sql = "SELECT attr_name, attr_value FROM feature_attributes WHERE feature_id::text = ANY($1)"
param = "id"
batch_by = "id"
CONFIG

echo "==> Pre-creating Elasticsearch index 'materials'"
curl -s -X DELETE "$ES_URL/materials" > /dev/null 2>&1 || true
curl -s -X PUT "$ES_URL/materials" \
  -H "Content-Type: application/json" \
  -d '{
    "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
    "mappings": {
      "properties": {
        "mat_no": { "type": "keyword" },
        "name": { "type": "text" },
        "status": { "type": "keyword" }
      }
    }
  }' | python3 -c "import sys; d=__import__('json').load(sys.stdin); assert d.get('acknowledged') or d.get('index') == 'materials', f'create index failed: {d}'"
echo "  Index created"

echo "==> Declaring RabbitMQ exchange 'pgx'"
curl -u guest:guest -X PUT http://localhost:15672/api/exchanges/%2F/pgx \
  -H "content-type: application/json" \
  -d '{"type":"topic","durable":true}' 2>/dev/null || true

echo "==> Starting pgx listen with rabbitmq contract downstream"
$PGX -U "$PGURL" listen -C materials rabbitmq \
  --amqp-url "$AMQP_URL" \
  --exchange pgx \
  --routing-key pgx.notify \
  --mode contract &
LISTEN_PID=$!
sleep 3

echo "==> Starting pgx consume with rabbitmq source and elasticsearch sink"
$PGX -U "$PGURL" consume \
  --source rabbitmq \
  --amqp-url "$AMQP_URL" \
  --queue pgx-events \
  --exchange pgx \
  --routing-key pgx.notify \
  --sink elasticsearch \
  --es-url "$ES_URL" \
  --index materials \
  --id-field mat_no \
  --query-mode contract > /tmp/pgx_consume_es.log 2>&1 &
CONSUME_PID=$!
sleep 3

echo "==> Triggering NOTIFY via PostgreSQL INSERT"
psql "$PGURL" \
  -c "INSERT INTO materials (mat_no, name, status) VALUES ('M004', 'Test Integration Material', 'active');"

sleep 3

echo "==> Verifying document appears in Elasticsearch"
ES_PASS=""
for i in $(seq 1 15); do
  ES_RESPONSE=$(curl -s "$ES_URL/materials/_doc/M004")
  if echo "$ES_RESPONSE" | python3 -c "
import sys, json
d = json.load(sys.stdin)
if d.get('found'):
    src = d.get('_source', {})
    assert src.get('mat_no') == 'M004', f'Expected M004, got {src.get(\"mat_no\")}'
    assert src.get('name') == 'Test Integration Material', f'Missing name field'
    assert 'sizes' in src, f'Missing sizes (graphql fields): {list(src.keys())}'
    assert 'colorways' in src, f'Missing colorways'
    print('Verification passed')
    sys.exit(0)
else:
    print(f'Not found yet: {d.get(\"_index\")}')
    sys.exit(1)
"; then
    ES_PASS=1
    break
  fi
  sleep 2
done

echo "==> Stopping processes"
cleanup $LISTEN_PID
cleanup $CONSUME_PID

if [ -z "$ES_PASS" ]; then
  echo "==> listen → rabbitmq → consume → elasticsearch: FAIL"
  echo "=== Consume logs ==="
  cat /tmp/pgx_consume_es.log
  echo "=== ES GET _doc/M004 ==="
  curl -s "$ES_URL/materials/_doc/M004" | python3 -m json.tool 2>/dev/null || true
  echo "=== ES search all ==="
  curl -s "$ES_URL/materials/_search?pretty" | head -60
  exit 1
fi

rm -f /tmp/pgx_consume_es.log
echo "==> listen → rabbitmq → consume → elasticsearch: PASS"
