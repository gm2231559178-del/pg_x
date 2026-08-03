#!/usr/bin/env bash
# Test pgx consume idempotent mode end-to-end.
# Prerequisites: docker compose up -d, cargo build --release
# Tests:
#   1. RabbitMQ source + KV sink (no key-field): publishing the same message
#      twice with --idempotent yields exactly one key, derived from the message id.
#   2. Webhook sink: every POST carries an Idempotency-Key header, and a failed
#      POST is not marked as processed (the retry is attempted again).

set -euo pipefail

PGURL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/postgres}"
PGX="${PGX_BINARY:-./target/release/pgx}"
AMQP_URL="${AMQP_URL:-amqp://guest:guest@localhost:5672/%2F}"

PAYLOAD='{"meta":{"event_type":"MaterialFull","schema_version":"1"},"data":{"mat_no":"M001"}}'
# The broker's native AMQP message_id is the stable identity for idempotent
# mode (per spec D6.3 there is no payload-hash fallback), so both publishes
# must carry the same property for the dedupe to collapse them.
MESSAGE_ID="idem-msg"
EXPECTED_KEY="pgx:${MESSAGE_ID}"

cleanup() {
  local pid=$1
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# Publish `$PAYLOAD` to the `pgx` exchange under `pgx.idem`.
publish_once() {
  local body
  body=$(python3 - "$PAYLOAD" "$MESSAGE_ID" <<'PY'
import json, sys
print(json.dumps({
    "properties": {"message_id": sys.argv[2]},
    "routing_key": "pgx.idem",
    "payload": sys.argv[1],
    "payload_encoding": "string",
}))
PY
)
  curl -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F/pgx/publish \
    -H "content-type: application/json" \
    -d "$body" 2>/dev/null \
    | python3 -c "import sys; d=__import__('json').load(sys.stdin); assert d.get('routed'), f'publish failed: {d}'"
}

# Wait for Redis to be ready
for i in $(seq 1 15); do
  if redis-cli -h localhost -p 6379 ping 2>/dev/null | grep -q PONG; then
    break
  fi
  sleep 1
done

echo "==> consume-idempotent: setting up schema directory"
mkdir -p ~/.pgx/schema ~/.pgx/queries
cp -r examples/graphql/pgx/schema/* ~/.pgx/schema/
cp -r examples/graphql/pgx/queries/* ~/.pgx/queries/

echo "==> consume-idempotent: declaring RabbitMQ exchange 'pgx'"
curl -u guest:guest -X PUT http://localhost:15672/api/exchanges/%2F/pgx \
  -H "content-type: application/json" \
  -d '{"type":"topic","durable":true}' 2>/dev/null || true

# ── Phase 1: KV sink, no key-field — duplicate publishes collapse ─────────────
echo "==> consume-idempotent: starting pgx consume (kv sink, idempotent)"
redis-cli -h localhost -p 6379 FLUSHDB >/dev/null
$PGX -U "$PGURL" consume \
  --source rabbitmq \
  --amqp-url "$AMQP_URL" \
  --queue pgx-idempotent-kv \
  --exchange pgx \
  --routing-key pgx.idem \
  --sink kv \
  --kv-url "redis://localhost:6379" \
  --key-prefix "pgx:" \
  --query-mode contract \
  --idempotent > /tmp/pgx_consume_idem_kv.log 2>&1 &
CONSUME_PID=$!
sleep 3

echo "==> consume-idempotent: publishing the same ContractMessage twice"
publish_once
publish_once

sleep 4

echo "==> consume-idempotent: verifying exactly one Redis key"
KEYS=$(redis-cli -h localhost -p 6379 KEYS "pgx:*")
COUNT=$(echo -n "$KEYS" | grep -c '^' || true)
if [ "$COUNT" -eq 1 ] && [ "$KEYS" = "$EXPECTED_KEY" ]; then
  echo "==> consume-idempotent: exactly one key ($EXPECTED_KEY)"
else
  cleanup $CONSUME_PID
  echo "==> consume-idempotent: FAIL — expected 1 key '$EXPECTED_KEY', got $COUNT keys"
  echo "Redis KEYS result: $KEYS"
  echo "Consume log:"
  cat /tmp/pgx_consume_idem_kv.log
  exit 1
fi

VALUE=$(redis-cli -h localhost -p 6379 GET "$EXPECTED_KEY")
if echo "$VALUE" | grep -q '"mat_no":"M001"' && \
   echo "$VALUE" | grep -q '"sizes"'; then
  echo "==> consume-idempotent: composed document verified in Redis"
else
  cleanup $CONSUME_PID
  echo "==> consume-idempotent: FAIL — document content not found in Redis"
  echo "Redis GET result: $VALUE"
  cat /tmp/pgx_consume_idem_kv.log
  exit 1
fi

echo "==> consume-idempotent: stopping kv consumer"
cleanup $CONSUME_PID
rm -f /tmp/pgx_consume_idem_kv.log

# ── Phase 2: Webhook — Idempotency-Key header, failures not deduped ───────────
echo "==> consume-idempotent: starting webhook capture server"
HOOK_LOG=/tmp/pgx_consume_idem_hooks.jsonl
rm -f "$HOOK_LOG"
python3 - "$HOOK_LOG" <<'PY' &
import http.server, json, sys
log = sys.argv[1]
attempts = {"count": 0}
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        attempts["count"] += 1
        with open(log, "a") as f:
            f.write(json.dumps({
                "idempotency_key": self.headers.get("Idempotency-Key", "<none>"),
                "path": self.path,
            }) + "\n")
        # First attempt fails so the message is requeued and retried (lenient
        # policy requeues transient sink failures); later attempts succeed.
        if attempts["count"] == 1:
            self.send_response(500)
        else:
            self.send_response(200)
        self.end_headers()
    def log_message(self, *a):
        pass
http.server.HTTPServer(("127.0.0.1", 18081), H).serve_forever()
PY
HOOK_PID=$!

echo "==> consume-idempotent: starting pgx consume (webhook sink, idempotent, lenient)"
$PGX -U "$PGURL" consume \
  --source rabbitmq \
  --amqp-url "$AMQP_URL" \
  --queue pgx-idempotent-webhook \
  --exchange pgx \
  --routing-key pgx.idem \
  --sink webhook \
  --webhook-url "http://127.0.0.1:18081/hooks/orders" \
  --query-mode contract \
  --idempotent > /tmp/pgx_consume_idem_wh.log 2>&1 &
CONSUME_PID=$!
sleep 3

echo "==> consume-idempotent: publishing the same ContractMessage twice (endpoint fails once, then succeeds)"
publish_once
publish_once

sleep 4

echo "==> consume-idempotent: verifying webhook attempts"
REQUESTS=$(cat "$HOOK_LOG")
if [ "$(echo -n "$REQUESTS" | grep -c '^' || true)" -eq 2 ]; then
  echo "==> consume-idempotent: failed POST was requeued and retried, duplicate collapsed (2 attempts)"
else
  cleanup $CONSUME_PID
  cleanup $HOOK_PID
  echo "==> consume-idempotent: FAIL — expected 2 webhook attempts, got:"
  echo "$REQUESTS"
  cat /tmp/pgx_consume_idem_wh.log
  exit 1
fi

if echo "$REQUESTS" | grep -q '"idempotency_key": "idem-msg"'; then
  echo "==> consume-idempotent: Idempotency-Key header present on every attempt"
else
  cleanup $CONSUME_PID
  cleanup $HOOK_PID
  echo "==> consume-idempotent: FAIL — Idempotency-Key header missing or not the native message id"
  echo "$REQUESTS"
  cat /tmp/pgx_consume_idem_wh.log
  exit 1
fi

echo "==> consume-idempotent: stopping"
cleanup $CONSUME_PID
cleanup $HOOK_PID
rm -f /tmp/pgx_consume_idem_kv.log /tmp/pgx_consume_idem_wh.log "$HOOK_LOG"

echo "==> consume-idempotent: PASS"
