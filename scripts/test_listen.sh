#!/usr/bin/env bash
# Test pgx listen command end-to-end.
# Prerequisites: docker compose up -d, cargo build --release

set -euo pipefail

PGURL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/postgres}"
PGX="${PGX_BINARY:-./target/release/pgx}"

echo "==> listen: starting pgx listen on channel 'user_updates' with shell downstream"
$PGX -U "$PGURL" listen -C user_updates shell \
  --command 'echo "[$PGX_CHANNEL] $PGX_PAYLOAD"' \
  --mode simple &
LISTEN_PID=$!
sleep 2

echo "==> listen: inserting test rows to trigger NOTIFY"
docker compose exec -T postgres psql -U postgres -d postgres \
  -c "INSERT INTO users (username, email) VALUES ('test_a', 'a@test.com'), ('test_b', 'b@test.com');"

sleep 1
kill $LISTEN_PID 2>/dev/null; wait $LISTEN_PID 2>/dev/null
echo "==> listen: PASS"
