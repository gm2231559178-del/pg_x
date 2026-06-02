#!/usr/bin/env bash
# Test pgx replicate command end-to-end.
# Prerequisites:
#   docker compose up -d
#   PostgreSQL configured with wal_level=logical
#   Publication pgx_pub exists for table users
#   cargo build --release

set -euo pipefail

PGURL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/postgres}"
PGX="${PGX_BINARY:-./target/release/pgx}"

echo "==> replicate: starting pgx replicate with stdout downstream"
$PGX -U "$PGURL" replicate \
  --slot pgx_slot \
  --publication pgx_pub \
  --temporary \
  stdout --pretty &
REPL_PID=$!
sleep 3

echo "==> replicate: inserting, updating, deleting rows"
psql "$PGURL" \
  -c "INSERT INTO users (username, email) VALUES ('rep_test', 'rep@test.com');" \
  -c "UPDATE users SET email = 'updated@test.com' WHERE username = 'rep_test';" \
  -c "DELETE FROM users WHERE username = 'test_a';"

sleep 1
kill $REPL_PID 2>/dev/null; wait $REPL_PID 2>/dev/null
echo "==> replicate: PASS"
