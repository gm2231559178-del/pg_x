#!/bin/sh
# Periodic Elasticsearch index refresh via `pgx export -m elasticsearch`.
# Rebuilds an index from a SQL query every REFRESH_INTERVAL seconds.
#
# Configured by env vars (set in docker-compose.yml):
#   DATABASE_URL     PostgreSQL connection URL
#   ES_URL           Elasticsearch base URL
#   REFRESH_QUERY    SQL SELECT whose rows become the indexed documents
#   REFRESH_INDEX    Destination index name
#   REFRESH_ID_FIELD Row column whose value becomes the document _id (upsert key)
#   REFRESH_INTERVAL Seconds between refreshes
set -eu

echo "Starting index refresh loop: query='${REFRESH_QUERY}' index='${REFRESH_INDEX}' every ${REFRESH_INTERVAL}s"

while true; do
  echo "Refreshing index '${REFRESH_INDEX}' ..."
  pgx export -q "${REFRESH_QUERY}" -m elasticsearch \
    --es-url "${ES_URL}" \
    --index "${REFRESH_INDEX}" \
    --id-field "${REFRESH_ID_FIELD}" \
    || echo "Refresh failed — retrying in ${REFRESH_INTERVAL}s"
  sleep "${REFRESH_INTERVAL}"
done
