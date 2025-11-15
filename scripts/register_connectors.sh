#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL=${CONNECT_URL:-http://localhost:8083}

echo "Registering Postgres source connector..."
curl -s -o /dev/null -w "%{http_code}\n" -X POST -H "Content-Type: application/json" \
  --data @connect/connectors/pg-source.json \
  "$CONNECT_URL/connectors" || true

echo "Registering Elasticsearch sink connector..."
curl -s -o /dev/null -w "%{http_code}\n" -X POST -H "Content-Type: application/json" \
  --data @connect/connectors/es-sink.json \
  "$CONNECT_URL/connectors" || true

echo "Done. Check $CONNECT_URL/connectors for status."

