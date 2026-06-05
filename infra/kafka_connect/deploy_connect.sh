#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

: "${KAFKA_CONNECT_URL:=http://localhost:8083/connectors}"

echo "Deploying Kafka Connect with Debezium PostgreSQL Connector..."
curl -X POST -H "Content-Type: application/json" --data @"$SCRIPT_DIR/postgres_source.json" "$KAFKA_CONNECT_URL"
