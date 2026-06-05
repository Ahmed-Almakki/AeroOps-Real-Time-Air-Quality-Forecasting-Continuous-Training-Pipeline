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

echo "Waiting for Kafka Connect REST API to come online..."

# Loop until a GET request to /connectors returns an HTTP 200 status
while : ; do
  HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$KAFKA_CONNECT_URL")
  
  if [ "$HTTP_STATUS" -eq 200 ]; then
    echo -e "\nREST API is up! Giving the worker 15 seconds to sync with the Kafka cluster..."
    # This is the magic buffer that lets the worker finish group coordination
    sleep 15 
    break
  fi
  
  echo -n "."
  sleep 5
done

echo "Deploying Kafka Connect with Debezium PostgreSQL Connector..."
curl -X POST -H "Content-Type: application/json" --data @"$SCRIPT_DIR/postgres_source.json" "$KAFKA_CONNECT_URL"