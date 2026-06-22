#!/bin/bash
set -uo pipefail

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
    # This buffer allows the worker to finish group coordination
    sleep 15 
    break
  fi
  
  echo -n "."
  sleep 5
done

echo -e "\nDeploying Kafka Connect with Debezium PostgreSQL Connector..."

# Loop until the POST request to deploy the connector is successful
while : ; do
  # Execute curl, capturing the response body and appending the HTTP status code on a new line
  RESPONSE=$(curl -s -w "\n%{http_code}" -X POST -H "Content-Type: application/json" --data @"$SCRIPT_DIR/postgres_source.json" "$KAFKA_CONNECT_URL")
  
  # Extract the HTTP status code (last line) and the response body (everything else)
  HTTP_STATUS=$(echo "$RESPONSE" | tail -n1)
  BODY=$(echo "$RESPONSE" | sed '$d')

  if [ "$HTTP_STATUS" -eq 201 ]; then
    echo "Success: Connector deployed!"
    echo "Response: $BODY"
    break
  elif [ "$HTTP_STATUS" -eq 409 ]; then
    echo "Success: Connector already exists (HTTP 409)."
    break
  else
    echo "Deployment failed with HTTP Status: $HTTP_STATUS"
    echo "Response: $BODY"
    echo "Retrying in 5 seconds..."
    sleep 5
  fi
done