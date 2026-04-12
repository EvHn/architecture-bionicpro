#!/bin/bash
echo "Waiting for Kafka Connect to start..."
while [ $(curl -s -o /dev/null -w %{http_code} http://connect:8083/connectors) -ne 200 ]; do
  echo "Kafka Connect not ready yet..."
  sleep 5
done
echo "Kafka Connect is ready. Registering PostgreSQL connector..."
curl -X POST http://connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/scripts/postgres-connector.json
echo "Connector registration complete."
tail -f /dev/null