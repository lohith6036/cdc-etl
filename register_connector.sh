#!/bin/bash

echo "Waiting for Debezium to be ready..."
max_retries=30
retries=0
while ! curl -s localhost:8083/ > /dev/null; do
  sleep 2
  retries=$((retries + 1))
  if [ $retries -eq $max_retries ]; then
    echo "Debezium failed to start."
    exit 1
  fi
  echo -n "."
done
echo " Debezium is ready!"

echo "Registering MySQL Connector..."

curl -s -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
localhost:8083/connectors -d '
{
  "name": "mysql-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "root",
    "database.password": "rootpassword",
    "database.server.id": "223344",
    "topic.prefix": "cdc_server",
    "database.include.list": "cdc_demo",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
    "schema.history.internal.kafka.topic": "schema-changes.cdc_demo"
  }
}'

echo ""
echo "Connector registered successfully!"
