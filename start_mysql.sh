#!/bin/bash

# Define variables
CONTAINER_NAME="cdc-mysql"
MYSQL_ROOT_PASSWORD="rootpassword"
MYSQL_DATABASE="cdc_demo"
PORT=3306

echo "Stopping and removing any existing container named $CONTAINER_NAME..."
docker stop $CONTAINER_NAME 2>/dev/null
docker rm $CONTAINER_NAME 2>/dev/null

echo "Starting new MySQL container..."
# Map port 3306, set root password, map init.sql to entrypoint directory so it runs on startup
docker run --name $CONTAINER_NAME \
  -e MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD \
  -v $(pwd)/mysql_setup/init.sql:/docker-entrypoint-initdb.d/init.sql \
  -p $PORT:3306 \
  -d mysql:8.0

echo "Waiting for MySQL to finish initialising (could take 15-30 seconds)..."

# Wait until MySQL is ready to accept connections
max_retries=30
retries=0
while ! docker exec $CONTAINER_NAME mysql -uroot -p$MYSQL_ROOT_PASSWORD -e "SELECT 1" >/dev/null 2>&1; do
  sleep 2
  retries=$((retries + 1))
  if [ $retries -eq $max_retries ]; then
    echo "MySQL failed to start within the expected time."
    exit 1
  fi
  echo -n "."
done

echo ""
echo "MySQL is up and running on port $PORT!"
echo "Database 'cdc_demo' and table 'customers' should be created with initial data."
