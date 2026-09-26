#!/bin/bash

echo "=== Start Kafka and Zookeeper services ==="
echo "Start time: $(date)"

# Create the network if it does not exist.
echo "Create the Docker network..."
docker network create kafka-network 2>/dev/null || echo "Network already exists"

# Start ZooKeeper
echo "Start ZooKeeper..."
docker run -d \
    --name zookeeper \
    --network kafka-network \
    -p 2181:2181 \
    -e ZOOKEEPER_CLIENT_PORT=2181 \
    -e ZOOKEEPER_TICK_TIME=2000 \
    zookeeper:3.8.3

# Wait for ZooKeeper to start
echo "Wait for ZooKeeper to start..."
sleep 10

# CheckZookeeperStatus
echo "CheckZookeeperStatus..."
docker logs zookeeper | tail -5

# Start Kafka
echo "Start Kafka..."
docker run -d \
    --name kafka \
    --network kafka-network \
    -p 9092:9092 \
    -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181 \
    -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092 \
    -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true \
    -e KAFKA_CFG_DELETE_TOPIC_ENABLE=true \
    -e KAFKA_CFG_LOG_RETENTION_HOURS=168 \
    -e KAFKA_CFG_LOG_SEGMENT_BYTES=1073741824 \
    -e KAFKA_CFG_LOG_RETENTION_CHECK_INTERVAL_MS=300000 \
    -e KAFKA_CFG_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_CFG_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_CFG_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS=60000 \
    -e KAFKA_CFG_TRANSACTION_REMOVE_EXPIRED_TRANSACTION_CLEANUP_INTERVAL_MS=3600000 \
    -e KAFKA_CFG_TRANSACTIONAL_ID_EXPIRATION_MS=900000 \
    -e KAFKA_CFG_ENABLE_IDEMPOTENCE=true \
    bitnami/kafka:3.6.1

# Wait for Kafka to start
echo "Wait for Kafka to start..."
sleep 15

# CheckService status
echo "=== Service statusCheck ==="
echo "DockerContainer status:"
docker ps

echo ""
echo "Kafka logs (last 10 lines):"
docker logs kafka | tail -10

echo ""
echo "ZooKeeper logs (last 5 lines):"
docker logs zookeeper | tail -5

echo ""
echo "CheckKafkaPort listeners:"
netstat -tlnp | grep 9092 || echo "Port 9092 is not listening"

echo ""
echo "=== Services started ==="
echo "Kafka address: localhost:9092"
echo "ZooKeeper address: localhost:2181"
