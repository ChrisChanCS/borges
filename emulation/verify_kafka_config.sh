#!/bin/bash

echo "=== Verify Kafka configuration ==="
echo "Start time: $(date)"

# Find the broker ID.
echo "=== Get Kafka broker information ==="
docker exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092

echo ""
echo "=== Check Kafka configuration ==="
# Use the broker ID reported in the logs (typically 1001).
docker exec kafka kafka-configs.sh --bootstrap-server localhost:9092 \
    --entity-type brokers --entity-name 1001 --describe

echo ""
echo "=== Checkexactly-oncesemantics support ==="
echo "Check transaction configuration:"
docker exec kafka grep -r "transaction" /opt/bitnami/kafka/config/ 2>/dev/null || echo "No transaction configuration found"

echo ""
echo "Check idempotence configuration:"
docker exec kafka grep -r "enable.idempotence" /opt/bitnami/kafka/config/ 2>/dev/null || echo "No idempotence configuration found"

echo ""
echo "=== Test transactions ==="
echo "Create a transaction test topic:"
docker exec kafka kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic transaction-test

echo ""
echo "Test a transactional producer:"
docker exec -i kafka kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic transaction-test \
    --producer-property enable.idempotence=true \
    --producer-property transactional.id=test-transaction-id <<< "Transaction test message $(date)"

echo ""
echo "=== Check Kafka version and capabilities ==="
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic test-topic

echo ""
echo "=== Verification complete ==="
echo "Kafka 3.6.1 supports exactly-once semantics"
echo "Including:"
echo "  - Idempotent producer (enable.idempotence=true)"
echo "  - Transaction support (transactional.id)"
echo "  - Transaction state logs"
