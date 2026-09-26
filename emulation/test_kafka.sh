#!/bin/bash

echo "=== Kafka functional test ==="
echo "Start time: $(date)"

# Wait for Kafka to fully start
echo "Wait for Kafka to fully start..."
sleep 10

# Test1: Check Kafka container logs
echo "=== Test1: CheckKafkalogs ==="
echo "Kafka container logs (last 10 lines):"
docker logs kafka | tail -10

echo ""
echo "ZooKeeper container logs (last 5 lines):"
docker logs zookeeper | tail -5

# Test2: Create a test topic
echo ""
echo "=== Test2: Create a test topic ==="
docker exec kafka kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic test-topic

# Test3: List topics
echo ""
echo "=== Test3: List topics ==="
docker exec kafka kafka-topics.sh --list \
    --bootstrap-server localhost:9092

# Test4: Send test messages
echo ""
echo "=== Test4: Send test messages ==="
echo "Sending test messages..."
docker exec -i kafka kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic test-topic <<< "Hello Kafka! This is a test message $(date)"

# Test5: Consume test messages
echo ""
echo "=== Test5: Consume test messages ==="
echo "Consuming messages (5-second timeout):"
timeout 5 docker exec kafka kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic test-topic \
    --from-beginning \
    --max-messages 1

# Test6: Check Kafka configuration
echo ""
echo "=== Test6: Check Kafka configuration ==="
echo "Checkexactly-oncesemantics support:"
docker exec kafka kafka-configs.sh --bootstrap-server localhost:9092 \
    --entity-type brokers --entity-name 0 --describe | grep -i transaction

echo ""
echo "Check idempotence support:"
docker exec kafka kafka-configs.sh --bootstrap-server localhost:9092 \
    --entity-type brokers --entity-name 0 --describe | grep -i enable.idempotence

# Test7: Test connectivity
echo ""
echo "=== Test7: Connection test ==="
if command -v telnet >/dev/null 2>&1; then
    echo "Test ZooKeeper connection (2181):"
    timeout 3 telnet localhost 2181 2>/dev/null && echo "✓ Zookeeper connection succeeded" || echo "✗ Zookeeper connection failed"

    echo "Test Kafka connection (9092):"
    timeout 3 telnet localhost 9092 2>/dev/null && echo "✓ Kafka connection succeeded" || echo "✗ Kafka connection failed"
else
    echo "Test connectivity with nc:"
    echo "Test ZooKeeper connection (2181):"
    timeout 3 nc -z localhost 2181 2>/dev/null && echo "✓ Zookeeper connection succeeded" || echo "✗ Zookeeper connection failed"

    echo "Test Kafka connection (9092):"
    timeout 3 nc -z localhost 9092 2>/dev/null && echo "✓ Kafka connection succeeded" || echo "✗ Kafka connection failed"
fi

echo ""
echo "=== Test complete ==="
echo "Kafka address: localhost:9092"
echo "ZooKeeper address: localhost:2181"
