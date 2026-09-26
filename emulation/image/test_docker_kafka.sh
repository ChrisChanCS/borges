#!/bin/bash

# Docker Kafka test script
# Used to verify Docker Kafka installation and configuration

set -e

echo "=== Docker Kafka test script ==="
echo "Test time: $(date)"
echo ""

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test helper
test_step() {
    local step_name="$1"
    local command="$2"

    echo -e "${YELLOW}Test: $step_name${NC}"
    if eval "$command"; then
        echo -e "${GREEN}✓ $step_name passed${NC}"
        return 0
    else
        echo -e "${RED}✗ $step_name Failed${NC}"
        return 1
    fi
}

# 1. Check Docker installation
echo "1. Check Docker installationStatus..."
test_step "DockerVersion" "docker --version"
test_step "Docker ComposeVersion" "docker-compose --version"
test_step "DockerService status" "systemctl is-active docker"

# 2. Check Docker images if present
echo ""
echo "2. CheckDocker images..."
if docker images | grep -q zookeeper; then
    test_step "Zookeeper image" "docker images | grep zookeeper"
else
    echo -e "${YELLOW}⚠ ZooKeeper image is unavailable; it will be pulled on first startup${NC}"
fi

if docker images | grep -q kafka; then
    test_step "Kafka image" "docker images | grep kafka"
else
    echo -e "${YELLOW}⚠ Kafka image is unavailable; it will be pulled on first startup${NC}"
fi

# 3. Checkconfiguration file
echo ""
echo "3. Checkconfiguration file..."
test_step "Docker Compose configuration" "test -f /opt/kafka-docker/docker-compose.yml"
test_step "start script" "test -f /opt/kafka-docker/start-kafka.sh"
test_step "stop script" "test -f /opt/kafka-docker/stop-kafka.sh"
test_step "check script" "test -f /opt/kafka-docker/check-kafka.sh"

# 4. Start services
echo ""
echo "4. Start Kafka and Zookeeper services..."
if [ -f "/opt/kafka-docker/start-kafka.sh" ]; then
    echo "Run the startup script..."
    /opt/kafka-docker/start-kafka.sh

    # Wait for services to start
    echo "Wait for services to start..."
    sleep 30

    test_step "Zookeepercontainer running" "docker ps | grep zookeeper"
    test_step "Kafkacontainer running" "docker ps | grep kafka"
else
    echo -e "${RED}✗ start scriptdoes not exist${NC}"
fi

# 5. CheckPort listeners
echo ""
echo "5. CheckPort listeners..."
test_step "Zookeeperport2181" "netstat -tlnp | grep :2181"
test_step "Kafkaport9092" "netstat -tlnp | grep :9092"

# 6. Test connectivity
echo ""
echo "6. Test network connections..."
test_step "ZooKeeper connection test" "timeout 5 bash -c '</dev/tcp/localhost/2181'"
test_step "Kafka connection test" "timeout 5 bash -c '</dev/tcp/localhost/9092'"

# 7. Test Kafka functionality
echo ""
echo "7. Test basic Kafka functionality..."
if docker ps | grep -q kafka; then
    echo "Wait for Kafka to fully start..."
    sleep 10

    # Create a test topic
    echo "Create a test topic..."
    if docker exec kafka kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>/dev/null; then
        echo -e "${GREEN}✓ Topic creation succeeded${NC}"

        # List topics
        echo "List topics..."
        docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092

        # Send test messages
        echo "Send test messages..."
        echo "Hello Docker Kafka Test" | docker exec -i kafka kafka-console-producer.sh --topic test-topic --bootstrap-server localhost:9092

        # Receive test messages
        echo "Receive test messages..."
        timeout 10 docker exec kafka kafka-console-consumer.sh --topic test-topic --bootstrap-server localhost:9092 --from-beginning --max-messages 1

        echo -e "${GREEN}✓ Kafka functional test passed${NC}"
    else
        echo -e "${RED}✗ Kafka functionality test failed${NC}"
    fi
else
    echo -e "${RED}✗ Kafka container is not running; skipping the functional test${NC}"
fi

# 8. Checklogs
echo ""
echo "8. Check service logs..."
echo "Zookeeperlogs (last 10 lines):"
docker logs zookeeper 2>&1 | tail -10 || echo "Cannot get Zookeeper logs"

echo ""
echo "Kafkalogs (last 10 lines):"
docker logs kafka 2>&1 | tail -10 || echo "Cannot get Kafka logs"

# 9. Performance check
echo ""
echo "9. Performance check..."
echo "Container resource usage:"
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}" | grep -E "(kafka|zookeeper)" || echo "Cannot get container statistics"

# 10. Test cleanup
echo ""
echo "10. Clean up test data..."
if docker ps | grep -q kafka; then
    echo "Delete the test topic..."
    docker exec kafka kafka-topics.sh --delete --topic test-topic --bootstrap-server localhost:9092 2>/dev/null || echo "Topic deletion failed or the topic does not exist"
fi

echo ""
echo "=== Test complete ==="
echo ""
echo "Usage:"
echo "  Start services: /opt/kafka-docker/start-kafka.sh"
echo "  Stop services: /opt/kafka-docker/stop-kafka.sh"
echo "  CheckStatus: /opt/kafka-docker/check-kafka.sh"
echo "  View logs: docker logs kafka"
echo ""
echo "Port information:"
echo "  Zookeeper: 2181"
echo "  Kafka: 9092"
echo ""
echo "Note: The first startup pulls Docker images and may take a few minutes."
