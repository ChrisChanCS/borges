# Docker Kafka User Guide

## Overview

This guide explains how to deploy Kafka and Zookeeper in the VM image with Docker and Docker Compose.

## Installed components

The VM image build automatically installs: 
- Docker Engine
- Docker Compose
- Docker Composeconfiguration file
- 启动、停止和check script

**Note**: Docker images(Zookeeper 3.8.3 和 Kafka 3.6.1)are pulled automatically when the service starts for the first time。

## Directory structure

```
/opt/kafka-docker/
├── docker-compose.yml      # Docker Composeconfiguration file
├── start-kafka.sh         # start script(包含镜像拉取)
├── stop-kafka.sh          # stop script
└── check-kafka.sh         # Statuscheck script
```

## Usage

### 1. Start Kafka and Zookeeper

```bash
/opt/kafka-docker/start-kafka.sh
```

**首次启动时会自动拉取Docker images**, 可能需要几分钟时间。

### 2. Stop Kafka and Zookeeper

```bash
/opt/kafka-docker/stop-kafka.sh
```

### 3. CheckService status

```bash
/opt/kafka-docker/check-kafka.sh
```

### 4. View container logs

```bash
# View Zookeeper logs
docker logs zookeeper

# View Kafka logs
docker logs kafka

# Follow logs in real time
docker logs -f kafka
```

## Port configuration

- **Zookeeper**: 2181
- **Kafka**: 9092

## Network configuration

Kafka is configured to listen on all network interfaces: 
- Internal listener: `INTERNAL://:29092`
- External listener: `EXTERNAL://:9092`
- Advertised address: `localhost:9092`

## Data persistence

Docker Composeconfigures persistent data volumes: 
- `zookeeper_data`: Zookeeperdata
- `zookeeper_datalog`: Zookeeperlogs
- `kafka_data`: Kafkadata

## Test connectivity

### Test with the Docker container

```bash
# Enter the Kafka container
docker exec -it kafka bash

# Create topic
kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# List topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# Send messages
echo "Hello Kafka" | kafka-console-producer.sh --topic test --bootstrap-server localhost:9092

# Receive messages
kafka-console-consumer.sh --topic test --bootstrap-server localhost:9092 --from-beginning
```

### Test with local tools

```bash
# Install Kafka command-line tools if needed
wget https://archive.apache.org/dist/kafka/3.6.1/kafka_2.13-3.6.1.tgz
tar -xzf kafka_2.13-3.6.1.tgz
cd kafka_2.13-3.6.1

# Create topic
bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# List topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

## Troubleshooting

### 1. 容器启动Failed

```bash
# CheckContainer status
docker ps -a

# 查看详细logs
docker logs zookeeper
docker logs kafka

# Restart services
/opt/kafka-docker/stop-kafka.sh
/opt/kafka-docker/start-kafka.sh
```

### 2. Port is already in use

```bash
# Checkport占用
netstat -tlnp | grep -E ":(2181|9092)"

# Stop the process using the port
sudo lsof -ti:2181 | xargs kill -9
sudo lsof -ti:9092 | xargs kill -9
```

### 3. Permission issues

```bash
# Ensure the Docker service is running
sudo systemctl status docker

# Restart the Docker service
sudo systemctl restart docker
```

### 4. 镜像拉取Failed

```bash
# Pull images manually
docker pull zookeeper:3.8.3
docker pull bitnami/kafka:3.6.1

# Check镜像
docker images | grep -E "(zookeeper|kafka)"
```

## Configuration changes

To modify Kafka configuration, edit `/opt/kafka-docker/docker-compose.yml` environment variables in the file: 

```yaml
environment:
  KAFKA_CFG_ZOOKEEPER_CONNECT: zookeeper:2181
  KAFKA_CFG_LISTENERS: INTERNAL://:29092,EXTERNAL://:9092
  KAFKA_CFG_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
  # Add other configuration...
```

修改后Restart services: 

```bash
/opt/kafka-docker/stop-kafka.sh
/opt/kafka-docker/start-kafka.sh
```

## Performance tuning

### Memory configuration

The default configuration is suitable for development. For production, adjust: 

```yaml
environment:
  KAFKA_HEAP_OPTS: "-Xmx2G -Xms2G"
  ZOO_MEMORY: "1G"
```

### Disk configuration

确保有足够的磁盘空间用于data存储。

## Security configuration

The current configuration uses PLAINTEXT and is suitable for development. For production, consider: 

1. Enable SASL authentication
2. Use SSL/TLS encryption
3. Configure access control lists (ACLs)

## Monitoring

### 基本Monitoring

```bash
# View container resource usage
docker stats

# View Kafka metrics
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe
```

### logsMonitoring

```bash
# 实时Monitoringlogs
docker logs -f kafka | grep ERROR
docker logs -f zookeeper | grep ERROR
```

## Backup and restore

### 备份data

```bash
# 备份Kafkadata
docker run --rm -v kafka_data:/data -v $(pwd):/backup alpine tar czf /backup/kafka_backup.tar.gz -C /data .

# 备份Zookeeperdata
docker run --rm -v zookeeper_data:/data -v $(pwd):/backup alpine tar czf /backup/zookeeper_backup.tar.gz -C /data .
```

### 恢复data

```bash
# Stop services
/opt/kafka-docker/stop-kafka.sh

# 恢复data
docker run --rm -v kafka_data:/data -v $(pwd):/backup alpine tar xzf /backup/kafka_backup.tar.gz -C /data

# Start services
/opt/kafka-docker/start-kafka.sh
```

## Upgrade

### UpgradeKafkaVersion

1. Change the image version in `docker-compose.yml`
2. Stop services
3. Pull the new image
4. Start services

```bash
# Run after changing the version
docker pull bitnami/kafka:new-version
/opt/kafka-docker/stop-kafka.sh
/opt/kafka-docker/start-kafka.sh
```

## FAQ

### Q: Container exits immediately after startup
A: Checklogs文件, 通常是配置Error或资源不足

### Q: Cannot connect to Kafka
A: Checkport是否正确监听, 防火墙设置

### Q: data丢失
A: 确保data卷正确挂载, Check磁盘空间

### Q: Performance issues
A: 调整Memory configuration, Check网络和磁盘I/O

### Q: First startup is slow
A: 这是正常的, 因为需要拉取Docker images

## Support

如遇到问题, 请Check: 
1. DockerService status
2. 容器logs
3. system resource usage
4. Network configuration 