#!/bin/bash
set -xeuo pipefail

sudo apt-get -y update
sudo apt-get -y upgrade
# sudo apt-get -y install --install-recommends linux-generic-hwe-22.04

# 5.19.0-50
sudo apt-get -y install --install-recommends linux-image-5.19.0-50-generic

# main line linux
# mkdir -p /etc/apt/keyrings/
# curl -fsSL https://pkgs.zabbly.com/key.asc -o /etc/apt/keyrings/zabbly.asc
#
# sh -c 'cat <<EOF > /etc/apt/sources.list.d/zabbly-kernel-stable.sources
# Enabled: yes
# Types: deb
# URIs: https://pkgs.zabbly.com/kernel/stable
# Suites: $(. /etc/os-release && echo ${VERSION_CODENAME})
# Components: main
# Architectures: $(dpkg --print-architecture)
# Signed-By: /etc/apt/keyrings/zabbly.asc
#
# EOF'

# sudo apt-get -y update
# apt-get install -y linux-image-6.9.9-zabbly+

echo "deb http://ddebs.ubuntu.com $(lsb_release -cs) main restricted universe multiverse
deb http://ddebs.ubuntu.com $(lsb_release -cs)-updates main restricted universe multiverse
deb http://ddebs.ubuntu.com $(lsb_release -cs)-proposed main restricted universe multiverse" | \
sudo tee -a /etc/apt/sources.list.d/ddebs.list > /dev/null
sudo apt install -y ubuntu-dbgsym-keyring

# azul java
sudo apt install -y gnupg ca-certificates curl

curl -s https://repos.azul.com/azul-repo.key | sudo gpg --dearmor -o /usr/share/keyrings/azul.gpg

echo "deb [signed-by=/usr/share/keyrings/azul.gpg] https://repos.azul.com/zulu/deb stable main" | sudo tee /etc/apt/sources.list.d/zulu.list > /dev/null

sudo apt-get -y update || true
kernel_image=$(sudo apt list --installed | egrep -o linux-image-[0-9].[0-9]*.0-[0-9]*-generic | tail -n1)
kernel_version=$(echo $kernel_image | sed 's/linux-image-//')
sudo apt install -y linux-tools-${kernel_version} linux-cloud-tools-${kernel_version} linux-modules-extra-${kernel_version} zulu8-jdk
sudo update-grub
wget https://apt.llvm.org/llvm.sh -O llvm.sh && chmod +x llvm.sh && sudo ./llvm.sh 18 clang-18 lldb-18 lld-18 \
    clang-tools-18 lld-18 lldb-18 llvm-18-tools

curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
        && sudo chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
        && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
        && sudo apt update \
        && sudo apt install -y gh

video_device=$(sudo lshw -C video)
if echo ${video_device} | grep -i nvidia >/dev/null 2>/dev/null; then
    if echo ${video_device} | grep -i "Tesla K40m"; then
        sudo apt-get install -y nvidia-utils-470-server nvidia-headless-470-server nvidia-kernel-common-470-server nvidia-modprobe
    else
        sudo apt-get install -y nvidia-headless-530 nvidia-utils-530 nvidia-kernel-common-530 nvidia-modprobe
    fi
elif echo ${video_device} | grep -i amd >/dev/null 2>/dev/null; then
    wget https://repo.radeon.com/amdgpu-install/5.4.2/ubuntu/jammy/amdgpu-install_5.4.50402-1_all.deb
    sudo apt-get install -y ./amdgpu-install_5.4.50402-1_all.deb
    sudo amdgpu-install -y --no-32  --accept-eula --usecase=dkms
    sudo apt-get -y install rocm-smi-lib
    rm amdgpu-install_5.4.50402-1_all.deb
fi

network_device=$(lspci -Dmnn | grep '\[02')
if echo ${network_device} | grep -i mellanox >/dev/null 2>/dev/null; then
    # cd /root
    # tar -xf ./MLNX_OFED_LINUX-23.04-0.5.3.3-ubuntu22.04-x86_64.tgz
    # cd ./MLNX_OFED_LINUX-23.04-0.5.3.3-ubuntu22.04-x86_64
    # sudo ./mlnxofedinstall --without-dkms --add-kernel-support --without-fw-update --force
    # cd ..
    # rm ./MLNX_OFED_LINUX-23.04-0.5.3.3-ubuntu22.04-x86_64.tgz
    # rm -rf ./MLNX_OFED_LINUX-23.04-0.5.3.3-ubuntu22.04-x86_64
    export DOCA_URL="https://linux.mellanox.com/public/repo/doca/2.7.0/ubuntu22.04/x86_64/"
    curl https://linux.mellanox.com/public/repo/doca/GPG-KEY-Mellanox.pub | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/GPG-KEY-Mellanox.pub > /dev/null
    echo "deb [signed-by=/etc/apt/trusted.gpg.d/GPG-KEY-Mellanox.pub] $DOCA_URL ./" | sudo tee /etc/apt/sources.list.d/doca.list > /dev/null
    sudo apt-get update
    sudo apt-get -y install doca-ofed
    echo "ib_ipoib" >> /etc/modules
fi

if echo ${network_device} | grep -i "Ethernet Controller E810-C" >/dev/null 2>/dev/null; then
    cd /root
    tar -xvf iavf-4.8.2.tar.gz
    cd ./iavf-4.8.2/src
    sudo make install
    cd ../..
    rm iavf-4.8.2.tar.gz
    rm -rf iavf-4.8.2
fi

wget https://github.com/photoszzt/mem_workloads/releases/download/v0.1-alpha-model/pcm-0000-Linux.deb
sudo apt-get install ./pcm-0000-Linux.deb
rm ./pcm-0000-Linux.deb
python3 -m pip install ruamel.yaml

# Install Go; 1.23.x is recommended
GO_VERSION=1.23.10
cd /tmp
wget https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz
rm -rf /usr/local/go && tar -C /usr/local -xzf go${GO_VERSION}.linux-amd64.tar.gz
rm -f go${GO_VERSION}.linux-amd64.tar.gz

# Configure Go environment variables for the root user
echo 'export PATH=$PATH:/usr/local/go/bin:/root/go/bin' >> /root/.bashrc

# Install goreman
export PATH=$PATH:/usr/local/go/bin
/usr/local/go/bin/go install github.com/mattn/goreman@latest

# Create /data directory and change ownership
mkdir -p /data
sudo chown -R root /data

# Install Docker
echo "=== Install Docker ==="
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg lsb-release

# Add the official Docker GPG key
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Set up the Docker repository
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker Engine
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Do not start the Docker service in the build environment; only create required configuration
# Create Docker service configuration so it starts automatically at system boot
mkdir -p /etc/systemd/system/docker.service.d
cat > /etc/systemd/system/docker.service.d/override.conf << 'EOF'
[Service]
ExecStart=
ExecStart=/usr/bin/dockerd -H fd:// --containerd=/run/containerd/containerd.sock
EOF

# Add the current user to the docker group, optional
sudo usermod -aG docker root

# Verify Docker installation
echo "Verify Docker installation..."
docker --version
docker compose version

# Install standalone Docker Compose if needed.
echo "Install standalone Docker Compose..."
DOCKER_COMPOSE_VERSION="v2.24.5"
sudo curl -L "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Verify Docker Compose installation
docker-compose --version

echo "=== Docker installation complete ==="

# Create Docker Compose configuration file
echo "=== Creating Docker Compose configuration ==="
mkdir -p /opt/kafka-docker
cat > /opt/kafka-docker/docker-compose.yml << 'EOF'
version: '3.8'

services:
  zookeeper:
    image: zookeeper:3.8.3
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOO_MY_ID: 1
      ZOO_SERVERS: server.1=0.0.0.0:2888:3888;2181
    volumes:
      - zookeeper_data:/data
      - zookeeper_datalog:/datalog
    networks:
      - kafka-network

  kafka:
    image: bitnami/kafka:3.6.1
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_CFG_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CFG_LISTENERS: INTERNAL://:29092,EXTERNAL://:9092
      KAFKA_CFG_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
      KAFKA_CFG_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_CFG_DELETE_TOPIC_ENABLE: "true"
      ALLOW_PLAINTEXT_LISTENER: "yes"
    volumes:
      - kafka_data:/bitnami/kafka
    depends_on:
      - zookeeper
    networks:
      - kafka-network

volumes:
  zookeeper_data:
  zookeeper_datalog:
  kafka_data:

networks:
  kafka-network:
    driver: bridge
EOF

echo "Docker Compose configuration file created: /opt/kafka-docker/docker-compose.yml"

# Create start script
cat > /opt/kafka-docker/start-kafka.sh << 'EOF'
#!/bin/bash
cd /opt/kafka-docker

# Pull images that are not available locally.
echo "Check and pull Docker images..."
docker pull zookeeper:3.8.3
docker pull bitnami/kafka:3.6.1

# Start services
docker-compose up -d
echo "Kafka and Zookeeper started"
echo "Zookeeperport: 2181"
echo "Kafkaport: 9092"
EOF

chmod +x /opt/kafka-docker/start-kafka.sh

# Create stop script
cat > /opt/kafka-docker/stop-kafka.sh << 'EOF'
#!/bin/bash
cd /opt/kafka-docker
docker-compose down
echo "Kafka and Zookeeper stopped"
EOF

chmod +x /opt/kafka-docker/stop-kafka.sh

# Create status-check script
cat > /opt/kafka-docker/check-kafka.sh << 'EOF'
#!/bin/bash
echo "=== Kafka and Zookeeper status check ==="
echo "DockerContainer status:"
docker ps | grep -E "(kafka|zookeeper)" || echo "No matching running containers found"

echo ""
echo "Port listenersStatus:"
netstat -tlnp | grep -E ":(2181|9092)" || echo "Ports are not listening"

echo ""
echo "Test connectivity:"
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
EOF

chmod +x /opt/kafka-docker/check-kafka.sh

echo "=== Docker and Kafka configuration complete ==="
echo "Usage:"
echo "  Start Kafka: /opt/kafka-docker/start-kafka.sh"
echo "  Stop Kafka: /opt/kafka-docker/stop-kafka.sh"
echo "  CheckStatus: /opt/kafka-docker/check-kafka.sh"
