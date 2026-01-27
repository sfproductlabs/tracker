####################################################################################
# Tracker with ClickHouse Server + Keeper
# Build: docker build -t tracker .
# Run:   docker run -p 8080:8080 -p 9000:9000 -p 8123:8123 -p 2181:2181 tracker
####################################################################################

FROM debian:bookworm-slim

# Ports: tracker-http(8080), tracker-https(8443), tracker-except(8880),
#        clickhouse-native(9000), clickhouse-http(8123),
#        keeper(2181), interserver(9009), raft(9444)
EXPOSE 8080 8443 8880 9000 8123 2181 9009 9444

####################################################################################
# INSTALL PACKAGES
####################################################################################

RUN apt update && apt upgrade -y && apt install -y \
    apt-transport-https \
    ca-certificates \
    dirmngr \
    gnupg2 \
    curl \
    dnsutils \
    jq \
    gettext-base \
    wget \
    build-essential \
    gcc \
    g++ \
    && apt autoclean -y \
    && apt autoremove -y

# Install Go 1.23.3 from official source
RUN wget https://go.dev/dl/go1.23.3.linux-arm64.tar.gz && \
    rm -rf /usr/local/go && \
    tar -C /usr/local -xzf go1.23.3.linux-arm64.tar.gz && \
    rm go1.23.3.linux-arm64.tar.gz && \
    ln -s /usr/local/go/bin/go /usr/bin/go && \
    ln -s /usr/local/go/bin/gofmt /usr/bin/gofmt

# Install ClickHouse
RUN mkdir -p /etc/apt/keyrings && \
    curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' \
    | gpg --dearmor -o /etc/apt/keyrings/clickhouse-keyring.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" \
    > /etc/apt/sources.list.d/clickhouse.list && \
    apt update && \
    DEBIAN_FRONTEND=noninteractive apt install -y clickhouse-server clickhouse-client && \
    apt autoclean -y && \
    apt autoremove -y

####################################################################################
# SYSTEM TUNING
####################################################################################

RUN echo "root hard nofile 16384" >> /etc/security/limits.conf && \
    echo "root soft nofile 16384" >> /etc/security/limits.conf && \
    echo "* hard nofile 16384" >> /etc/security/limits.conf && \
    echo "* soft nofile 16384" >> /etc/security/limits.conf && \
    echo "net.core.somaxconn = 8192" >> /etc/sysctl.conf && \
    echo "net.ipv4.tcp_max_tw_buckets = 1440000" >> /etc/sysctl.conf && \
    echo "net.ipv6.conf.all.disable_ipv6 = 1" >> /etc/sysctl.conf && \
    echo "net.ipv4.ip_local_port_range = 5000 65000" >> /etc/sysctl.conf && \
    echo "net.ipv4.tcp_fin_timeout = 15" >> /etc/sysctl.conf && \
    echo "net.ipv4.tcp_window_scaling = 1" >> /etc/sysctl.conf && \
    echo "net.ipv4.tcp_syncookies = 1" >> /etc/sysctl.conf && \
    echo "net.ipv4.tcp_max_syn_backlog = 8192" >> /etc/sysctl.conf && \
    echo "fs.file-max=65536" >> /etc/sysctl.conf

####################################################################################
# SETUP
####################################################################################

# Create ClickHouse directories (defaults, can be overridden with ENV vars)
RUN mkdir -p /var/lib/clickhouse /var/log/clickhouse-server /etc/clickhouse-server/config.d && \
    chown -R clickhouse:clickhouse /var/lib/clickhouse /var/log/clickhouse-server

# Note: Actual directories are created at runtime by entrypoint.sh based on env vars

# Setup tracker
WORKDIR /app/tracker
COPY .setup/clickhouse /app/tracker/schema
COPY .setup/geoip /app/tracker/.setup/geoip
COPY .setup/keys /app/tracker/.setup/keys
COPY *.go go.mod go.sum /app/tracker/
COPY config.json /app/tracker/config.json
COPY clickhouse-config.template.xml /app/tracker/
COPY clickhouse-users.xml /etc/clickhouse-server/
COPY entrypoint.sh /app/tracker/
COPY public /app/tracker/public
COPY templates /app/tracker/templates
RUN chmod +x /app/tracker/entrypoint.sh

# Build tracker
RUN cd /app/tracker && go build -o tracker .

####################################################################################
# CONFIGURATION
####################################################################################

# Default cluster configuration (override with -e)
ENV SHARD=1 \
    REPLICA=1 \
    LAYER=1 \
    SERVER_ID=1 \
    CONTAINER_NAME_PATTERN="v4-tracker" \
    NUM_NODES=1 \
    KEEPER_SERVERS="" \
    CLUSTER_NAME="tracker_cluster" \
    CLUSTER_TOPOLOGY="" \
    CLICKHOUSE_DATA_DIR="/var/lib/clickhouse" \
    CLICKHOUSE_LOG_DIR="/var/log/clickhouse-server"

####################################################################################
# STARTUP
####################################################################################

ENTRYPOINT ["/app/tracker/entrypoint.sh"]

####################################################################################
# USAGE EXAMPLES
####################################################################################
#
# 1. Single-node cluster (default):
#    docker build -t tracker .
#    docker run -p 8080:8080 -p 8443:8443 -p 8880:8880 -p 9000:9000 -p 8123:8123 -p 2181:2181 tracker
#
# 2. Three-node cluster (auto-discovery):
#
#    # Create network first
#    docker network create tracker-net
#
#    # Machine 1 (v4-tracker-1):
#    docker run -d --name v4-tracker-1 \
#      --network tracker-net \
#      -p 8080:8080 -p 8443:8443 -p 8880:8880 -p 9000:9000 -p 8123:8123 -p 2181:2181 -p 9444:9444 \
#      -e SHARD=1 \
#      -e SERVER_ID=1 \
#      -e NUM_NODES=3 \
#      -e CONTAINER_NAME_PATTERN="v4-tracker" \
#      tracker
#
#    # Machine 2 (v4-tracker-2):
#    docker run -d --name v4-tracker-2 \
#      --network tracker-net \
#      -p 8080:8080 -p 8443:8443 -p 8880:8880 -p 9000:9000 -p 8123:8123 -p 2181:2181 -p 9444:9444 \
#      -e SHARD=2 \
#      -e SERVER_ID=2 \
#      -e NUM_NODES=3 \
#      -e CONTAINER_NAME_PATTERN="v4-tracker" \
#      tracker
#
#    # Machine 3 (v4-tracker-3):
#    docker run -d --name v4-tracker-3 \
#      --network tracker-net \
#      -p 8080:8080 -p 8443:8443 -p 8880:8880 -p 9000:9000 -p 8123:8123 -p 2181:2181 -p 9444:9444 \
#      -e SHARD=3 \
#      -e SERVER_ID=3 \
#      -e NUM_NODES=3 \
#      -e CONTAINER_NAME_PATTERN="v4-tracker" \
#      tracker
#
# Note: Containers must be on same Docker network for hostname resolution.
#       KEEPER_SERVERS and CLUSTER_TOPOLOGY are auto-generated from NUM_NODES.
#
# 3. AWS/Production deployment with persistent volumes:
#
#    # Using AWS EBS volume mounted at /mnt/clickhouse-data
#    docker run -d --name tracker \
#      -v /mnt/clickhouse-data:/data/clickhouse \
#      -v /mnt/clickhouse-logs:/logs/clickhouse \
#      -p 8080:8080 -p 8443:8443 -p 8880:8880 -p 9000:9000 -p 8123:8123 -p 2181:2181 \
#      -e CLICKHOUSE_DATA_DIR="/data/clickhouse" \
#      -e CLICKHOUSE_LOG_DIR="/logs/clickhouse" \
#      tracker
#
#    # Or using docker-compose with environment variables:
#    environment:
#      - CLICKHOUSE_DATA_DIR=/data/clickhouse
#      - CLICKHOUSE_LOG_DIR=/logs/clickhouse
#    volumes:
#      - /mnt/ebs-volume:/data/clickhouse
#      - /mnt/ebs-logs:/logs/clickhouse
#
####################################################################################
