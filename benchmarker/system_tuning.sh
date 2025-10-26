#!/bin/bash

set -Eeuxo pipefail

# File descriptor limits
sysctl -w fs.file-max=2097152
sysctl -w fs.nr_open=2097152
ulimit -n 1048576 2>/dev/null || true

# Virtual memory settings
sysctl -w vm.swappiness=10
sysctl -w vm.dirty_ratio=15
sysctl -w vm.dirty_background_ratio=5
sysctl -w vm.vfs_cache_pressure=50

# AIO limits
sysctl -w fs.aio-max-nr=1048576

# Network buffer sizes (256MB max)
sysctl -w net.core.rmem_max=268435456
sysctl -w net.core.wmem_max=268435456
sysctl -w net.core.rmem_default=16777216
sysctl -w net.core.wmem_default=16777216

# TCP buffer auto-tuning
sysctl -w net.ipv4.tcp_rmem="4096 87380 268435456"
sysctl -w net.ipv4.tcp_wmem="4096 65536 268435456"
sysctl -w net.ipv4.tcp_mem="786432 1048576 26777216"

# Connection handling
sysctl -w net.core.somaxconn=65535
sysctl -w net.core.netdev_max_backlog=250000
sysctl -w net.ipv4.tcp_max_syn_backlog=8192
sysctl -w net.ipv4.tcp_syncookies=1

# TCP optimization
sysctl -w net.ipv4.tcp_fin_timeout=15
sysctl -w net.ipv4.tcp_tw_reuse=1
sysctl -w net.ipv4.tcp_max_tw_buckets=2000000
sysctl -w net.ipv4.tcp_slow_start_after_idle=0
sysctl -w net.ipv4.tcp_mtu_probing=1

# Local port range for more connections
sysctl -w net.ipv4.ip_local_port_range="10000 65535"
