# Multi-VM Setup Guide

## Overview
This guide explains how to configure multiple VM-sync instances targeting a single database for high availability and load distribution.

## Supported Architectures

### ✅ Scenario 1: Multiple VMs, Different Collections
**Best Practice - Recommended**
```
VM-Sync-1 -> Collections: users, orders
VM-Sync-2 -> Collections: products, inventory
VM-Sync-3 -> Collections: analytics, logs
```

### ✅ Scenario 2: Multiple VMs, Overlapping Collections (Redundancy)
**High Availability Setup**
```
VM-Sync-Primary   -> Collections: users, orders, products
VM-Sync-Secondary -> Collections: users, orders, products (backup)
```

### ✅ Scenario 3: Load Balanced Multiple VMs
**Geographic Distribution**
```
VM-Sync-US-East -> All Collections (US East Region)
VM-Sync-EU-West -> All Collections (EU West Region)
```

## Configuration Requirements

### 1. Unique Client Identification
Each VM-sync instance MUST have:
- **Unique Client ID**: Automatically generated with hostname + PID + timestamp
- **Unique HTTP Port**: Different ports per instance
- **Unique TCP Listen Address**: Different ports for TCP transport
- **Unique Checkpoint Directory**: Separate checkpoint storage

### 2. Configuration Template for Multiple Instances

#### Instance 1 Configuration (`vm-config-1.yaml`)
```yaml
cloud_sync:
  http_url: "http://cloud-sync:8080"
  ws_url: "ws://cloud-sync:8080/ws"

mongodb:
  uri: "mongodb://vm-mongodb:27017/target_db"

collections:
  - "target_db.users"
  - "target_db.orders"

sync:
  transport:
    mode: "tcp"
    tcp_receiver:
      listen_addr: "0.0.0.0:9001"  # Unique port
      checkpoint_dir: "/app/checkpoints/vm-1"  # Unique directory

# Set unique environment variables
# VM_SYNC_PORT=8081
# VM_SYNC_LICENSE=uuid-1
```

#### Instance 2 Configuration (`vm-config-2.yaml`)
```yaml
cloud_sync:
  http_url: "http://cloud-sync:8080"
  ws_url: "ws://cloud-sync:8080/ws"

mongodb:
  uri: "mongodb://vm-mongodb:27017/target_db"

collections:
  - "target_db.products"
  - "target_db.inventory"

sync:
  transport:
    mode: "tcp"
    tcp_receiver:
      listen_addr: "0.0.0.0:9002"  # Unique port
      checkpoint_dir: "/app/checkpoints/vm-2"  # Unique directory

# Set unique environment variables
# VM_SYNC_PORT=8082
# VM_SYNC_LICENSE=uuid-2
```

### 3. Docker Compose for Multiple Instances

```yaml
services:
  vm-sync-1:
    build:
      context: .
      dockerfile: Dockerfile.vm-sync
    container_name: vm-sync-1
    environment:
      - VM_SYNC_LICENSE=987fcdeb-51a2-43d7-b654-321098765432
      - VM_SYNC_PORT=8081
    volumes:
      - ./vm-config-1.yaml:/app/config.yaml:ro
      - vm_sync_1_checkpoints:/app/checkpoints/vm-1
    ports:
      - "8081:8081"  # HTTP
      - "9001:9001"  # TCP
    networks:
      - sync-network

  vm-sync-2:
    build:
      context: .
      dockerfile: Dockerfile.vm-sync
    container_name: vm-sync-2
    environment:
      - VM_SYNC_LICENSE=123e4567-e89b-12d3-a456-426614174000
      - VM_SYNC_PORT=8082
    volumes:
      - ./vm-config-2.yaml:/app/config.yaml:ro
      - vm_sync_2_checkpoints:/app/checkpoints/vm-2
    ports:
      - "8082:8082"  # HTTP
      - "9002:9002"  # TCP
    networks:
      - sync-network

volumes:
  vm_sync_1_checkpoints:
  vm_sync_2_checkpoints:
```

## Coordination Mechanisms

### 1. Client ID Generation (Fixed)
```go
// Automatic unique client ID with hostname, PID, and timestamp
hostname, _ := os.Hostname()
pid := os.Getpid()
clientID = fmt.Sprintf("vm-sync-%s-%d-%d", hostname, pid, time.Now().Unix())
```

### 2. Checkpoint Isolation
- Each instance uses separate checkpoint directories
- No conflicts in resume token management
- Independent recovery capabilities

### 3. TCP Transport Coordination
- Each instance listens on different TCP ports
- Cloud-sync can route to appropriate instance
- Frame-level client identification

## Best Practices

### ✅ DO:
1. **Use Different Collections**: Assign different collections to different instances
2. **Unique Ports**: Ensure HTTP and TCP ports don't conflict
3. **Separate Checkpoints**: Use unique checkpoint directories
4. **Monitor Resources**: Each instance consumes memory/CPU
5. **License Management**: Each instance needs a valid license

### ❌ DON'T:
1. **Same Collection Overlap**: Avoid multiple instances syncing identical collections simultaneously
2. **Shared Checkpoint Dirs**: Don't share checkpoint directories between instances
3. **Identical Ports**: Don't use same ports for different instances
4. **Resource Overload**: Don't run too many instances on limited hardware

## Monitoring Multiple Instances

### Health Checks
```bash
# Check instance 1
curl http://localhost:8081/health

# Check instance 2
curl http://localhost:8082/health
```

### TCP Transport Status
```bash
# Check TCP receiver stats via logs
docker logs vm-sync-1 | grep "TCP RECEIVER"
docker logs vm-sync-2 | grep "TCP RECEIVER"
```

### Collection Sync Status
```bash
# Check specific collection checkpoints
curl http://localhost:8081/api/v1/checkpoint/target_db.users
curl http://localhost:8082/api/v1/checkpoint/target_db.products
```

## Troubleshooting

### Issue: Port Conflicts
**Solution**: Ensure each instance uses unique ports in config and docker-compose

### Issue: Checkpoint Conflicts
**Solution**: Use separate checkpoint directories per instance

### Issue: Client ID Collisions
**Solution**: The new client ID generation includes hostname+PID, preventing collisions

### Issue: License Conflicts
**Solution**: Each instance needs its own valid license UUID

## Performance Considerations

### Memory Usage
- Each instance: ~100-500MB RAM
- Scale based on collection size and throughput

### Network Bandwidth
- TCP transport uses ~1-10 Mbps per instance
- WebSocket connections use minimal bandwidth

### CPU Usage
- Each instance: ~1-2 CPU cores under load
- BSON processing and compression are CPU-intensive

## Summary

Multiple VM-sync instances targeting a single database **IS POSSIBLE** with proper configuration:

1. ✅ **Fixed**: Unique client ID generation
2. ✅ **Supported**: Independent checkpoint management
3. ✅ **Supported**: TCP transport with different ports
4. ✅ **Supported**: WebSocket connections per instance
5. ✅ **Supported**: Collection-level isolation

The system is designed for this use case and works well with proper configuration!