# Go Data Sync - Deployment Guide

## 🚀 **Overview**

This guide covers deployment scenarios from local development to production Kubernetes clusters with cross-domain architecture support.

## 📋 **Prerequisites**

### **System Requirements**
- **CPU**: 2+ cores (8+ for production)
- **Memory**: 4GB+ (16GB+ for production)
- **Storage**: 20GB+ (500GB+ SSD for production)
- **Go**: 1.21+, **MongoDB**: 5.0+, **Docker**: 20.10+, **Kubernetes**: 1.25+

### **Network Ports**
```
Cloud-Sync: 8080 (HTTP/WebSocket), 9090 (Metrics)
VM-Sync: 8081 (HTTP), 9000 (TCP)
MongoDB: 27017
```

---

## 💻 **Local Development**

### **Quick Start**
```bash
# Build and start
git clone <repo>
cd go-data-sync-http
make runnables

# Start MongoDB with replica set
docker run -d --name mongodb -p 27017:27017 mongo:5.0 --replSet rs0
docker exec mongodb mongosh --eval \"rs.initiate()\"

# Start services (separate terminals)
./runnables/scripts/start-cloud-sync.sh
./runnables/scripts/start-vm-sync.sh

# Verify
curl http://localhost:8080/health
curl http://localhost:8081/health
open http://localhost:8080/dashboard
```

---

## 🐳 **Docker Deployment**

### **Single Node Setup**
```bash
# Create environment
cat > .env << EOF
MONGODB_URI=mongodb://mongodb:27017/?replicaSet=rs0
VM_SYNC_CLIENT_ID=vm_sync_dev_client
VM_SYNC_CLIENT_SECRET=dev_secret_key
ENCRYPTION_KEY=dGVzdC1lbmNyeXB0aW9uLWtleS0yNTYtYml0cy1mb3ItYWVzLWdj
ENCRYPTION_KEY_ID=cloud-sync-key-001
EOF

# Deploy
docker-compose up -d
docker-compose logs -f cloud-sync

# Scale VM clients
docker-compose up -d --scale vm-sync=3
```

### **Multi-VM Configuration**
```yaml
# docker-compose-multi.yml
version: '3.8'
services:
  cloud-sync:
    build: .
    ports: [\"8080:8080\"]
    environment:
      - MONGODB_URI=mongodb://mongodb:27017/?replicaSet=rs0
  
  vm-sync-1:
    build: .
    ports: [\"8081:8081\", \"9001:9000\"]
    environment:
      - VM_SYNC_PORT=8081
  
  vm-sync-2:
    build: .
    ports: [\"8082:8081\", \"9002:9000\"]
    environment:
      - VM_SYNC_PORT=8081
```

---

## ☸️ **Kubernetes Deployment**

### **Single Cluster**
```bash
# Setup
kubectl create namespace data-sync
./k8s-configs/create-k8s-secrets.sh

# Deploy
kubectl apply -f k8s-configs/

# Monitor
kubectl get all -n data-sync
kubectl logs -f deployment/cloud-sync -n data-sync

# Test
kubectl port-forward service/cloud-sync-service 8080:80 -n data-sync
```

### **Cross-Domain Architecture**
```
┌─────────────────────┐     ┌─────────────────────┐
│    darshan.com      │────▶│      xyz.com        │
│   Cloud-Sync        │     │    VM-Sync          │
│   - HTTP :8080      │     │   - HTTP :8081      │
│   - TCP Sender      │     │   - TCP Recv :9000  │
└─────────────────────┘     └─────────────────────┘
```

**Key Configuration:**
```yaml
# ⚠️ CRITICAL: Both services must use host: \"0.0.0.0\" in Kubernetes
server:
  host: \"0.0.0.0\"  # Required for K8s networking
  port: 8080       # cloud-sync
  # port: 8081     # vm-sync

# Cross-domain communication
cloud_sync:
  transport:
    tcp_sender:
      address: \"xyz.com:9000\"  # Target VM domain

vm_sync:
  cloud_sync:
    http_url: \"https://darshan.com\"
    ws_url: \"wss://darshan.com/ws\"
```

---

## 🏭 **Production Deployment**

### **Production Checklist**
- [ ] TLS certificates for both domains
- [ ] OAuth2 credentials in secrets
- [ ] Resource limits and requests
- [ ] High availability (multiple replicas)
- [ ] Monitoring and alerting
- [ ] Backup strategy
- [ ] Network policies

### **High Availability Setup**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cloud-sync
spec:
  replicas: 3  # HA setup
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 1
  template:
    spec:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
          - labelSelector:
              matchLabels:
                app: cloud-sync
            topologyKey: kubernetes.io/hostname
      containers:
      - name: cloud-sync
        resources:
          requests:
            memory: \"1Gi\"
            cpu: \"500m\"
          limits:
            memory: \"4Gi\"
            cpu: \"2000m\"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 10
```

### **Production Configuration**
```yaml
# Optimized for production
server:
  host: \"0.0.0.0\"
  read_timeout: \"60s\"
  write_timeout: \"60s\"
  idle_timeout: \"300s\"

sync:
  transport:
    mode: \"tcp\"
    compression_type: \"zstd\"
    tcp_sender:
      parallel_conns: 16
      buffer_size: 4194304    # 4MB
      max_batch_size: 268435456  # 256MB
    tcp_receiver:
      max_connections: 32
      read_timeout: 300s

encryption:
  enabled: true
  algorithm: \"AES-256-GCM\"
  key_id: \"${ENCRYPTION_KEY_ID}\"
  key: \"${ENCRYPTION_KEY}\"
```

---

## 📊 **Monitoring**

### **Health Checks**
```bash
# Comprehensive health check
#!/bin/bash
echo \"=== Health Check ===\"
kubectl get pods -n data-sync
curl -s http://localhost:8080/health || echo \"Cloud-Sync: FAILED\"
curl -s http://localhost:8081/health || echo \"VM-Sync: FAILED\"
kubectl top pods -n data-sync
echo \"=== Complete ===\"
```

### **Key Alerts**
```yaml
# Critical alerts
- alert: CloudSyncDown
  expr: up{job=\"cloud-sync\"} == 0
  for: 5m
  
- alert: HighSyncLatency
  expr: sync_latency_seconds > 30
  for: 10m
  
- alert: VMSyncDisconnected
  expr: websocket_connections{type=\"vm-sync\"} == 0
  for: 5m
```

---

## 🔧 **Troubleshooting**

### **Common Issues**

**1. Connection Refused**
```bash
# Check if services are running
kubectl get pods -n data-sync
kubectl logs deployment/cloud-sync -n data-sync

# Test connectivity
kubectl exec -it deployment/vm-sync -- curl http://cloud-sync-service:8080/health
```

**2. Authentication Failed**
```bash
# Check secrets
kubectl get secrets -n data-sync

# Test OAuth2 token
curl -X POST https://darshan.com/api/auth/token \\n  -H \"Content-Type: application/json\" \\n  -d '{\"grant_type\":\"client_credentials\",\"client_id\":\"...\",\"client_secret\":\"...\"}'
```

**3. Slow Performance**
```bash
# Check resource usage
kubectl top pods -n data-sync

# Verify TCP transport
kubectl logs deployment/cloud-sync -n data-sync | grep \"TCP\"

# Solutions:
# - Increase resource limits
# - Enable TCP transport
# - Tune batch sizes
# - Enable compression
```

### **Performance Tuning**
- **TCP Transport**: 5x faster than HTTP
- **Batch Size**: Increase for better throughput
- **Compression**: Enable Zstd for bandwidth savings
- **Parallel Connections**: More for high-volume sync
- **Resource Limits**: Scale based on data volume

---

## 🎯 **Deployment Commands Reference**

```bash
# Local Development
make runnables
./runnables/scripts/start-cloud-sync.sh
./runnables/scripts/start-vm-sync.sh

# Docker
docker-compose up -d
docker-compose logs -f cloud-sync
docker-compose scale vm-sync=3

# Kubernetes
kubectl create namespace data-sync
./k8s-configs/create-k8s-secrets.sh
kubectl apply -f k8s-configs/
kubectl get pods -n data-sync
kubectl logs -f deployment/cloud-sync -n data-sync

# Health Checks
curl http://localhost:8080/health
curl http://localhost:8081/health
curl http://localhost:8080/api/sync/status

# Monitoring
open http://localhost:8080/dashboard
kubectl port-forward service/cloud-sync-service 8080:80 -n data-sync
```

---

**For detailed architecture information, see [ARCHITECTURE_SOLUTION.md](ARCHITECTURE_SOLUTION.md)**