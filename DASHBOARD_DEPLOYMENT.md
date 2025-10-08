# 🚀 GOD MODE Dashboard - Deployment Guide

## 🎯 Quick Start

### 1. **Build and Start Services**
```bash
# Build optimized binaries
make build

# Start cloud-sync service (includes dashboard)
./bin/cloud-sync

# Start vm-sync service (optional, for full functionality)
./bin/vm-sync
```

### 2. **Access Dashboard**
```bash
# Open in browser
http://localhost:8080/dashboard

# Or test API endpoints
curl http://localhost:8080/api/metrics
curl http://localhost:8080/health
```

---

## 📊 Dashboard Features Verification

### **Real-Time Metrics** ✅
- Navigate to: `http://localhost:8080/dashboard`
- Verify metrics auto-refresh every 5 seconds
- Check WebSocket connection indicator (green dot)

### **API Endpoints** ✅
- `GET /api/metrics` - Comprehensive system metrics
- `GET /health` - System health with component status
- `GET /api/logs` - Enhanced logging with search/filter
- `POST /api/control/{action}` - System control actions

### **Enhanced Logging** ✅
- Real-time log streaming with correlation IDs
- Advanced filtering by stage, status, search
- Stack trace capture for errors
- Performance timing information

---

## 🛠️ Configuration

### **Environment Variables**
```bash
# Optional: Configure logging level
export LOG_LEVEL=debug

# Optional: Configure server port
export PORT=8080
```

### **Config Files**
- `cloud-config.yaml` - Main cloud-sync configuration
- `vm-config.yaml` - VM-sync client configuration

---

## 🔧 Troubleshooting

### **Dashboard Not Loading**
```bash
# Check service status
ps aux | grep cloud-sync

# Check port binding
netstat -tulpn | grep :8080

# Check logs
tail -f /var/log/cloud-sync.log
```

### **API Errors**
```bash
# Test endpoints directly
curl -v http://localhost:8080/health
curl -v http://localhost:8080/api/metrics

# Check service logs
./bin/cloud-sync 2>&1 | grep -i error
```

### **WebSocket Connection Issues**
- Verify firewall allows port 8080
- Check browser console for WebSocket errors
- Ensure no proxy blocking WebSocket upgrades

---

## 🚀 Production Deployment

### **Docker Deployment**
```bash
# Build and deploy with Docker Compose
docker-compose up -d

# Access dashboard through nginx proxy
http://localhost/dashboard
```

### **Kubernetes Deployment**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cloud-sync-dashboard
spec:
  replicas: 1
  selector:
    matchLabels:
      app: cloud-sync
  template:
    metadata:
      labels:
        app: cloud-sync
    spec:
      containers:
      - name: cloud-sync
        image: go-data-sync:latest
        ports:
        - containerPort: 8080
        env:
        - name: LOG_LEVEL
          value: "info"
```

### **Nginx Reverse Proxy**
```nginx
server {
    listen 80;
    server_name dashboard.example.com;
    
    location / {
        proxy_pass http://localhost:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
    
    location /ws {
        proxy_pass http://localhost:8080;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

---

## 📈 Monitoring & Alerts

### **Health Monitoring**
```bash
# Health check endpoint for monitoring tools
curl -f http://localhost:8080/health || exit 1

# Metrics for Prometheus
curl http://localhost:8080/metrics
```

### **Log Aggregation**
```bash
# Forward logs to centralized logging
./bin/cloud-sync 2>&1 | logger -t cloud-sync

# Or use structured logging output
export LOG_FORMAT=json
./bin/cloud-sync >> /var/log/cloud-sync.json
```

---

## 🎉 Success Verification

When successfully deployed, you should see:

### **Dashboard Interface** ✅
- Modern glassmorphism design with real-time updates
- Pipeline status showing connection health
- Key metrics with live data updates
- Enhanced logging table with filtering
- WebSocket connection indicator (green dot)

### **API Responses** ✅
```json
// GET /health
{
  "source_mongo": "connected",
  "cloud_sync": "connected",
  "vm_sync": "connected",
  "system_health": {
    "memory_usage_mb": 45.2,
    "cpu_usage_percent": 12.3,
    "active_connections": 2
  },
  "features": {
    "tcp_transport": true,
    "encryption": true,
    "buffer_free": true
  }
}

// GET /api/metrics  
{
  "dashboard_metrics": {
    "total_documents": 150000,
    "sync_rate": 245.7,
    "active_watchers": 6,
    "avg_latency": 23.4
  },
  "enhanced_logging": {
    "total_entries": 1250,
    "error_entries": 3,
    "warning_entries": 12
  }
}
```

### **Real-Time Updates** ✅
- Metrics refresh every 5 seconds
- Log entries stream in real-time
- Connection status updates immediately
- WebSocket maintains persistent connection

---

## 🛡️ Security Considerations

### **Access Control**
- Dashboard runs on same port as API (8080)
- No authentication by default (add nginx auth for production)
- WebSocket connections from same origin only

### **Production Hardening**
```bash
# Run as non-root user
useradd -r -s /bin/false cloudsync
sudo -u cloudsync ./bin/cloud-sync

# Bind to localhost only
export BIND_ADDRESS=127.0.0.1

# Use TLS in production
# Configure nginx with SSL termination
```

---

## 📞 Support

### **Logs Location**
- Application logs: `stdout/stderr` or configured log file
- Enhanced logging: Available via `/api/logs` endpoint
- System logs: `/var/log/syslog` (if using systemd)

### **Debug Mode**
```bash
# Enable debug logging
export LOG_LEVEL=debug
./bin/cloud-sync

# Or check specific components
curl "http://localhost:8080/api/logs?search=error&limit=100"
```

### **Performance Monitoring**
```bash
# Monitor resource usage
top -p $(pgrep cloud-sync)

# Check memory usage
curl -s http://localhost:8080/api/metrics | jq '.system_metrics.memory_usage'

# Monitor connections
netstat -an | grep :8080
```

---

**🎉 Your GOD MODE Dashboard is now deployed and operational!**

The comprehensive monitoring interface provides enterprise-grade visibility into your data synchronization system with real-time metrics, enhanced logging, and professional monitoring capabilities.