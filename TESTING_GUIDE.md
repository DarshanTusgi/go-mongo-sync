# 🧪 Go Data Sync Testing Guide

## 📦 Built Executables

The following executables are ready for testing in the `bin/` directory:

### Native Binaries (macOS)
- `bin/cloud-sync` (12M) - Cloud sync server for macOS
- `bin/vm-sync` (9.7M) - VM sync client for macOS

### Cross-Platform Binaries
- `bin/cloud-sync-linux` (13M) - Cloud sync server for Linux
- `bin/vm-sync-linux` (10M) - VM sync client for Linux
- `bin/cloud-sync.exe` (13M) - Cloud sync server for Windows
- `bin/vm-sync.exe` (10M) - VM sync client for Windows
- `bin/cloud-sync-darwin` (13M) - Cloud sync server for macOS (alternative)
- `bin/vm-sync-darwin` (10M) - VM sync client for macOS (alternative)

## 🚀 Quick Test Commands

### Basic Binary Verification
```bash
# Verify binaries work
./bin/cloud-sync --help
./bin/vm-sync --help

# Check binary info
file bin/cloud-sync
file bin/vm-sync
```

### Configuration Test
```bash
# Test with example configs
./bin/cloud-sync -config examples/cloud-config.yaml
./bin/vm-sync -config examples/vm-config.yaml
```

## 🧪 Test Scenarios

### 1. Basic Functionality Test
```bash
# Terminal 1: Start cloud-sync
./bin/cloud-sync -config examples/cloud-config.yaml

# Terminal 2: Start vm-sync
./bin/vm-sync -config examples/vm-config.yaml

# Terminal 3: Check health endpoints
curl http://localhost:8080/health
curl http://localhost:8081/health  # vm-sync health
```

### 2. Docker Test
```bash
# Build and test Docker images
make docker
docker-compose up -d

# Check container status
docker-compose ps
docker-compose logs cloud-sync
docker-compose logs vm-sync
```

### 3. Transport Protocol Test
```bash
# Test WebSocket connection
curl -H "Upgrade: websocket" -H "Connection: Upgrade" \
     -H "Sec-WebSocket-Key: test" -H "Sec-WebSocket-Version: 13" \
     http://localhost:8080/ws

# Test TCP port (should be listening)
telnet localhost 9000
```

### 4. Dashboard Test
```bash
# Access web dashboard
open http://localhost:8080/dashboard

# Check metrics endpoint
curl http://localhost:9090/metrics
```

### 5. License Validation Test
```bash
# Test with valid license
export CLOUD_SYNC_LICENSE="987fcdeb-51a2-43d7-b654-321098765432"
export VM_SYNC_LICENSE="987fcdeb-51a2-43d7-b654-321098765432"

./bin/cloud-sync -config examples/cloud-config.yaml

# Test with invalid license (should fail)
export VM_SYNC_LICENSE="invalid-license"
./bin/vm-sync -config examples/vm-config.yaml
```

## 🔍 Performance Test

### Memory and CPU Usage
```bash
# Monitor resource usage
ps aux | grep -E "(cloud-sync|vm-sync)"
top -p $(pgrep cloud-sync)

# Test with load
./bin/cloud-sync -config examples/cloud-config.yaml &
sleep 5
./bin/vm-sync -config examples/vm-config.yaml &

# Monitor for 5 minutes
watch -n 1 'ps aux | grep -E "(cloud-sync|vm-sync)" | grep -v grep'
```

### Connection Test
```bash
# Test multiple connections
for i in {1..5}; do
    curl -s http://localhost:8080/health &
done
wait

# Check connection metrics
curl -s http://localhost:9090/metrics | grep connection
```

## 🐛 Error Testing

### Configuration Errors
```bash
# Test with missing config
./bin/cloud-sync -config non-existent.yaml

# Test with invalid MongoDB URI
# Edit examples/cloud-config.yaml and set invalid mongodb.uri
./bin/cloud-sync -config examples/cloud-config.yaml
```

### Network Errors
```bash
# Test port conflicts
./bin/cloud-sync -config examples/cloud-config.yaml &
./bin/cloud-sync -config examples/cloud-config.yaml  # Should fail

# Test MongoDB connection failure
# Stop MongoDB and try to start services
./bin/cloud-sync -config examples/cloud-config.yaml
```

## 📊 Expected Results

### ✅ Success Indicators
- Binaries start without errors
- Health endpoints return 200 OK
- WebSocket connections establish successfully
- TCP port 9000 accepts connections
- Dashboard loads at http://localhost:8080
- Metrics available at http://localhost:9090/metrics
- License validation works correctly

### ❌ Failure Scenarios to Test
- Invalid configuration files
- Missing MongoDB connection
- Port conflicts
- Invalid licenses
- Network connectivity issues
- Resource exhaustion

## 🔧 Debugging Commands

### Log Analysis
```bash
# Check application logs
./bin/cloud-sync -config examples/cloud-config.yaml 2>&1 | tee cloud-sync.log
./bin/vm-sync -config examples/vm-config.yaml 2>&1 | tee vm-sync.log

# Parse JSON logs
tail -f cloud-sync.log | jq .
tail -f vm-sync.log | jq .
```

### Network Debugging
```bash
# Check listening ports
netstat -tulpn | grep -E "(8080|9000|8081|8082|9090)"
lsof -i :8080
lsof -i :9000

# Test connectivity
nc -zv localhost 8080
nc -zv localhost 9000
```

### Performance Profiling
```bash
# Enable CPU profiling
go tool pprof http://localhost:8080/debug/pprof/profile

# Memory profiling
go tool pprof http://localhost:8080/debug/pprof/heap
```

## 📋 Test Checklist

- [ ] All binaries execute without errors
- [ ] Configuration files load correctly
- [ ] MongoDB connections establish
- [ ] WebSocket server starts on port 8080
- [ ] TCP server starts on port 9000
- [ ] Health checks pass
- [ ] Dashboard accessible
- [ ] Metrics endpoint functional
- [ ] License validation works
- [ ] Docker images build successfully
- [ ] Cross-platform binaries work
- [ ] Error handling graceful
- [ ] Resource usage reasonable
- [ ] Network protocols function correctly

## 🆘 Common Issues & Solutions

### Issue: "Permission denied"
```bash
chmod +x bin/cloud-sync bin/vm-sync
```

### Issue: "Port already in use"
```bash
# Find and kill processes using the port
lsof -ti:8080 | xargs kill -9
lsof -ti:9000 | xargs kill -9
```

### Issue: "MongoDB connection failed"
```bash
# Start MongoDB locally
brew services start mongodb-community
# Or with Docker
docker run -d -p 27017:27017 mongo:7.0
```

### Issue: "Config file not found"
```bash
# Use absolute path or copy config
cp examples/cloud-config.yaml ./config.yaml
./bin/cloud-sync  # Uses default config.yaml
```

---

## 📞 Contact

If you encounter any issues during testing, please:
1. Check the logs for error messages
2. Verify configuration settings
3. Ensure all prerequisites are met
4. Report findings with log outputs

Happy testing! 🚀