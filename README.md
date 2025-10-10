# Go Data Sync
# Go Data Sync - High-Performance MongoDB Synchronization

[![Go Version](https://img.shields.io/badge/Go-1.21+-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/Docker-Ready-blue.svg)](docker-compose.yml)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Ready-blue.svg)](k8s-configs/)

A high-performance, production-ready data synchronization system for MongoDB collections between cloud and edge environments. Features multi-protocol transport (TCP, WebSocket, HTTP), OAuth2 authentication, adaptive resource management, and Kubernetes-native deployment.

## 🚀 **Key Features**

- **🏎️ High-Performance TCP Transport**: 5x faster than HTTP with binary protocol
- **🔄 Real-Time Synchronization**: MongoDB change streams with WebSocket delivery
- **🔐 OAuth2 Security**: JWT-based authentication with client credentials flow
- **🎯 Adaptive Resource Management**: Dynamic parameter tuning based on system load
- **☸️ Kubernetes Native**: Ready for production container orchestration
- **🛡️ Fault Tolerant**: Automatic recovery, resume tokens, connection health monitoring
- **📊 Observable**: Built-in metrics, logging, and web dashboard
- **🎛️ Zero-Config VM Management**: Automatic collection distribution

## 🏗️ **Architecture Overview**

```
┌─────────────────┐    Multi-Protocol   ┌─────────────────┐
│   Cloud-Sync    │◄─────────────────►│    VM-Sync      │
│   (darshan.com) │   OAuth2 + TLS    │   (xyz.com)     │
├─────────────────┤                   ├─────────────────┤
│ • REST API      │                   │ • Data Receiver │
│ • WebSocket Hub │                   │ • TCP Receiver  │
│ • TCP Sender    │                   │ • HTTP Server   │
│ • OAuth2 Auth   │                   │ • Sync Engine   │
│ • Web Dashboard │                   │ • Health Checks │
└─────────────────┘                   └─────────────────┘
        │                                      │
        ▼                                      ▼
┌─────────────────┐                  ┌─────────────────┐
│  Cloud MongoDB  │                  │  Local MongoDB  │
│   (Source)      │                  │   (Replica)     │
│ • Change Streams│                  │ • Sync Target   │
│ • Resume Tokens │                  │ • Checkpoints   │
└─────────────────┘                  └─────────────────┘
```

## 🛠️ **Technology Stack**

### **Backend Technologies**
- **Language**: Go 1.21+
- **Database**: MongoDB 5.0+ with Change Streams
- **Authentication**: OAuth2 JWT with HMAC-SHA256
- **Transport**: TCP (binary), WebSocket (real-time), HTTP/REST (fallback)
- **Compression**: Zstd, LZ4 algorithms
- **Encryption**: AES-256-GCM

### **Infrastructure**
- **Containerization**: Docker with multi-stage builds
- **Orchestration**: Kubernetes with Helm charts
- **Networking**: Nginx Ingress, LoadBalancer services
- **Monitoring**: Prometheus metrics, structured logging
- **Security**: TLS 1.3, OAuth2, RBAC

### **Development Tools**
- **Build System**: Make with automated scripts
- **Code Quality**: Go fmt, Go vet, Static analysis
- **Testing**: Unit tests, Integration tests, Benchmarks
- **Documentation**: Comprehensive guides and API docs

## 📁 **Project Structure**

```
go-data-sync-http/
├── cmd/                    # Application entry points
│   ├── cloud-sync/         # Cloud synchronization server
│   └── vm-sync/            # VM synchronization client
├── pkg/                    # Reusable packages
│   ├── adaptive/           # Resource-aware adaptive management
│   ├── auth/              # OAuth2 JWT authentication
│   ├── transport/         # Multi-protocol transport layer
│   ├── telemetry/         # Performance metrics collection
│   ├── resume/            # Resume token management
│   ├── models/            # Data models and configurations
│   └── ...
├── config/                # Configuration files
│   ├── cloud-sync-config.yaml
│   └── vm-sync-config.yaml
├── k8s-configs/           # Kubernetes deployment manifests
│   ├── cloud-sync-deployment.yaml
│   ├── vm-sync-deployment.yaml
│   └── ...
├── docs/                  # Comprehensive documentation
│   ├── ARCHITECTURE_SOLUTION.md
│   ├── DEPLOYMENT_GUIDE.md
│   └── api-swagger.yaml
├── web/                   # Web dashboard frontend
├── scripts/               # Build and deployment scripts
└── Makefile              # Build automation
```

## ⚡ **Quick Start**

### **Option 1: Using Make (Recommended)**

```bash
# Clone and build everything
git clone <repository-url>
cd go-data-sync-http

# Build binaries and generate start scripts
make runnables

# Start services (separate terminals)
./runnables/scripts/start-cloud-sync.sh
./runnables/scripts/start-vm-sync.sh

# Check status
curl http://localhost:8080/health
curl http://localhost:8081/health
```

### **Option 2: Docker Compose**

```bash
# Start all services with MongoDB
docker-compose up -d

# View logs
docker-compose logs -f cloud-sync
docker-compose logs -f vm-sync

# Scale VM clients
docker-compose up -d --scale vm-sync=3
```

### **Option 3: Kubernetes**

```bash
# Create namespace and secrets
kubectl create namespace data-sync
./k8s-configs/create-k8s-secrets.sh

# Deploy services
kubectl apply -f k8s-configs/

# Check status
kubectl get pods -n data-sync
kubectl logs -f deployment/cloud-sync -n data-sync
```

## ⚙️ **Configuration**

### **Server Configuration**

```yaml
# Cloud-Sync (config/cloud-sync-config.yaml)
server:
  port: 8080
  host: "0.0.0.0"          # ⚠️ Must be 0.0.0.0 for Kubernetes
  read_timeout: "30s"
  write_timeout: "30s"
  idle_timeout: "120s"

# VM-Sync (config/vm-sync-config.yaml)
server:
  port: 8081
  host: "0.0.0.0"          # ⚠️ Must be 0.0.0.0 for Kubernetes
  read_timeout: "60s"
  write_timeout: "60s"
  idle_timeout: "300s"
```

### **Transport Configuration**

```yaml
# High-performance TCP transport
transport:
  mode: "tcp"              # Options: tcp, http, websocket
  http_fallback: true      # Fallback to HTTP if TCP fails
  compression_type: "zstd" # Options: zstd, lz4, none
  
  tcp_sender:              # Cloud-sync TCP settings
    address: "xyz.com:9000"
    parallel_conns: 8
    buffer_size: 1048576   # 1MB
    max_batch_size: 67108864 # 64MB
  
  tcp_receiver:            # VM-sync TCP settings
    listen_addr: "0.0.0.0:9000"
    max_connections: 20
    disk_checkpoint: true
```

### **OAuth2 Authentication**

```yaml
cloud_sync:
  oauth2:
    enabled: true
    client_id: "${VM_SYNC_CLIENT_ID}"        # From environment
    client_secret: "${VM_SYNC_CLIENT_SECRET}" # From environment  
    token_url: "https://darshan.com/api/auth/token"
```

## 🚀 **Performance Benchmarks**

| Transport | Throughput | Latency | CPU Usage | Memory |
|-----------|------------|---------|-----------|--------|
| **TCP** | 50k docs/sec | 10ms | 40% | 256MB |
| **WebSocket** | 25k docs/sec | 20ms | 50% | 384MB |
| **HTTP** | 10k docs/sec | 50ms | 60% | 512MB |

### **TCP Transport Benefits**
- 🔥 **5x Higher Throughput** than HTTP
- ⚡ **5x Lower Latency** for bulk operations
- 📦 **50% Less Memory** usage
- 🛡️ **Automatic Resume** on network failures
- 🗜️ **Advanced Compression** (Zstd/LZ4)

## 🌐 **API Endpoints**

### **Cloud-Sync API** (Port 8080)

```bash
# Health check
GET /health

# Trigger initial sync
POST /api/sync/initial
{
  "client_id": "optional-specific-client",
  "force": false
}

# Get sync status
GET /api/sync/status

# Web dashboard
GET /dashboard

# WebSocket endpoint
WS /ws

# OAuth2 token endpoint
POST /api/auth/token
```

### **VM-Sync API** (Port 8081)

```bash
# Health check
GET /health

# Push data endpoint (used by cloud-sync)
POST /api/v1/push/{database}/{collection}

# TCP receiver (binary protocol)
TCP :9000
```

## 📊 **Monitoring & Observability**

### **Web Dashboard**
```bash
# Access real-time dashboard
http://localhost:8080/dashboard

# Features:
- Live connection status
- Sync progress monitoring
- Performance metrics
- Error tracking
- Resource usage graphs
```

### **Metrics Endpoints**
```bash
# Prometheus-compatible metrics
GET /metrics

# Custom metrics API
GET /api/metrics/charts

# Health status with details
GET /api/health
```

### **Logging**
```bash
# Structured JSON logs
{"level":"info","time":"2024-01-01T12:00:00Z","component":"sync","message":"Document synced","docs_count":1000}

# Log levels: debug, info, warn, error
# Configurable output: stdout, file, syslog
```

## 🔐 **Security Features**

### **Authentication & Authorization**
- 🔑 **OAuth2 Client Credentials**: Industry-standard authentication
- 🎫 **JWT Tokens**: Stateless, cryptographically signed
- 🔒 **HMAC-SHA256**: Secure token signing
- ⏰ **Token Expiration**: Automatic refresh every hour

### **Data Protection**
- 🛡️ **AES-256-GCM Encryption**: End-to-end data encryption
- 🚀 **TLS 1.3**: Transport layer security
- 🔐 **BCrypt Hashing**: Secure credential storage
- 🎯 **Scope-based Access**: Fine-grained permissions

## 🚧 **Development**

### **Local Development Setup**

```bash
# Install dependencies
go mod download

# Run tests
go test ./...

# Build and run locally
make runnables
./runnables/scripts/start-cloud-sync.sh
./runnables/scripts/start-vm-sync.sh

# Development with hot reload
air  # Requires 'air' tool for auto-recompilation
```

### **Build Commands**

```bash
# Build everything
make all

# Build specific services
make cloud-sync
make vm-sync

# Build Docker images
make docker-build

# Generate deployment scripts
make runnables

# Clean build artifacts
make clean
```

## 📖 **Documentation**

Comprehensive documentation is available in the `docs/` directory:

- **[Architecture Solution](docs/ARCHITECTURE_SOLUTION.md)** - Complete system architecture
- **[Deployment Guide](docs/DEPLOYMENT_GUIDE.md)** - Production deployment instructions
- **[API Documentation](docs/api-swagger.yaml)** - OpenAPI/Swagger specification

## 🤝 **Contributing**

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 **License**

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 **Support & Troubleshooting**

### **Common Issues**

1. **Connection Refused**: Check if services are running and ports are accessible
2. **Authentication Failed**: Verify OAuth2 credentials and token endpoints
3. **Sync Slow**: Consider switching from HTTP to TCP transport
4. **Memory Issues**: Adjust batch sizes and enable compression

### **Getting Help**

- 📖 Check the [documentation](docs/)
- 🐛 Open an [issue](issues/) for bugs
- 💬 Start a [discussion](discussions/) for questions
- 📧 Contact maintainers for urgent issues

---

**Built with ❤️ for high-performance data synchronization**

# Scale vm-sync clients
docker-compose up -d --scale vm-sync=3

# View service status
docker-compose ps

# Stop all services
docker-compose down
```

## 📊 Monitoring

### Web Dashboard
Access the dashboard at: `http://localhost:8080/dashboard`

### Metrics
Prometheus metrics available at: `http://localhost:8080/metrics`

### Health Checks
```bash
# Check cloud-sync health
curl http://localhost:8080/health

# Check service status
docker-compose ps
```

## 🔍 Data Filtering

### Document Filtering
Filter documents based on field values:

```yaml
document_filters:
  - field: "status"
    operator: "eq"
    value: "active"
  - field: "price"
    operator: "gt"
    value: 0
```

### Field Filtering
Control which fields are synchronized:

```yaml
field_filters:
  include: ["_id", "name", "price", "status"]
  # OR
  exclude: ["internal_notes", "sensitive_data"]
```

## 🚨 Troubleshooting

### Common Issues

1. **License Validation Failed**
   ```bash
   # Check license format: {tag}-{uuid}
   echo $CLOUD_SYNC_LICENSE
   ```

2. **WebSocket Connection Failed**
   ```bash
   # Test connectivity
   telnet cloud-server 8080
   ```

3. **MongoDB Connection Issues**
   ```bash
   # Test MongoDB connection
   mongosh "your-mongodb-uri"
   ```

### Debug Mode
```bash
# Enable debug logging
export LOG_LEVEL=debug

# Or in Docker Compose
LOG_LEVEL=debug docker-compose up
```

## 🧪 Testing

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run integration tests
go test -tags=integration ./test/
```

## 📦 Building

### Local Build
```bash
# Build both components
make build

# Or manually
go build -o bin/cloud-sync ./cmd/cloud-sync
go build -o bin/vm-sync ./cmd/vm-sync
```

### Cross-Platform Build
```bash
# Linux
GOOS=linux GOARCH=amd64 go build -o bin/cloud-sync-linux ./cmd/cloud-sync

# Windows
GOOS=windows GOARCH=amd64 go build -o bin/cloud-sync.exe ./cmd/cloud-sync

# macOS
GOOS=darwin GOARCH=amd64 go build -o bin/cloud-sync-darwin ./cmd/cloud-sync
```

## 🔐 Security

- **License Authentication**: Secure WebSocket connections with license validation
- **TLS Support**: Enable WSS for encrypted data transmission
- **Non-root Containers**: Docker images run as non-privileged users
- **Input Validation**: Comprehensive validation of all inputs

## 📈 Performance

- **Parallel Processing**: Configurable worker pools for optimal throughput
- **Batch Operations**: Efficient bulk MongoDB operations
- **Memory Management**: Optimized memory usage with streaming
- **Connection Pooling**: MongoDB connection pooling for scalability

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-feature`
3. Make changes and test: `go test ./...`
4. Commit changes: `git commit -m "Add new feature"`
5. Push to branch: `git push origin feature/new-feature`
6. Create Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Documentation**: [docs/USER_MANUAL.md](docs/USER_MANUAL.md) | [docs/DEVELOPER_GUIDE.md](docs/DEVELOPER_GUIDE.md)
- **Issues**: [GitHub Issues](https://github.com/your-org/go-data-sync/issues)
- **Email**: support@your-company.com

## 🎯 Roadmap

- [ ] Hot configuration reload
- [ ] Multi-database support
- [ ] Advanced conflict resolution
- [ ] GraphQL API
- [ ] Kubernetes operators
- [ ] Advanced monitoring dashboards

---

**Made with ❤️ by the Go Data Sync Team**