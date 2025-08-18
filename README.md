# Go Data Sync

[![Go Version](https://img.shields.io/badge/Go-1.21+-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/Docker-Ready-blue.svg)](docker-compose.yml)

A high-performance, real-time data synchronization system for MongoDB collections between cloud and local environments.

## 🚀 Quick Start

### Using Docker Compose (Recommended)

```bash
# Clone the repository
git clone <repository-url>
cd go-data-sync-http

# Set your license keys
export CLOUD_SYNC_LICENSE="your-cloud-license-key"
export VM_SYNC_LICENSE="your-vm-license-key"

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f cloud-sync
docker-compose logs -f vm-sync
```

### Using Binaries

```bash
# Build the applications
go build -o bin/cloud-sync ./cmd/cloud-sync
go build -o bin/vm-sync ./cmd/vm-sync

# Set license keys
export CLOUD_SYNC_LICENSE="your-license-key"
export VM_SYNC_LICENSE="your-license-key"

# Start cloud sync (server)
./bin/cloud-sync -config examples/cloud-config.yaml

# Start vm sync (client)
./bin/vm-sync -config examples/vm-config.yaml
```

## 📋 Features

- **Real-time Synchronization**: Instant data sync using MongoDB change streams
- **Selective Filtering**: Document and field-level filtering for optimized sync
- **Secure Authentication**: License-based WebSocket authentication
- **Automatic Recovery**: Built-in reconnection and error handling
- **Web Dashboard**: Real-time monitoring and status visualization
- **Docker Ready**: Complete containerization with Docker Compose
- **Production Ready**: Comprehensive logging, metrics, and health checks

## 🏗️ Architecture

```
┌─────────────────┐    WebSocket     ┌─────────────────┐
│   Cloud-Sync    │◄────────────────►│    VM-Sync      │
│   (Server)      │   Secure Auth    │   (Client)      │
├─────────────────┤                  ├─────────────────┤
│ • License Auth  │                  │ • License Auth  │
│ • WebSocket Hub │                  │ • Data Receiver │
│ • Data Provider │                  │ • Local Storage │
│ • Change Stream │                  │ • Sync Manager  │
└─────────────────┘                  └─────────────────┘
        │                                      │
        ▼                                      ▼
┌─────────────────┐                  ┌─────────────────┐
│  Cloud MongoDB  │                  │  Local MongoDB  │
│   (Source)      │                  │   (Replica)     │
└─────────────────┘                  └─────────────────┘
```

## 📚 Documentation

- **[User Manual](USER_MANUAL.md)** - Complete setup and usage guide
- **[Developer Guide](DEVELOPER_GUIDE.md)** - Technical implementation details
- **[Configuration Examples](examples/)** - Sample configuration files

## 🔧 Configuration

### Basic Cloud Sync Configuration

```yaml
license:
  required: true
  env_var: "CLOUD_SYNC_LICENSE"

mongodb:
  uri: "mongodb+srv://user:pass@cluster.mongodb.net"
  database: "production-db"

websocket:
  host: "0.0.0.0"
  port: 8080

sync:
  collections:
    - name: "products"
      document_filters:
        - field: "status"
          operator: "eq"
          value: "active"
      field_filters:
        include: ["_id", "name", "price", "category"]
```

### Basic VM Sync Configuration

```yaml
license:
  required: true
  env_var: "VM_SYNC_LICENSE"

mongodb:
  uri: "mongodb://localhost:27017"
  database: "local-replica"

websocket:
  url: "ws://cloud-server:8080/ws"

sync:
  initial_sync: true
  batch_size: 1000
```

## 🐳 Docker Deployment

### Build Images

```bash
# Build cloud-sync image
docker build -f Dockerfile.cloud-sync -t go-data-sync/cloud-sync .

# Build vm-sync image
docker build -f Dockerfile.vm-sync -t go-data-sync/vm-sync .
```

### Run with Docker Compose

```bash
# Start all services
docker-compose up -d

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

- **Documentation**: [User Manual](USER_MANUAL.md) | [Developer Guide](DEVELOPER_GUIDE.md)
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