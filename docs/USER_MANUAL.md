# Go Data Sync - User Manual

## Table of Contents

1. [Introduction](#introduction)
2. [System Requirements](#system-requirements)
3. [Quick Start Guide](#quick-start-guide)
4. [Installation](#installation)
5. [Configuration](#configuration)
6. [Running the System](#running-the-system)
7. [Data Filtering](#data-filtering)
8. [Monitoring and Logs](#monitoring-and-logs)
9. [Troubleshooting](#troubleshooting)
10. [FAQ](#faq)
11. [Support](#support)

## Introduction

Go Data Sync is a real-time data synchronization solution that keeps your MongoDB collections synchronized between cloud and local environments. It provides:

- **Real-time synchronization** of MongoDB collections
- **Selective data filtering** to sync only what you need
- **Secure license-based authentication**
- **Automatic reconnection** and error recovery
- **Web dashboard** for monitoring sync status

### How It Works

```
┌─────────────────┐    Real-time     ┌─────────────────┐
│   Cloud Data    │    WebSocket     │   Local Data    │
│   (MongoDB)     │─────────────────►│   (MongoDB)     │
│                 │   Sync Process   │                 │
└─────────────────┘                  └─────────────────┘
        ▲                                      ▲
        │                                      │
   cloud-sync                              vm-sync
   (Server)                               (Client)
```

## System Requirements

### Minimum Requirements

- **Operating System**: Linux, macOS, or Windows
- **Memory**: 512 MB RAM
- **Storage**: 100 MB free space
- **Network**: Internet connection for cloud sync

### Software Dependencies

- **MongoDB**: Version 4.4 or later
- **Go Runtime**: Version 1.21 or later (for building from source)

### Network Requirements

- **Outbound**: Port 443 (HTTPS) and custom WebSocket port
- **Inbound**: WebSocket port (default 8080) for cloud-sync
- **MongoDB**: Standard MongoDB ports (27017)

## Quick Start Guide

### Step 1: Download Binaries

Download the latest release for your platform:

```bash
# Linux
wget https://github.com/your-org/go-data-sync/releases/latest/download/go-data-sync-linux.tar.gz
tar -xzf go-data-sync-linux.tar.gz

# macOS
wget https://github.com/your-org/go-data-sync/releases/latest/download/go-data-sync-darwin.tar.gz
tar -xzf go-data-sync-darwin.tar.gz

# Windows
# Download and extract go-data-sync-windows.zip
```

### Step 2: Get Your License Keys

Contact your administrator to obtain:
- Cloud sync license key
- VM sync license key

License keys follow the format: `987fcdeb-51a2-43d7-b654-321098765432`

### Step 3: Basic Configuration

Create configuration files:

**cloud-config.yaml:**
```yaml
license:
  required: true
  env_var: "CLOUD_SYNC_LICENSE"

mongodb:
  uri: "mongodb+srv://user:password@cluster.mongodb.net"
  database: "your-database"

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
```

**vm-config.yaml:**
```yaml
license:
  required: true
  env_var: "VM_SYNC_LICENSE"

mongodb:
  uri: "mongodb://localhost:27017"
  database: "local-replica"

websocket:
  url: "ws://your-cloud-server:8080/ws"
```

### Step 4: Set Environment Variables

```bash
# Set license keys
export CLOUD_SYNC_LICENSE="your-cloud-license-key"
export VM_SYNC_LICENSE="your-vm-license-key"
```

### Step 5: Start Services

```bash
# Start cloud sync (on server)
./bin/cloud-sync -config cloud-config.yaml

# Start VM sync (on client)
./bin/vm-sync -config vm-config.yaml
```

## Installation

### Option 1: Binary Installation

1. **Download the appropriate binary** for your platform
2. **Extract the archive** to your desired location
3. **Make binaries executable** (Linux/macOS):
   ```bash
   chmod +x bin/cloud-sync bin/vm-sync
   ```
4. **Add to PATH** (optional):
   ```bash
   export PATH=$PATH:/path/to/go-data-sync/bin
   ```

### Option 2: Docker Installation

```bash
# Pull Docker images
docker pull go-data-sync/cloud-sync:latest
docker pull go-data-sync/vm-sync:latest

# Run with Docker Compose
docker-compose up -d
```

### Option 3: Build from Source

```bash
# Clone repository
git clone https://github.com/your-org/go-data-sync.git
cd go-data-sync

# Build binaries
go build -o bin/cloud-sync ./cmd/cloud-sync
go build -o bin/vm-sync ./cmd/vm-sync
```

## Configuration

### Cloud Sync Configuration

The cloud sync server requires configuration for:

#### License Settings
```yaml
license:
  required: true                    # Enable license validation
  env_var: "CLOUD_SYNC_LICENSE"     # Environment variable name
```

#### MongoDB Connection
```yaml
mongodb:
  uri: "mongodb+srv://user:pass@cluster.mongodb.net"
  database: "production-db"         # Source database name
  timeout: 30s                      # Connection timeout
  pool_size: 10                     # Connection pool size
```

#### WebSocket Server
```yaml
websocket:
  host: "0.0.0.0"                   # Listen address
  port: 8080                        # Listen port
  path: "/ws"                       # WebSocket endpoint
  max_connections: 1000             # Maximum concurrent connections
  read_timeout: 60s                 # Read timeout
  write_timeout: 60s                # Write timeout
```

#### Synchronization Settings
```yaml
sync:
  collections:
    - name: "products"              # Collection name
      document_filters:             # Filter documents
        - field: "status"
          operator: "eq"
          value: "active"
        - field: "price"
          operator: "gt"
          value: 0
      field_filters:                # Filter fields
        include: ["_id", "name", "price", "category"]
    
    - name: "users"
      field_filters:
        exclude: ["password", "internal_notes"]
```

#### Logging Configuration
```yaml
logging:
  level: "info"                     # Log level: debug, info, warn, error
  format: "json"                    # Format: json, text
  output: "stdout"                  # Output: stdout, stderr, file path
  file: "/var/log/cloud-sync.log"   # Log file (if output is file path)
```

### VM Sync Configuration

The VM sync client requires:

#### License Settings
```yaml
license:
  required: true
  env_var: "VM_SYNC_LICENSE"
```

#### Local MongoDB
```yaml
mongodb:
  uri: "mongodb://localhost:27017"
  database: "local-replica"         # Local database name
  timeout: 30s
```

#### WebSocket Client
```yaml
websocket:
  url: "ws://cloud-server:8080/ws"  # Cloud sync WebSocket URL
  reconnect_interval: 5s            # Reconnection interval
  max_reconnect_attempts: 10        # Maximum reconnection attempts
  ping_interval: 30s                # Ping interval for keepalive
```

#### Sync Behavior
```yaml
sync:
  initial_sync: true                # Perform initial full sync
  batch_size: 1000                  # Batch size for bulk operations
  parallel_workers: 4               # Number of parallel workers
  resume_on_restart: true           # Resume from last position
```

### Environment Variables

You can override configuration values using environment variables:

```bash
# License keys
export CLOUD_SYNC_LICENSE="your-license-key"
export VM_SYNC_LICENSE="your-license-key"

# MongoDB URIs
export MONGO_URI="mongodb://localhost:27017"
export CLOUD_MONGO_URI="mongodb+srv://user:pass@cluster.mongodb.net"

# Logging
export LOG_LEVEL="debug"
export LOG_FORMAT="json"

# WebSocket
export WS_PORT="8080"
export WS_HOST="0.0.0.0"
```

## Running the System

### Starting Cloud Sync

```bash
# Basic startup
./bin/cloud-sync -config examples/cloud-config.yaml

# With environment variables
CLOUD_SYNC_LICENSE="your-license" ./bin/cloud-sync -config cloud-config.yaml

# With custom log level
LOG_LEVEL=debug ./bin/cloud-sync -config cloud-config.yaml

# Background process
nohup ./bin/cloud-sync -config cloud-config.yaml > cloud-sync.log 2>&1 &
```

### Starting VM Sync

```bash
# Basic startup
./bin/vm-sync -config examples/vm-config.yaml

# With environment variables
VM_SYNC_LICENSE="your-license" ./bin/vm-sync -config vm-config.yaml

# Background process
nohup ./bin/vm-sync -config vm-config.yaml > vm-sync.log 2>&1 &
```

### Using Docker

#### Docker Compose Setup

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  cloud-sync:
    image: go-data-sync/cloud-sync:latest
    ports:
      - "8080:8080"
    environment:
      - CLOUD_SYNC_LICENSE=${CLOUD_SYNC_LICENSE}
    volumes:
      - ./cloud-config.yaml:/app/config.yaml
    command: ["-config", "/app/config.yaml"]

  vm-sync:
    image: go-data-sync/vm-sync:latest
    environment:
      - VM_SYNC_LICENSE=${VM_SYNC_LICENSE}
    volumes:
      - ./vm-config.yaml:/app/config.yaml
    command: ["-config", "/app/config.yaml"]
    depends_on:
      - cloud-sync

  mongodb:
    image: mongo:latest
    ports:
      - "27017:27017"
    volumes:
      - mongodb_data:/data/db

volumes:
  mongodb_data:
```

Start services:

```bash
# Set environment variables
export CLOUD_SYNC_LICENSE="your-cloud-license"
export VM_SYNC_LICENSE="your-vm-license"

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f cloud-sync
docker-compose logs -f vm-sync
```

### Service Management

#### Systemd Service (Linux)

Create `/etc/systemd/system/cloud-sync.service`:

```ini
[Unit]
Description=Go Data Sync Cloud Service
After=network.target

[Service]
Type=simple
User=datasync
WorkingDirectory=/opt/go-data-sync
Environment=CLOUD_SYNC_LICENSE=your-license-key
ExecStart=/opt/go-data-sync/bin/cloud-sync -config /opt/go-data-sync/cloud-config.yaml
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl enable cloud-sync
sudo systemctl start cloud-sync
sudo systemctl status cloud-sync
```

## Data Filtering

### Document Filtering

Filter which documents to synchronize based on field values:

```yaml
sync:
  collections:
    - name: "products"
      document_filters:
        # Only active products
        - field: "status"
          operator: "eq"
          value: "active"
        
        # Only products with price > 0
        - field: "price"
          operator: "gt"
          value: 0
        
        # Only specific categories
        - field: "category"
          operator: "in"
          value: ["electronics", "books", "clothing"]
        
        # Exclude test products
        - field: "name"
          operator: "ne"
          value: "test"
```

#### Supported Operators

| Operator | Description | Example |
|----------|-------------|----------|
| `eq` | Equal | `value: "active"` |
| `ne` | Not equal | `value: "inactive"` |
| `gt` | Greater than | `value: 100` |
| `gte` | Greater than or equal | `value: 0` |
| `lt` | Less than | `value: 1000` |
| `lte` | Less than or equal | `value: 999` |
| `in` | In array | `value: ["a", "b", "c"]` |
| `nin` | Not in array | `value: ["test", "demo"]` |

### Field Filtering

Control which fields are synchronized:

```yaml
sync:
  collections:
    - name: "users"
      field_filters:
        # Include only specific fields
        include: ["_id", "name", "email", "status"]
    
    - name: "products"
      field_filters:
        # Exclude sensitive fields
        exclude: ["internal_notes", "cost_price", "supplier_info"]
```

### Combined Filtering

Use both document and field filters together:

```yaml
sync:
  collections:
    - name: "orders"
      document_filters:
        # Only completed orders from last 30 days
        - field: "status"
          operator: "eq"
          value: "completed"
        - field: "created_at"
          operator: "gte"
          value: "2024-01-01T00:00:00Z"
      field_filters:
        # Exclude payment details
        exclude: ["payment_method", "card_details", "billing_address"]
```

## Monitoring and Logs

### Web Dashboard

Access the web dashboard at: `http://your-server:8080/dashboard`

The dashboard shows:
- Connection status
- Sync progress
- Error messages
- Performance metrics

### Log Levels

- **DEBUG**: Detailed debugging information
- **INFO**: General information about operations
- **WARN**: Warning messages for potential issues
- **ERROR**: Error messages for failures

### Log Formats

#### JSON Format
```json
{
  "level": "info",
  "timestamp": "2024-01-15T10:30:00Z",
  "component": "websocket",
  "message": "Client connected",
  "client_id": "abc123",
  "license_tag": "987fcdeb"
}
```

#### Text Format
```
2024-01-15T10:30:00Z INFO [websocket] Client connected client_id=abc123 license_tag=987fcdeb
```

### Monitoring Commands

```bash
# View real-time logs
tail -f cloud-sync.log
tail -f vm-sync.log

# Filter by log level
grep "ERROR" cloud-sync.log
grep "WARN" vm-sync.log

# Monitor with JSON logs
tail -f cloud-sync.log | jq 'select(.level == "error")'

# Check connection status
curl http://localhost:8080/health
```

### Performance Metrics

Metrics are available at: `http://your-server:8080/metrics`

Key metrics:
- `sync_documents_total`: Total documents synchronized
- `sync_errors_total`: Total synchronization errors
- `websocket_connections`: Active WebSocket connections
- `mongodb_operations_total`: MongoDB operations count

## Troubleshooting

### Common Issues

#### 1. License Validation Failed

**Error:**
```
ERROR License validation failed: invalid license format
```

**Solutions:**
- Verify license key format: `{tag}-{uuid}`
- Check environment variable is set: `echo $CLOUD_SYNC_LICENSE`
- Ensure no extra spaces or characters
- Contact administrator for valid license

#### 2. WebSocket Connection Failed

**Error:**
```
ERROR WebSocket connection failed: dial tcp: connection refused
```

**Solutions:**
- Verify cloud-sync is running: `ps aux | grep cloud-sync`
- Check port accessibility: `telnet server-ip 8080`
- Review firewall settings
- Verify WebSocket URL in vm-config.yaml

#### 3. MongoDB Connection Issues

**Error:**
```
ERROR Failed to connect to MongoDB: connection timeout
```

**Solutions:**
- Test MongoDB connection: `mongosh "your-mongodb-uri"`
- Verify credentials and permissions
- Check network connectivity
- Review MongoDB URI format

#### 4. Sync Not Working

**Error:**
```
WARN No documents synchronized
```

**Solutions:**
- Check document filters are not too restrictive
- Verify source collection has matching documents
- Review field filter configuration
- Enable debug logging: `LOG_LEVEL=debug`

#### 5. High Memory Usage

**Symptoms:**
- Process consuming excessive memory
- System becoming slow

**Solutions:**
- Reduce batch size in configuration
- Decrease parallel workers
- Add field filters to reduce document size
- Monitor with: `top -p $(pgrep cloud-sync)`

### Debug Mode

Enable detailed logging:

```bash
# Set debug level
export LOG_LEVEL=debug

# Or in configuration
logging:
  level: "debug"
  format: "json"
```

Debug logs include:
- WebSocket message details
- MongoDB operation details
- Filter evaluation results
- Performance timing information

### Health Checks

```bash
# Check cloud-sync health
curl http://localhost:8080/health

# Expected response
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z",
  "connections": 2,
  "uptime": "2h30m15s"
}
```

### Log Analysis

```bash
# Count error messages
grep -c "ERROR" cloud-sync.log

# Find connection issues
grep "connection" vm-sync.log

# Monitor sync progress
grep "documents synchronized" cloud-sync.log

# Check license issues
grep "license" *.log
```

## FAQ

### General Questions

**Q: How often does synchronization occur?**
A: Synchronization happens in real-time. Initial sync occurs when vm-sync starts, then changes are synchronized immediately as they occur in the source database.

**Q: Can I sync multiple databases?**
A: Currently, each instance syncs one database. You can run multiple instances with different configurations for multiple databases.

**Q: What happens if the network connection is lost?**
A: vm-sync automatically attempts to reconnect with exponential backoff. When reconnected, it resumes from the last synchronized position.

**Q: Is the data encrypted during transmission?**
A: Yes, when using WSS (WebSocket Secure), data is encrypted in transit. Configure your cloud-sync with TLS certificates for production use.

### Configuration Questions

**Q: Can I change filters without restarting?**
A: Currently, configuration changes require a restart. Hot-reload functionality is planned for future releases.

**Q: How do I sync only recent data?**
A: Use document filters with date fields:
```yaml
document_filters:
  - field: "created_at"
    operator: "gte"
    value: "2024-01-01T00:00:00Z"
```

**Q: Can I exclude large fields to save bandwidth?**
A: Yes, use field filters to exclude large fields:
```yaml
field_filters:
  exclude: ["large_blob", "image_data", "attachments"]
```

### Performance Questions

**Q: How can I improve sync performance?**
A: 
- Increase batch size for bulk operations
- Add more parallel workers
- Use field filters to reduce document size
- Ensure proper MongoDB indexing

**Q: What's the maximum number of documents that can be synced?**
A: There's no hard limit, but performance depends on document size, network bandwidth, and system resources.

### Troubleshooting Questions

**Q: Why are some documents not syncing?**
A: Check your document filters. Documents must match ALL filter criteria to be synchronized.

**Q: How do I reset the sync state?**
A: Stop vm-sync, clear the local database, and restart vm-sync to perform a fresh initial sync.

## Support

### Getting Help

1. **Check this manual** for common issues and solutions
2. **Review logs** with debug level enabled
3. **Test configuration** with minimal settings
4. **Contact support** with detailed error information

### Support Information to Provide

- Go Data Sync version
- Operating system and version
- Configuration files (remove sensitive data)
- Error logs with timestamps
- Steps to reproduce the issue

### Contact Information

- **Email**: support@your-company.com
- **Documentation**: https://docs.your-company.com/go-data-sync
- **GitHub Issues**: https://github.com/your-org/go-data-sync/issues

### Community Resources

- **User Forum**: https://forum.your-company.com
- **Knowledge Base**: https://kb.your-company.com
- **Video Tutorials**: https://videos.your-company.com

---

**Thank you for using Go Data Sync! For technical details, please refer to the Developer Guide.**