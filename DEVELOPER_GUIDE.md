# Go Data Sync - Developer Guide

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Project Structure](#project-structure)
3. [Development Setup](#development-setup)
4. [Core Components](#core-components)
5. [Configuration System](#configuration-system)
6. [License Validation](#license-validation)
7. [Filtering System](#filtering-system)
8. [WebSocket Communication](#websocket-communication)
9. [Database Integration](#database-integration)
10. [Testing](#testing)
11. [Building and Deployment](#building-and-deployment)
12. [Troubleshooting](#troubleshooting)

## Architecture Overview

Go Data Sync is a real-time data synchronization system designed to sync MongoDB collections between cloud and local environments. The system consists of two main components:

- **cloud-sync**: Server component that runs in the cloud, manages WebSocket connections, and serves as the data source
- **vm-sync**: Client component that runs locally, connects to cloud-sync, and maintains local MongoDB replica

### High-Level Architecture

```
┌─────────────────┐    WebSocket     ┌─────────────────┐
│   Cloud-Sync    │◄────────────────►│    VM-Sync      │
│   (Server)      │                  │   (Client)      │
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

## Project Structure

```
go-data-sync-http/
├── cmd/                    # Application entry points
│   ├── cloud-sync/         # Cloud server main
│   └── vm-sync/            # VM client main
├── pkg/                    # Core packages
│   ├── cluster/           # Cluster management
│   ├── config/            # Configuration handling
│   ├── crypto/            # Cryptographic utilities
│   ├── fence/             # Fencing mechanisms
│   ├── filtering/         # Data filtering logic
│   ├── license/           # License validation
│   ├── logging/           # Structured logging
│   ├── memory/            # Memory management
│   ├── metrics/           # Performance metrics
│   ├── models/            # Data models
│   ├── mongodb/           # MongoDB operations
│   ├── parallel/          # Parallel processing
│   ├── resilience/        # Error handling & retry
│   ├── resume/            # Resume functionality
│   ├── sequence/          # Sequence management
│   ├── sync/              # Synchronization logic
│   ├── tracking/          # Progress tracking
│   ├── transport/         # WebSocket transport
│   └── watermarks/        # Sync watermarks
├── examples/              # Configuration examples
│   ├── cloud-config.yaml # Cloud sync config
│   └── vm-config.yaml    # VM sync config
├── bin/                   # Compiled binaries
├── web/                   # Web dashboard
└── test/                  # Test files
```

## Development Setup

### Prerequisites

- Go 1.21 or later
- MongoDB 4.4 or later
- Git
- Make (optional)

### Environment Setup

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd go-data-sync-http
   ```

2. **Install dependencies:**
   ```bash
   go mod download
   ```

3. **Set up environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

4. **Build the applications:**
   ```bash
   go build -o bin/cloud-sync ./cmd/cloud-sync
   go build -o bin/vm-sync ./cmd/vm-sync
   ```

### Development Environment Variables

```bash
# License Configuration
CLOUD_SYNC_LICENSE="your-cloud-license-key"
VM_SYNC_LICENSE="your-vm-license-key"

# MongoDB Configuration
MONGO_URI="mongodb://localhost:27017"
CLOUD_MONGO_URI="mongodb+srv://user:pass@cluster.mongodb.net"

# Logging
LOG_LEVEL="debug"
LOG_FORMAT="json"

# WebSocket Configuration
WS_PORT="8080"
WS_HOST="0.0.0.0"
```

## Core Components

### 1. Configuration System (`pkg/config`)

The configuration system supports YAML-based configuration with environment variable overrides.

```go
type Config struct {
    License    LicenseConfig    `yaml:"license"`
    MongoDB    MongoDBConfig    `yaml:"mongodb"`
    WebSocket  WebSocketConfig  `yaml:"websocket"`
    Sync       SyncConfig       `yaml:"sync"`
    Logging    LoggingConfig    `yaml:"logging"`
}
```

**Key Features:**
- YAML configuration files
- Environment variable overrides
- Validation and defaults
- Hot-reload capability

### 2. License Validation (`pkg/license`)

Secure license validation system with UUID-based keys.

```go
type License struct {
    Tag  string `json:"tag"`
    UUID string `json:"uuid"`
}

func ValidateLicense(licenseKey string) (*License, error)
func IsValidUUID(uuid string) bool
```

**Security Features:**
- UUID format validation
- Tag-based license types
- Environment variable protection
- WebSocket authentication

### 3. Filtering System (`pkg/filtering`)

Advanced filtering for documents and fields during synchronization.

```go
type DocumentFilter struct {
    Field    string      `yaml:"field"`
    Operator string      `yaml:"operator"`
    Value    interface{} `yaml:"value"`
}

type FieldFilter struct {
    Include []string `yaml:"include"`
    Exclude []string `yaml:"exclude"`
}
```

**Filter Types:**
- **Document Filters**: Filter documents based on field values
- **Field Filters**: Include/exclude specific fields
- **Combined Filters**: Apply both document and field filtering

### 4. WebSocket Transport (`pkg/transport`)

Real-time communication between cloud-sync and vm-sync.

```go
type Hub struct {
    clients    map[*Client]bool
    broadcast  chan []byte
    register   chan *Client
    unregister chan *Client
}

type Client struct {
    hub    *Hub
    conn   *websocket.Conn
    send   chan []byte
    license *license.License
}
```

**Features:**
- License-based authentication
- Message broadcasting
- Connection management
- Error handling and reconnection

### 5. MongoDB Integration (`pkg/mongodb`)

MongoDB operations with change stream support.

```go
type Client struct {
    client   *mongo.Client
    database *mongo.Database
}

func (c *Client) WatchChanges(ctx context.Context, pipeline []bson.M) (*mongo.ChangeStream, error)
func (c *Client) SyncCollection(ctx context.Context, config CollectionConfig) error
```

**Capabilities:**
- Change stream monitoring
- Bulk operations
- Index management
- Connection pooling

## Configuration System

### Cloud-Sync Configuration

```yaml
license:
  required: true
  env_var: "CLOUD_SYNC_LICENSE"

mongodb:
  uri: "mongodb+srv://user:pass@cluster.mongodb.net"
  database: "production-db"
  timeout: 30s

websocket:
  host: "0.0.0.0"
  port: 8080
  path: "/ws"
  max_connections: 1000

sync:
  collections:
    - name: "products"
      document_filters:
        - field: "status"
          operator: "eq"
          value: "active"
        - field: "price"
          operator: "gt"
          value: 0
      field_filters:
        include: ["_id", "name", "price", "category", "status", "createdAt"]

logging:
  level: "info"
  format: "json"
  output: "stdout"
```

### VM-Sync Configuration

```yaml
license:
  required: true
  env_var: "VM_SYNC_LICENSE"

mongodb:
  uri: "mongodb://localhost:27017"
  database: "local-replica"
  timeout: 30s

websocket:
  url: "ws://cloud-sync:8080/ws"
  reconnect_interval: 5s
  max_reconnect_attempts: 10

sync:
  initial_sync: true
  batch_size: 1000
  parallel_workers: 4

logging:
  level: "info"
  format: "text"
  output: "stdout"
```

## License Validation

### License Key Format

License keys follow the format: `{tag}-{uuid}`

Example: `987fcdeb-51a2-43d7-b654-321098765432`

### Implementation

```go
// pkg/license/validator.go
func ValidateLicense(licenseKey string) (*License, error) {
    if licenseKey == "" {
        return nil, errors.New("license key is required")
    }
    
    parts := strings.Split(licenseKey, "-")
    if len(parts) < 5 {
        return nil, errors.New("invalid license format")
    }
    
    tag := parts[0]
    uuid := strings.Join(parts[1:], "-")
    
    if !IsValidUUID(uuid) {
        return nil, errors.New("invalid UUID format")
    }
    
    return &License{Tag: tag, UUID: uuid}, nil
}
```

### WebSocket Authentication

```go
// WebSocket upgrade with license validation
func (h *Hub) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
    licenseKey := r.Header.Get("X-License-Key")
    license, err := license.ValidateLicense(licenseKey)
    if err != nil {
        http.Error(w, "Invalid license", http.StatusUnauthorized)
        return
    }
    
    conn, err := upgrader.Upgrade(w, r, nil)
    if err != nil {
        return
    }
    
    client := &Client{
        hub:     h,
        conn:    conn,
        send:    make(chan []byte, 256),
        license: license,
    }
    
    h.register <- client
}
```

## Filtering System

### Document Filtering

Filter documents based on field values before synchronization.

```yaml
document_filters:
  - field: "status"
    operator: "eq"
    value: "active"
  - field: "price"
    operator: "gt"
    value: 0
  - field: "category"
    operator: "in"
    value: ["electronics", "books"]
```

**Supported Operators:**
- `eq`: Equal
- `ne`: Not equal
- `gt`: Greater than
- `gte`: Greater than or equal
- `lt`: Less than
- `lte`: Less than or equal
- `in`: In array
- `nin`: Not in array

### Field Filtering

Include or exclude specific fields during synchronization.

```yaml
field_filters:
  include: ["_id", "name", "price", "status"]
  # OR
  exclude: ["internal_notes", "admin_data"]
```

### Implementation

```go
// pkg/filtering/document.go
func ApplyDocumentFilters(doc bson.M, filters []DocumentFilter) bool {
    for _, filter := range filters {
        if !evaluateFilter(doc, filter) {
            return false
        }
    }
    return true
}

// pkg/filtering/field.go
func ApplyFieldFilters(doc bson.M, filters FieldFilter) bson.M {
    result := bson.M{}
    
    if len(filters.Include) > 0 {
        for _, field := range filters.Include {
            if value, exists := doc[field]; exists {
                result[field] = value
            }
        }
    } else {
        result = doc
        for _, field := range filters.Exclude {
            delete(result, field)
        }
    }
    
    return result
}
```

## WebSocket Communication

### Message Protocol

```go
type Message struct {
    Type      string      `json:"type"`
    Collection string     `json:"collection,omitempty"`
    Operation string      `json:"operation,omitempty"`
    Document  interface{} `json:"document,omitempty"`
    Timestamp time.Time   `json:"timestamp"`
}
```

**Message Types:**
- `sync_start`: Begin synchronization
- `sync_complete`: Synchronization finished
- `document_change`: Real-time document change
- `error`: Error notification
- `heartbeat`: Connection keepalive

### Client Implementation

```go
// pkg/transport/client.go
func (c *Client) Connect(url string, license string) error {
    header := http.Header{}
    header.Set("X-License-Key", license)
    
    conn, _, err := websocket.DefaultDialer.Dial(url, header)
    if err != nil {
        return err
    }
    
    c.conn = conn
    go c.readPump()
    go c.writePump()
    
    return nil
}

func (c *Client) readPump() {
    defer c.conn.Close()
    
    for {
        var msg Message
        err := c.conn.ReadJSON(&msg)
        if err != nil {
            log.Printf("Read error: %v", err)
            break
        }
        
        c.handleMessage(msg)
    }
}
```

## Database Integration

### Change Stream Monitoring

```go
// pkg/mongodb/changestream.go
func (c *Client) WatchChanges(ctx context.Context, pipeline []bson.M) (*mongo.ChangeStream, error) {
    opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
    
    stream, err := c.database.Watch(ctx, pipeline, opts)
    if err != nil {
        return nil, err
    }
    
    return stream, nil
}

func (c *Client) ProcessChangeStream(ctx context.Context, stream *mongo.ChangeStream, hub *transport.Hub) {
    defer stream.Close(ctx)
    
    for stream.Next(ctx) {
        var change bson.M
        if err := stream.Decode(&change); err != nil {
            log.Printf("Decode error: %v", err)
            continue
        }
        
        msg := transport.Message{
            Type:      "document_change",
            Operation: change["operationType"].(string),
            Document:  change["fullDocument"],
            Timestamp: time.Now(),
        }
        
        hub.Broadcast(msg)
    }
}
```

### Bulk Operations

```go
// pkg/mongodb/bulk.go
func (c *Client) BulkWrite(ctx context.Context, collection string, operations []mongo.WriteModel) error {
    coll := c.database.Collection(collection)
    
    opts := options.BulkWrite().SetOrdered(false)
    result, err := coll.BulkWrite(ctx, operations, opts)
    if err != nil {
        return err
    }
    
    log.Printf("Bulk write result: %+v", result)
    return nil
}
```

## Testing

### Unit Tests

```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run specific package tests
go test ./pkg/license
go test ./pkg/filtering
```

### Integration Tests

```bash
# Run integration tests
go test -tags=integration ./test/

# Run with MongoDB
docker run -d -p 27017:27017 mongo:latest
go test -tags=integration ./test/
```

### Test Configuration

```yaml
# test/config/test-config.yaml
license:
  required: false

mongodb:
  uri: "mongodb://localhost:27017"
  database: "test-db"

logging:
  level: "debug"
  format: "text"
```

## Building and Deployment

### Local Build

```bash
# Build both components
go build -o bin/cloud-sync ./cmd/cloud-sync
go build -o bin/vm-sync ./cmd/vm-sync

# Build with version info
VERSION=$(git describe --tags --always)
go build -ldflags "-X main.version=$VERSION" -o bin/cloud-sync ./cmd/cloud-sync
```

### Cross-Platform Build

```bash
# Linux
GOOS=linux GOARCH=amd64 go build -o bin/cloud-sync-linux ./cmd/cloud-sync
GOOS=linux GOARCH=amd64 go build -o bin/vm-sync-linux ./cmd/vm-sync

# Windows
GOOS=windows GOARCH=amd64 go build -o bin/cloud-sync.exe ./cmd/cloud-sync
GOOS=windows GOARCH=amd64 go build -o bin/vm-sync.exe ./cmd/vm-sync

# macOS
GOOS=darwin GOARCH=amd64 go build -o bin/cloud-sync-darwin ./cmd/cloud-sync
GOOS=darwin GOARCH=amd64 go build -o bin/vm-sync-darwin ./cmd/vm-sync
```

### Docker Build

```bash
# Build cloud-sync image
docker build -f Dockerfile.cloud-sync -t go-data-sync/cloud-sync .

# Build vm-sync image
docker build -f Dockerfile.vm-sync -t go-data-sync/vm-sync .
```

## Troubleshooting

### Common Issues

#### 1. License Validation Errors

```
Error: Invalid license format
```

**Solution:**
- Ensure license key follows format: `{tag}-{uuid}`
- Verify UUID is valid (36 characters with hyphens)
- Check environment variable is set correctly

#### 2. WebSocket Connection Issues

```
Error: WebSocket connection failed
```

**Solution:**
- Verify cloud-sync is running and accessible
- Check firewall settings
- Validate license key in WebSocket headers
- Review network connectivity

#### 3. MongoDB Connection Problems

```
Error: Failed to connect to MongoDB
```

**Solution:**
- Verify MongoDB URI format
- Check database credentials
- Ensure MongoDB is running
- Review network access rules

#### 4. Filtering Not Working

```
Error: Documents not filtered correctly
```

**Solution:**
- Verify filter configuration syntax
- Check field names match document structure
- Review operator usage
- Test filters with sample data

### Debug Mode

Enable debug logging for detailed troubleshooting:

```bash
# Set environment variable
export LOG_LEVEL=debug

# Or in configuration
logging:
  level: "debug"
  format: "json"
```

### Performance Monitoring

```go
// Enable metrics collection
metrics:
  enabled: true
  port: 9090
  path: "/metrics"
```

Access metrics at: `http://localhost:9090/metrics`

### Log Analysis

```bash
# Filter logs by level
jq 'select(.level == "error")' < app.log

# Filter by component
jq 'select(.component == "websocket")' < app.log

# Search for specific errors
grep "connection failed" app.log
```

## Contributing

### Development Workflow

1. **Fork the repository**
2. **Create feature branch**: `git checkout -b feature/new-feature`
3. **Make changes and test**: `go test ./...`
4. **Commit changes**: `git commit -m "Add new feature"`
5. **Push to branch**: `git push origin feature/new-feature`
6. **Create Pull Request**

### Code Standards

- Follow Go formatting: `go fmt ./...`
- Run linter: `golangci-lint run`
- Add tests for new features
- Update documentation
- Follow semantic versioning

### Package Guidelines

- Keep packages focused and cohesive
- Use clear, descriptive names
- Document public APIs
- Handle errors appropriately
- Follow Go best practices

---

**For additional support, please refer to the User Manual or contact the development team.**