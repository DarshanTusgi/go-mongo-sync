# How to Use the Transport Library as a Standalone Module

The transport library in `pkg/transport/` is designed to be a reusable, standalone package that can be used in any Go project requiring high-performance bulk data transfer over TCP.

## Option 1: Using as a Package within this Project

Since the library is already implemented within this project, you can use it directly:

```go
import "go-data-sync-http/pkg/transport"
```

## Option 2: Extracting as a Separate Module

To create a completely separate Go module that can be imported by other projects:

### Step 1: Create a New Module

```bash
# Create a new directory for the standalone library
mkdir go-bulk-transport
cd go-bulk-transport

# Initialize as a new Go module
go mod init github.com/your-org/go-bulk-transport
```

### Step 2: Copy the Transport Package

Copy the following files from this project:

```bash
# Copy the transport package files
cp /path/to/go-data-sync-http/pkg/transport/*.go ./

# Copy the example
mkdir examples
cp /path/to/go-data-sync-http/examples/transport_example.go ./examples/
```

### Step 3: Update Module Dependencies

```bash
# Add required dependencies
go get github.com/klauspost/compress/zstd
go get github.com/pierrec/lz4/v4
```

### Step 4: Update Imports in Example

In the copied example file, change the import from:
```go
import "go-data-sync-http/pkg/transport"
```

To:
```go
import "github.com/your-org/go-bulk-transport"
```

## Option 3: Git Subtree (Recommended for Sharing)

If you want to maintain the library as part of this project but also make it available as a standalone module:

```bash
# Create a subtree of just the transport package
git subtree push --prefix=pkg/transport origin transport-lib

# In a new repository
git clone -b transport-lib https://github.com/your-org/go-data-sync-http.git go-bulk-transport
cd go-bulk-transport

# Initialize as new module
go mod init github.com/your-org/go-bulk-transport
```

## Using the Library in Other Projects

Once you have the library as a separate module, other projects can use it:

### 1. Install the Library

```bash
go get github.com/your-org/go-bulk-transport
```

### 2. Use in Your Project

```go
package main

import (
    "log"
    "time"
    
    "github.com/your-org/go-bulk-transport"
    "go.mongodb.org/mongo-driver/bson"
)

func main() {
    // Create and start receiver
    receiver, err := transport.NewReceiver(transport.ReceiverConfig{
        ListenAddr: "0.0.0.0:9000",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer receiver.Stop()

    // Handle incoming batches
    receiver.OnBatch(func(stream string, batchSeq uint64, docs [][]byte) error {
        log.Printf("Received %d documents for stream %s", len(docs), stream)
        
        // Parse BSON and process documents
        for _, doc := range docs {
            var parsed bson.M
            if err := bson.Unmarshal(doc, &parsed); err != nil {
                return err
            }
            
            // Process the document
            log.Printf("Processing document: %+v", parsed)
        }
        
        return nil
    })

    // Start the receiver
    if err := receiver.Start(); err != nil {
        log.Fatal(err)
    }
    
    // Keep running
    select {}
}
```

## API Reference

### Core Types

```go
type Sender interface {
    SendBatch(stream string, batch [][]byte) error
    SendBatchAsync(stream string, batch [][]byte) error
    WaitForAcks(timeout time.Duration) error
    Resume(stream string, fromSeq uint64) error
    Stats() SenderStats
    Close() error
}

type Receiver interface {
    OnBatch(handler BatchHandler)
    OnError(handler ErrorHandler)
    Start() error
    Stop() error
    Stats() ReceiverStats
    GetCheckpoint(stream string) uint64
    SetCheckpoint(stream string, seq uint64) error
}
```

### Configuration

```go
type SenderConfig struct {
    Address       string
    ParallelConns int
    WindowSize    int
    Compression   CompressionType
    TLSConfig     *tls.Config
    // ... other fields
}

type ReceiverConfig struct {
    ListenAddr        string
    MaxConnections    int
    TLSConfig         *tls.Config
    DiskCheckpoint    bool
    CheckpointDir     string
    // ... other fields
}
```

### Compression Options

```go
const (
    CompressionNone CompressionType = 0  // No compression
    CompressionZstd CompressionType = 1  // Zstandard compression
    CompressionLZ4  CompressionType = 2  // LZ4 compression
)
```

## Integration Examples

### With MongoDB

```go
import (
    "context"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/bson"
    "github.com/your-org/go-bulk-transport"
)

// Sender: Read from MongoDB and send
cursor, err := collection.Find(context.Background(), bson.M{})
if err != nil {
    log.Fatal(err)
}

var batch [][]byte
for cursor.Next(context.Background()) {
    raw := cursor.Current
    batch = append(batch, raw)
    
    if len(batch) >= 1000 { // Send in batches of 1000
        err := sender.SendBatch("collection_name", batch)
        if err != nil {
            log.Printf("Failed to send batch: %v", err)
        }
        batch = batch[:0] // Reset batch
    }
}

// Receiver: Receive and insert into MongoDB
receiver.OnBatch(func(stream string, batchSeq uint64, docs [][]byte) error {
    var documents []interface{}
    
    for _, doc := range docs {
        var parsed bson.M
        if err := bson.Unmarshal(doc, &parsed); err != nil {
            return err
        }
        documents = append(documents, parsed)
    }
    
    // Bulk insert
    collection := client.Database("mydb").Collection(stream)
    _, err := collection.InsertMany(context.Background(), documents)
    return err
})
```

### With Other Databases

The library works with any database that can serialize data to byte slices:

```go
// For PostgreSQL with JSON documents
import "encoding/json"

// Sender
jsonData, _ := json.Marshal(document)
batch := [][]byte{jsonData}
sender.SendBatch("table_name", batch)

// Receiver
receiver.OnBatch(func(stream string, batchSeq uint64, docs [][]byte) error {
    for _, doc := range docs {
        var document map[string]interface{}
        json.Unmarshal(doc, &document)
        
        // Insert into PostgreSQL
        // db.Exec("INSERT INTO table_name (data) VALUES ($1)", document)
    }
    return nil
})
```

## Performance Tuning

### High Throughput Configuration

```go
config := transport.SenderConfig{
    Address:       "receiver:9000",
    ParallelConns: 8,           // More connections
    WindowSize:    128,         // Larger window
    BufferSize:    1024 * 1024, // 1MB buffers
    Compression:   transport.CompressionLZ4, // Fast compression
    MaxBatchSize:  32 * 1024 * 1024, // 32MB max batch
}
```

### Memory-Efficient Configuration

```go
config := transport.SenderConfig{
    Address:       "receiver:9000",
    ParallelConns: 2,           // Fewer connections
    WindowSize:    32,          // Smaller window
    BufferSize:    64 * 1024,   // 64KB buffers
    Compression:   transport.CompressionZstd, // Better compression
    MaxBatchSize:  4 * 1024 * 1024, // 4MB max batch
}
```

## Testing

The library includes comprehensive tests:

```bash
# Run tests
go test -v

# Run benchmarks
go test -bench=.

# Test with race detection
go test -race
```

## Production Deployment

### Docker Example

```dockerfile
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o app ./cmd/your-app

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/app .
CMD ["./app"]
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: transport-receiver
spec:
  replicas: 3
  selector:
    matchLabels:
      app: transport-receiver
  template:
    metadata:
      labels:
        app: transport-receiver
    spec:
      containers:
      - name: receiver
        image: your-registry/transport-receiver:latest
        ports:
        - containerPort: 9000
        env:
        - name: LISTEN_ADDR
          value: "0.0.0.0:9000"
        resources:
          limits:
            memory: "1Gi"
            cpu: "500m"
          requests:
            memory: "512Mi"
            cpu: "250m"
```

This documentation shows how the transport library can be easily extracted and used as a standalone Go module in other projects.