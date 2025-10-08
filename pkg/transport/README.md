# Go Bulk Transport Library

A high-performance TCP-based bulk data transfer library for transferring billions of MongoDB documents (BSON encoded) between clusters, avoiding HTTP/REST overhead.

## Features

- **Raw TCP Protocol**: Length-framed binary messages with 32-byte fixed header
- **High Performance**: Multiple parallel TCP connections with configurable worker pools
- **Reliability**: Sliding window protocol with acknowledgments and resume capability
- **Compression**: Support for Zstd, LZ4, or no compression
- **Production Ready**: Fault tolerance, error handling, and comprehensive statistics
- **Easy Integration**: Clean Go API that's easy to use in existing applications

## Protocol Specification

### Message Framing
- 32-byte fixed header + variable payload
- Header fields:
  - `frame_len` (uint32, big-endian): Total frame length including header
  - `flags` (uint8): Bitfield for compressed, control, last_in_batch
  - `msg_type` (uint8): DOC_BATCH, ACK, HEARTBEAT, RESUME_REQUEST, etc.
  - `version` (uint16): Protocol version (currently 1)
  - `stream_id` (uint64): Stream identifier
  - `batch_seq` (uint64): Batch sequence number
  - `payload_checksum` (uint64): CRC64 checksum of payload

### Message Types
- `DOC_BATCH` (0x01): Concatenated BSON documents
- `ACK` (0x02): Acknowledgment with stream_id and ack_up_to
- `HEARTBEAT` (0x03): Keep-alive message
- `RESUME_REQUEST` (0x04): Request to resume from sequence number
- `RESUME_RESPONSE` (0x05): Response to resume request
- `CONTROL` (0x06): Control messages

## Quick Start

### Installation

```bash
# In your Go project
go get your-repo/go-data-sync-http/pkg/transport
```

### Basic Usage

#### Receiver (Server)

```go
package main

import (
    "log"
    "your-repo/go-data-sync-http/pkg/transport"
)

func main() {
    // Create receiver
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
        
        // Process documents (e.g., insert into MongoDB)
        // for _, doc := range docs {
        //     // Parse BSON and insert into database
        // }
        
        return nil
    })

    // Handle errors
    receiver.OnError(func(err error) {
        log.Printf("Receiver error: %v", err)
    })

    // Start listening
    log.Println("Starting receiver on :9000")
    if err := receiver.Start(); err != nil {
        log.Fatal(err)
    }
}
```

#### Sender (Client)

```go
package main

import (
    "log"
    "time"
    "your-repo/go-data-sync-http/pkg/transport"
)

func main() {
    // Create sender
    sender, err := transport.NewSender(transport.SenderConfig{
        Address:       "receiver:9000",
        ParallelConns: 4,
        WindowSize:    64,
        Compression:   transport.CompressionZstd,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer sender.Close()

    // Send a batch of BSON documents
    batch := [][]byte{
        bsonDoc1, // Your BSON documents
        bsonDoc2,
        bsonDoc3,
    }
    
    err = sender.SendBatch("users", batch)
    if err != nil {
        log.Fatal(err)
    }

    // Wait for acknowledgments
    err = sender.WaitForAcks(30 * time.Second)
    if err != nil {
        log.Printf("Timeout waiting for ACKs: %v", err)
    }
}
```

## Configuration

### SenderConfig

```go
type SenderConfig struct {
    Address       string              // Target address (e.g., "receiver:9000")
    ParallelConns int                 // Number of parallel connections (default: 4)
    WindowSize    int                 // Sliding window size (default: 64)
    Compression   CompressionType     // Compression algorithm (default: none)
    TLSConfig     *tls.Config         // Optional TLS configuration
    BatchTimeout  time.Duration       // Batch send timeout (default: 5s)
    ConnTimeout   time.Duration       // Connection timeout (default: 30s)
    KeepAlive     time.Duration       // Keep-alive interval (default: 30s)
    MaxRetries    int                 // Maximum retry attempts (default: 3)
    RetryBackoff  time.Duration       // Retry backoff (default: 1s)
    BufferSize    int                 // Buffer size per connection (default: 256KB)
    MaxBatchSize  int                 // Maximum batch size (default: 16MB)
}
```

### ReceiverConfig

```go
type ReceiverConfig struct {
    ListenAddr        string              // Listen address (e.g., "0.0.0.0:9000")
    TLSConfig         *tls.Config         // Optional TLS configuration
    MaxConnections    int                 // Max concurrent connections (default: 100)
    ReadTimeout       time.Duration       // Read timeout (default: 60s)
    WriteTimeout      time.Duration       // Write timeout (default: 30s)
    BufferSize        int                 // Buffer size (default: 256KB)
    DiskCheckpoint    bool                // Enable disk checkpointing (default: false)
    CheckpointDir     string              // Checkpoint directory
    HeartbeatInterval time.Duration       // Heartbeat interval (default: 10s)
    MaxBatchSize      int                 // Maximum batch size (default: 16MB)
}
```

## Compression

The library supports three compression algorithms:

- `CompressionNone`: No compression (fastest, largest size)
- `CompressionZstd`: Zstandard compression (good balance of speed and compression)
- `CompressionLZ4`: LZ4 compression (very fast, moderate compression)

## Advanced Features

### Resume Functionality

The sender can resume from a specific sequence number if the connection is interrupted:

```go
// Resume sending from sequence 1000 for stream "users"
err := sender.Resume("users", 1000)
```

### Statistics

Both sender and receiver provide detailed statistics:

```go
// Sender statistics
stats := sender.Stats()
fmt.Printf("Bytes sent: %d, Batches: %d, Documents: %d\n", 
    stats.BytesSent, stats.BatchesSent, stats.DocumentsSent)

// Receiver statistics
stats := receiver.Stats()
fmt.Printf("Bytes received: %d, Batches: %d, Documents: %d\n", 
    stats.BytesReceived, stats.BatchesReceived, stats.DocumentsReceived)
```

### Checkpointing

Enable disk-based checkpointing for reliability:

```go
config := transport.ReceiverConfig{
    ListenAddr:     "0.0.0.0:9000",
    DiskCheckpoint: true,
    CheckpointDir:  "/var/lib/transport/checkpoints",
}
```

## Performance Tuning

### For High Throughput

```go
config := transport.SenderConfig{
    Address:       "receiver:9000",
    ParallelConns: 8,          // More connections
    WindowSize:    128,        // Larger window
    BufferSize:    1024 * 1024, // 1MB buffers
    Compression:   transport.CompressionLZ4, // Fast compression
}
```

### For High Compression

```go
config := transport.SenderConfig{
    Address:     "receiver:9000",
    Compression: transport.CompressionZstd, // Better compression
    WindowSize:  32,         // Smaller window to reduce memory
}
```

## Error Handling

The library provides comprehensive error handling:

```go
receiver.OnError(func(err error) {
    switch {
    case errors.Is(err, context.DeadlineExceeded):
        log.Println("Timeout occurred")
    case errors.Is(err, syscall.ECONNRESET):
        log.Println("Connection reset by peer")
    default:
        log.Printf("Unexpected error: %v", err)
    }
})
```

## MongoDB Integration Example

```go
import (
    "context"
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
)

// Handle incoming batches and insert into MongoDB
receiver.OnBatch(func(stream string, batchSeq uint64, docs [][]byte) error {
    var documents []interface{}
    
    // Parse BSON documents
    for _, doc := range docs {
        var parsed bson.M
        if err := bson.Unmarshal(doc, &parsed); err != nil {
            return err
        }
        documents = append(documents, parsed)
    }
    
    // Bulk insert into MongoDB
    collection := client.Database("mydb").Collection(stream)
    _, err := collection.InsertMany(context.Background(), documents)
    return err
})
```

## Testing

Run the example to test the library:

```bash
# Terminal 1: Start receiver
go run examples/transport_example.go receiver

# Terminal 2: Start sender
go run examples/transport_example.go sender
```

## Production Deployment

### With TLS

```go
// Load TLS certificates
cert, err := tls.LoadX509KeyPair("server.crt", "server.key")
if err != nil {
    log.Fatal(err)
}

config := transport.ReceiverConfig{
    ListenAddr: "0.0.0.0:9000",
    TLSConfig: &tls.Config{
        Certificates: []tls.Certificate{cert},
    },
}
```

### Docker Deployment

```dockerfile
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o transport-receiver ./examples/transport_example.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/transport-receiver /usr/local/bin/
CMD ["transport-receiver", "receiver"]
```

## License

This library is part of the go-data-sync-http project.