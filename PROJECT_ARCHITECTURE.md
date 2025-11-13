# Go Data Sync - Complete Project Architecture & Implementation Guide

## 🎯 Project Overview

**Purpose:** Enterprise-grade MongoDB synchronization system for real-time data replication between cloud (Atlas) and on-premise (VM) MongoDB instances.

**Architecture Pattern:** Push-based synchronization with change data capture (CDC)

**Transport Protocols:** 
- TCP (Primary - for initial dump)
- HTTP/WebSocket (Incremental sync & real-time coordination)

---

## 📁 Project Structure

```
go-data-sync-http/
├── cmd/
│   ├── cloud-sync/     # Cloud-side service (data source)
│   │   └── main.go     # 7697 lines - Main orchestrator
│   └── vm-sync/        # VM-side service (data target)
│       └── main.go     # Main receiver
├── pkg/
│   ├── adaptive/       # Self-optimizing performance tuner
│   ├── auth/          # OAuth2 client credentials flow
│   ├── cluster/       # Multi-instance coordination
│   ├── crypto/        # AES-256-GCM encryption
│   ├── distribution/  # Intelligent collection distribution
│   ├── fence/         # Cluster time fencing for consistency
│   ├── filtering/     # Document & field filtering engine
│   ├── license/       # License validation
│   ├── logging/       # Structured logging system
│   ├── memory/        # Memory-efficient buffer management
│   ├── metrics/       # Prometheus-style metrics & alerts
│   ├── models/        # Data models & config structures
│   ├── parallel/      # Parallel processing & partitioning
│   ├── resilience/    # Retry logic & circuit breakers
│   ├── resume/        # Resume token management (CDC)
│   ├── sequence/      # Global sequence generation
│   ├── telemetry/     # System health monitoring
│   ├── tracking/      # Exactly-once delivery tracking
│   ├── transport/     # TCP & HTTP transport layers
│   └── watermarks/    # High-water mark tracking
├── configs/           # Configuration files
├── examples/          # Example configurations
├── docs/             # API documentation
└── web/              # Dashboard UI (if any)
```

---

## 🔄 Core Data Flow

### 1. Initial Dump (Bulk Transfer)
```
Cloud MongoDB Atlas
    ↓ (Read with filters)
Cloud-Sync Service
    ↓ (TCP/4 parallel connections)
VM-Sync Service
    ↓ (Write with mapping)
Local MongoDB
```

### 2. Incremental Sync (Change Data Capture)
```
Cloud MongoDB Change Streams
    ↓ (Resume tokens)
Cloud-Sync Change Handler
    ↓ (HTTP POST with filters)
VM-Sync HTTP API
    ↓ (Upsert operations)
Local MongoDB
```

---

## 🔑 Key Features Implemented

### ✅ Document Filtering
- **Location:** `pkg/filtering/filter.go` + `cmd/cloud-sync/main.go`
- **Function:** `matchesDocumentFilter()` (line 1640+)
- **Operators:** eq, ne, gt, gte, lt, lte, in, nin, regex
- **Applied:** Both initial dump AND incremental sync

### ✅ Field Filtering
- **Location:** `buildAggregationPipeline()` in cloud-sync
- **Method:** MongoDB aggregation $project stage
- **Config:** `field_filter.include_fields` in collections JSON

### ✅ Database Name Routing
- **Location:** `getTargetDatabaseForVMSync()` (line 3124)
- **Pattern:** `${database_name}` expansion → Target mapping
- **Example:** `real_transfer-${database_name}` → `real_transfer-1kosmos`

### ✅ Resume Token Management
- **Location:** `pkg/resume/checkpoint.go`
- **Storage:** MongoDB Atlas (`sync_checkpoints_default_default.resume_tokens_default_default`)
- **Persistence:** Every 5 seconds
- **Purpose:** Fault-tolerant CDC with exactly-once semantics

### ✅ TCP Transport (Ultra-Stable)
- **Location:** `pkg/transport/tcp_sender.go` & `tcp_receiver.go`
- **Connections:** 4 parallel with adaptive windowing
- **Compression:** Zstd algorithm
- **Features:** Auto-reconnect, checkpointing, batch ACKs

### ✅ HTTP Transport
- **Location:** `sendIncrementalChangesViaHTTP()` (line 2042)
- **Purpose:** Incremental sync reliability
- **Endpoint:** `POST /api/v1/push/{database}/{collection}`

### ✅ OAuth2 Authentication
- **Location:** `pkg/auth/client_credentials.go`
- **Flow:** Client credentials grant (RFC 6749)
- **Tokens:** JWT with 1-hour expiry
- **Storage:** MongoDB (`oauth2_auth_default_default`)

### ✅ Adaptive Performance Tuning
- **Location:** `pkg/adaptive/optimizer.go`
- **Monitors:** CPU, Memory, Latency
- **Adjusts:** Fetch workers, Push workers, Batch sizes
- **Trigger:** Every 15 seconds

### ✅ Multi-Tenancy
- **Location:** Config expansion in `pkg/models/config.go`
- **Pattern:** `{database}_{tenant}_{community}`
- **Env Vars:** `TENANT_NAME`, `COMMUNITY_NAME`

---

## 📊 Configuration Structure

### Cloud-Sync Config (`configs/cloud-config.yaml`)
```yaml
mongodb:
  uri: "mongodb+srv://..."  # Atlas connection
  collections_config_file: "configs/collections-test.json"

sync:
  initial_sync: true
  scheduler_sync: true       # 1-minute incremental sync
  scheduler_interval: "1m"
  transport:
    mode: "tcp"              # Primary transport

checkpoint:
  enabled: true
  database: "sync_checkpoints"
  collection: "resume_tokens"
  save_interval_seconds: 5

tenant:
  name: ""  # From TENANT_NAME env var
  community: ""  # From COMMUNITY_NAME env var
```

### Collections Config (`configs/collections-test.json`)
```json
{
  "databases": [{
    "name": "real_transfer-${database_name:-test}",
    "target_database_name": "real_transfer-1kosmos",
    "collections": [{
      "name": "products",
      "document_filter": {
        "criteria": [{
          "field": "status",
          "operator": "ne",
          "value": "deleted"
        }]
      },
      "field_filter": {
        "include_fields": ["_id", "product_id", "name", "price", "category", "status"]
      }
    }]
  }]
}
```

---

## 🔧 Critical Code Sections

### 1. Change Stream Processing with Filters (Cloud-Sync)
**File:** `cmd/cloud-sync/main.go` (lines 1392-1434)
```go
for changeStream.Next(ctx) {
    var changeEvent bson.M
    changeStream.Decode(&changeEvent)
    
    if fullDoc, ok := changeEvent["fullDocument"]; ok {
        // CRITICAL: Apply document filters
        if !matchesDocumentFilter(database, collection, fullDoc) {
            log.Printf("🚫 FILTER SKIP: Document filtered out")
            continue // Skip this document
        }
        
        docBytes, _ := bson.Marshal(fullDoc)
        documents = append(documents, docBytes)
    }
}
```

### 2. Database Routing Function
**File:** `cmd/cloud-sync/main.go` (lines 3124-3138)
```go
func getTargetDatabaseForVMSync(sourceDatabase string) string {
    for _, db := range config.MongoDB.Databases {
        if db.Name == sourceDatabase {
            if db.TargetDatabaseName != "" {
                return db.TargetDatabaseName
            }
            break
        }
    }
    return sourceDatabase
}
```

### 3. Resume Token Persistence
**File:** `pkg/resume/checkpoint.go` (lines 109-138)
```go
func (cm *CheckpointManager) UpdateCheckpoint(database, collection string, 
    resumeToken bson.Raw, eventTime time.Time) error {
    
    key := fmt.Sprintf("%s.%s", database, collection)
    checkpoint := cm.checkpoints[key]
    
    if checkpoint == nil {
        checkpoint = &Checkpoint{
            ID: key,
            Database: database,
            Collection: collection,
            Status: "active",
        }
        cm.checkpoints[key] = checkpoint
    }
    
    if resumeToken != nil && len(resumeToken) > 0 {
        checkpoint.ResumeToken = resumeToken
    }
    checkpoint.LastEventTime = eventTime
    checkpoint.ProcessedCount++
    checkpoint.LastUpdated = time.Now()
    
    return nil
}
```

### 4. TCP Batch Sending
**File:** `pkg/transport/tcp_sender.go`
```go
func (ts *TCPSender) SendBatch(batch *DataBatch) error {
    // Compress batch
    compressed, err := ts.compressor.Compress(batch.Data)
    
    // Create frame
    frame := &Frame{
        StreamID: batch.StreamID,
        Sequence: batch.Sequence,
        Data: compressed,
    }
    
    // Send via least-loaded connection
    conn := ts.selectConnection()
    return conn.WriteFrame(frame)
}
```

---

## 🛡️ Production Readiness Checklist

### ✅ Completed Features
- [x] Document filtering (initial + incremental)
- [x] Field filtering (projection)
- [x] Database name routing/mapping
- [x] Resume token persistence
- [x] TCP ultra-stable transport
- [x] HTTP fallback transport
- [x] OAuth2 authentication
- [x] Encryption (AES-256-GCM)
- [x] Multi-tenancy support
- [x] Adaptive performance tuning
- [x] Exactly-once delivery tracking
- [x] Comprehensive error handling
- [x] Graceful shutdown
- [x] Health check endpoints
- [x] Metrics & monitoring
- [x] Structured logging

### 🔄 Tested Scenarios
- [x] Initial dump with 400+ documents
- [x] Document filter exclusion (status != "deleted")
- [x] Field filter projection (6 fields)
- [x] Database routing (abc → 1kosmos)
- [x] Incremental sync with filters
- [x] Resume token recovery
- [x] TCP connection recovery
- [x] WebSocket reconnection
- [x] OAuth2 token refresh

---

## 🚀 Deployment

### Environment Variables (Cloud-Sync)
```bash
database_name=abc                    # Database name expansion
TENANT_NAME=kotak                    # Tenant identifier
COMMUNITY_NAME=default               # Community identifier
INFRA_LICENSE_KEY=admin-key-123     # License key
```

### Start Commands
```bash
# VM-Sync (Target)
./runnables/vm-sync -config examples/vm-config.yaml

# Cloud-Sync (Source)
database_name=abc TENANT_NAME=kotak COMMUNITY_NAME=default \
  INFRA_LICENSE_KEY=admin-key-123 \
  ./runnables/cloud-sync -config configs/cloud-config.yaml
```

### Build
```bash
make runnables  # Builds both binaries
```

---

## 🔍 Monitoring & Observability

### Metrics Endpoints
- Cloud-Sync: `http://localhost:8080/metrics`
- VM-Sync: `http://localhost:8081/metrics`

### Health Checks
- Cloud-Sync: `http://localhost:8080/health`
- VM-Sync: `http://localhost:8081/health`

### Dashboard
- Cloud-Sync: `http://localhost:8080/dashboard`

### Key Metrics
- `sync_documents_total`: Total documents synced
- `sync_duration_seconds`: Sync operation duration
- `tcp_bytes_sent`: TCP transfer volume
- `http_requests_total`: HTTP API calls
- `resume_token_updates`: CDC checkpoint updates

---

## 🐛 Known Behaviors

### Normal Warnings
1. **TCP Init Failures (1-5 attempts):** Expected during startup before VM-sync connects
2. **Duplicate Key Errors:** Expected when re-running initial dump on existing data
3. **Stream Timeout (25s):** Normal behavior for change stream polling
4. **Resume Token Invalidation:** Handled with automatic recovery

### Expected Logs
- `✅ INITIAL DUMP COMPLETED`: Bulk transfer finished
- `🔍 CHECKPOINT PERSIST`: Resume tokens being saved
- `📄 CHANGE DETECTED`: Incremental change found
- `🚫 FILTER SKIP`: Document filtered out (working correctly)
- `📋 DB ROUTING`: Database name transformation applied

---

## 🔐 Security Features

1. **OAuth2 Authentication:** Client credentials flow between cloud-sync and vm-sync
2. **AES-256-GCM Encryption:** Data encryption at rest and in transit
3. **License Validation:** Infrastructure license key verification
4. **TLS Support:** Optional TLS for TCP transport
5. **Token Expiry:** 1-hour JWT tokens with auto-refresh

---

## 📝 Important Notes

### Resume Token Behavior
- Tokens advance EVEN IF no changes detected (prevents missing events)
- Tokens stored in Atlas MongoDB (not local)
- Collection-level granularity (one token per collection)

### Filter Application
- Document filters: Applied BEFORE sync (both initial & incremental)
- Field filters: Applied via MongoDB aggregation pipeline
- Filters are cumulative (AND logic)

### Database Routing
- Source name: Expanded from `${database_name}` env var
- Target name: From `target_database_name` in config
- Mapping is one-to-one per database

### Sync Timing
- Initial dump: Runs once on startup
- Incremental sync: Every 1 minute (configurable)
- Change stream timeout: 25 seconds per collection
- Resume token save: Every 5 seconds

---

## 🎓 Architecture Decisions

### Why TCP for Initial Dump?
- **Performance:** 4 parallel connections with window-based flow control
- **Reliability:** Built-in ACKs and checkpointing
- **Efficiency:** Zstd compression reduces bandwidth
- **Resumability:** Can restart from last checkpoint

### Why HTTP for Incremental Sync?
- **Simplicity:** Standard REST API easier to debug
- **Compatibility:** Works through proxies and load balancers
- **Idempotency:** Upsert operations safe to retry
- **Monitoring:** HTTP status codes for observability

### Why Separate Checkpoint Manager?
- **Fault Tolerance:** Survives process crashes
- **Multi-Instance:** Shared state across cloud-sync instances
- **Observability:** Can inspect resume state independently
- **Recovery:** Can manually reset checkpoints if needed

### Why Buffer-Free Change Streams?
- **Memory Safety:** No unbounded memory growth
- **Backpressure:** Resume tokens provide natural flow control
- **Reliability:** Direct write to MongoDB (no in-memory buffer loss)
- **Scalability:** Can handle millions of events without OOM

---

## 🏁 Production Deployment Recommendations

### Infrastructure
- **Cloud-Sync:** 2+ instances behind load balancer
- **VM-Sync:** 1 instance per datacenter/region
- **MongoDB:** Atlas M10+ cluster (source), M10+ replica set (target)
- **Network:** Dedicated VPN or private link for TCP traffic

### Resource Requirements
- **Cloud-Sync:** 2 CPU, 4GB RAM minimum
- **VM-Sync:** 2 CPU, 4GB RAM minimum
- **Network:** 10 Mbps sustained, 100 Mbps burst

### Monitoring Alerts
1. Resume token age > 5 minutes (sync lag)
2. TCP connection failures > 3 retries
3. HTTP 5xx errors > 1%
4. Memory usage > 80%
5. Initial dump duration > expected baseline

---

**Document Version:** 1.0  
**Last Updated:** 2025-11-13  
**Status:** Production Ready ✅
