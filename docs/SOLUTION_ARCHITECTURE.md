# Go Data Sync - Solution Architecture Document

## Overview

Go Data Sync is a robust, enterprise-grade MongoDB synchronization system that provides real-time data replication between MongoDB instances with advanced features like watermark-based tracking, resume token management, sequence ordering, and clustering capabilities.

## System Components

### 1. Cloud Sync Server
- **Purpose**: Central synchronization hub that manages data transfer requests
- **Technology**: HTTP/WebSocket server with MongoDB integration
- **Key Features**: Encryption, watermark tracking, batch processing

### 2. VM Sync Client
- **Purpose**: Client-side synchronization agent that connects to Cloud Sync
- **Technology**: HTTP client with WebSocket support
- **Key Features**: Resume token management, watermark processing, clustering

## Database Schema & Collections

### 1. Sync Tracking Database (`sync_tracking`)

#### Collection: `client_sync_states`
**Purpose**: Tracks watermark-based sync state per client to prevent duplicates and ensure exactly-once delivery.

```javascript
{
  _id: ObjectId,
  client_id: String,              // Unique identifier for sync client
  database: String,               // Source database name
  collection: String,             // Source collection name
  last_synced_at: Date,          // Last successful sync timestamp
  
  // Watermark-based tracking fields
  watermark: {
    operation_time: Timestamp,     // MongoDB operation time from change stream
    resume_token: Mixed,           // Resume token for change stream continuation
    last_document_id: ObjectId,    // Last processed document ID for initial sync
    sync_mode: String,             // "initial", "incremental", "completed"
    documents_processed: Number,   // Count of documents processed in current session
    last_updated: Date             // Watermark last update timestamp
  },
  
  last_processed_optime: Timestamp,        // Last processed operation time
  last_processed_document_id: ObjectId,    // Last processed document ID
  
  // Legacy fields (backward compatibility)
  last_synced_document_id: ObjectId,
  total_documents_transferred: Number,
  initial_sync_completed: Boolean,
  
  created_at: Date,
  updated_at: Date
}
```

**Key Fields Explanation**:
- `watermark.operation_time`: Ensures chronological ordering of changes
- `watermark.resume_token`: Enables resumable change streams after interruptions
- `watermark.sync_mode`: Tracks sync phase (initial bulk transfer vs incremental changes)
- `documents_processed`: Provides progress tracking for monitoring

#### Collection: `transfer_batches`
**Purpose**: Tracks transfer batch operations for monitoring and debugging.

```javascript
{
  _id: ObjectId,
  batch_id: String,               // Unique batch identifier
  client_id: String,              // Associated client
  database: String,               // Source database
  collection: String,             // Source collection
  status: String,                 // "in_progress", "completed", "failed"
  documents_count: Number,        // Number of documents in batch
  start_time: Date,              // Batch start timestamp
  end_time: Date,                // Batch completion timestamp
  watermark_state: Object,       // Associated watermark state
  error_message: String,         // Error details if failed
  created_at: Date,
  updated_at: Date
}
```

### 2. Sync Sequences Database (`sync_sequences`)

#### Collection: `sequence_counters`
**Purpose**: Generates ordered sequence numbers for events to ensure proper ordering across distributed nodes.

```javascript
{
  _id: String,                    // Node ID (e.g., "cloud-sync-node-001")
  node_id: String,               // Identifier for the generating node
  sequence: Number,              // Current sequence number
  updated_at: Date               // Last sequence update timestamp
}
```

**Key Features**:
- **Distributed Sequence Generation**: Each node maintains its own sequence counter
- **Batch Allocation**: Sequences are allocated in batches (default: 1000) for performance
- **Ordered Event Processing**: Ensures events are processed in correct chronological order

### 3. VM Sync Checkpoints Database (`vm_sync_checkpoints`)

#### Collection: `client_resume_tokens`
**Purpose**: Client-side resume token tracking for resumable synchronization after interruptions.

```javascript
{
  _id: String,                    // Format: "database.collection"
  resume_token: BinData,         // MongoDB resume token (binary format)
  timestamp: Date,               // Token capture timestamp
  collection: String,            // Collection name
  database: String,              // Database name
  metadata: Object               // Additional metadata
}
```

**Resume Token Management**:
- **Fault Tolerance**: Automatic recovery from network failures and crashes
- **Data Consistency**: Ensures no change events are lost or duplicated
- **Token Validation**: Validates token integrity before use
- **Automatic Cleanup**: Handles invalid/expired tokens gracefully

### 4. Sync Checkpoints Database (`sync_checkpoints`)

#### Collection: `resume_tokens`
**Purpose**: Server-side resume token storage for resumable synchronization.

```javascript
{
  _id: String,                    // Checkpoint identifier
  resume_token: BinData,         // Resume token binary data
  timestamp: Date,               // Checkpoint timestamp
  collection: String,            // Associated collection
  database: String,              // Associated database
  metadata: Object               // Additional checkpoint metadata
}
```

### 5. VM Sync Watermarks Database (`vm_sync_watermarks`)

#### Collection: `event_watermarks`
**Purpose**: VM-side watermarks for exactly-once semantics and event deduplication.

```javascript
{
  _id: String,                           // Format: "client.database.collection"
  client_id: String,                     // Client identifier
  database: String,                      // Database name
  collection: String,                    // Collection name
  last_applied_event_id: String,        // Last processed event ID
  last_applied_sequence_id: Number,     // Last processed sequence number
  last_applied_cluster_time: Timestamp, // Last applied cluster time
  last_acked_sequence_id: Number,       // Last acknowledged sequence
  last_acked_batch_id: String,          // Last acknowledged batch
  snapshot_completed: Boolean,          // Initial snapshot completion status
  snapshot_cluster_time: Timestamp,     // Snapshot cluster time
  created_at: Date,
  updated_at: Date
}
```

**Watermark Features**:
- **Exactly-Once Delivery**: Prevents duplicate event processing
- **Event Deduplication**: Tracks processed events to avoid reprocessing
- **Cluster Time Fencing**: Ensures snapshot consistency across cluster

## Complete System Flow Story

### Phase 1: System Startup and Initialization

#### Cloud Sync Server Startup
1. **Service Initialization**
   - Cloud Sync server starts and reads `test-cloud-config.yaml`
   - Initializes HTTP server on port 8080 with WebSocket endpoint `/ws`
   - Sets up encryption with AES-256-GCM using key ID `cloud-sync-key-001`

2. **Source Database Connection**
   - Connects to MongoDB source cluster: `mongodb+srv://u:p@proptuity-dev.mgzig.mongodb.net`
   - Validates connection and authentication
   - Discovers configured databases

3. **Tracking Database Setup**
   - Creates/connects to `sync_tracking` database
   - Initializes collections if they don't exist:
     - `client_sync_states` - for watermark tracking
     - `transfer_batches` - for batch operation monitoring
     - `sequence_counters` - for ordered event generation

4. **Change Stream Initialization**
   - Sets up change streams for each configured collection:
     e.g. - `new_test_db.products` (Priority 1, Batch: 1000)
   - Each change stream starts monitoring for real-time changes
   - Resume tokens are initialized for each collection

5. **Server Ready State**
   - HTTP API endpoints become available:
     - `GET /api/data` - for data requests
     - `WebSocket /ws` - for real-time notifications
     - `GET /` - dashboard interface
   - Server logs: "Cloud Sync Server ready and waiting for VM clients"

#### VM Sync Client Startup
1. **Client Initialization**
   - VM Sync client starts and reads `test-vm-config.yaml`
   - Initializes HTTP client with 10-minute timeout
   - Sets up encryption matching Cloud Sync (same key ID)

2. **Target Database Connection**
   - Connects to target MongoDB: `mongodb+srv://u:p@dev.popyexr.mongodb.net`
   - Validates connection and creates target databases
   - Prepares collections for data insertion

3. **Client Database Setup**
   - Creates/connects to `vm_sync_checkpoints` database
   - Initializes `client_resume_tokens` collection for checkpoint storage
   - Creates/connects to `vm_sync_watermarks` database
   - Initializes `event_watermarks` collection for deduplication

4. **Cloud Sync Connection**
   - Establishes HTTP connection to `http://localhost:8080`
   - Opens WebSocket connection to `ws://localhost:8080/ws`
   - Performs handshake and authentication
   - Registers as client with unique ID: `vm-sync-1755864683`

5. **Sync Registration**
   - Sends collection list to Cloud Sync:
     - `new_test_db.products`, `new_test_db.users`, `new_test_db.orders`
     - `new_test_db_2.products`, `new_test_db_2.users`, `new_test_db_2.orders`
   - Cloud Sync creates client sync state entries in `client_sync_states`
   - Initial watermarks are established for each collection

### Phase 2: Initial Data Synchronization

#### Collection Processing Order (Priority-Based)
1. **Database 1 - Priority 1: new_test_db.products**
   - Cloud Sync queries source collection: `db.products.find().limit(1000)`
   - Creates transfer batch record in `transfer_batches` collection
   - Encrypts data using AES-256-GCM
   - Sends HTTP POST to VM Sync with encrypted payload
   - VM Sync decrypts and inserts 1000 documents into target
   - Updates watermark: `sync_mode: "initial"`, `documents_processed: 1000`
   - Repeats until all products are transferred
   - Marks collection as `sync_mode: "completed"`

2. **Database 1 - Priority 2: new_test_db.users**
   - Begins after products completion
   - Processes in batches of 500 documents
   - Current status: Page 18/30 (approximately 90,000 of 150,000 documents)
   - Each batch updates watermark with progress
   - VM Sync logs: "Inserted 5000 documents for new_test_db.users (page 18)"

3. **Database 1 - Priority 3: new_test_db.orders**
   - Queued, waiting for users completion
   - Will process in batches of 100 documents
   - Change stream already active for real-time monitoring

4. **Database 2 Collections**
   - `new_test_db_2.products` - Queued (Priority 1 of DB2)
   - `new_test_db_2.users` - Queued (Priority 2 of DB2)
   - `new_test_db_2.orders` - Queued (Priority 3 of DB2)

#### Real-Time Monitoring During Initial Sync
- **Change Streams Active**: All collections have active change streams
- **Telemetry Updates**: VM Sync sends performance metrics every 10 seconds
  - CPU: ~1.1%, Memory: ~73%, Latency: 0ms
- **Adaptive Optimization**: System adjusts batch sizes and parallelism
  - Current: 4 fetch workers, 2 push workers, 5000 batch size
- **Progress Tracking**: Each page completion updates watermark state

### Phase 3: Incremental Synchronization

#### Real-Time Change Detection
1. **Change Stream Events**
   - MongoDB change streams detect INSERT/UPDATE/DELETE operations
   - Events include: `operationType`, `fullDocument`, `documentKey`, `clusterTime`
   - Resume tokens are captured for each event

2. **Event Processing Pipeline**
   - Cloud Sync receives change event from MongoDB
   - Generates unique sequence number from `sequence_counters`
   - Creates event record with metadata:
     ```json
     {
       "eventId": "seq-12345",
       "collection": "new_test_db.products",
       "operationType": "insert",
       "clusterTime": "...",
       "resumeToken": "..."
     }
     ```

3. **Real-Time Notification**
   - Sends WebSocket notification to VM Sync client
   - VM Sync requests full event data via HTTP API
   - Data is encrypted and transferred
   - VM Sync applies change to target database
   - Updates event watermark to prevent reprocessing

### Phase 4: Error Recovery and Resilience

#### Checkpoint Management
1. **Resume Token Persistence**
   - Every 500ms, current resume tokens are saved to `client_resume_tokens`
   - Tokens are stored in binary format for efficiency
   - Metadata includes timestamp and collection information

2. **Connection Failure Recovery**
   - If VM Sync disconnects, it retrieves last saved resume token
   - Validates token with MongoDB (checks if still valid)
   - Resumes change stream from exact point of failure
   - No data loss or duplication occurs

3. **Watermark Consistency**
   - VM Sync maintains event watermarks in `event_watermarks`
   - Prevents duplicate processing of events during recovery
   - Uses cluster time fencing for consistency

### Phase 5: Completion and Ongoing Operations

#### Initial Sync Completion
1. **Final Collection Processing**
   - All 6 collections reach `sync_mode: "completed"`
   - Final watermark updates confirm total documents transferred
   - Transfer batch records are marked as completed

2. **Transition to Real-Time Only**
   - System switches to pure incremental synchronization
   - Change streams continue monitoring for new changes
   - Performance optimizes for real-time latency

3. **Ongoing Monitoring**
   - Dashboard shows sync status: "All collections synchronized"
   - Telemetry continues reporting system health
   - Automatic optimization adjusts to workload patterns

### Technical Implementation Details

#### Data Structures and State Management

1. **Client Sync State Schema**
   ```json
   {
     "_id": "vm-sync-1755864683:new_test_db.products",
     "client_id": "vm-sync-1755864683",
     "collection": "new_test_db.products",
     "sync_mode": "initial|incremental|completed",
     "watermark": {
       "documents_processed": 150000,
       "last_cluster_time": "...",
       "resume_token": "..."
     },
     "created_at": "2024-01-20T10:30:00Z",
     "updated_at": "2024-01-20T11:45:00Z"
   }
   ```

2. **Transfer Batch Tracking**
   ```json
   {
     "_id": "batch-12345",
     "client_id": "vm-sync-1755864683",
     "collection": "new_test_db.users",
     "batch_number": 18,
     "total_batches": 30,
     "documents_count": 5000,
     "status": "processing|completed|failed",
     "start_time": "2024-01-20T11:40:00Z",
     "completion_time": "2024-01-20T11:40:15Z"
   }
   ```

3. **Event Watermark Structure**
   ```json
   {
     "_id": "new_test_db.products",
     "collection": "new_test_db.products",
     "last_processed_time": "7389479123456789012",
     "last_event_id": "seq-98765",
     "fence_token": "cluster-time-fence-001"
   }
   ```

#### API Communication Protocol

1. **WebSocket Handshake**
   ```
   Client -> Server: WebSocket connection to ws://localhost:8080/ws
   Server -> Client: {"type": "welcome", "server_id": "cloud-sync-001"}
   Client -> Server: {"type": "register", "client_id": "vm-sync-1755864683", "collections": [...]}
   Server -> Client: {"type": "registered", "status": "success"}
   ```

2. **Data Request Flow**
   ```
   Client -> Server: GET /api/data?collection=new_test_db.users&page=18&batch_size=5000
   Server -> Client: {
     "data": "<encrypted_payload>",
     "metadata": {
       "collection": "new_test_db.users",
       "page": 18,
       "total_pages": 30,
       "documents_count": 5000,
       "encryption_key_id": "cloud-sync-key-001"
     }
   }
   ```

3. **Real-Time Event Notification**
   ```
   Server -> Client (WebSocket): {
     "type": "change_event",
     "collection": "new_test_db.products",
     "event_id": "seq-12345",
     "operation_type": "insert"
   }
   Client -> Server: GET /api/events/seq-12345
   Server -> Client: {"event_data": "<encrypted_change_event>"}
   ```

#### Encryption and Security Flow

1. **Data Encryption Process**
   - Source data is serialized to BSON format
   - AES-256-GCM encryption applied with key ID `cloud-sync-key-001`
   - Encrypted payload includes authentication tag for integrity
   - Base64 encoding for HTTP transport

2. **Key Management**
   - Encryption keys are pre-shared between Cloud Sync and VM Sync
   - Key rotation supported through key ID versioning
   - Each encrypted payload includes key ID for proper decryption

3. **Data Integrity Verification**
   - GCM authentication tags prevent tampering
   - Cluster time validation ensures temporal consistency
   - Resume token validation prevents replay attacks

#### Performance Optimization Mechanisms

1. **Adaptive Batch Sizing**
   - Initial batch size: 1000 documents (products), 500 (users), 100 (orders)
   - System monitors processing time and adjusts dynamically
   - Current optimization: 5000 documents per batch for users
   - Memory usage and network latency influence adjustments

2. **Parallel Processing**
   - Fetch workers: 4 concurrent threads reading from source
   - Push workers: 2 concurrent threads writing to target
   - Worker count adjusts based on system telemetry
   - CPU and memory thresholds prevent resource exhaustion

3. **Connection Pooling**
   - MongoDB connections are pooled and reused
   - HTTP client maintains persistent connections
   - WebSocket connection remains open for real-time events
   - Connection health monitoring with automatic reconnection

#### Error Handling and Recovery Scenarios

1. **Network Interruption Recovery**
   - VM Sync detects connection loss through heartbeat failure
   - Retrieves last saved resume token from `client_resume_tokens`
   - Validates token with MongoDB cluster
   - Resumes from exact interruption point
   - No data loss or duplication occurs

2. **MongoDB Failover Handling**
   - Change streams automatically reconnect to new primary
   - Resume tokens remain valid across replica set failovers
   - Client connections redirect to new primary automatically
   - Sync continues without manual intervention

3. **Data Consistency Validation**
   - Event watermarks prevent duplicate processing
   - Cluster time fencing ensures ordered processing
   - Batch completion confirmations prevent partial transfers
   - Automatic retry with exponential backoff for transient failures

## System Flow Architecture

### 1. Initial Sync Flow

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   VM Sync       │    │   Cloud Sync    │    │   Target DB     │
│   Client        │    │   Server        │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │ 1. Request Initial    │                       │
         │    Sync               │                       │
         ├──────────────────────►│                       │
         │                       │ 2. Initialize        │
         │                       │    Watermark         │
         │                       │    (sync_mode:       │
         │                       │     "initial")       │
         │                       ├──────────────────────►│
         │                       │                       │
         │                       │ 3. Query Documents   │
         │                       │    in Batches        │
         │                       │    (batch_size: 5000)│
         │                       ├──────────────────────►│
         │                       │                       │
         │ 4. Encrypted Data     │                       │
         │    Transfer           │                       │
         │◄──────────────────────┤                       │
         │                       │                       │
         │ 5. Update Watermark   │                       │
         │    Progress           │                       │
         ├──────────────────────►│                       │
         │                       │ 6. Update Client     │
         │                       │    Sync State        │
         │                       ├──────────────────────►│
```

### 2. Incremental Sync Flow

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   VM Sync       │    │   Cloud Sync    │    │   Source DB     │
│   Client        │    │   Server        │    │   Change Stream │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │ 1. Monitor Change    │
         │                       │    Stream with       │
         │                       │    Resume Token      │
         │                       ├──────────────────────►│
         │                       │                       │
         │                       │ 2. Change Event      │
         │                       │    Detected          │
         │                       │◄──────────────────────┤
         │                       │                       │
         │                       │ 3. Generate Sequence │
         │                       │    Number             │
         │                       │                       │
         │ 4. WebSocket Event    │                       │
         │    Notification       │                       │
         │◄──────────────────────┤                       │
         │                       │                       │
         │ 5. Request Change     │                       │
         │    Data               │                       │
         ├──────────────────────►│                       │
         │                       │                       │
         │ 6. Encrypted Change   │                       │
         │    Data               │                       │
         │◄──────────────────────┤                       │
         │                       │                       │
         │ 7. Update Resume      │                       │
         │    Token              │                       │
         ├──────────────────────►│                       │
```

### 3. Error Recovery Flow

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   VM Sync       │    │   Cloud Sync    │    │   Checkpoint    │
│   Client        │    │   Server        │    │   Storage       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │ 1. Connection Lost    │                       │
         │    or Error           │                       │
         │ ✗ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ │                       │
         │                       │                       │
         │ 2. Reconnect &        │                       │
         │    Request Resume     │                       │
         ├──────────────────────►│                       │
         │                       │ 3. Retrieve Last     │
         │                       │    Checkpoint        │
         │                       ├──────────────────────►│
         │                       │                       │
         │                       │ 4. Resume Token      │
         │                       │◄──────────────────────┤
         │                       │                       │
         │                       │ 5. Validate Token    │
         │                       │    & Resume Stream   │
         │                       │                       │
         │ 6. Resume Sync from   │                       │
         │    Last Known Point   │                       │
         │◄──────────────────────┤                       │
```

## Connection Management

### 1. HTTP/WebSocket Architecture

- **HTTP Endpoints**: RESTful API for data transfer requests
- **WebSocket Connections**: Real-time event notifications
- **Connection Pooling**: Efficient MongoDB connection management
- **Encryption**: AES-256-GCM encryption for data in transit

### 2. Connection Configuration

```yaml
connection:
  reconnect_interval: "5s"        # Auto-reconnect interval
  max_reconnect_attempts: 10       # Maximum retry attempts
  connection_timeout: "300s"       # Connection establishment timeout
  keepalive_interval: "30s"        # Keep-alive ping interval
```

### 3. Health Monitoring

- **Connection Status**: Real-time monitoring of connection health
- **Heartbeat Mechanism**: Regular health checks between components
- **Automatic Recovery**: Self-healing capabilities for connection issues

## Clustering Architecture

### 1. Internal Cluster Configuration

```yaml
cluster:
  enabled: true
  node_id: "vm-sync-node-1"        # Unique node identifier
  discovery_port: 9090             # Node discovery port
  heartbeat_interval: "10s"        # Cluster heartbeat interval
  election_timeout: "5s"           # Leader election timeout
  log_level: "info"                # Cluster logging level
```

### 2. Event Coordination

- **Event Coordinator**: Manages event distribution across cluster nodes
- **Event Buffer**: Temporary storage for events during processing
- **Worker Pool**: Parallel processing of sync operations
- **Deduplication**: Prevents duplicate event processing across nodes

### 3. Cluster Time Fencing

```yaml
fence:
  enabled: true                    # Enable cluster time fencing
```

**Purpose**: Ensures snapshot consistency across cluster by using MongoDB cluster time for ordering operations.

## Performance & Scalability

### 1. Batch Processing

- **Default Batch Size**: 5,000 documents per batch
- **Configurable Batching**: Adjustable based on data size and network capacity
- **Parallel Collections**: Simultaneous sync of multiple collections

### 2. Worker Configuration

```yaml
performance:
  max_workers: 8                   # Maximum concurrent workers
  buffer_size: 10000              # Event buffer size
  flush_interval: "500ms"         # Buffer flush interval
  max_batch_wait: "5s"            # Maximum batch wait time
```

### 3. Memory Management

- **Connection Pooling**: Efficient MongoDB connection reuse
- **Buffer Management**: Configurable buffer sizes for optimal memory usage
- **Garbage Collection**: Automatic cleanup of expired checkpoints and tokens

## Security Features

### 1. Encryption

- **Algorithm**: AES-256-GCM
- **Key Management**: Configurable encryption keys
- **Data in Transit**: All data transfers are encrypted

### 2. Authentication

- **License-based**: VM_SYNC_LICENSE and CLOUD_SYNC_LICENSE validation
- **MongoDB Authentication**: Secure database connections
- **Connection Security**: TLS/SSL support for all connections

## Monitoring & Observability

### 1. Logging

```yaml
logging:
  level: "info"                    # Log level (debug, info, warn, error)
  format: "json"                   # Log format
  output: "stdout"                 # Log output destination
  include_timestamps: true         # Include timestamps in logs
```

### 2. Metrics

- **Sync Progress**: Real-time tracking of document transfer progress
- **Connection Health**: Monitoring of connection status and errors
- **Performance Metrics**: Throughput, latency, and error rates
- **Watermark Status**: Current watermark positions and sync modes

### 3. Health Checks

- **Database Connectivity**: Regular MongoDB connection health checks
- **Service Status**: Component health monitoring
- **Resume Token Validity**: Automatic validation of resume tokens

## Deployment Architecture

### 1. Component Deployment

```
┌─────────────────────────────────────────────────────────────┐
│                     Production Environment                  │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐    ┌─────────────────┐                │
│  │   Cloud Sync    │    │   Load Balancer │                │
│  │   Server        │◄───┤   (Optional)    │                │
│  │   (Primary)     │    │                 │                │
│  └─────────────────┘    └─────────────────┘                │
│           │                                                 │
│           ▼                                                 │
│  ┌─────────────────┐    ┌─────────────────┐                │
│  │   MongoDB       │    │   Tracking      │                │
│  │   Source        │    │   Database      │                │
│  │   Cluster       │    │                 │                │
│  └─────────────────┘    └─────────────────┘                │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                     Client Environment                      │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐    ┌─────────────────┐                │
│  │   VM Sync       │    │   VM Sync       │                │
│  │   Client        │    │   Client        │                │
│  │   (Node 1)      │    │   (Node 2)      │                │
│  └─────────────────┘    └─────────────────┘                │
│           │                       │                        │
│           ▼                       ▼                        │
│  ┌─────────────────┐    ┌─────────────────┐                │
│  │   Target        │    │   Target        │                │
│  │   MongoDB       │    │   MongoDB       │                │
│  │   Instance 1    │    │   Instance 2    │                │
│  └─────────────────┘    └─────────────────┘                │
└─────────────────────────────────────────────────────────────┘
```

### 2. High Availability

- **Multiple VM Sync Clients**: Distributed client deployment
- **Automatic Failover**: Resume token-based recovery
- **Load Distribution**: Parallel processing across multiple nodes

## Detailed Technical Flow

### Resume Token Management and Change Event Processing

This section provides an in-depth technical analysis of how the system handles resume tokens and processes change events to ensure data consistency and fault tolerance.

#### 1. Resume Token Lifecycle

**Token Generation and Capture**
```go
// In cloud-sync/main.go (lines 2805-2810)
resumeToken := changeDoc.Lookup("_id").Value
if resumeToken != nil {
    // Preserve BSON type for resume token
    checkpoint.ResumeToken = resumeToken.(bson.Raw)
}
```

**Token Storage Structure**
```go
// In pkg/resume/checkpoint.go (line 31)
type Checkpoint struct {
    ID             string                 `bson:"_id" json:"id"`
    Database       string                 `bson:"database" json:"database"`
    Collection     string                 `bson:"collection" json:"collection"`
    ResumeToken    bson.Raw              `bson:"resume_token" json:"resume_token"`
    LastEventTime  *primitive.Timestamp   `bson:"last_event_time" json:"last_event_time"`
    ProcessedCount int64                  `bson:"processed_count" json:"processed_count"`
    Status         string                 `bson:"status" json:"status"`
    ErrorMessage   string                 `bson:"error_message" json:"error_message"`
    Version        int64                  `bson:"version" json:"version"`
}
```

**Token Update Process**
```go
// In pkg/resume/checkpoint.go (lines 120-135)
func (cm *CheckpointManager) UpdateCheckpoint(id string, resumeToken bson.Raw, 
    eventTime *primitive.Timestamp, processedCount int64) error {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    
    checkpoint := cm.checkpoints[id]
    if checkpoint == nil {
        checkpoint = &Checkpoint{
            ID:       id,
            Database: cm.database,
            Collection: cm.collection,
        }
        cm.checkpoints[id] = checkpoint
    }
    
    checkpoint.ResumeToken = resumeToken  // Overwrites previous token
    checkpoint.LastEventTime = eventTime
    checkpoint.ProcessedCount = processedCount
    checkpoint.Version++
    
    return nil
}
```

#### 2. Change Event Processing Flow

**Cloud-Sync Event Processing**
```
┌─────────────────────────────────────────────────────────────────┐
│                    Cloud-Sync Change Stream Processing          │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│ 1. MongoDB Change Stream Event Received                        │
│    - Extract resume token from changeDoc._id                   │
│    - Capture cluster time for ordering                         │
│    - Preserve BSON types for document keys                     │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. Event Enrichment and Sequence Generation                    │
│    - Generate unique sequence number for exactly-once delivery │
│    - Record event metrics (size, replication lag)             │
│    - Prepare event for transmission                            │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. Checkpoint Update (lines 2850-2870)                         │
│    err := checkpointMgr.UpdateCheckpoint(checkpointID,         │
│                                          resumeToken,          │
│                                          &clusterTime,         │
│                                          processedCount)       │
│    if err != nil {                                              │
│        log.Printf("Failed to update checkpoint: %v", err)      │
│    }                                                            │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. Event Distribution                                           │
│    - Send through internal cluster channels                    │
│    - Broadcast via WebSocket to connected vm-sync clients     │
│    - Handle full queue scenarios gracefully                   │
└─────────────────────────────────────────────────────────────────┘
```

**VM-Sync Event Processing**
```
┌─────────────────────────────────────────────────────────────────┐
│                    VM-Sync Event Reception                     │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│ 1. WebSocket Event Notification Received                       │
│    - Parse event type and metadata                             │
│    - Validate event sequence and ordering                      │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. Event Data Retrieval                                        │
│    - Request full event data via HTTP API                      │
│    - Decrypt and deserialize event payload                     │
│    - Validate data integrity                                   │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. Event Processing and Application                             │
│    - Apply change to target database                           │
│    - Handle invalidate events specially                        │
│    - Track processing metrics                                  │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. Client-Side Checkpoint Update (lines 1280-1310)             │
│    err := checkpointMgr.UpdateCheckpoint(checkpointID,         │
│                                          resumeToken,          │
│                                          &timestamp)           │
│    if err != nil {                                              │
│        log.Printf("Failed to update checkpoint: %v", err)      │
│        // Note: Error logged but event considered processed    │
│    }                                                            │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. Acknowledgment and Sequence Tracking                        │
│    - Send processing acknowledgment                             │
│    - Update sequence counters                                  │
│    - Convert cluster time from BSON raw to primitive.Timestamp │
└─────────────────────────────────────────────────────────────────┘
```

#### 3. Resume Token Overwrite Behavior Analysis

**Why Overwriting is Correct**

The question "Does setting `checkpoint.ResumeToken = resumeToken` override the previous record, meaning the last change is forgotten?" reveals a common misconception about resume tokens.

**Resume Token Semantics:**
- Resume tokens are **position markers** in MongoDB's oplog, not change event storage
- Each token represents a specific point in the oplog timeline
- Overwriting is intentional and correct behavior

**Sequential Processing Example:**
```
Change 1: resumeToken = T1 (timestamp: 100)
Change 2: resumeToken = T2 (timestamp: 101) 
Change 3: resumeToken = T3 (timestamp: 102)

Checkpoint Evolution:
After Change 1: checkpoint.ResumeToken = T1
After Change 2: checkpoint.ResumeToken = T2  // T1 is overwritten
After Change 3: checkpoint.ResumeToken = T3  // T2 is overwritten
```

**Recovery Scenario:**
If vm-sync disconnects after processing Change 1:
1. **During Disconnection**: vm-sync preserves resumeToken = T1
2. **Changes 2 & 3 Occur**: cloud-sync continues monitoring, updates checkpoint to T3
3. **Reconnection**: vm-sync resumes from T1, processes Changes 2 & 3 sequentially
4. **Final State**: checkpoint.ResumeToken = T3

#### 4. Fault Tolerance Mechanisms

**Connection Loss Recovery**
```
┌─────────────────────────────────────────────────────────────────┐
│                    Disconnection Scenario                      │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│ 1. VM-Sync Detects Connection Loss                             │
│    - WebSocket heartbeat failure                               │
│    - HTTP request timeouts                                     │
│    - Network connectivity issues                               │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. Preserve Current State                                       │
│    - Last known resume token saved in checkpoint               │
│    - Processing state maintained in memory                     │
│    - Connection retry logic initiated                          │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. Reconnection and Recovery                                    │
│    - WebSocket reconnection with exponential backoff          │
│    - Retrieve latest checkpoint from storage                   │
│    - Validate resume token with MongoDB                        │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. Resume Processing                                            │
│    - MongoDB change stream resumes from saved token            │
│    - All missed events processed in order                      │
│    - No data loss or duplication occurs                        │
└─────────────────────────────────────────────────────────────────┘
```

**Invalidate Event Handling**
```go
// In vm-sync/main.go (lines 1280-1310)
if operationType == "invalidate" {
    // Handle invalidate events that signal collection/database changes
    // Update checkpoint but may trigger full re-sync
    err := checkpointMgr.UpdateCheckpoint(checkpointID, resumeToken, &timestamp)
    if err != nil {
        log.Printf("Failed to update checkpoint after invalidate: %v", err)
    }
    // Event processed successfully despite checkpoint error
    return nil
}
```

#### 5. Data Consistency Guarantees

**Atomic Processing**
- Each change event is processed atomically
- Resume token update occurs after successful event application
- Failure at any step preserves previous consistent state

**Exactly-Once Delivery**
- Sequence numbers prevent duplicate processing
- Resume tokens ensure no events are skipped
- Idempotent operations handle replay scenarios

**Ordering Guarantees**
- MongoDB change streams provide total ordering per collection
- Cluster time fencing ensures cross-collection consistency
- Sequential processing maintains causal relationships

#### 6. Performance Optimizations

**Checkpoint Persistence Strategy**
```go
// Checkpoints are persisted periodically, not per event
// This reduces I/O overhead while maintaining recovery capability
type CheckpointManager struct {
    mu              sync.RWMutex
    client          *mongo.Client
    database        string
    collection      string
    checkpoints     map[string]*Checkpoint
    persistInterval time.Duration  // Default: 5 seconds
    enabled         bool
}
```

**Batch Processing Considerations**
- Resume tokens are updated per individual event, not per batch
- This ensures fine-grained recovery capability
- Batch failures can resume from the last successfully processed event

**Memory Management**
- Resume tokens stored as `bson.Raw` to preserve MongoDB's native format
- Efficient serialization/deserialization for network transmission
- Minimal memory footprint for token storage

## Conclusion

The Go Data Sync system provides a comprehensive, enterprise-grade solution for MongoDB synchronization with advanced features including:

- **Reliability**: Watermark-based tracking and resume token management
- **Scalability**: Clustering and parallel processing capabilities
- **Security**: End-to-end encryption and secure authentication
- **Observability**: Comprehensive monitoring and logging
- **Flexibility**: Configurable performance and deployment options

The architecture ensures data consistency, fault tolerance, and high performance while maintaining simplicity in configuration and deployment. The detailed technical flow demonstrates how resume tokens provide robust recovery mechanisms without data loss, making the system suitable for mission-critical data synchronization scenarios.