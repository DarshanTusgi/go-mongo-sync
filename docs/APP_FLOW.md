# Go Data Sync - Application Flow

## Core Flow

**1. System Startup**
- Cloud-sync starts → connects to MongoDB → initializes WebSocket server on port 8080 and TCP server on port 9000
- Creates change streams for monitored collections → starts listening for database changes
- VM-sync starts → connects to cloud-sync via WebSocket → validates license with timeout
- Cloud-sync authenticates VM-sync → adds client to active connections registry
- VM-sync sends initial sync request with last known resume token (if any)

**2. Real-time Sync Process**
- MongoDB change stream detects document changes → generates change event with resume token
- Cloud-sync receives change event → applies document/field filters → creates sync message
- Cloud-sync broadcasts filtered change to all connected VM-sync clients via WebSocket
- VM-sync receives change → validates message integrity → applies to local MongoDB
- VM-sync sends acknowledgment back to cloud-sync → cloud-sync updates client sync status
- Cloud-sync stores resume token in `resume_tokens` collection for each client

**3. Initial Data Transfer - Dual Transport Options**

**HTTP REST Transport (Legacy):**
- VM-sync requests initial dump using HTTP GET endpoints
- Cloud-sync streams complete collections in batches via HTTP chunked transfer
- Each batch includes documents and current resume token
- VM-sync applies initial data to local MongoDB and stores resume token
- After initial dump completes, switches to real-time WebSocket sync

**TCP Transport (High Performance):**
- VM-sync establishes TCP connection to cloud-sync on port 9000
- Cloud-sync prepares data with binary protocol and compression (zstd/LZ4)
- Data streamed through multiple parallel TCP connections for maximum throughput
- Built-in acknowledgment, windowing, and resume capability ensure reliability
- 200-500% faster throughput and 30-70% less bandwidth compared to HTTP REST
- Automatic fallback to HTTP if TCP connection fails

**4. Resume Token Management - Detailed Process**

**How VM-sync Gets Resume Token:**
- **Initial Connection**: VM-sync connects without resume token → cloud-sync starts from current time
- **Initial Data Dump**: For new clients, cloud-sync provides full dataset via TCP or HTTP
- **First Sync**: Cloud-sync sends change event with resume token → VM-sync stores token locally
- **Ongoing Sync**: Each change event includes next resume token → VM-sync updates local storage
- **Disconnection**: VM-sync saves last received resume token to local checkpoint file
- **Reconnection**: VM-sync reads checkpoint file → sends resume token in reconnection message
- **Cloud-sync Validation**: Validates token → resumes change stream from that exact point
- **Catch-up Sync**: If token is old, cloud-sync replays missed changes in sequence
- **Full Resync Fallback**: If resume token is expired/invalid, triggers new initial dump

**Resume Token Storage:**
- **Cloud-side**: `resume_tokens` collection stores `{client_id, token, timestamp, collection}`
- **VM-side**: Local checkpoint file stores `{token, timestamp, sync_status, last_applied_change}`
- **Backup Strategy**: Multiple token checkpoints maintained for rollback scenarios

## Key Collections

**Primary Data Collections**
- User's business collections (e.g., `users`, `orders`, `products`)
- Monitored by change streams for real-time sync
- Each collection can have custom filters and field mappings
- Supports document-level and field-level synchronization

**System Collections**
- `resume_tokens`: Stores sync checkpoints for fault recovery
  - Structure: `{_id, client_id, collection_name, resume_token, timestamp, status}`
  - Indexed by client_id and timestamp for fast lookups
  - TTL index removes old tokens after 30 days
- `client_metadata`: Tracks connected VM-sync clients and their state
  - Structure: `{_id, client_id, license_info, connection_time, last_seen, sync_status}`
  - Stores client capabilities, version, and configuration
- `sync_metrics`: Performance and telemetry data
  - Structure: `{_id, client_id, timestamp, latency, throughput, error_count, memory_usage}`
  - Used for adaptive optimization and monitoring
- `sync_conflicts`: Handles data conflicts during synchronization
  - Structure: `{_id, document_id, collection, conflict_type, resolution_strategy, timestamp}`
- `filter_configs`: Stores document and field filtering rules
  - Structure: `{_id, collection_name, document_filters, field_filters, active}`

## Component Interaction

**Cloud-Sync (Server) - Detailed Architecture**
- **HTTP API Server**: Handles REST API requests and client management
  - RESTful endpoints for data export and client registration
  - Chunked transfer encoding for large dataset streaming
  - Request authentication and rate limiting
  - Progress tracking and resumable downloads
- **TCP Transport Server**: High-performance bulk data transfer
  - Binary protocol with 32-byte header and length-framed messages
  - Built-in compression (zstd/LZ4) with 98% compression ratio
  - Sliding window protocol with acknowledgments for reliability
  - Multiple parallel connections for maximum throughput
  - Automatic fallback to HTTP if TCP fails
- **WebSocket Hub**: Manages concurrent client connections with goroutine pools
  - Connection registry with client metadata and health status
  - Heartbeat mechanism to detect disconnected clients
  - Rate limiting and connection throttling for stability
- **Change Stream Monitor**: Watches MongoDB collections for modifications
  - Parallel change streams for multiple collections
  - Change event filtering and transformation pipeline
  - Resume token generation and validation
- **Message Broker**: Distributes changes to appropriate VM-sync clients
  - Client-specific filtering based on subscription rules
  - Message queuing for offline clients (limited buffer)
  - Acknowledgment tracking and retry mechanisms
- **Data Export Engine**: Handles initial data dump generation
  - Consistent snapshot creation with resume token alignment
  - Batch processing and memory-efficient streaming
  - Document and field filtering for client-specific exports
  - Compression and integrity validation
- **License Validator**: Authenticates and authorizes VM-sync clients
  - JWT token validation with expiration checks
  - Client capability verification and feature gating
  - Usage tracking and quota enforcement

**VM-Sync (Client) - Detailed Architecture**
- **HTTP Client**: Handles REST API requests (fallback option)
  - RESTful API client for data import requests
  - Chunked response processing and streaming
  - Download resumption and error recovery
  - Progress tracking and bandwidth throttling
- **TCP Transport Client**: High-performance bulk data receiver
  - Connects to cloud-sync TCP server on port 9000
  - Processes binary protocol with compression
  - Handles multiple parallel connections
  - Provides acknowledgments and resume capability
  - 200-500% faster throughput than HTTP REST
- **Connection Manager**: Handles WebSocket connectivity and reconnection
  - Exponential backoff retry strategy
  - Connection health monitoring and automatic recovery
  - TLS certificate validation and secure communication
- **Sync Engine**: Processes incoming change events and initial data
  - Message validation and integrity checks
  - Local MongoDB transaction management
  - Conflict detection and resolution strategies
  - Bulk import operations for initial data dump
- **Data Import Engine**: Manages initial data dump processing
  - Batch processing and memory management
  - Collection and index creation
  - Data validation and integrity verification
  - Resume token extraction and storage
- **Checkpoint Manager**: Manages resume token persistence
  - Local file-based storage with atomic writes
  - Periodic checkpoint creation and cleanup
  - Recovery from corrupted checkpoint files
- **Telemetry Reporter**: Sends performance metrics to cloud-sync
  - Latency measurements and throughput statistics
  - Error reporting and diagnostic information
  - Resource usage monitoring (CPU, memory, disk)

## Data Flow Path

**Initial Data Dump (TCP/HTTP) → Real-time Sync (WebSocket)**

**Phase 1: Initial Data Dump via TCP Transport (Preferred)**
1. **Connection Establishment**: VM-sync establishes TCP connection to cloud-sync on port 9000
   - Includes client authentication and collection subscriptions
   - Negotiates compression algorithm (zstd/LZ4) and connection parameters
   - Cloud-sync validates license and permissions
2. **Data Preparation**: Cloud-sync prepares complete dataset for transfer
   - Applies document and field filters based on client permissions
   - Generates consistent snapshot with current resume token
   - Organizes data into manageable batches
3. **TCP Streaming**: Cloud-sync streams data using binary protocol
   - Multiple parallel TCP connections for maximum throughput
   - Built-in compression reduces bandwidth by 30-70%
   - Sliding window protocol with acknowledgments ensures reliability
   - Resume capability handles network interruptions
4. **Local Import**: VM-sync imports data into local MongoDB
   - Creates collections and indexes as needed
   - Uses bulk operations for efficient insertion
   - Validates data integrity using checksums
   - Stores final resume token from last batch
5. **Sync Transition**: After TCP dump completes, switches to WebSocket
   - VM-sync establishes WebSocket connection with resume token
   - Cloud-sync validates token and starts real-time change stream
   - System transitions to Phase 3 for ongoing synchronization

**Phase 2: Initial Data Dump via HTTP (Fallback)**
1. **Dump Request**: VM-sync sends HTTP GET request to cloud-sync for initial data
   - Includes client authentication and collection subscriptions
   - Specifies batch size and compression preferences
   - Cloud-sync validates license and permissions
2. **Data Preparation**: Cloud-sync prepares complete dataset for transfer
   - Applies document and field filters based on client permissions
   - Generates consistent snapshot with current resume token
   - Organizes data into manageable batches (default: 1000 documents)
3. **HTTP Streaming**: Cloud-sync streams data using HTTP chunked transfer encoding
   - Each chunk contains batch of documents + metadata
   - Includes progress indicators and batch checksums
   - VM-sync processes batches incrementally to avoid memory issues
4. **Local Import**: VM-sync imports data into local MongoDB
   - Creates collections and indexes as needed
   - Uses bulk operations for efficient insertion
   - Validates data integrity using checksums
   - Stores final resume token from last batch
5. **Sync Transition**: After HTTP dump completes, switches to WebSocket
   - VM-sync establishes WebSocket connection with resume token
   - Cloud-sync validates token and starts real-time change stream
   - System transitions to Phase 3 for ongoing synchronization

**Phase 3: Real-time Change Sync via WebSocket**
1. **Change Capture**: MongoDB change stream captures document modification
   - Full document or delta changes based on configuration
   - Includes operation type (insert, update, delete, replace)
   - Generates unique resume token for each change event
2. **Event Processing**: Cloud-sync receives and processes change event
   - Validates change event structure and resume token
   - Applies document filters (e.g., only sync active users)
   - Applies field filters (e.g., exclude sensitive fields like passwords)
   - Enriches event with metadata (timestamp, client routing info)
3. **Client Routing**: Determines which VM-sync clients should receive the change
   - Checks client subscriptions and collection permissions
   - Applies client-specific transformations and field mappings
   - Queues messages for offline clients (with size limits)
4. **Message Distribution**: Broadcasts filtered change to relevant VM-sync clients
   - Uses WebSocket binary frames for efficiency
   - Includes message ID for acknowledgment tracking
   - Implements message compression for large payloads
5. **Local Application**: VM-sync validates and applies change to local MongoDB
   - Verifies message integrity using checksums
   - Starts local transaction for atomic operations
   - Applies change with conflict detection
   - Updates local resume token checkpoint
6. **Acknowledgment Flow**: Confirmation sent back to cloud-sync
   - Includes message ID and processing status
   - Reports any errors or conflicts encountered
   - Updates client sync metrics and health status

```
MongoDB Change → Change Stream → Cloud-Sync → WebSocket → VM-Sync → Local MongoDB
                                     ↓
                              Resume Token Stored
```

## Fault Tolerance

**Connection Recovery (Advanced)**
- **Reconnection Strategy**: Automatic reconnection with exponential backoff
  - Initial retry after 1 second, doubles up to 60 seconds maximum
  - Jitter added to prevent thundering herd problems
  - Circuit breaker pattern to avoid overwhelming failed servers
- **Resume Token Recovery**: Ensures no data loss during disconnections
  - Client sends last known resume token on reconnection
  - Server validates token and resumes from exact point
  - Handles token expiration with full resync fallback
- **Catch-up Sync**: Replays missed changes in correct chronological order
  - Batches multiple changes for efficiency
  - Respects rate limits to avoid overwhelming client
- **TCP Transport Resilience**: Handles network interruptions during bulk transfer
  - Sliding window protocol tracks acknowledged data
  - Resumes from last acknowledged point after reconnection
  - Disk checkpointing for long-running transfers
  - Automatic fallback to HTTP if TCP connection fails completely