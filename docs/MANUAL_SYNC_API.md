# Manual Initial Sync API Documentation

This document describes the new manual initial sync API endpoints that have been added to cloud-sync to address the initial dump architecture limitation.

## Overview

The manual initial sync API allows operators to trigger bulk data transfers on-demand, providing a backup mechanism when the automatic initial dump doesn't work as expected. This addresses the architecture limitation where bulk initial transfer was not being triggered automatically.

## API Endpoints

### 1. Trigger Initial Sync

**Endpoint:** `POST /api/sync/trigger`

**Description:** Manually triggers an initial data sync from cloud-sync to vm-sync.

**Request Body:**
```json
{
  "databases": ["database1", "database2"],     // Optional: specific databases to sync
  "collections": ["db.collection1", "db.collection2"], // Optional: specific collections
  "force_resync": false                        // Optional: force full resync even if completed
}
```

**Request Examples:**

1. **Sync all configured collections:**
```bash
curl -X POST http://localhost:8080/api/sync/trigger \
  -H "Content-Type: application/json" \
  -d '{}'
```

2. **Sync specific databases:**
```bash
curl -X POST http://localhost:8080/api/sync/trigger \
  -H "Content-Type: application/json" \
  -d '{"databases": ["real_transfer_test"]}'
```

3. **Sync specific collections:**
```bash
curl -X POST http://localhost:8080/api/sync/trigger \
  -H "Content-Type: application/json" \
  -d '{"collections": ["real_transfer_test.products", "real_transfer_test.customers"]}'
```

4. **Force full resync (ignore previous sync state):**
```bash
curl -X POST http://localhost:8080/api/sync/trigger \
  -H "Content-Type: application/json" \
  -d '{"force_resync": true}'
```

**Response (202 Accepted):**
```json
{
  "success": true,
  "message": "Initial sync triggered successfully",
  "timestamp": "2024-01-15T10:30:00Z",
  "status": "started"
}
```

**Error Response (409 Conflict - sync already running):**
```json
{
  "success": false,
  "message": "Sync already in progress. Use force_resync=true to override."
}
```

**Error Response (400 Bad Request - invalid format):**
```json
{
  "success": false,
  "message": "Invalid request body: invalid collection format 'invalid', expected 'database.collection'"
}
```

### 2. Check Sync Status

**Endpoint:** `GET /api/sync/status`

**Description:** Returns the current status of manual sync operations.

**Response:**
```json
{
  "overall_status": "syncing",
  "total_databases": 4,
  "synced_databases": 2,
  "collection_status": [
    {
      "database": "real_transfer_test",
      "collection": "products",
      "status": "completed",
      "document_count": 1000,
      "transferred_docs": 1000,
      "started_at": "2024-01-15T10:30:00Z",
      "completed_at": "2024-01-15T10:32:00Z"
    },
    {
      "database": "real_transfer_test",
      "collection": "customers",
      "status": "syncing",
      "started_at": "2024-01-15T10:30:30Z"
    },
    {
      "database": "real_transfer_test",
      "collection": "orders",
      "status": "error",
      "error_message": "Connection timeout",
      "started_at": "2024-01-15T10:31:00Z",
      "completed_at": "2024-01-15T10:31:30Z"
    }
  ],
  "started_at": "2024-01-15T10:30:00Z",
  "completed_at": null,
  "last_error": ""
}
```

**Status Values:**
- **overall_status**: `"idle"`, `"syncing"`, `"completed"`, `"error"`
- **collection status**: `"pending"`, `"syncing"`, `"completed"`, `"error"`

## Usage Scenarios

### 1. Troubleshooting Automatic Initial Dump

If the automatic initial dump doesn't work:

1. Check sync status:
```bash
curl http://localhost:8080/api/sync/status
```

2. Trigger manual sync:
```bash
curl -X POST http://localhost:8080/api/sync/trigger \
  -H "Content-Type: application/json" \
  -d '{}'
```

3. Monitor progress:
```bash
# Check status every 30 seconds
watch -n 30 'curl -s http://localhost:8080/api/sync/status | jq'
```

### 2. Selective Collection Sync

Sync only specific collections for testing:

```bash
curl -X POST http://localhost:8080/api/sync/trigger \
  -H "Content-Type: application/json" \
  -d '{"collections": ["real_transfer_test.products"]}'
```

### 3. Force Complete Resync

Force a complete resync ignoring previous state:

```bash
curl -X POST http://localhost:8080/api/sync/trigger \
  -H "Content-Type: application/json" \
  -d '{"force_resync": true}'
```

## Integration with Existing System

### Relationship to Automatic Sync

- The manual API uses the same underlying sync functions as automatic sync
- `force_resync: false` uses `pushCollectionDataWithResume()` (checks for existing sync state)
- `force_resync: true` uses `pushCollectionData()` (full bulk transfer)
- Manual sync does not interfere with real-time change stream sync

### Logging and Monitoring

Manual sync operations are logged with special prefixes:
- `🚀 MANUAL SYNC TRIGGER:` - API trigger messages
- `📊 MANUAL SYNC:` - Sync process messages  
- `✅ MANUAL SYNC:` - Success messages
- `❌ MANUAL SYNC:` - Error messages

### Status Tracking

- Global sync status is tracked in memory
- Collection-level status provides detailed progress
- Status persists until next manual sync is triggered
- No persistence across service restarts (by design)

## Error Handling

### Common Errors

1. **Sync Already Running:**
   - Return 409 Conflict
   - Suggest using `force_resync: true`

2. **Invalid Collection Format:**
   - Return 400 Bad Request
   - Expected format: `"database.collection"`

3. **No Collections Found:**
   - Return 400 Bad Request
   - Check configuration and database/collection filters

4. **VM-Sync Connection Issues:**
   - Collection status shows "error"
   - Error message includes connection details

### Recovery Strategies

1. **Check VM-Sync Availability:**
```bash
curl http://localhost:8081/health
```

2. **Check Cloud-Sync Health:**
```bash
curl http://localhost:8080/health
```

3. **Force Retry with Resync:**
```bash
curl -X POST http://localhost:8080/api/sync/trigger \
  -H "Content-Type: application/json" \
  -d '{"force_resync": true}'
```

## Architecture Fix Summary

The manual initial sync API addresses the discovered architecture limitation by:

1. **Providing Manual Control:** Operators can trigger initial dumps when automatic triggers fail
2. **Enhanced Logging:** Better visibility into sync operations with emoji-based log levels
3. **Status Monitoring:** Real-time sync status and progress tracking
4. **Flexible Targeting:** Sync all, specific databases, or specific collections
5. **Force Override:** Bypass sync state checks when needed
6. **Error Recovery:** Detailed error messages and recovery strategies

This ensures that bulk initial data transfer can always be initiated, either automatically or manually, solving the core architecture gap that was preventing initial dumps from occurring.