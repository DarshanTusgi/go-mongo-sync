# Resume Token Management in MongoDB Data Synchronization

## What This Document Is About (In Simple Terms)

Imagine you're watching a live TV show, but your internet connection keeps dropping. Every time you reconnect, you want to continue watching from exactly where you left off, not from the beginning or some random point. That's exactly what resume tokens do for our database synchronization system.

**Here's the simple story:**

1. **The Problem**: Our system watches for changes in a MongoDB database (like new records, updates, deletions). But networks fail, servers restart, and things go wrong. When this happens, we need to know exactly where we left off watching.

2. **The Solution**: MongoDB gives us "resume tokens" - think of them as bookmarks that say "you were here when things stopped working." These tokens are like GPS coordinates in the stream of database changes.

3. **What We Do**: 
   - We save these bookmarks frequently (like auto-saving a document)
   - When something breaks, we use the bookmark to restart from the exact right spot
   - If a bookmark is too old or corrupted, we start fresh from "now" and keep going

4. **Why It Matters**: This ensures we never miss database changes or process the same change twice, even when things go wrong.

**The Journey Through This Document:**
- First, we'll explain what resume tokens actually are
- Then, how our system stores and manages them
- Next, what happens when things go wrong and how we recover
- Finally, best practices and troubleshooting tips

Now let's dive into the technical details...

---

This document provides a comprehensive guide to understanding how our MongoDB data synchronization system manages resume tokens to ensure resilient, resumable change stream operations.

## Table of Contents

1. [Overview](#overview)
2. [Resume Token Fundamentals](#resume-token-fundamentals)
3. [Token Tracking Architecture](#token-tracking-architecture)
4. [Checkpoint Management](#checkpoint-management)
5. [Error Handling and Recovery](#error-handling-and-recovery)
6. [Resilience Mechanisms](#resilience-mechanisms)
7. [Implementation Details](#implementation-details)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)

## Overview

Resume tokens are MongoDB's mechanism for ensuring change streams can be resumed from a specific point in the oplog (operations log) after interruptions. Our system implements a sophisticated token management strategy that provides:

- **Fault Tolerance**: Automatic recovery from network failures, database restarts, and application crashes
- **Data Consistency**: Ensures no change events are lost or duplicated during recovery
- **Scalability**: Efficient token storage and retrieval for multiple collections and databases
- **Monitoring**: Real-time tracking of token validity and change stream health

## Resume Token Fundamentals

### What is a Resume Token?

A resume token is a MongoDB-generated identifier that represents a specific point in time within the database's change stream. It contains:

```json
{
  "_data": "8267A1B2C3000000012B0229296E04",
  "_typeBits": "BinData(0, 00)"
}
```

- **`_data`**: Base64-encoded binary data containing timestamp and operation metadata
- **`_typeBits`**: Additional type information for proper deserialization

### Token Lifecycle

1. **Generation**: MongoDB generates a token for each change event
2. **Capture**: Our system captures and stores the token
3. **Persistence**: Token is saved to checkpoint storage
4. **Validation**: System validates token before use
5. **Recovery**: Token is used to resume change streams after interruptions

## Token Tracking Architecture

### Core Components

#### 1. Checkpoint Manager (`pkg/resume/checkpoint.go`)

```go
type CheckpointManager struct {
    client     *mongo.Client
    database   string
    collection string
    logger     *logrus.Logger
}

type Checkpoint struct {
    ID          string                 `bson:"_id"`
    ResumeToken bson.Raw              `bson:"resumeToken"`
    Timestamp   time.Time             `bson:"timestamp"`
    Collection  string                `bson:"collection"`
    Database    string                `bson:"database"`
    Metadata    map[string]interface{} `bson:"metadata,omitempty"`
}
```

**Responsibilities:**
- Store and retrieve resume tokens for each collection
- Maintain token metadata (timestamps, collection info)
- Handle token serialization/deserialization
- Provide atomic token updates

#### 2. Change Stream Monitor (`cmd/cloud-sync/main.go`)

```go
func watchCollection(client *mongo.Client, dbName, collName string, 
                    resumeToken bson.Raw) (*mongo.ChangeStream, error) {
    collection := client.Database(dbName).Collection(collName)
    
    var pipeline mongo.Pipeline
    var opts *options.ChangeStreamOptions
    
    if resumeToken != nil {
        opts = options.ChangeStream().SetResumeAfter(resumeToken)
    } else {
        opts = options.ChangeStream().SetStartAtOperationTime(&primitive.Timestamp{
            T: uint32(time.Now().Unix()),
            I: 0,
        })
    }
    
    return collection.Watch(context.Background(), pipeline, opts)
}
```

**Responsibilities:**
- Initialize change streams with appropriate resume tokens
- Monitor change stream health and connectivity
- Handle token-related errors and recovery
- Coordinate with checkpoint manager for token persistence

#### 3. Error Detection System

```go
func isInvalidateResumeTokenError(err error) bool {
    if err == nil {
        return false
    }
    
    errStr := err.Error()
    
    // Check for various resume token error patterns
    invalidTokenPatterns := []string{
        "InvalidResumeToken",
        "invalidate notification",
        "cannot resume stream; the resume token was not found",
        "ChangeStreamFatalError",
    }
    
    for _, pattern := range invalidTokenPatterns {
        if strings.Contains(errStr, pattern) {
            return true
        }
    }
    
    return false
}
```

**Responsibilities:**
- Detect various types of resume token errors
- Classify error severity and recovery strategies
- Trigger appropriate recovery mechanisms

## Checkpoint Management

### Storage Strategy

Our system uses MongoDB itself to store checkpoint data, ensuring:

- **Consistency**: Checkpoints are stored in the same database cluster
- **Atomicity**: Token updates are atomic operations
- **Durability**: Leverages MongoDB's durability guarantees
- **Accessibility**: Easy to query and monitor checkpoint status

### Checkpoint Structure

```go
type Checkpoint struct {
    ID          string                 `bson:"_id"` // Format: "db.collection"
    ResumeToken bson.Raw              `bson:"resumeToken"`
    Timestamp   time.Time             `bson:"timestamp"`
    Collection  string                `bson:"collection"`
    Database    string                `bson:"database"`
    Metadata    map[string]interface{} `bson:"metadata,omitempty"`
}
```

### Token Persistence Flow

1. **Change Event Received**
   ```go
   for changeStream.Next(ctx) {
       var change bson.M
       if err := changeStream.Decode(&change); err != nil {
           log.Error("Failed to decode change event:", err)
           continue
       }
       
       // Process the change event
       processChangeEvent(change)
       
       // Update checkpoint with new resume token
       resumeToken := changeStream.ResumeToken()
       updateCheckpoint(dbName, collName, resumeToken)
   }
   ```

2. **Checkpoint Update**
   ```go
   func (cm *CheckpointManager) UpdateCheckpoint(database, collection string, 
                                                 resumeToken bson.Raw) error {
       checkpoint := Checkpoint{
           ID:          fmt.Sprintf("%s.%s", database, collection),
           ResumeToken: resumeToken,
           Timestamp:   time.Now(),
           Collection:  collection,
           Database:    database,
       }
       
       _, err := cm.collection.ReplaceOne(
           context.Background(),
           bson.M{"_id": checkpoint.ID},
           checkpoint,
           options.Replace().SetUpsert(true),
       )
       
       return err
   }
   ```

3. **Token Retrieval**
   ```go
   func (cm *CheckpointManager) GetCheckpoint(database, collection string) (*Checkpoint, error) {
       var checkpoint Checkpoint
       err := cm.collection.FindOne(
           context.Background(),
           bson.M{"_id": fmt.Sprintf("%s.%s", database, collection)},
       ).Decode(&checkpoint)
       
       if err == mongo.ErrNoDocuments {
           return nil, nil // No checkpoint exists
       }
       
       return &checkpoint, err
   }
   ```

## Error Handling and Recovery

### Error Classification

Our system handles multiple types of resume token errors:

#### 1. InvalidResumeToken Errors
- **Cause**: Token is too old or corrupted
- **Recovery**: Clear token and restart from current time
- **Example**: `"InvalidResumeToken: resume token is invalid"`

#### 2. ChangeStreamFatalError
- **Cause**: Resume token not found in oplog
- **Recovery**: Clear token and restart change stream
- **Example**: `"ChangeStreamFatalError: cannot resume stream; the resume token was not found"`

#### 3. Invalidate Notification Errors
- **Cause**: Collection was dropped or renamed
- **Recovery**: Clear token and restart monitoring
- **Example**: `"InvalidResumeToken: invalidate notification"`

### Recovery Mechanisms

#### Automatic Token Invalidation

```go
func handleChangeStreamError(err error, dbName, collName string) {
    if isInvalidateResumeTokenError(err) {
        log.Warnf("Invalid resume token detected for %s.%s: %v", dbName, collName, err)
        
        // Clear the invalid token
        if err := checkpointManager.ClearCheckpoint(dbName, collName); err != nil {
            log.Errorf("Failed to clear checkpoint: %v", err)
        }
        
        // Restart change stream from current time
        restartChangeStream(dbName, collName, nil)
    }
}
```

#### Graceful Restart Strategy

```go
func restartChangeStream(dbName, collName string, resumeToken bson.Raw) {
    // Close existing change stream
    if existingStream != nil {
        existingStream.Close(context.Background())
    }
    
    // Wait before restart to avoid rapid retry loops
    time.Sleep(5 * time.Second)
    
    // Create new change stream
    newStream, err := watchCollection(client, dbName, collName, resumeToken)
    if err != nil {
        log.Errorf("Failed to restart change stream: %v", err)
        // Implement exponential backoff for retries
        scheduleRetry(dbName, collName, resumeToken)
        return
    }
    
    log.Infof("Successfully restarted change stream for %s.%s", dbName, collName)
}
```

#### Exponential Backoff

```go
func scheduleRetry(dbName, collName string, resumeToken bson.Raw) {
    retryCount := getRetryCount(dbName, collName)
    backoffDuration := time.Duration(math.Pow(2, float64(retryCount))) * time.Second
    
    // Cap maximum backoff at 5 minutes
    if backoffDuration > 5*time.Minute {
        backoffDuration = 5 * time.Minute
    }
    
    log.Infof("Scheduling retry for %s.%s in %v (attempt %d)", 
              dbName, collName, backoffDuration, retryCount+1)
    
    time.AfterFunc(backoffDuration, func() {
        restartChangeStream(dbName, collName, resumeToken)
    })
}
```

## Resilience Mechanisms

### 1. Health Monitoring

```go
func monitorChangeStreamHealth() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            for dbName, collections := range activeStreams {
                for collName, stream := range collections {
                    if !isStreamHealthy(stream) {
                        log.Warnf("Change stream for %s.%s appears unhealthy", dbName, collName)
                        restartChangeStream(dbName, collName, getLastResumeToken(dbName, collName))
                    }
                }
            }
        }
    }
}
```

### 2. Token Validation

```go
func validateResumeToken(token bson.Raw) bool {
    if len(token) == 0 {
        return false
    }
    
    // Check if token is properly formatted
    var tokenDoc bson.M
    if err := bson.Unmarshal(token, &tokenDoc); err != nil {
        return false
    }
    
    // Verify required fields exist
    if _, exists := tokenDoc["_data"]; !exists {
        return false
    }
    
    return true
}
```

### 3. Checkpoint Integrity

```go
func verifyCheckpointIntegrity() error {
    checkpoints, err := checkpointManager.GetAllCheckpoints()
    if err != nil {
        return fmt.Errorf("failed to retrieve checkpoints: %w", err)
    }
    
    for _, checkpoint := range checkpoints {
        if !validateResumeToken(checkpoint.ResumeToken) {
            log.Warnf("Invalid resume token found for %s.%s, clearing checkpoint", 
                     checkpoint.Database, checkpoint.Collection)
            checkpointManager.ClearCheckpoint(checkpoint.Database, checkpoint.Collection)
        }
    }
    
    return nil
}
```

## Implementation Details

### Token Serialization

Resume tokens are stored as `bson.Raw` to preserve their exact binary representation:

```go
type ResumeTokenWrapper struct {
    Token bson.Raw `bson:"token"`
}

func serializeResumeToken(token bson.Raw) ([]byte, error) {
    wrapper := ResumeTokenWrapper{Token: token}
    return bson.Marshal(wrapper)
}

func deserializeResumeToken(data []byte) (bson.Raw, error) {
    var wrapper ResumeTokenWrapper
    if err := bson.Unmarshal(data, &wrapper); err != nil {
        return nil, err
    }
    return wrapper.Token, nil
}
```

### Concurrent Access Control

```go
type SafeCheckpointManager struct {
    *CheckpointManager
    mutex sync.RWMutex
}

func (scm *SafeCheckpointManager) UpdateCheckpoint(database, collection string, 
                                                  resumeToken bson.Raw) error {
    scm.mutex.Lock()
    defer scm.mutex.Unlock()
    
    return scm.CheckpointManager.UpdateCheckpoint(database, collection, resumeToken)
}

func (scm *SafeCheckpointManager) GetCheckpoint(database, collection string) (*Checkpoint, error) {
    scm.mutex.RLock()
    defer scm.mutex.RUnlock()
    
    return scm.CheckpointManager.GetCheckpoint(database, collection)
}
```

### Memory Management

```go
type TokenCache struct {
    cache map[string]bson.Raw
    mutex sync.RWMutex
    maxSize int
}

func (tc *TokenCache) Set(key string, token bson.Raw) {
    tc.mutex.Lock()
    defer tc.mutex.Unlock()
    
    if len(tc.cache) >= tc.maxSize {
        // Implement LRU eviction
        tc.evictOldest()
    }
    
    tc.cache[key] = token
}
```

## Best Practices

### 1. Token Update Frequency

- **High-Frequency Updates**: Update tokens after every change event for maximum resilience
- **Batch Updates**: For high-throughput scenarios, consider batching token updates
- **Periodic Persistence**: Ensure tokens are persisted at regular intervals

```go
func processChangeEventsWithBatching() {
    batchSize := 100
    batch := make([]ChangeEvent, 0, batchSize)
    
    for changeStream.Next(ctx) {
        var change ChangeEvent
        changeStream.Decode(&change)
        
        batch = append(batch, change)
        
        if len(batch) >= batchSize {
            processBatch(batch)
            updateCheckpoint(changeStream.ResumeToken())
            batch = batch[:0] // Reset batch
        }
    }
    
    // Process remaining events
    if len(batch) > 0 {
        processBatch(batch)
        updateCheckpoint(changeStream.ResumeToken())
    }
}
```

### 2. Error Handling Strategy

- **Immediate Recovery**: Handle token errors immediately to minimize data loss
- **Logging**: Comprehensive logging of token-related events for debugging
- **Monitoring**: Set up alerts for token invalidation events

### 3. Testing Resume Token Logic

```go
func TestResumeTokenRecovery(t *testing.T) {
    // Setup test environment
    client := setupTestMongoDB()
    checkpointManager := NewCheckpointManager(client, "test_db", "checkpoints")
    
    // Insert test data
    collection := client.Database("test_db").Collection("test_collection")
    collection.InsertOne(ctx, bson.M{"test": "data1"})
    
    // Start change stream and capture token
    stream, _ := collection.Watch(ctx, mongo.Pipeline{})
    collection.InsertOne(ctx, bson.M{"test": "data2"})
    
    stream.Next(ctx)
    resumeToken := stream.ResumeToken()
    stream.Close(ctx)
    
    // Simulate restart with resume token
    newStream, err := collection.Watch(ctx, mongo.Pipeline{}, 
                                      options.ChangeStream().SetResumeAfter(resumeToken))
    assert.NoError(t, err)
    
    // Verify stream resumes correctly
    collection.InsertOne(ctx, bson.M{"test": "data3"})
    assert.True(t, newStream.Next(ctx))
}
```

## Troubleshooting

### Common Issues and Solutions

#### 1. "Resume token not found" Error

**Symptoms:**
```
ChangeStreamFatalError: cannot resume stream; the resume token was not found
```

**Causes:**
- Token is older than the oplog retention window
- Database was restored from backup
- Oplog was truncated

**Solutions:**
- Clear the checkpoint and restart from current time
- Increase oplog size to retain more history
- Implement more frequent checkpoint updates

#### 2. "InvalidResumeToken" Error

**Symptoms:**
```
InvalidResumeToken: resume token is invalid
```

**Causes:**
- Token corruption during storage/retrieval
- Version mismatch between MongoDB versions
- Malformed token data

**Solutions:**
- Validate token format before use
- Implement token integrity checks
- Clear corrupted tokens and restart

#### 3. Change Stream Stops Unexpectedly

**Symptoms:**
- No new change events received
- Stream appears to be "stuck"

**Debugging Steps:**
1. Check change stream health status
2. Verify network connectivity
3. Examine recent error logs
4. Validate current resume token

**Solutions:**
- Implement health monitoring
- Add automatic restart mechanisms
- Use heartbeat monitoring

### Monitoring and Alerting

```go
func setupTokenMonitoring() {
    // Monitor token age
    go func() {
        for {
            checkpoints, _ := checkpointManager.GetAllCheckpoints()
            for _, checkpoint := range checkpoints {
                age := time.Since(checkpoint.Timestamp)
                if age > 1*time.Hour {
                    log.Warnf("Checkpoint for %s.%s is %v old", 
                             checkpoint.Database, checkpoint.Collection, age)
                }
            }
            time.Sleep(5 * time.Minute)
        }
    }()
    
    // Monitor change stream health
    go func() {
        for {
            for dbName, collections := range activeStreams {
                for collName, stream := range collections {
                    if err := stream.Err(); err != nil {
                        log.Errorf("Change stream error for %s.%s: %v", dbName, collName, err)
                        if isInvalidateResumeTokenError(err) {
                            handleTokenError(dbName, collName, err)
                        }
                    }
                }
            }
            time.Sleep(30 * time.Second)
        }
    }()
}
```

## Conclusion

Our resume token management system provides a robust foundation for resilient MongoDB change stream operations. By implementing comprehensive error handling, automatic recovery mechanisms, and thorough monitoring, the system ensures data consistency and high availability even in the face of various failure scenarios.

Key takeaways:
- Resume tokens are critical for change stream resilience
- Proper error classification enables targeted recovery strategies
- Regular token persistence minimizes data loss during failures
- Health monitoring and automatic restart mechanisms ensure system reliability
- Comprehensive testing validates recovery scenarios

For additional support or questions about resume token management, refer to the MongoDB documentation or contact the development team.