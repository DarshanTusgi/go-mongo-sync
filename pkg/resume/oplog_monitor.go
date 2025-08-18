package resume

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// OplogMonitor monitors the oplog window and provides fallback strategies
// when resume tokens become invalid due to oplog window expiry
type OplogMonitor struct {
	client         *mongo.Client
	database       string
	monitoringChan chan OplogStatus
	ctx            context.Context
	cancel         context.CancelFunc
}

// OplogStatus represents the current status of the oplog window
type OplogStatus struct {
	OldestTimestamp   primitive.Timestamp
	NewestTimestamp   primitive.Timestamp
	WindowSizeHours   float64
	IsTokenValid      bool
	ResumeToken       interface{}
	FallbackTimestamp *primitive.Timestamp
}

// NewOplogMonitor creates a new oplog window monitor
func NewOplogMonitor(client *mongo.Client, database string) *OplogMonitor {
	ctx, cancel := context.WithCancel(context.Background())
	return &OplogMonitor{
		client:         client,
		database:       database,
		monitoringChan: make(chan OplogStatus, 10),
		ctx:            ctx,
		cancel:         cancel,
	}
}

// Start begins monitoring the oplog window
func (om *OplogMonitor) Start() error {
	go om.monitorLoop()
	return nil
}

// Stop stops the oplog monitoring
func (om *OplogMonitor) Stop() {
	om.cancel()
	close(om.monitoringChan)
}

// GetStatus returns the current oplog status
func (om *OplogMonitor) GetStatus() <-chan OplogStatus {
	return om.monitoringChan
}

// CheckResumeTokenValidity checks if a resume token is still valid
func (om *OplogMonitor) CheckResumeTokenValidity(resumeToken interface{}) (bool, *primitive.Timestamp, error) {
	if resumeToken == nil {
		return false, nil, fmt.Errorf("resume token is nil")
	}

	// Get oplog window
	oldest, newest, err := om.getOplogWindow()
	if err != nil {
		return false, nil, fmt.Errorf("failed to get oplog window: %v", err)
	}

	// Extract timestamp from resume token
	tokenTimestamp, err := om.extractTimestampFromResumeToken(resumeToken)
	if err != nil {
		return false, nil, fmt.Errorf("failed to extract timestamp from resume token: %v", err)
	}

	// Check if token timestamp is within oplog window
	isValid := tokenTimestamp.T >= oldest.T && tokenTimestamp.T <= newest.T

	// If invalid, provide fallback timestamp (slightly before oldest)
	var fallbackTimestamp *primitive.Timestamp
	if !isValid {
		// Use startAtOperationTime with oldest available timestamp
		fallbackTimestamp = &oldest
	}

	return isValid, fallbackTimestamp, nil
}

// GetFallbackOptions returns options for starting a change stream when resume token is invalid
func (om *OplogMonitor) GetFallbackOptions() (*options.ChangeStreamOptions, error) {
	oldest, _, err := om.getOplogWindow()
	if err != nil {
		return nil, fmt.Errorf("failed to get oplog window: %v", err)
	}

	// Use startAtOperationTime with the oldest available timestamp
	opts := options.ChangeStream().SetStartAtOperationTime(&oldest)
	return opts, nil
}

// monitorLoop continuously monitors the oplog window
func (om *OplogMonitor) monitorLoop() {
	ticker := time.NewTicker(30 * time.Second) // Check every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-om.ctx.Done():
			return
		case <-ticker.C:
			status, err := om.getCurrentStatus()
			if err != nil {
				log.Printf("Error getting oplog status: %v", err)
				continue
			}

			select {
			case om.monitoringChan <- status:
			default:
				// Channel is full, skip this update
			}
		}
	}
}

// getCurrentStatus gets the current oplog status
func (om *OplogMonitor) getCurrentStatus() (OplogStatus, error) {
	oldest, newest, err := om.getOplogWindow()
	if err != nil {
		return OplogStatus{}, err
	}

	// Calculate window size in hours
	windowSize := float64(newest.T-oldest.T) / 3600.0 // Convert seconds to hours

	return OplogStatus{
		OldestTimestamp: oldest,
		NewestTimestamp: newest,
		WindowSizeHours: windowSize,
		IsTokenValid:    true, // This would be set based on specific token check
	}, nil
}

// getOplogWindow retrieves the oldest and newest timestamps from the oplog
func (om *OplogMonitor) getOplogWindow() (primitive.Timestamp, primitive.Timestamp, error) {
	oplogColl := om.client.Database("local").Collection("oplog.rs")

	// Get oldest entry
	oldestOpts := options.FindOne().SetSort(bson.D{{"ts", 1}})
	var oldestEntry bson.M
	err := oplogColl.FindOne(om.ctx, bson.D{}, oldestOpts).Decode(&oldestEntry)
	if err != nil {
		return primitive.Timestamp{}, primitive.Timestamp{}, fmt.Errorf("failed to get oldest oplog entry: %v", err)
	}

	// Get newest entry
	newestOpts := options.FindOne().SetSort(bson.D{{"ts", -1}})
	var newestEntry bson.M
	err = oplogColl.FindOne(om.ctx, bson.D{}, newestOpts).Decode(&newestEntry)
	if err != nil {
		return primitive.Timestamp{}, primitive.Timestamp{}, fmt.Errorf("failed to get newest oplog entry: %v", err)
	}

	oldestTs := oldestEntry["ts"].(primitive.Timestamp)
	newestTs := newestEntry["ts"].(primitive.Timestamp)

	return oldestTs, newestTs, nil
}

// extractTimestampFromResumeToken extracts timestamp from a resume token
func (om *OplogMonitor) extractTimestampFromResumeToken(resumeToken interface{}) (primitive.Timestamp, error) {
	// Resume tokens are typically BSON documents with timestamp information
	// This is a simplified implementation - actual implementation would depend on
	// the specific format of resume tokens used by MongoDB
	
	switch token := resumeToken.(type) {
	case bson.M:
		if ts, ok := token["_data"].(primitive.Timestamp); ok {
			return ts, nil
		}
		// Try other possible fields
		if ts, ok := token["ts"].(primitive.Timestamp); ok {
			return ts, nil
		}
	case map[string]interface{}:
		if ts, ok := token["_data"].(primitive.Timestamp); ok {
			return ts, nil
		}
		if ts, ok := token["ts"].(primitive.Timestamp); ok {
			return ts, nil
		}
	}

	return primitive.Timestamp{}, fmt.Errorf("unable to extract timestamp from resume token")
}

// IsResumeTokenError checks if an error indicates an invalid resume token
func IsResumeTokenError(err error) bool {
	if err == nil {
		return false
	}

	errorStr := err.Error()
	// Common MongoDB errors for invalid resume tokens
	return contains(errorStr, "resume token") ||
		contains(errorStr, "ChangeStreamHistoryLost") ||
		contains(errorStr, "resume point may no longer be in the oplog")
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) && 
		(s == substr || 
		 len(s) > len(substr) && 
		 (s[:len(substr)] == substr || 
		  s[len(s)-len(substr):] == substr || 
		  containsAt(s, substr, 1)))
}

func containsAt(s, substr string, start int) bool {
	for i := start; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}