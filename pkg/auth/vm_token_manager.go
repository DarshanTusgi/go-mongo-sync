package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// VMTokenManager handles token management for VM-Sync clients
type VMTokenManager struct {
	mongoClient     *mongo.Client
	database        string
	cloudSyncURL    string
	httpClient      *http.Client
	
	// Token cache
	mu          sync.RWMutex
	currentToken string
	expiresAt    time.Time
	
	// Credentials
	credentials VMStoredCredentials
	
	// Auto-refresh
	refreshTicker *time.Ticker
	stopChan      chan struct{}
}

// NewVMTokenManager creates a new VM token manager
func NewVMTokenManager(mongoClient *mongo.Client, database, cloudSyncURL string) *VMTokenManager {
	return &VMTokenManager{
		mongoClient:  mongoClient,
		database:     database,
		cloudSyncURL: cloudSyncURL,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
		stopChan:     make(chan struct{}),
	}
}

// StoreCredentials stores client credentials in VM local database
func (tm *VMTokenManager) StoreCredentials(ctx context.Context, appID, clientID, clientSecret string) error {
	credentials := VMStoredCredentials{
		ID:           primitive.NewObjectID(),
		AppID:        appID,
		ClientID:     clientID,
		ClientSecret: clientSecret, // TODO: Encrypt this in production
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		Active:       true,
	}
	
	collection := tm.mongoClient.Database(tm.database).Collection("vm_credentials")
	
	// Upsert credentials (replace if exists)
	filter := bson.M{"app_id": appID}
	update := bson.M{"$set": credentials}
	opts := options.Update().SetUpsert(true)
	
	_, err := collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return fmt.Errorf("failed to store credentials: %w", err)
	}
	
	tm.credentials = credentials
	return nil
}

// LoadCredentials loads stored credentials from VM local database
func (tm *VMTokenManager) LoadCredentials(ctx context.Context, appID string) error {
	collection := tm.mongoClient.Database(tm.database).Collection("vm_credentials")
	
	var credentials VMStoredCredentials
	err := collection.FindOne(ctx, bson.M{
		"app_id": appID,
		"active": true,
	}).Decode(&credentials)
	
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return fmt.Errorf("no credentials found for app_id: %s", appID)
		}
		return fmt.Errorf("failed to load credentials: %w", err)
	}
	
	tm.credentials = credentials
	return nil
}

// GetValidToken returns a valid token, refreshing if necessary
func (tm *VMTokenManager) GetValidToken(ctx context.Context) (string, error) {
	tm.mu.RLock()
	// Check if current token is still valid (with 10 minute buffer for safety)
	if tm.currentToken != "" && time.Now().Add(10*time.Minute).Before(tm.expiresAt) {
		token := tm.currentToken
		tm.mu.RUnlock()
		fmt.Printf("🔍 DEBUG: Using cached token (expires at %v)\n", tm.expiresAt.Format("2006-01-02 15:04:05"))
		return token, nil
	}
	tm.mu.RUnlock()
	
	fmt.Printf("🔍 DEBUG: Token cache miss or expired, refreshing token...\n")
	// Need to refresh token
	return tm.refreshToken(ctx)
}

// GetFreshToken always gets a fresh token, ignoring cache
func (tm *VMTokenManager) GetFreshToken(ctx context.Context) (string, error) {
	fmt.Printf("🔍 DEBUG: Forcing fresh token refresh...\n")
	return tm.refreshToken(ctx)
}

// refreshToken requests a new token from cloud-sync
func (tm *VMTokenManager) refreshToken(ctx context.Context) (string, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	
	// Double-check in case another goroutine already refreshed
	if tm.currentToken != "" && time.Now().Add(5*time.Minute).Before(tm.expiresAt) {
		return tm.currentToken, nil
	}
	
	// Prepare token request
	tokenReq := TokenRequest{
		GrantType:    "client_credentials",
		ClientID:     tm.credentials.ClientID,
		ClientSecret: tm.credentials.ClientSecret,
		Scope:        "vm-sync data:read data:write stream:create stream:read stream:write metrics:read health:read",
	}
	
	reqBody, err := json.Marshal(tokenReq)
	if err != nil {
		return "", fmt.Errorf("failed to marshal token request: %w", err)
	}
	
	// Make HTTP request to cloud-sync
	url := fmt.Sprintf("%s/api/auth/token", tm.cloudSyncURL)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}
	
	httpReq.Header.Set("Content-Type", "application/json")
	
	resp, err := tm.httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("failed to request token: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token request failed with status: %d", resp.StatusCode)
	}
	
	var tokenResp TokenResponse
	err = json.NewDecoder(resp.Body).Decode(&tokenResp)
	if err != nil {
		return "", fmt.Errorf("failed to decode token response: %w", err)
	}
	
	// Update internal state
	tm.currentToken = tokenResp.AccessToken
	tm.expiresAt = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
	
	// Cache token in database
	err = tm.cacheToken(ctx, tokenResp)
	if err != nil {
		// Log error but don't fail - token is still valid in memory
		fmt.Printf("Warning: failed to cache token: %v\n", err)
	}
	
	return tm.currentToken, nil
}

// cacheToken stores token in VM local database
func (tm *VMTokenManager) cacheToken(ctx context.Context, tokenResp TokenResponse) error {
	tokenCache := VMTokenCache{
		ID:          primitive.NewObjectID(),
		AppID:       tm.credentials.AppID,
		ClientID:    tm.credentials.ClientID,
		AccessToken: tokenResp.AccessToken,
		RefreshToken: tokenResp.RefreshToken,
		ExpiresAt:   time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second),
		Scopes:      []string{tokenResp.Scope},
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	
	collection := tm.mongoClient.Database(tm.database).Collection("vm_token_cache")
	
	// Replace existing token cache for this app
	filter := bson.M{"app_id": tm.credentials.AppID}
	update := bson.M{"$set": tokenCache}
	opts := options.Update().SetUpsert(true)
	
	_, err := collection.UpdateOne(ctx, filter, update, opts)
	return err
}

// StartAutoRefresh starts automatic token refresh
func (tm *VMTokenManager) StartAutoRefresh(ctx context.Context) {
	// Refresh token every 15 minutes (more aggressive than 30 minutes)
	tm.refreshTicker = time.NewTicker(15 * time.Minute)
	
	go func() {
		for {
			select {
			case <-tm.refreshTicker.C:
				// Auto-refresh token with fresh context and timeout
				refreshCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				_, err := tm.refreshToken(refreshCtx)
				cancel() // Always cancel the context
				
				if err != nil {
					fmt.Printf("Auto token refresh failed: %v\n", err)
				} else {
					fmt.Printf("Token auto-refreshed successfully at %v\n", time.Now().Format("2006-01-02 15:04:05"))
				}
				
			case <-tm.stopChan:
				tm.refreshTicker.Stop()
				return
			}
		}
	}()
}

// StopAutoRefresh stops automatic token refresh
func (tm *VMTokenManager) StopAutoRefresh() {
	if tm.refreshTicker != nil {
		close(tm.stopChan)
	}
}

// IsTokenValid checks if current token is still valid
func (tm *VMTokenManager) IsTokenValid() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	
	return tm.currentToken != "" && time.Now().Before(tm.expiresAt)
}

// GetTokenForProtocol returns token with Bearer prefix for protocol headers
func (tm *VMTokenManager) GetTokenForProtocol(ctx context.Context) (string, error) {
	token, err := tm.GetValidToken(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Bearer %s", token), nil
}