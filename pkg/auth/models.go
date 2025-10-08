package auth

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ClientCredentials represents OAuth2-style client credentials stored in cloud-sync database
type ClientCredentials struct {
	ID           primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	ClientID     string             `bson:"client_id" json:"client_id"`
	ClientSecret string             `bson:"client_secret" json:"client_secret"` // Hashed with bcrypt
	AppID        string             `bson:"app_id" json:"app_id"`
	Name         string             `bson:"name" json:"name"`
	Description  string             `bson:"description" json:"description"`
	Scopes       []string           `bson:"scopes" json:"scopes"`
	Active       bool               `bson:"active" json:"active"`
	CreatedAt    time.Time          `bson:"created_at" json:"created_at"`
	UpdatedAt    time.Time          `bson:"updated_at" json:"updated_at"`
	ExpiresAt    *time.Time         `bson:"expires_at,omitempty" json:"expires_at,omitempty"`
	CreatedBy    string             `bson:"created_by" json:"created_by"`
	
	// VM-specific metadata
	VMMetadata VMClientMetadata `bson:"vm_metadata" json:"vm_metadata"`
}

// VMClientMetadata contains VM-specific information
type VMClientMetadata struct {
	Hostname         string            `bson:"hostname" json:"hostname"`
	MaxCollections   int               `bson:"max_collections" json:"max_collections"`
	SupportedStreams []string          `bson:"supported_streams" json:"supported_streams"`
	Capabilities     map[string]bool   `bson:"capabilities" json:"capabilities"`
	ResourceLimits   ResourceLimits    `bson:"resource_limits" json:"resource_limits"`
	NetworkInfo      NetworkInfo       `bson:"network_info" json:"network_info"`
}

// ResourceLimits defines VM resource constraints
type ResourceLimits struct {
	MaxMemoryMB    int `bson:"max_memory_mb" json:"max_memory_mb"`
	MaxConcurrency int `bson:"max_concurrency" json:"max_concurrency"`
	MaxBandwidthMB int `bson:"max_bandwidth_mb" json:"max_bandwidth_mb"`
}

// NetworkInfo contains network configuration
type NetworkInfo struct {
	TCPEndpoint  string `bson:"tcp_endpoint" json:"tcp_endpoint"`
	HTTPEndpoint string `bson:"http_endpoint" json:"http_endpoint"`
	WSEndpoint   string `bson:"ws_endpoint" json:"ws_endpoint"`
}

// TokenRequest represents OAuth2 token request
type TokenRequest struct {
	GrantType    string `json:"grant_type"`    // "client_credentials"
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	Scope        string `json:"scope,omitempty"`
}

// TokenResponse represents OAuth2 token response
type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`    // "Bearer"
	ExpiresIn    int64  `json:"expires_in"`    // seconds
	Scope        string `json:"scope,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

// TokenClaims represents JWT token claims (RFC 7519 + custom claims)
type TokenClaims struct {
	// Standard JWT claims (RFC 7519)
	IssuedAt     int64    `json:"iat"`
	ExpiresAt    int64    `json:"exp"`
	NotBefore    int64    `json:"nbf"`
	Issuer       string   `json:"iss"`
	Subject      string   `json:"sub"`
	Audience     []string `json:"aud"`
	JWTID        string   `json:"jti"`
	
	// Custom claims for TCP protocol
	ClientID     string            `json:"client_id"`
	ClientType   string            `json:"client_type"`   // "vm-sync"
	AppID        string            `json:"app_id"`
	Scopes       []string          `json:"scopes"`
	VMMetadata   VMClientMetadata  `json:"vm_metadata"`
	SecurityContext SecurityContext `json:"security_context"`
}

// Implement jwt.Claims interface
func (c TokenClaims) Valid() error {
	now := time.Now().Unix()
	if c.ExpiresAt > 0 && now > c.ExpiresAt {
		return fmt.Errorf("token expired")
	}
	if c.NotBefore > 0 && now < c.NotBefore {
		return fmt.Errorf("token not valid yet")
	}
	return nil
}

func (c TokenClaims) GetExpirationTime() (*jwt.NumericDate, error) {
	if c.ExpiresAt == 0 {
		return nil, nil
	}
	return jwt.NewNumericDate(time.Unix(c.ExpiresAt, 0)), nil
}

func (c TokenClaims) GetIssuedAt() (*jwt.NumericDate, error) {
	if c.IssuedAt == 0 {
		return nil, nil
	}
	return jwt.NewNumericDate(time.Unix(c.IssuedAt, 0)), nil
}

func (c TokenClaims) GetNotBefore() (*jwt.NumericDate, error) {
	if c.NotBefore == 0 {
		return nil, nil
	}
	return jwt.NewNumericDate(time.Unix(c.NotBefore, 0)), nil
}

func (c TokenClaims) GetIssuer() (string, error) {
	return c.Issuer, nil
}

func (c TokenClaims) GetSubject() (string, error) {
	return c.Subject, nil
}

func (c TokenClaims) GetAudience() (jwt.ClaimStrings, error) {
	return jwt.ClaimStrings(c.Audience), nil
}

// SecurityContext contains security-related information
type SecurityContext struct {
	IPAddress     string    `json:"ip_address"`
	UserAgent     string    `json:"user_agent"`
	TokenVersion  int       `json:"token_version"`
	LastActivity  time.Time `json:"last_activity"`
	SessionID     string    `json:"session_id"`
}

// CreateClientRequest represents request to create new client credentials
type CreateClientRequest struct {
	AppID       string           `json:"app_id"`
	Name        string           `json:"name"`
	Description string           `json:"description"`
	Scopes      []string         `json:"scopes"`
	ExpiresIn   *int64           `json:"expires_in,omitempty"` // seconds
	VMMetadata  VMClientMetadata `json:"vm_metadata"`
}

// CreateClientResponse represents response for client creation
type CreateClientResponse struct {
	ClientID     string    `json:"client_id"`
	ClientSecret string    `json:"client_secret"`
	AppID        string    `json:"app_id"`
	Name         string    `json:"name"`
	Scopes       []string  `json:"scopes"`
	CreatedAt    time.Time `json:"created_at"`
	ExpiresAt    *time.Time `json:"expires_at,omitempty"`
}

// Scope definitions for different operations
const (
	ScopeVMSync         = "vm-sync"
	ScopeDataRead       = "data:read"
	ScopeDataWrite      = "data:write"
	ScopeStreamCreate   = "stream:create"
	ScopeStreamRead     = "stream:read"
	ScopeStreamWrite    = "stream:write"
	ScopeMetricsRead    = "metrics:read"
	ScopeHealthRead     = "health:read"
	ScopeConfigRead     = "config:read"
	ScopeAdminAll       = "admin:*"
)

// Standard scopes for VM-Sync clients
var DefaultVMSyncScopes = []string{
	ScopeVMSync,
	ScopeDataRead,
	ScopeDataWrite,
	ScopeStreamCreate,
	ScopeStreamRead,
	ScopeStreamWrite,
	ScopeMetricsRead,
	ScopeHealthRead,
}

// VMStoredCredentials represents credentials stored in VM-Sync local database
type VMStoredCredentials struct {
	ID           primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	AppID        string             `bson:"app_id" json:"app_id"`
	ClientID     string             `bson:"client_id" json:"client_id"`
	ClientSecret string             `bson:"client_secret" json:"client_secret"` // Encrypted locally
	CreatedAt    time.Time          `bson:"created_at" json:"created_at"`
	UpdatedAt    time.Time          `bson:"updated_at" json:"updated_at"`
	LastUsed     time.Time          `bson:"last_used" json:"last_used"`
	Active       bool               `bson:"active" json:"active"`
}

// VMTokenCache represents cached tokens in VM-Sync local database
type VMTokenCache struct {
	ID           primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	AppID        string             `bson:"app_id" json:"app_id"`
	ClientID     string             `bson:"client_id" json:"client_id"`
	AccessToken  string             `bson:"access_token" json:"access_token"`
	RefreshToken string             `bson:"refresh_token,omitempty" json:"refresh_token,omitempty"`
	ExpiresAt    time.Time          `bson:"expires_at" json:"expires_at"`
	Scopes       []string           `bson:"scopes" json:"scopes"`
	CreatedAt    time.Time          `bson:"created_at" json:"created_at"`
	UpdatedAt    time.Time          `bson:"updated_at" json:"updated_at"`
}