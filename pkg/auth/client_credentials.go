package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/crypto/bcrypt"
)

// ClientCredentialsService handles OAuth2 client credentials flow
type ClientCredentialsService struct {
	mongoClient *mongo.Client
	database    string
	collection  string
	jwtSecret   []byte
	issuer      string
	audience    []string
}

// NewClientCredentialsService creates a new client credentials service
func NewClientCredentialsService(mongoClient *mongo.Client, database, collection string, jwtSecret []byte, issuer string, audience []string) *ClientCredentialsService {
	return &ClientCredentialsService{
		mongoClient: mongoClient,
		database:    database,
		collection:  collection,
		jwtSecret:   jwtSecret,
		issuer:      issuer,
		audience:    audience,
	}
}

// CreateClient creates new OAuth2 client credentials (Admin endpoint)
func (s *ClientCredentialsService) CreateClient(ctx context.Context, req CreateClientRequest, createdBy string) (*CreateClientResponse, error) {
	// Generate client ID and secret
	clientID := s.generateClientID()
	clientSecret := s.generateClientSecret()

	// Hash the client secret for storage
	hashedSecret, err := bcrypt.GenerateFromPassword([]byte(clientSecret), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("failed to hash client secret: %w", err)
	}

	now := time.Now()
	var expiresAt *time.Time
	if req.ExpiresIn != nil {
		expiry := now.Add(time.Duration(*req.ExpiresIn) * time.Second)
		expiresAt = &expiry
	}

	// Create client credentials document
	credentials := ClientCredentials{
		ID:           primitive.NewObjectID(),
		ClientID:     clientID,
		ClientSecret: string(hashedSecret),
		AppID:        req.AppID,
		Name:         req.Name,
		Description:  req.Description,
		Scopes:       req.Scopes,
		Active:       true,
		CreatedAt:    now,
		UpdatedAt:    now,
		ExpiresAt:    expiresAt,
		CreatedBy:    createdBy,
		VMMetadata:   req.VMMetadata,
	}

	// Store in database
	collection := s.mongoClient.Database(s.database).Collection(s.collection)
	_, err = collection.InsertOne(ctx, credentials)
	if err != nil {
		return nil, fmt.Errorf("failed to store client credentials: %w", err)
	}

	return &CreateClientResponse{
		ClientID:     clientID,
		ClientSecret: clientSecret, // Return plain text secret (only time it's visible)
		AppID:        req.AppID,
		Name:         req.Name,
		Scopes:       req.Scopes,
		CreatedAt:    now,
		ExpiresAt:    expiresAt,
	}, nil
}

// GetToken implements OAuth2 client credentials flow
func (s *ClientCredentialsService) GetToken(ctx context.Context, req TokenRequest) (*TokenResponse, error) {
	// Validate grant type
	if req.GrantType != "client_credentials" {
		return nil, fmt.Errorf("unsupported grant type: %s", req.GrantType)
	}

	// Find client credentials
	collection := s.mongoClient.Database(s.database).Collection(s.collection)
	var credentials ClientCredentials
	err := collection.FindOne(ctx, bson.M{
		"client_id": req.ClientID,
		"active":    true,
	}).Decode(&credentials)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("invalid client credentials")
		}
		return nil, fmt.Errorf("failed to find client: %w", err)
	}

	// Check if client has expired
	if credentials.ExpiresAt != nil && time.Now().After(*credentials.ExpiresAt) {
		return nil, fmt.Errorf("client credentials have expired")
	}

	// Verify client secret
	err = bcrypt.CompareHashAndPassword([]byte(credentials.ClientSecret), []byte(req.ClientSecret))
	if err != nil {
		return nil, fmt.Errorf("invalid client credentials")
	}

	// Generate JWT token
	token, expiresAt, err := s.generateJWT(credentials)
	if err != nil {
		return nil, fmt.Errorf("failed to generate token: %w", err)
	}

	// Update last used timestamp
	collection.UpdateOne(ctx, bson.M{"client_id": req.ClientID}, bson.M{
		"$set": bson.M{"updated_at": time.Now()},
	})

	expiresIn := time.Until(expiresAt).Seconds()

	return &TokenResponse{
		AccessToken: token,
		TokenType:   "Bearer",
		ExpiresIn:   int64(expiresIn),
		Scope:       fmt.Sprintf("%v", credentials.Scopes),
	}, nil
}

// ValidateToken validates JWT token and returns claims
func (s *ClientCredentialsService) ValidateToken(tokenString string) (*TokenClaims, error) {
	fmt.Printf("🔍 DEBUG: Validating token (first 20 chars): %s...\n", tokenString[:min(20, len(tokenString))])

	token, err := jwt.ParseWithClaims(tokenString, &TokenClaims{}, func(token *jwt.Token) (interface{}, error) {
		// Verify signing method
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return s.jwtSecret, nil
	})

	if err != nil {
		fmt.Printf("🔍 DEBUG: Token parsing failed: %v\n", err)
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	claims, ok := token.Claims.(*TokenClaims)
	if !ok || !token.Valid {
		fmt.Printf("🔍 DEBUG: Token claims invalid or token not valid. Claims OK: %v, Token Valid: %v\n", ok, token.Valid)
		return nil, fmt.Errorf("invalid token claims")
	}

	// Check expiration with debug info
	now := time.Now().Unix()
	fmt.Printf("🔍 DEBUG: Token expiration check - Now: %d, Expires: %d, Diff: %d seconds\n", now, claims.ExpiresAt, claims.ExpiresAt-now)
	if now > claims.ExpiresAt {
		fmt.Printf("🔍 DEBUG: Token expired by %d seconds\n", now-claims.ExpiresAt)
		return nil, fmt.Errorf("token has expired")
	}

	fmt.Printf("🔍 DEBUG: Token validation successful for client: %s\n", claims.ClientID)
	return claims, nil
}

// RevokeClient deactivates client credentials
func (s *ClientCredentialsService) RevokeClient(ctx context.Context, clientID string) error {
	collection := s.mongoClient.Database(s.database).Collection(s.collection)
	_, err := collection.UpdateOne(ctx, bson.M{"client_id": clientID}, bson.M{
		"$set": bson.M{
			"active":     false,
			"updated_at": time.Now(),
		},
	})
	return err
}

// ListClients returns all client credentials (admin function)
func (s *ClientCredentialsService) ListClients(ctx context.Context) ([]ClientCredentials, error) {
	collection := s.mongoClient.Database(s.database).Collection(s.collection)
	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var clients []ClientCredentials
	err = cursor.All(ctx, &clients)
	if err != nil {
		return nil, err
	}

	// Remove sensitive data
	for i := range clients {
		clients[i].ClientSecret = "[REDACTED]"
	}

	return clients, nil
}

// generateClientID generates a unique client ID
func (s *ClientCredentialsService) generateClientID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return fmt.Sprintf("vm_sync_%s", hex.EncodeToString(bytes)[:16])
}

// generateClientSecret generates a secure client secret
func (s *ClientCredentialsService) generateClientSecret() string {
	bytes := make([]byte, 32)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// generateJWT creates a JWT token with claims
func (s *ClientCredentialsService) generateJWT(credentials ClientCredentials) (string, time.Time, error) {
	now := time.Now()
	expiresAt := now.Add(1 * time.Hour) // 1 hour token validity

	claims := TokenClaims{
		// Standard JWT claims (RFC 7519)
		IssuedAt:  now.Unix(),
		ExpiresAt: expiresAt.Unix(),
		NotBefore: now.Unix(),
		Issuer:    s.issuer,
		Subject:   credentials.ClientID,
		Audience:  s.audience,
		JWTID:     generateJTI(),

		// Custom claims for TCP protocol
		ClientID:   credentials.ClientID,
		ClientType: "vm-sync",
		AppID:      credentials.AppID,
		Scopes:     credentials.Scopes,
		VMMetadata: credentials.VMMetadata,
		SecurityContext: SecurityContext{
			TokenVersion: 1,
			LastActivity: now,
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(s.jwtSecret)
	if err != nil {
		return "", time.Time{}, err
	}

	return tokenString, expiresAt, nil
}

// generateJTI generates a unique JWT ID
func generateJTI() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	hash := sha256.Sum256(bytes)
	return hex.EncodeToString(hash[:8])
}
