package auth

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/mux"
)

// setupAuthRoutes sets up OAuth2 authentication routes
func SetupAuthRoutes(router *mux.Router, authService *ClientCredentialsService) {
	// Admin endpoints for client management (require authentication)
	// Using a single route with method-based handlers to ensure consistent middleware application
	router.Handle("/api/auth/admin/clients", adminAuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			handleCreateClient(authService)(w, r)
		case "GET":
			handleListClients(authService)(w, r)
		default:
			http.Error(w, `{"error":"method_not_allowed","error_description":"Method not allowed"}`, http.StatusMethodNotAllowed)
		}
	}))).Methods("POST", "GET")

	router.Handle("/api/auth/admin/clients/{client_id}", adminAuthMiddleware(http.HandlerFunc(handleRevokeClient(authService)))).Methods("DELETE")

	// OAuth2 token endpoint (public)
	router.HandleFunc("/api/auth/token", handleGetToken(authService)).Methods("POST")

	// Token validation endpoint (internal)
	router.HandleFunc("/api/auth/validate", handleValidateToken(authService)).Methods("POST")
}

// handleCreateClient creates new OAuth2 client credentials
func handleCreateClient(authService *ClientCredentialsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var req CreateClientRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid_request","error_description":"Invalid JSON"}`, http.StatusBadRequest)
			return
		}

		// Validate required fields
		if req.AppID == "" || req.Name == "" {
			http.Error(w, `{"error":"invalid_request","error_description":"app_id and name are required"}`, http.StatusBadRequest)
			return
		}

		// Set default scopes if not provided
		if len(req.Scopes) == 0 {
			req.Scopes = DefaultVMSyncScopes
		}

		// Set default creator since no admin authentication required
		createdBy := "swagger-ui"

		resp, err := authService.CreateClient(r.Context(), req, createdBy)
		if err != nil {
			log.Printf("Failed to create client: %v", err)
			http.Error(w, `{"error":"server_error","error_description":"Failed to create client"}`, http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(resp)

		log.Printf("Created new OAuth2 client: %s (app_id: %s)", resp.ClientID, resp.AppID)
	}
}

// handleGetToken implements OAuth2 client credentials flow
func handleGetToken(authService *ClientCredentialsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var req TokenRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid_request","error_description":"Invalid JSON"}`, http.StatusBadRequest)
			return
		}

		// Validate required fields
		if req.GrantType == "" || req.ClientID == "" || req.ClientSecret == "" {
			http.Error(w, `{"error":"invalid_request","error_description":"grant_type, client_id, and client_secret are required"}`, http.StatusBadRequest)
			return
		}

		if req.GrantType != "client_credentials" {
			http.Error(w, `{"error":"unsupported_grant_type","error_description":"Only client_credentials grant type is supported"}`, http.StatusBadRequest)
			return
		}

		resp, err := authService.GetToken(r.Context(), req)
		if err != nil {
			if strings.Contains(err.Error(), "invalid client credentials") {
				http.Error(w, `{"error":"invalid_client","error_description":"Invalid client credentials"}`, http.StatusUnauthorized)
			} else if strings.Contains(err.Error(), "expired") {
				http.Error(w, `{"error":"invalid_client","error_description":"Client credentials have expired"}`, http.StatusUnauthorized)
			} else {
				log.Printf("Token generation failed: %v", err)
				http.Error(w, `{"error":"server_error","error_description":"Token generation failed"}`, http.StatusInternalServerError)
			}
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)

		log.Printf("Issued token for client: %s", req.ClientID)
	}
}

// handleValidateToken validates JWT tokens
func handleValidateToken(authService *ClientCredentialsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var req struct {
			Token string `json:"token"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid_request","error_description":"Invalid JSON"}`, http.StatusBadRequest)
			return
		}

		if req.Token == "" {
			http.Error(w, `{"error":"invalid_request","error_description":"token is required"}`, http.StatusBadRequest)
			return
		}

		claims, err := authService.ValidateToken(req.Token)
		if err != nil {
			http.Error(w, `{"error":"invalid_token","error_description":"Token validation failed"}`, http.StatusUnauthorized)
			return
		}

		response := map[string]interface{}{
			"valid":       true,
			"client_id":   claims.ClientID,
			"client_type": claims.ClientType,
			"app_id":      claims.AppID,
			"scopes":      claims.Scopes,
			"expires_at":  claims.ExpiresAt,
		}

		json.NewEncoder(w).Encode(response)
	}
}

// handleListClients lists all OAuth2 clients (admin only)
func handleListClients(authService *ClientCredentialsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		clients, err := authService.ListClients(r.Context())
		if err != nil {
			log.Printf("Failed to list clients: %v", err)
			http.Error(w, `{"error":"server_error","error_description":"Failed to list clients"}`, http.StatusInternalServerError)
			return
		}

		json.NewEncoder(w).Encode(map[string]interface{}{
			"clients": clients,
			"total":   len(clients),
		})
	}
}

// handleRevokeClient revokes OAuth2 client credentials (admin only)
func handleRevokeClient(authService *ClientCredentialsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		vars := mux.Vars(r)
		clientID := vars["client_id"]

		if clientID == "" {
			http.Error(w, `{"error":"invalid_request","error_description":"client_id is required"}`, http.StatusBadRequest)
			return
		}

		err := authService.RevokeClient(r.Context(), clientID)
		if err != nil {
			log.Printf("Failed to revoke client %s: %v", clientID, err)
			http.Error(w, `{"error":"server_error","error_description":"Failed to revoke client"}`, http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
		log.Printf("Revoked OAuth2 client: %s", clientID)
	}
}

// adminAuthMiddleware validates admin access
func adminAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check for licensekey header and validate against environment variable
		licenseKey := r.Header.Get("licensekey")
		expectedLicenseKey := os.Getenv("INFRA_LICENSE_KEY")
		
		if licenseKey == "" || expectedLicenseKey == "" || licenseKey != expectedLicenseKey {
			http.Error(w, `{"error":"unauthorized","error_description":"Invalid or missing licensekey header"}`, http.StatusUnauthorized)
			return
		}

		// Add creator info to context
		r = r.WithContext(setCreatorInContext(r.Context(), "admin"))
		next.ServeHTTP(w, r)
	})
}

// jwtAuthMiddleware validates JWT tokens for protected endpoints
func jwtAuthMiddleware(authService *ClientCredentialsService) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				http.Error(w, `{"error":"unauthorized","error_description":"Authorization header required"}`, http.StatusUnauthorized)
				return
			}

			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || parts[0] != "Bearer" {
				http.Error(w, `{"error":"unauthorized","error_description":"Bearer token required"}`, http.StatusUnauthorized)
				return
			}

			claims, err := authService.ValidateToken(parts[1])
			if err != nil {
				http.Error(w, `{"error":"invalid_token","error_description":"Token validation failed"}`, http.StatusUnauthorized)
				return
			}

			// Add claims to context
			r = r.WithContext(setClaimsInContext(r.Context(), claims))
			next.ServeHTTP(w, r)
		})
	}
}

// Helper functions for context management
func setCreatorInContext(ctx context.Context, creator string) context.Context {
	return context.WithValue(ctx, "creator", creator)
}

func getCreatorFromContext(r *http.Request) string {
	if creator, ok := r.Context().Value("creator").(string); ok {
		return creator
	}
	return "unknown"
}

func setClaimsInContext(ctx context.Context, claims *TokenClaims) context.Context {
	return context.WithValue(ctx, "claims", claims)
}

func getClaimsFromContext(r *http.Request) *TokenClaims {
	if claims, ok := r.Context().Value("claims").(*TokenClaims); ok {
		return claims
	}
	return nil
}
