package tenantinfo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

// TenantInfo represents tenant information from API
type TenantInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// CommunityInfo represents community information from API
type CommunityInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// CommunityInfoResponse is the response from community info API
type CommunityInfoResponse struct {
	Tenant    TenantInfo    `json:"tenant"`
	Community CommunityInfo `json:"community"`
}

// ServiceDiscoveryResponse is the response from service discovery API
type ServiceDiscoveryResponse struct {
	GlobalCaas string `json:"global_caas"`
}

// FetchTenantInfoIfNeeded fetches tenant information from BlockID API if not already set in environment
// This is BLOCKING and REQUIRED - service will crash if fetch fails
func FetchTenantInfoIfNeeded() error {
	// Check if already provided
	if os.Getenv("TENANT_ID") != "" && os.Getenv("COMMUNITY_ID") != "" && os.Getenv("ROOT_TENANT_NAME") != "" {
		log.Println("✅ TENANT INFO: Using provided environment variables")
		return nil
	}

	tenantDNS := os.Getenv("TENANT_DNS")
	communityName := os.Getenv("COMMUNITY_NAME")

	if tenantDNS == "" {
		return fmt.Errorf("TENANT_DNS must be set to fetch tenant info, or provide TENANT_ID, COMMUNITY_ID, and ROOT_TENANT_NAME directly")
	}
	if communityName == "" {
		return fmt.Errorf("COMMUNITY_NAME must be set")
	}

	log.Println("🔍 TENANT INFO: Fetching from API (MongoDB connected, before OAuth2 init)...")

	httpClient := &http.Client{Timeout: 30 * time.Second}

	// STEP 1: Fetch tenant/community info
	apiURL := fmt.Sprintf("https://%s/api/r1/system/community_info/fetch", tenantDNS)
	requestBody := map[string]string{"dns": tenantDNS, "communityName": communityName}
	jsonData, _ := json.Marshal(requestBody)

	log.Printf("📡 HTTP CALL 1/3: POST %s", apiURL)
	resp, err := httpClient.Post(apiURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(body))
	}

	var response CommunityInfoResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	os.Setenv("TENANT_ID", response.Tenant.ID)
	if os.Getenv("TENANT_NAME") == "" {
		os.Setenv("TENANT_NAME", response.Tenant.Name)
	}
	os.Setenv("COMMUNITY_ID", response.Community.ID)
	log.Printf("✅ HTTP CALL 1/3: TENANT_ID=%s, COMMUNITY_ID=%s", response.Tenant.ID, response.Community.ID)

	// STEP 2: Fetch service discovery
	sdURL := fmt.Sprintf("https://%s/caas/sd", tenantDNS)
	log.Printf("📡 HTTP CALL 2/3: GET %s", sdURL)
	sdResp, err := httpClient.Get(sdURL)
	if err != nil {
		return fmt.Errorf("service discovery failed: %w", err)
	}
	defer sdResp.Body.Close()

	sdBody, _ := io.ReadAll(sdResp.Body)
	if sdResp.StatusCode != 200 {
		return fmt.Errorf("service discovery returned %d: %s", sdResp.StatusCode, string(sdBody))
	}

	var sdResponse ServiceDiscoveryResponse
	if err := json.Unmarshal(sdBody, &sdResponse); err != nil {
		return fmt.Errorf("failed to parse service discovery: %w", err)
	}
	log.Printf("✅ HTTP CALL 2/3: global_caas=%s", sdResponse.GlobalCaas)

	// STEP 3: Fetch root tenant
	globalCaasDomain := strings.TrimPrefix(sdResponse.GlobalCaas, "https://")
	globalCaasDomain = strings.TrimPrefix(globalCaasDomain, "http://")
	if idx := strings.Index(globalCaasDomain, "/"); idx != -1 {
		globalCaasDomain = globalCaasDomain[:idx]
	}

	rootAPIURL := fmt.Sprintf("https://%s/api/r1/system/community_info/fetch", globalCaasDomain)
	rootRequestBody := map[string]string{"dns": globalCaasDomain, "communityName": "default"}
	rootJsonData, _ := json.Marshal(rootRequestBody)

	log.Printf("📡 HTTP CALL 3/3: POST %s", rootAPIURL)
	rootResp, err := httpClient.Post(rootAPIURL, "application/json", bytes.NewBuffer(rootJsonData))
	if err != nil {
		return fmt.Errorf("root tenant fetch failed: %w", err)
	}
	defer rootResp.Body.Close()

	rootBody, _ := io.ReadAll(rootResp.Body)
	if rootResp.StatusCode != 200 {
		return fmt.Errorf("root tenant API returned %d: %s", rootResp.StatusCode, string(rootBody))
	}

	var rootResponse CommunityInfoResponse
	if err := json.Unmarshal(rootBody, &rootResponse); err != nil {
		return fmt.Errorf("failed to parse root tenant response: %w", err)
	}

	os.Setenv("ROOT_TENANT_NAME", rootResponse.Tenant.Name)
	log.Printf("✅ HTTP CALL 3/3: ROOT_TENANT_NAME=%s", rootResponse.Tenant.Name)

	httpClient.CloseIdleConnections()
	log.Println("✅ TENANT INFO: All fetched, connections closed")

	return nil
}
