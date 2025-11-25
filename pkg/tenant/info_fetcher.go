package tenant

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"
)

// ServiceDiscoveryResponse represents the response from /caas/sd
type ServiceDiscoveryResponse struct {
	GlobalCaas              string `json:"global_caas"`
	Sessions                string `json:"sessions"`
	Licenses                string `json:"licenses"`
	Adminconsole            string `json:"adminconsole"`
	Authz                   string `json:"authz"`
	Reports                 string `json:"reports"`
	Webauthn                string `json:"webauthn"`
	PublicAssetsUploadURL   string `json:"public_assets_upload_url"`
	PublicAssetsDownloadURL string `json:"public_assets_download_url"`
	Docuverify              string `json:"docuverify"`
	Oauth2                  string `json:"oauth2"`
	RulesEngine             string `json:"rules_engine"`
	Webhooks                string `json:"webhooks"`
	Walletapi               string `json:"walletapi"`
	Events                  string `json:"events"`
	UserManagement          string `json:"user_management"`
	Authn                   string `json:"authn"`
	Ipfsproxy               string `json:"ipfsproxy"`
	BlockidConsole          string `json:"blockid_console"`
}

// TenantInfo represents tenant information from the API
type TenantInfo struct {
	ID                          string   `json:"id"`
	Firstname                   *string  `json:"firstname"`
	Lastname                    *string  `json:"lastname"`
	TenantType                  string   `json:"tenanttype"`
	TenantTag                   string   `json:"tenanttag"`
	Name                        string   `json:"name"`
	OtherDNS                    []string `json:"otherDNS"`
	DisplayTenantInfoForPersona bool     `json:"displayTenantInfoForPersona"`
}

// CommunityInfo represents community information from the API
type CommunityInfo struct {
	ID                string  `json:"id"`
	TenantID          string  `json:"tenantid"`
	MobileLogo        *string `json:"mobileLogo"`
	Name              string  `json:"name"`
	PublicKey         string  `json:"publicKey"`
	AccountsPerPerson int     `json:"accountsPerPerson"`
	PersonsPerAccount int     `json:"personsPerAccount"`
	PersonLimitRule   string  `json:"personLimitRule"`
	OTPSeed           *string `json:"otpSeed"`
}

// CommunityInfoResponse represents the API response
type CommunityInfoResponse struct {
	Tenant    TenantInfo    `json:"tenant"`
	Community CommunityInfo `json:"community"`
	Message   *string       `json:"message"`
}

// FetchRequest represents the request body
type FetchRequest struct {
	DNS           string `json:"dns"`
	CommunityName string `json:"communityName"`
}

// FetchCommunityInfo fetches tenant and community information from the API
// Step 1: Calls community_info/fetch on TENANT_DNS to get TENANT_ID and COMMUNITY_ID
// Step 2: Calls /caas/sd to get service discovery URLs
// Step 3: Extracts global_caas domain
// Step 4: Calls community_info/fetch on global_caas domain to get ROOT_TENANT_NAME
func FetchCommunityInfo() (*CommunityInfoResponse, error) {
	// Get environment variables
	tenantDNS := os.Getenv("TENANT_DNS")
	communityName := os.Getenv("COMMUNITY_NAME")

	if tenantDNS == "" {
		return nil, fmt.Errorf("TENANT_DNS environment variable is not set")
	}

	if communityName == "" {
		return nil, fmt.Errorf("COMMUNITY_NAME environment variable is not set")
	}

	// STEP 1: Fetch tenant/community info from TENANT_DNS to get TENANT_ID and COMMUNITY_ID
	apiURL := fmt.Sprintf("https://%s/api/r1/system/community_info/fetch", tenantDNS)
	response, err := fetchCommunityInfoFromURL(apiURL, tenantDNS, communityName)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch community info from TENANT_DNS: %w", err)
	}

	// Set TENANT_ID, TENANT_NAME, and COMMUNITY_ID from first call
	os.Setenv("TENANT_ID", response.Tenant.ID)
	os.Setenv("TENANT_NAME", response.Tenant.Name)
	os.Setenv("COMMUNITY_ID", response.Community.ID)

	// STEP 2: Call service discovery endpoint on TENANT_DNS
	sdURL := fmt.Sprintf("https://%s/caas/sd", tenantDNS)
	serviceDiscovery, err := fetchServiceDiscovery(sdURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch service discovery: %w", err)
	}

	// STEP 3: Extract domain from global_caas URL
	if serviceDiscovery.GlobalCaas == "" {
		return nil, fmt.Errorf("global_caas not found in service discovery response")
	}

	globalCaasDomain, err := extractDomainFromURL(serviceDiscovery.GlobalCaas)
	if err != nil {
		return nil, fmt.Errorf("failed to extract domain from global_caas URL: %w", err)
	}

	// STEP 4: Fetch root tenant info from global CAAS domain
	rootAPIURL := fmt.Sprintf("https://%s/api/r1/system/community_info/fetch", globalCaasDomain)
	rootResponse, err := fetchCommunityInfoFromURL(rootAPIURL, globalCaasDomain, "default")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch root tenant info from global CAAS: %w", err)
	}

	// Set ROOT_TENANT_NAME from global CAAS call
	os.Setenv("ROOT_TENANT_NAME", rootResponse.Tenant.Name)

	return response, nil
}

// fetchServiceDiscovery calls the /caas/sd endpoint to get service URLs
func fetchServiceDiscovery(sdURL string) (*ServiceDiscoveryResponse, error) {
	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Create HTTP request
	req, err := http.NewRequest("GET", sdURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Execute request
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute HTTP request: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// Read response body
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// Parse response
	var sdResponse ServiceDiscoveryResponse
	if err := json.Unmarshal(bodyBytes, &sdResponse); err != nil {
		return nil, fmt.Errorf("failed to parse response JSON: %w", err)
	}

	return &sdResponse, nil
}

// extractDomainFromURL extracts the domain from a URL
// Example: "https://1k-dev.1kosmos.net/caas" -> "1k-dev.1kosmos.net"
func extractDomainFromURL(rawURL string) (string, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL: %w", err)
	}

	if parsedURL.Host == "" {
		return "", fmt.Errorf("no host found in URL: %s", rawURL)
	}

	return parsedURL.Host, nil
}

// fetchCommunityInfoFromURL fetches community info from a specific URL
func fetchCommunityInfoFromURL(apiURL, tenantDNS, communityName string) (*CommunityInfoResponse, error) {
	// Create request body
	requestBody := FetchRequest{
		DNS:           tenantDNS,
		CommunityName: communityName,
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute HTTP request: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// Read response body
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// Parse response
	var response CommunityInfoResponse
	if err := json.Unmarshal(bodyBytes, &response); err != nil {
		return nil, fmt.Errorf("failed to parse response JSON: %w", err)
	}

	return &response, nil
}

// GetTenantID returns the TENANT_ID from environment or fetches it
func GetTenantID() (string, error) {
	tenantID := os.Getenv("TENANT_ID")
	if tenantID != "" {
		return tenantID, nil
	}

	// Fetch from API
	response, err := FetchCommunityInfo()
	if err != nil {
		return "", err
	}

	return response.Tenant.ID, nil
}

// GetCommunityID returns the COMMUNITY_ID from environment or fetches it
func GetCommunityID() (string, error) {
	communityID := os.Getenv("COMMUNITY_ID")
	if communityID != "" {
		return communityID, nil
	}

	// Fetch from API
	response, err := FetchCommunityInfo()
	if err != nil {
		return "", err
	}

	return response.Community.ID, nil
}
