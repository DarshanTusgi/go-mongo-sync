package license

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/google/uuid"
)

// LicenseKey represents a license key with UUID
type LicenseKey struct {
	UUID string `json:"uuid"`
}

// LicenseValidator handles license validation for WebSocket connections
type LicenseValidator struct {
	mu           sync.RWMutex
	cloudLicense *LicenseKey
}

// NewLicenseValidator creates a new license validator
func NewLicenseValidator() (*LicenseValidator, error) {
	validator := &LicenseValidator{}

	// Load cloud-sync license from environment
	cloudLicense, err := loadLicenseFromEnv("CLOUD_SYNC_LICENSE")
	if err != nil {
		return nil, fmt.Errorf("failed to load cloud-sync license: %w", err)
	}
	validator.cloudLicense = cloudLicense

	return validator, nil
}

// loadLicenseFromEnv loads a license key from environment variable
func loadLicenseFromEnv(envVar string) (*LicenseKey, error) {
	licenseStr := os.Getenv(envVar)
	if licenseStr == "" {
		return nil, fmt.Errorf("environment variable %s is not set", envVar)
	}

	return parseLicenseKey(licenseStr)
}

// parseLicenseKey parses a license key string in UUID format
func parseLicenseKey(licenseStr string) (*LicenseKey, error) {
	uuidStr := strings.TrimSpace(licenseStr)

	if uuidStr == "" {
		return nil, fmt.Errorf("license uuid cannot be empty")
	}

	// Validate UUID format
	_, err := uuid.Parse(uuidStr)
	if err != nil {
		return nil, fmt.Errorf("invalid UUID format '%s': %w", uuidStr, err)
	}

	return &LicenseKey{
		UUID: uuidStr,
	}, nil
}

// ValidateVMConnection validates if a VM-sync instance can connect
func (lv *LicenseValidator) ValidateVMConnection(vmLicense *LicenseKey) error {
	lv.mu.RLock()
	defer lv.mu.RUnlock()

	if lv.cloudLicense == nil {
		return fmt.Errorf("cloud license not configured")
	}

	if vmLicense == nil {
		return fmt.Errorf("vm license not provided")
	}

	// Check if the VM license UUID matches the cloud license UUID
	if vmLicense.UUID != lv.cloudLicense.UUID {
		return fmt.Errorf("vm license UUID '%s' does not match cloud license UUID '%s'", vmLicense.UUID, lv.cloudLicense.UUID)
	}

	return nil
}

// GetCloudLicense returns the cloud-sync license
func (lv *LicenseValidator) GetCloudLicense() *LicenseKey {
	lv.mu.RLock()
	defer lv.mu.RUnlock()
	return lv.cloudLicense
}



// LoadVMLicense loads a VM license from the VM_SYNC_LICENSE environment variable
func LoadVMLicense() (*LicenseKey, error) {
	return loadLicenseFromEnv("VM_SYNC_LICENSE")
}

// String returns a string representation of the license key
func (lk *LicenseKey) String() string {
	return lk.UUID
}

// Equals checks if two license keys are equal
func (lk *LicenseKey) Equals(other *LicenseKey) bool {
	if lk == nil || other == nil {
		return lk == other
	}
	return lk.UUID == other.UUID
}