package transport

import (
	"fmt"
	"sync"
)

// VMEndpoints stores both TCP and HTTP endpoints for a VM
type VMEndpoints struct {
	TCPAddress  string
	HTTPAddress string
}

// AddressManager manages dynamically discovered VM-sync TCP and HTTP addresses
type AddressManager struct {
	mu        sync.RWMutex
	endpoints map[string]*VMEndpoints // clientID -> endpoints
}

var (
	globalAddressManager *AddressManager
	once                 sync.Once
)

// GetAddressManager returns the singleton instance
func GetAddressManager() *AddressManager {
	once.Do(func() {
		globalAddressManager = &AddressManager{
			endpoints: make(map[string]*VMEndpoints),
		}
	})
	return globalAddressManager
}

// SetAddress stores the TCP address for a client (backward compatibility)
func (am *AddressManager) SetAddress(clientID, address string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	if am.endpoints[clientID] == nil {
		am.endpoints[clientID] = &VMEndpoints{}
	}
	am.endpoints[clientID].TCPAddress = address
}

// SetEndpoints stores both TCP and HTTP addresses for a client
func (am *AddressManager) SetEndpoints(clientID, tcpAddress, httpAddress string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.endpoints[clientID] = &VMEndpoints{
		TCPAddress:  tcpAddress,
		HTTPAddress: httpAddress,
	}
}

// GetAddress retrieves the TCP address for a client
func (am *AddressManager) GetAddress(clientID string) (string, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	
	endpoints, exists := am.endpoints[clientID]
	if !exists || endpoints.TCPAddress == "" {
		return "", fmt.Errorf("no TCP address found for client %s", clientID)
	}
	return endpoints.TCPAddress, nil
}

// GetHTTPAddress retrieves the HTTP address for a client
func (am *AddressManager) GetHTTPAddress(clientID string) (string, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	
	endpoints, exists := am.endpoints[clientID]
	if !exists || endpoints.HTTPAddress == "" {
		return "", fmt.Errorf("no HTTP address found for client %s", clientID)
	}
	return endpoints.HTTPAddress, nil
}

// GetAnyAddress returns any available TCP address (for single VM scenarios)
func (am *AddressManager) GetAnyAddress() (string, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	
	if len(am.endpoints) == 0 {
		return "", fmt.Errorf("no TCP addresses available")
	}
	
	// Return first available TCP address
	for _, ep := range am.endpoints {
		if ep.TCPAddress != "" {
			return ep.TCPAddress, nil
		}
	}
	return "", fmt.Errorf("no TCP addresses available")
}

// GetAnyHTTPAddress returns any available HTTP address (for single VM scenarios)
func (am *AddressManager) GetAnyHTTPAddress() (string, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	
	if len(am.endpoints) == 0 {
		return "", fmt.Errorf("no HTTP addresses available")
	}
	
	// Return first available HTTP address
	for _, ep := range am.endpoints {
		if ep.HTTPAddress != "" {
			return ep.HTTPAddress, nil
		}
	}
	return "", fmt.Errorf("no HTTP addresses available")
}

// RemoveAddress removes the endpoints for a client
func (am *AddressManager) RemoveAddress(clientID string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	delete(am.endpoints, clientID)
}

// HasAddress checks if a client has registered endpoints
func (am *AddressManager) HasAddress(clientID string) bool {
	am.mu.RLock()
	defer am.mu.RUnlock()
	_, exists := am.endpoints[clientID]
	return exists
}

// GetAllAddresses returns all registered TCP addresses (for debugging)
func (am *AddressManager) GetAllAddresses() map[string]string {
	am.mu.RLock()
	defer am.mu.RUnlock()
	
	// Return a copy to avoid race conditions
	copy := make(map[string]string, len(am.endpoints))
	for k, v := range am.endpoints {
		if v.TCPAddress != "" {
			copy[k] = v.TCPAddress
		}
	}
	return copy
}

// GetAllEndpoints returns all registered endpoints (for debugging)
func (am *AddressManager) GetAllEndpoints() map[string]*VMEndpoints {
	am.mu.RLock()
	defer am.mu.RUnlock()
	
	// Return a copy to avoid race conditions
	copy := make(map[string]*VMEndpoints, len(am.endpoints))
	for k, v := range am.endpoints {
		copy[k] = &VMEndpoints{
			TCPAddress:  v.TCPAddress,
			HTTPAddress: v.HTTPAddress,
		}
	}
	return copy
}
