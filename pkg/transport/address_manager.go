package transport

import (
	"fmt"
	"sync"
)

// AddressManager manages dynamically discovered VM-sync TCP addresses
type AddressManager struct {
	mu        sync.RWMutex
	addresses map[string]string // clientID -> TCP address
}

var (
	globalAddressManager *AddressManager
	once                 sync.Once
)

// GetAddressManager returns the singleton instance
func GetAddressManager() *AddressManager {
	once.Do(func() {
		globalAddressManager = &AddressManager{
			addresses: make(map[string]string),
		}
	})
	return globalAddressManager
}

// SetAddress stores the TCP address for a client
func (am *AddressManager) SetAddress(clientID, address string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.addresses[clientID] = address
}

// GetAddress retrieves the TCP address for a client
func (am *AddressManager) GetAddress(clientID string) (string, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	
	address, exists := am.addresses[clientID]
	if !exists {
		return "", fmt.Errorf("no TCP address found for client %s", clientID)
	}
	return address, nil
}

// GetAnyAddress returns any available TCP address (for single VM scenarios)
func (am *AddressManager) GetAnyAddress() (string, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	
	if len(am.addresses) == 0 {
		return "", fmt.Errorf("no TCP addresses available")
	}
	
	// Return first available address
	for _, addr := range am.addresses {
		return addr, nil
	}
	return "", fmt.Errorf("no TCP addresses available")
}

// RemoveAddress removes the TCP address for a client
func (am *AddressManager) RemoveAddress(clientID string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	delete(am.addresses, clientID)
}

// HasAddress checks if a client has a registered TCP address
func (am *AddressManager) HasAddress(clientID string) bool {
	am.mu.RLock()
	defer am.mu.RUnlock()
	_, exists := am.addresses[clientID]
	return exists
}

// GetAllAddresses returns all registered addresses (for debugging)
func (am *AddressManager) GetAllAddresses() map[string]string {
	am.mu.RLock()
	defer am.mu.RUnlock()
	
	// Return a copy to avoid race conditions
	copy := make(map[string]string, len(am.addresses))
	for k, v := range am.addresses {
		copy[k] = v
	}
	return copy
}
