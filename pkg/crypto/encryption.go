package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"

	"go-data-sync-http/pkg/models"
)

// EncryptionManager handles AES-256-GCM encryption/decryption for data sync
type EncryptionManager struct {
	enabled bool
	key     []byte
	keyID   string
	gcm     cipher.AEAD
}

// NewEncryptionManager creates a new encryption manager
func NewEncryptionManager() *EncryptionManager {
	return &EncryptionManager{
		enabled: false,
	}
}

// Initialize sets up encryption with the provided configuration
func (em *EncryptionManager) Initialize(config models.EncryptionConfig) error {
	if !config.Enabled || config.Key == "" {
		em.enabled = false
		return nil
	}

	// Decode base64 key or use raw string
	var key []byte
	var err error
	
	if decoded, decodeErr := base64.StdEncoding.DecodeString(config.Key); decodeErr == nil {
		key = decoded
	} else {
		// Use SHA256 hash of the string as key
		hash := sha256.Sum256([]byte(config.Key))
		key = hash[:]
	}

	// Ensure key is 32 bytes for AES-256
	if len(key) != 32 {
		hash := sha256.Sum256(key)
		key = hash[:]
	}

	// Create AES cipher
	block, err := aes.NewCipher(key)
	if err != nil {
		return fmt.Errorf("failed to create AES cipher: %w", err)
	}

	// Create GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return fmt.Errorf("failed to create GCM: %w", err)
	}

	em.key = key
	em.keyID = config.KeyID
	em.gcm = gcm
	em.enabled = true

	return nil
}

// IsEnabled returns whether encryption is enabled
func (em *EncryptionManager) IsEnabled() bool {
	return em.enabled
}

// GetKeyID returns the key ID
func (em *EncryptionManager) GetKeyID() string {
	return em.keyID
}

// EncryptJSON encrypts JSON data using AES-256-GCM
func (em *EncryptionManager) EncryptJSON(data interface{}) ([]byte, error) {
	if !em.enabled {
		return json.Marshal(data)
	}

	// Marshal to JSON first
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}

	return em.Encrypt(jsonData)
}

// DecryptJSON decrypts data and unmarshals to JSON
func (em *EncryptionManager) DecryptJSON(data []byte, target interface{}) error {
	if !em.enabled {
		return json.Unmarshal(data, target)
	}

	// Decrypt first
	decryptedData, err := em.Decrypt(data)
	if err != nil {
		return fmt.Errorf("failed to decrypt data: %w", err)
	}

	// Unmarshal JSON
	return json.Unmarshal(decryptedData, target)
}

// Encrypt encrypts data using AES-256-GCM
func (em *EncryptionManager) Encrypt(data []byte) ([]byte, error) {
	if !em.enabled {
		return data, nil
	}

	// Generate random nonce
	nonce := make([]byte, em.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Encrypt data
	ciphertext := em.gcm.Seal(nonce, nonce, data, nil)
	return ciphertext, nil
}

// Decrypt decrypts data using AES-256-GCM
func (em *EncryptionManager) Decrypt(data []byte) ([]byte, error) {
	if !em.enabled {
		return data, nil
	}

	// Extract nonce
	nonceSize := em.gcm.NonceSize()
	if len(data) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]

	// Decrypt data
	plaintext, err := em.gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w", err)
	}

	return plaintext, nil
}

// GenerateKey generates a new random 256-bit key
func GenerateKey() (string, error) {
	key := make([]byte, 32) // 256 bits
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return "", fmt.Errorf("failed to generate key: %w", err)
	}
	return base64.StdEncoding.EncodeToString(key), nil
}