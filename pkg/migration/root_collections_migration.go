package migration

import (
	"context"
	"fmt"
	"log"
	"os"
	"regexp"
	"time"

	"go-data-sync-http/pkg/models"
	"go-data-sync-http/pkg/transport"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// LicenseAuth represents a document from the licenseauths collection
type LicenseAuth struct {
	ID            interface{} `bson:"_id"`
	KeyTag        string      `bson:"keyTag"`
	CommunityID   string      `bson:"communityId"`
	CommunityName string      `bson:"communityName"`
	IsAuthorized  bool        `bson:"isAuthorized"`
	Expiry        time.Time   `bson:"expiry"`
}

// ServiceKey represents a document from the servicekeys collection
type ServiceKey struct {
	ID        interface{} `bson:"_id"`
	Tag       string      `bson:"tag"`
	KeyID     string      `bson:"keyId"`
	KeySecret string      `bson:"keySecret"`
	Type      string      `bson:"type"`
	Disabled  bool        `bson:"disabled"`
	Expiry    time.Time   `bson:"expiry"`
	AuthLevel string      `bson:"authLevel"`
	V         int         `bson:"__v"`
}

// ConfigStore represents a document from the configstores collection
type ConfigStore struct {
	ID    interface{}            `bson:"_id"`
	Key   string                 `bson:"key"`
	Value map[string]interface{} `bson:"value"`
}

// ServiceDirectory represents a document from the servicedirectory collection
type ServiceDirectory struct {
	ID   interface{} `bson:"_id"`
	URL  string      `bson:"url"`
	Name string      `bson:"name"`
	// Include other fields as map to preserve unknown fields
	OtherFields map[string]interface{} `bson:",inline"`
}

// RootCollectionsMigration handles migration of licenses and service keys from root tenant
type RootCollectionsMigration struct {
	rootMongoClient *mongo.Client
	rootURI         string
	tcpSender       transport.Sender
	tenantName      string
	communityID     string
	rootTenantName  string
}

// NewRootCollectionsMigration creates a new root collections migration instance
func NewRootCollectionsMigration(cfg *models.Config, tcpSender transport.Sender) (*RootCollectionsMigration, error) {
	rootURI := cfg.MongoDB.RootURI
	if rootURI == "" {
		log.Println("⚠️  ROOT MIGRATION: root_uri not configured, migration disabled")
		return nil, nil // Return nil without error - migration is optional
	}

	tenantName := os.Getenv("TENANT_NAME")
	if tenantName == "" {
		tenantName = "default"
	}

	communityID := os.Getenv("COMMUNITY_ID")
	if communityID == "" {
		log.Println("⚠️  ROOT MIGRATION: COMMUNITY_ID not set, migration disabled")
		return nil, nil // Return nil without error - migration is optional
	}

	rootTenantName := os.Getenv("ROOT_TENANT_NAME")
	if rootTenantName == "" {
		log.Printf("⚠️  WARNING: ROOT_TENANT_NAME not set, will try to use TENANT_NAME")
		rootTenantName = tenantName
	}

	return &RootCollectionsMigration{
		rootURI:        rootURI,
		tcpSender:      tcpSender,
		tenantName:     tenantName,
		communityID:    communityID,
		rootTenantName: rootTenantName,
	}, nil
}

// Connect establishes connection to root MongoDB
func (rcm *RootCollectionsMigration) Connect(ctx context.Context) error {
	clientOptions := options.Client().ApplyURI(rcm.rootURI)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to root MongoDB: %w", err)
	}

	// Ping to verify connection
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("failed to ping root MongoDB: %w", err)
	}

	rcm.rootMongoClient = client
	log.Printf("✅ ROOT MIGRATION: Connected to root MongoDB successfully")
	return nil
}

// Close closes the root MongoDB connection
func (rcm *RootCollectionsMigration) Close(ctx context.Context) error {
	if rcm.rootMongoClient != nil {
		return rcm.rootMongoClient.Disconnect(ctx)
	}
	return nil
}

// MigrateServiceKeys performs the complete migration workflow
func (rcm *RootCollectionsMigration) MigrateServiceKeys(ctx context.Context) error {
	log.Println("🔄 ROOT MIGRATION: Starting service keys migration...")
	log.Printf("   Root Tenant: %s, Community ID: %s", rcm.rootTenantName, rcm.communityID)

	// Step 1: Get database name
	licenseDBName := fmt.Sprintf("licenses-%s", rcm.rootTenantName)
	log.Printf("📊 ROOT MIGRATION: Querying database '%s'", licenseDBName)

	// Step 2: Query licenseauths collection
	keyTags, licenseAuths, err := rcm.fetchAuthorizedKeyTags(ctx, licenseDBName)
	if err != nil {
		return fmt.Errorf("failed to fetch authorized key tags: %w", err)
	}

	if len(keyTags) == 0 {
		log.Printf("⚠️  ROOT MIGRATION: No authorized licenses found for community %s", rcm.communityID)
		return nil
	}

	log.Printf("✅ ROOT MIGRATION: Found %d authorized key tags: %v", len(keyTags), keyTags)

	// Step 3: Query serviceskeys collection
	serviceKeys, err := rcm.fetchServiceKeys(ctx, licenseDBName, keyTags)
	if err != nil {
		return fmt.Errorf("failed to fetch service keys: %w", err)
	}

	if len(serviceKeys) == 0 {
		log.Printf("⚠️  ROOT MIGRATION: No service keys found for tags: %v", keyTags)
		log.Printf("⚠️  Checked collection 'serviceskeys' (note: plural form)")
		return nil
	}

	log.Printf("✅ ROOT MIGRATION: Found %d service keys to migrate", len(serviceKeys))

	// Step 4: Send licenseauths to VM-sync via TCP
	if err := rcm.sendLicenseAuthsToVMSync(ctx, licenseAuths); err != nil {
		return fmt.Errorf("failed to send license auths to VM-sync: %w", err)
	}

	log.Printf("✅ ROOT MIGRATION: Successfully migrated %d license auths to VM-sync", len(licenseAuths))

	// Step 5: Send service keys to VM-sync via TCP
	if err := rcm.sendServiceKeysToVMSync(ctx, serviceKeys); err != nil {
		return fmt.Errorf("failed to send service keys to VM-sync: %w", err)
	}

	log.Printf("✅ ROOT MIGRATION: Successfully migrated %d service keys to VM-sync", len(serviceKeys))
	return nil
}

// MigrateConfigStores migrates specific config documents from root tenant
func (rcm *RootCollectionsMigration) MigrateConfigStores(ctx context.Context) error {
	log.Println("🔄 ROOT MIGRATION: Starting config stores migration...")
	log.Printf("   Root Tenant: %s", rcm.rootTenantName)

	// Step 1: Get database name
	caasDBName := fmt.Sprintf("caas-%s", rcm.rootTenantName)
	log.Printf("📊 ROOT MIGRATION: Querying database '%s'", caasDBName)

	// Step 2: Query configstores collection for specific keys
	configStores, err := rcm.fetchConfigStores(ctx, caasDBName)
	if err != nil {
		return fmt.Errorf("failed to fetch config stores: %w", err)
	}

	if len(configStores) == 0 {
		log.Printf("⚠️  ROOT MIGRATION: No config stores found")
		return nil
	}

	log.Printf("✅ ROOT MIGRATION: Found %d config stores to migrate", len(configStores))

	// Step 3: Send config stores to VM-sync via TCP
	if err := rcm.sendConfigStoresToVMSync(ctx, configStores); err != nil {
		return fmt.Errorf("failed to send config stores to VM-sync: %w", err)
	}

	log.Printf("✅ ROOT MIGRATION: Successfully migrated %d config stores to VM-sync", len(configStores))

	// Step 4: Migrate servicedirectory collection with URL transformation
	if err := rcm.MigrateServiceDirectory(ctx); err != nil {
		return fmt.Errorf("failed to migrate servicedirectory: %w", err)
	}

	return nil
}

// fetchConfigStores queries configstores collection for specific config keys
func (rcm *RootCollectionsMigration) fetchConfigStores(ctx context.Context, dbName string) ([]ConfigStore, error) {
	collection := rcm.rootMongoClient.Database(dbName).Collection("configstores")

	// Query for specific config keys
	filter := bson.M{
		"key": bson.M{
			"$in": []string{
				"appId-adminconsole.global",
				"appId-platform_internal-true",
			},
		},
	}

	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to query configstores: %w", err)
	}
	defer cursor.Close(ctx)

	var configStores []ConfigStore
	for cursor.Next(ctx) {
		var config ConfigStore
		if err := cursor.Decode(&config); err != nil {
			log.Printf("⚠️  WARNING: Failed to decode config store document: %v", err)
			continue
		}
		configStores = append(configStores, config)
		log.Printf("   Found config key: %s", config.Key)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return configStores, nil
}

// sendConfigStoresToVMSync sends config stores to VM-sync via TCP transport
func (rcm *RootCollectionsMigration) sendConfigStoresToVMSync(ctx context.Context, configStores []ConfigStore) error {
	if rcm.tcpSender == nil {
		return fmt.Errorf("TCP sender not initialized")
	}

	// Target database on VM side: hardcoded to caas-1kosmos
	targetDBName := "caas-1kosmos"
	targetCollectionName := "configstores"

	// Stream name format: database.collection
	stream := fmt.Sprintf("%s.%s", targetDBName, targetCollectionName)

	log.Printf("📤 ROOT MIGRATION: Sending %d config stores to VM-sync", len(configStores))
	log.Printf("   Target: %s (stream: %s)", stream, stream)

	// Convert config stores to BSON byte arrays
	var batchBytes [][]byte
	for _, config := range configStores {
		doc := bson.M{
			"_id":   config.ID,
			"key":   config.Key,
			"value": config.Value,
		}

		// Marshal to BSON bytes
		bsonBytes, err := bson.Marshal(doc)
		if err != nil {
			log.Printf("⚠️  WARNING: Failed to marshal config store %s: %v", config.Key, err)
			continue
		}
		batchBytes = append(batchBytes, bsonBytes)
	}

	if len(batchBytes) == 0 {
		return fmt.Errorf("no valid config stores to send")
	}

	// Send via TCP using the stream name
	if err := rcm.tcpSender.SendBatch(stream, batchBytes); err != nil {
		return fmt.Errorf("failed to send batch via TCP: %w", err)
	}

	log.Printf("✅ ROOT MIGRATION: TCP batch sent successfully (%d documents)", len(batchBytes))
	return nil
}

// MigrateServiceDirectory migrates servicedirectory collection with URL domain transformation
func (rcm *RootCollectionsMigration) MigrateServiceDirectory(ctx context.Context) error {
	log.Println("🔄 ROOT MIGRATION: Starting servicedirectory migration with URL transformation...")
	log.Printf("   Root Tenant: %s, Target Tenant: %s", rcm.rootTenantName, rcm.tenantName)

	// Step 1: Get source database name
	caasDBName := fmt.Sprintf("caas-%s", rcm.rootTenantName)
	log.Printf("📊 ROOT MIGRATION: Querying database '%s'", caasDBName)

	// Step 2: Fetch servicedirectory documents
	serviceDirectories, err := rcm.fetchServiceDirectories(ctx, caasDBName)
	if err != nil {
		return fmt.Errorf("failed to fetch servicedirectory: %w", err)
	}

	if len(serviceDirectories) == 0 {
		log.Printf("⚠️  ROOT MIGRATION: No servicedirectory documents found")
		return nil
	}

	log.Printf("✅ ROOT MIGRATION: Found %d servicedirectory documents to migrate", len(serviceDirectories))

	// Step 3: Transform URLs and send to VM-sync
	if err := rcm.sendServiceDirectoriesToVMSync(ctx, serviceDirectories); err != nil {
		return fmt.Errorf("failed to send servicedirectory to VM-sync: %w", err)
	}

	log.Printf("✅ ROOT MIGRATION: Successfully migrated %d servicedirectory documents with URL transformation", len(serviceDirectories))
	return nil
}

// fetchServiceDirectories queries servicedirectory collection
func (rcm *RootCollectionsMigration) fetchServiceDirectories(ctx context.Context, dbName string) ([]bson.M, error) {
	collection := rcm.rootMongoClient.Database(dbName).Collection("servicedirectory")

	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("failed to query servicedirectory: %w", err)
	}
	defer cursor.Close(ctx)

	var docs []bson.M
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			log.Printf("⚠️  WARNING: Failed to decode servicedirectory document: %v", err)
			continue
		}
		docs = append(docs, doc)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return docs, nil
}

// sendServiceDirectoriesToVMSync sends servicedirectory documents with URL transformation
func (rcm *RootCollectionsMigration) sendServiceDirectoriesToVMSync(ctx context.Context, serviceDirectories []bson.M) error {
	if rcm.tcpSender == nil {
		return fmt.Errorf("TCP sender not initialized")
	}

	// Target database: caas-{TENANT_NAME}
	targetDBName := fmt.Sprintf("caas-%s", rcm.tenantName)
	targetCollectionName := "servicedirectory"
	stream := fmt.Sprintf("%s.%s", targetDBName, targetCollectionName)

	log.Printf("📤 ROOT MIGRATION: Transforming and sending %d servicedirectory documents", len(serviceDirectories))
	log.Printf("   Target: %s (stream: %s)", stream, stream)

	// Get TENANT_DNS for URL transformation
	tenantDNS := os.Getenv("TENANT_DNS")
	if tenantDNS == "" {
		log.Printf("⚠️  WARNING: TENANT_DNS not set, URL transformation will be skipped")
	}

	var batchBytes [][]byte
	for _, doc := range serviceDirectories {
		// Transform URL field if present
		if tenantDNS != "" && doc["url"] != nil {
			if urlStr, ok := doc["url"].(string); ok {
				transformedURL := transformServiceDirectoryURL(urlStr, tenantDNS)
				if transformedURL != urlStr {
					log.Printf("🔄 URL TRANSFORMED: %s -> %s", urlStr, transformedURL)
					doc["url"] = transformedURL
				}
			}
		}

		// Marshal to BSON bytes
		bsonBytes, err := bson.Marshal(doc)
		if err != nil {
			log.Printf("⚠️  WARNING: Failed to marshal servicedirectory document: %v", err)
			continue
		}
		batchBytes = append(batchBytes, bsonBytes)
	}

	if len(batchBytes) == 0 {
		return fmt.Errorf("no valid servicedirectory documents to send")
	}

	// Send via TCP
	if err := rcm.tcpSender.SendBatch(stream, batchBytes); err != nil {
		return fmt.Errorf("failed to send batch via TCP: %w", err)
	}

	log.Printf("✅ ROOT MIGRATION: TCP batch sent successfully (%d documents)", len(batchBytes))
	return nil
}

// fetchAuthorizedKeyTags queries licenseauths and returns both authorized key tags and the licenseauths documents
func (rcm *RootCollectionsMigration) fetchAuthorizedKeyTags(ctx context.Context, dbName string) ([]string, []LicenseAuth, error) {
	collection := rcm.rootMongoClient.Database(dbName).Collection("licenseauths")

	filter := bson.M{
		"communityId":  rcm.communityID,
		"isAuthorized": true,
	}

	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query licenseauths: %w", err)
	}
	defer cursor.Close(ctx)

	var keyTags []string
	var licenseAuths []LicenseAuth
	for cursor.Next(ctx) {
		var auth LicenseAuth
		if err := cursor.Decode(&auth); err != nil {
			log.Printf("⚠️  WARNING: Failed to decode license auth document: %v", err)
			continue
		}
		if auth.KeyTag != "" {
			keyTags = append(keyTags, auth.KeyTag)
		}
		licenseAuths = append(licenseAuths, auth) // Store for later migration
	}

	if err := cursor.Err(); err != nil {
		return nil, nil, fmt.Errorf("cursor error: %w", err)
	}

	return keyTags, licenseAuths, nil
}

// fetchServiceKeys queries serviceskeys collection (note: plural 'services') for given tags
func (rcm *RootCollectionsMigration) fetchServiceKeys(ctx context.Context, dbName string, keyTags []string) ([]ServiceKey, error) {
	collection := rcm.rootMongoClient.Database(dbName).Collection("serviceskeys") // Note: 'serviceskeys' not 'servicekeys'

	filter := bson.M{
		"tag": bson.M{"$in": keyTags},
	}

	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to query servicekeys: %w", err)
	}
	defer cursor.Close(ctx)

	var serviceKeys []ServiceKey
	for cursor.Next(ctx) {
		var key ServiceKey
		if err := cursor.Decode(&key); err != nil {
			log.Printf("⚠️  WARNING: Failed to decode service key document: %v", err)
			continue
		}
		serviceKeys = append(serviceKeys, key)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return serviceKeys, nil
}

// sendLicenseAuthsToVMSync sends license auths to VM-sync via TCP transport
func (rcm *RootCollectionsMigration) sendLicenseAuthsToVMSync(ctx context.Context, licenseAuths []LicenseAuth) error {
	if rcm.tcpSender == nil {
		return fmt.Errorf("TCP sender not initialized")
	}

	// Target database on VM side: hardcoded to licenses-1kosmos
	targetDBName := "licenses-1kosmos"
	targetCollectionName := "licenseauths"

	// Stream name format: database.collection
	stream := fmt.Sprintf("%s.%s", targetDBName, targetCollectionName)

	log.Printf("📤 ROOT MIGRATION: Sending %d license auths to VM-sync", len(licenseAuths))
	log.Printf("   Target: %s (stream: %s)", stream, stream)

	// Convert license auths to BSON byte arrays
	var batchBytes [][]byte
	for _, auth := range licenseAuths {
		doc := bson.M{
			"_id":           auth.ID,
			"keyTag":        auth.KeyTag,
			"communityId":   auth.CommunityID,
			"communityName": auth.CommunityName,
			"isAuthorized":  auth.IsAuthorized,
			"expiry":        auth.Expiry,
		}

		// Marshal to BSON bytes
		bsonBytes, err := bson.Marshal(doc)
		if err != nil {
			log.Printf("⚠️  WARNING: Failed to marshal license auth %s: %v", auth.KeyTag, err)
			continue
		}
		batchBytes = append(batchBytes, bsonBytes)
	}

	if len(batchBytes) == 0 {
		return fmt.Errorf("no valid license auths to send")
	}

	// Send via TCP using the stream name
	if err := rcm.tcpSender.SendBatch(stream, batchBytes); err != nil {
		return fmt.Errorf("failed to send batch via TCP: %w", err)
	}

	log.Printf("✅ ROOT MIGRATION: TCP batch sent successfully (%d documents)", len(batchBytes))
	return nil
}

// sendServiceKeysToVMSync sends service keys to VM-sync via TCP transport
func (rcm *RootCollectionsMigration) sendServiceKeysToVMSync(ctx context.Context, serviceKeys []ServiceKey) error {
	if rcm.tcpSender == nil {
		return fmt.Errorf("TCP sender not initialized")
	}

	// Target database on VM side: hardcoded to licenses-1kosmos
	targetDBName := "licenses-1kosmos"
	targetCollectionName := "serviceskeys" // Note: plural 'serviceskeys'

	// Stream name format: database.collection
	stream := fmt.Sprintf("%s.%s", targetDBName, targetCollectionName)

	log.Printf("📤 ROOT MIGRATION: Sending %d service keys to VM-sync", len(serviceKeys))
	log.Printf("   Target: %s (stream: %s)", stream, stream)

	// Convert service keys to BSON byte arrays
	var batchBytes [][]byte
	for _, key := range serviceKeys {
		doc := bson.M{
			"_id":       key.ID,
			"tag":       key.Tag,
			"keyId":     key.KeyID,
			"keySecret": key.KeySecret,
			"type":      key.Type,
			"disabled":  key.Disabled,
			"expiry":    key.Expiry,
			"authLevel": key.AuthLevel,
			"__v":       key.V,
		}

		// Marshal to BSON bytes
		bsonBytes, err := bson.Marshal(doc)
		if err != nil {
			log.Printf("⚠️  WARNING: Failed to marshal service key %s: %v", key.Tag, err)
			continue
		}
		batchBytes = append(batchBytes, bsonBytes)
	}

	if len(batchBytes) == 0 {
		return fmt.Errorf("no valid service keys to send")
	}

	// Send via TCP using the stream name
	if err := rcm.tcpSender.SendBatch(stream, batchBytes); err != nil {
		return fmt.Errorf("failed to send batch via TCP: %w", err)
	}

	log.Printf("✅ ROOT MIGRATION: TCP batch sent successfully (%d documents)", len(batchBytes))
	return nil
}

// RunAfterInitialDump waits for initial dump completion and triggers migration
func (rcm *RootCollectionsMigration) RunAfterInitialDump(initialDumpCompleted <-chan bool) {
	go func() {
		log.Println("⏳ ROOT MIGRATION: Waiting for initial dump to complete...")

		// Wait for initial dump signal
		<-initialDumpCompleted

		log.Println("🚀 ROOT MIGRATION: Initial dump completed, starting migration...")

		// Give a small delay to ensure TCP connections are stable
		time.Sleep(2 * time.Second)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		// Connect to root MongoDB
		if err := rcm.Connect(ctx); err != nil {
			log.Printf("❌ ROOT MIGRATION: Failed to connect to root MongoDB: %v", err)
			return
		}
		defer rcm.Close(ctx)

		// Perform license/service keys migration
		if err := rcm.MigrateServiceKeys(ctx); err != nil {
			log.Printf("❌ ROOT MIGRATION: License/ServiceKeys migration failed: %v", err)
			// Continue to config migration even if this fails
		}

		// Perform config stores migration
		if err := rcm.MigrateConfigStores(ctx); err != nil {
			log.Printf("❌ ROOT MIGRATION: ConfigStores migration failed: %v", err)
			return
		}

		log.Println("✅ ROOT MIGRATION: Migration workflow completed successfully")
	}()
}

// transformServiceDirectoryURL transforms URLs by replacing the domain with TENANT_DNS
// Example: "https://1k-dev.1kosmos.net/vcs" -> "https://blockid-dev.1kosmos.net/vcs"
func transformServiceDirectoryURL(originalURL, tenantDNS string) string {
	if originalURL == "" || tenantDNS == "" {
		return originalURL
	}

	// Regular expression to match domain in URL
	// Pattern: protocol://domain/path -> replace domain with tenantDNS
	domainRegex := regexp.MustCompile(`^(https?://)[^/]+(/.*)?$`)

	if domainRegex.MatchString(originalURL) {
		// Extract protocol and path
		matches := domainRegex.FindStringSubmatch(originalURL)
		if len(matches) >= 2 {
			protocol := matches[1] // "https://" or "http://"
			path := ""
			if len(matches) >= 3 {
				path = matches[2] // "/vcs" or empty
			}

			// Construct new URL with TENANT_DNS
			newURL := fmt.Sprintf("%s%s%s", protocol, tenantDNS, path)
			return newURL
		}
	}

	// If no match, return original
	return originalURL
}
