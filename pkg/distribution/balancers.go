package distribution

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"go-data-sync-http/pkg/models"
)

// CollectionDistributor handles intelligent distribution of collections to VM instances
type CollectionDistributor struct {
	mu                 sync.RWMutex
	connectedVMs       map[string]*VMInstance
	collectionRegistry map[string]*CollectionInfo
	distributionMode   DistributionMode
	loadBalancer       LoadBalancer
	ctx                context.Context
	cancel             context.CancelFunc
}

// VMInstance represents a connected VM-sync client
type VMInstance struct {
	ClientID      string                 `json:"client_id"`
	ConnectedAt   time.Time              `json:"connected_at"`
	LastSeen      time.Time              `json:"last_seen"`
	Capabilities  VMCapabilities         `json:"capabilities"`
	AssignedColls map[string]bool        `json:"assigned_collections"`
	LoadMetrics   *VMLoadMetrics         `json:"load_metrics,omitempty"`
	Status        VMStatus               `json:"status"`
	TCPEndpoint   string                 `json:"tcp_endpoint,omitempty"`
	HTTPEndpoint  string                 `json:"http_endpoint,omitempty"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
}

// VMCapabilities represents what a VM instance can handle
type VMCapabilities struct {
	MaxCollections     int      `json:"max_collections"`
	PreferredDatabases []string `json:"preferred_databases,omitempty"`
	ExcludedDatabases  []string `json:"excluded_databases,omitempty"`
	SupportsTCP        bool     `json:"supports_tcp"`
	SupportsHTTP       bool     `json:"supports_http"`
	MaxConcurrency     int      `json:"max_concurrency"`
	MemoryLimitMB      int      `json:"memory_limit_mb,omitempty"`
}

// VMLoadMetrics tracks VM performance and load
type VMLoadMetrics struct {
	CPUUsage       float64   `json:"cpu_usage"`
	MemoryUsage    float64   `json:"memory_usage"`
	ActiveSyncs    int       `json:"active_syncs"`
	ThroughputMBps float64   `json:"throughput_mbps"`
	LastUpdated    time.Time `json:"last_updated"`
	SyncLatencyMs  float64   `json:"sync_latency_ms"`
	ErrorRate      float64   `json:"error_rate"`
	AvailableSlots int       `json:"available_slots"`
}

// VMStatus represents the current status of a VM instance
type VMStatus string

const (
	VMStatusHealthy     VMStatus = "healthy"
	VMStatusOverloaded  VMStatus = "overloaded"
	VMStatusUnreachable VMStatus = "unreachable"
	VMStatusMaintenance VMStatus = "maintenance"
)

// CollectionInfo represents metadata about a collection to be synced
type CollectionInfo struct {
	Database        string    `json:"database"`
	Collection      string    `json:"collection"`
	EstimatedSizeMB int64     `json:"estimated_size_mb"`
	DocumentCount   int64     `json:"document_count"`
	Priority        Priority  `json:"priority"`
	SyncMode        SyncMode  `json:"sync_mode"`
	AssignedTo      string    `json:"assigned_to,omitempty"`
	LastSynced      time.Time `json:"last_synced,omitempty"`
	IsActive        bool      `json:"is_active"`
}

// DistributionMode defines how collections are distributed
type DistributionMode string

const (
	DistributionModeRoundRobin DistributionMode = "round_robin"
	DistributionModeWeighted   DistributionMode = "weighted"
	DistributionModeHashBased  DistributionMode = "hash_based"
	DistributionModeCapability DistributionMode = "capability_based"
	DistributionModeLoad       DistributionMode = "load_balanced"
	DistributionModeGeographic DistributionMode = "geographic"
)

// Priority defines sync priority levels
type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityMedium Priority = "medium"
	PriorityLow    Priority = "low"
)

// SyncMode defines how collection should be synced
type SyncMode string

const (
	SyncModeInitialOnly  SyncMode = "initial_only"
	SyncModeRealtimeOnly SyncMode = "realtime_only"
	SyncModeBoth         SyncMode = "both"
)

// LoadBalancer interface for different load balancing strategies
type LoadBalancer interface {
	SelectVM(collection *CollectionInfo, availableVMs []*VMInstance) (*VMInstance, error)
	UpdateVMMetrics(clientID string, metrics *VMLoadMetrics)
	GetVMHealth(clientID string) VMStatus
}

// DistributionPlan represents the result of auto-distribution
type DistributionPlan struct {
	GeneratedAt      time.Time              `json:"generated_at"`
	Assignments      map[string][]string    `json:"assignments"` // VM ID -> Collections
	LoadBalancing    map[string]*VMLoadInfo `json:"load_balancing"`
	TotalCollections int                    `json:"total_collections"`
	ActiveVMs        int                    `json:"active_vms"`
}

// VMLoadInfo provides load information for a VM
type VMLoadInfo struct {
	ClientID            string   `json:"client_id"`
	AssignedCollections int      `json:"assigned_collections"`
	MaxCapacity         int      `json:"max_capacity"`
	LoadPercentage      float64  `json:"load_percentage"`
	Status              VMStatus `json:"status"`
}

// DistributionStatus provides overall distribution status
type DistributionStatus struct {
	Mode                DistributionMode    `json:"mode"`
	TotalVMs            int                 `json:"total_vms"`
	HealthyVMs          int                 `json:"healthy_vms"`
	TotalCollections    int                 `json:"total_collections"`
	AssignedCollections int                 `json:"assigned_collections"`
	VMStatuses          map[string]VMStatus `json:"vm_statuses"`
	LastUpdated         time.Time           `json:"last_updated"`
}

// NewCollectionDistributor creates a new intelligent collection distributor
func NewCollectionDistributor(mode DistributionMode) *CollectionDistributor {
	ctx, cancel := context.WithCancel(context.Background())

	var lb LoadBalancer
	switch mode {
	case DistributionModeLoad:
		lb = NewLoadBasedBalancer()
	case DistributionModeWeighted:
		lb = NewWeightedBalancer()
	case DistributionModeCapability:
		lb = NewCapabilityBasedBalancer()
	default:
		lb = NewRoundRobinBalancer()
	}

	return &CollectionDistributor{
		connectedVMs:       make(map[string]*VMInstance),
		collectionRegistry: make(map[string]*CollectionInfo),
		distributionMode:   mode,
		loadBalancer:       lb,
		ctx:                ctx,
		cancel:             cancel,
	}
}

// RegisterVM registers a new VM instance for distribution
func (cd *CollectionDistributor) RegisterVM(clientID string, capabilities VMCapabilities, endpoints map[string]string) error {
	cd.mu.Lock()
	defer cd.mu.Unlock()

	vm := &VMInstance{
		ClientID:      clientID,
		ConnectedAt:   time.Now(),
		LastSeen:      time.Now(),
		Capabilities:  capabilities,
		AssignedColls: make(map[string]bool),
		Status:        VMStatusHealthy,
		TCPEndpoint:   endpoints["tcp"],
		HTTPEndpoint:  endpoints["http"],
		Metadata:      make(map[string]interface{}),
	}

	cd.connectedVMs[clientID] = vm
	log.Printf("🔗 VM REGISTERED: %s with %d max collections (TCP: %s, HTTP: %s)",
		clientID, capabilities.MaxCollections, endpoints["tcp"], endpoints["http"])

	return nil
}

// RegisterCollections registers collections for distribution (REMOVES MANUAL CONFIG)
func (cd *CollectionDistributor) RegisterCollections(collections []models.DatabaseConfig) error {
	cd.mu.Lock()
	defer cd.mu.Unlock()

	for _, dbConfig := range collections {
		if !dbConfig.Enabled {
			continue
		}

		for _, collConfig := range dbConfig.Collections {
			collKey := fmt.Sprintf("%s.%s", dbConfig.Name, collConfig.Name)

			// Auto-detect collection info (size, priority, etc.)
			collInfo := &CollectionInfo{
				Database:        dbConfig.Name,
				Collection:      collConfig.Name,
				EstimatedSizeMB: 100,            // TODO: Auto-detect from MongoDB stats
				DocumentCount:   1000,           // TODO: Auto-detect
				Priority:        PriorityMedium, // TODO: Smart priority detection
				SyncMode:        SyncModeBoth,
				IsActive:        true,
			}

			cd.collectionRegistry[collKey] = collInfo
			log.Printf("📋 COLLECTION REGISTERED: %s (auto-detected)", collKey)
		}
	}

	// Trigger automatic distribution
	return cd.redistributeCollections()
}

// AutoDistribute intelligently distributes all collections to available VMs
func (cd *CollectionDistributor) AutoDistribute() (*DistributionPlan, error) {
	cd.mu.Lock()
	defer cd.mu.Unlock()

	plan := &DistributionPlan{
		GeneratedAt:      time.Now(),
		Assignments:      make(map[string][]string),
		LoadBalancing:    make(map[string]*VMLoadInfo),
		TotalCollections: len(cd.collectionRegistry),
		ActiveVMs:        len(cd.connectedVMs),
	}

	// Get available VMs
	availableVMs := cd.getHealthyVMs()
	if len(availableVMs) == 0 {
		return nil, fmt.Errorf("no healthy VMs available for distribution")
	}

	// Get unassigned collections
	unassignedColls := cd.getUnassignedCollections()

	log.Printf("🎯 AUTO-DISTRIBUTE: %d collections across %d VMs", len(unassignedColls), len(availableVMs))

	// Distribute collections using load balancer
	for _, coll := range unassignedColls {
		selectedVM, err := cd.loadBalancer.SelectVM(coll, availableVMs)
		if err != nil {
			log.Printf("❌ DISTRIBUTION ERROR: Failed to select VM for %s.%s: %v", coll.Database, coll.Collection, err)
			continue
		}

		// Assign collection to VM
		collKey := fmt.Sprintf("%s.%s", coll.Database, coll.Collection)
		coll.AssignedTo = selectedVM.ClientID
		selectedVM.AssignedColls[collKey] = true

		// Update plan
		if plan.Assignments[selectedVM.ClientID] == nil {
			plan.Assignments[selectedVM.ClientID] = make([]string, 0)
		}
		plan.Assignments[selectedVM.ClientID] = append(plan.Assignments[selectedVM.ClientID], collKey)

		log.Printf("✅ ASSIGNED: %s → VM %s", collKey, selectedVM.ClientID)
	}

	// Calculate load balancing info
	for clientID, vm := range cd.connectedVMs {
		assignedCount := len(vm.AssignedColls)
		plan.LoadBalancing[clientID] = &VMLoadInfo{
			ClientID:            clientID,
			AssignedCollections: assignedCount,
			MaxCapacity:         vm.Capabilities.MaxCollections,
			LoadPercentage:      float64(assignedCount) / float64(vm.Capabilities.MaxCollections) * 100,
			Status:              vm.Status,
		}
	}

	log.Printf("🎉 DISTRIBUTION COMPLETE: %d collections distributed across %d VMs",
		len(cd.collectionRegistry), len(availableVMs))

	return plan, nil
}

// GetDistributionStatus returns current distribution status
func (cd *CollectionDistributor) GetDistributionStatus() *DistributionStatus {
	cd.mu.RLock()
	defer cd.mu.RUnlock()

	status := &DistributionStatus{
		Mode:                cd.distributionMode,
		TotalVMs:            len(cd.connectedVMs),
		HealthyVMs:          len(cd.getHealthyVMs()),
		TotalCollections:    len(cd.collectionRegistry),
		AssignedCollections: cd.countAssignedCollections(),
		VMStatuses:          make(map[string]VMStatus),
		LastUpdated:         time.Now(),
	}

	for clientID, vm := range cd.connectedVMs {
		status.VMStatuses[clientID] = vm.Status
	}

	return status
}

// Helper methods
func (cd *CollectionDistributor) redistributeCollections() error {
	_, err := cd.AutoDistribute()
	return err
}

func (cd *CollectionDistributor) getHealthyVMs() []*VMInstance {
	var healthy []*VMInstance
	for _, vm := range cd.connectedVMs {
		if vm.Status == VMStatusHealthy && len(vm.AssignedColls) < vm.Capabilities.MaxCollections {
			healthy = append(healthy, vm)
		}
	}
	return healthy
}

func (cd *CollectionDistributor) getUnassignedCollections() []*CollectionInfo {
	var unassigned []*CollectionInfo
	for _, coll := range cd.collectionRegistry {
		if coll.AssignedTo == "" && coll.IsActive {
			unassigned = append(unassigned, coll)
		}
	}
	return unassigned
}

func (cd *CollectionDistributor) countAssignedCollections() int {
	count := 0
	for _, coll := range cd.collectionRegistry {
		if coll.AssignedTo != "" {
			count++
		}
	}
	return count
}

// LoadBasedBalancer implements load-aware VM selection
type LoadBasedBalancer struct {
	mu        sync.RWMutex
	vmMetrics map[string]*VMLoadMetrics
	vmHealth  map[string]VMStatus
}

func NewLoadBasedBalancer() *LoadBasedBalancer {
	return &LoadBasedBalancer{
		vmMetrics: make(map[string]*VMLoadMetrics),
		vmHealth:  make(map[string]VMStatus),
	}
}

func (lb *LoadBasedBalancer) SelectVM(collection *CollectionInfo, availableVMs []*VMInstance) (*VMInstance, error) {
	if len(availableVMs) == 0 {
		return nil, fmt.Errorf("no available VMs")
	}

	// Sort VMs by load score (lower is better)
	sort.Slice(availableVMs, func(i, j int) bool {
		scoreI := lb.calculateLoadScore(availableVMs[i])
		scoreJ := lb.calculateLoadScore(availableVMs[j])
		return scoreI < scoreJ
	})

	selectedVM := availableVMs[0]
	log.Printf("🎯 LOAD-BASED SELECTION: %s selected for %s.%s (load score: %.2f)",
		selectedVM.ClientID, collection.Database, collection.Collection, lb.calculateLoadScore(selectedVM))

	return selectedVM, nil
}

func (lb *LoadBasedBalancer) calculateLoadScore(vm *VMInstance) float64 {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	metrics := lb.vmMetrics[vm.ClientID]
	if metrics == nil {
		return 0.0 // New VM gets priority
	}

	// Weighted load score considering multiple factors
	cpuWeight := 0.4
	memoryWeight := 0.3
	concurrencyWeight := 0.2
	errorWeight := 0.1

	cpuScore := metrics.CPUUsage / 100.0
	memoryScore := metrics.MemoryUsage / 100.0

	// Concurrency score based on active syncs vs max capacity
	concurrencyScore := float64(metrics.ActiveSyncs) / float64(vm.Capabilities.MaxCollections)
	if concurrencyScore > 1.0 {
		concurrencyScore = 1.0
	}

	errorScore := metrics.ErrorRate / 100.0
	if errorScore > 1.0 {
		errorScore = 1.0
	}

	totalScore := (cpuWeight * cpuScore) +
		(memoryWeight * memoryScore) +
		(concurrencyWeight * concurrencyScore) +
		(errorWeight * errorScore)

	return totalScore
}

func (lb *LoadBasedBalancer) UpdateVMMetrics(clientID string, metrics *VMLoadMetrics) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.vmMetrics[clientID] = metrics
}

func (lb *LoadBasedBalancer) GetVMHealth(clientID string) VMStatus {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	if status, exists := lb.vmHealth[clientID]; exists {
		return status
	}
	return VMStatusHealthy
}

// WeightedBalancer implements weighted round-robin based on VM capabilities
type WeightedBalancer struct {
	mu         sync.RWMutex
	vmWeights  map[string]int
	roundRobin int
}

func NewWeightedBalancer() *WeightedBalancer {
	return &WeightedBalancer{
		vmWeights: make(map[string]int),
	}
}

func (wb *WeightedBalancer) SelectVM(collection *CollectionInfo, availableVMs []*VMInstance) (*VMInstance, error) {
	if len(availableVMs) == 0 {
		return nil, fmt.Errorf("no available VMs")
	}

	// Calculate total weight
	totalWeight := 0
	for _, vm := range availableVMs {
		weight := wb.getVMWeight(vm)
		totalWeight += weight
	}

	if totalWeight == 0 {
		// Fallback to round-robin
		wb.mu.Lock()
		selected := availableVMs[wb.roundRobin%len(availableVMs)]
		wb.roundRobin++
		wb.mu.Unlock()
		return selected, nil
	}

	// Weighted selection
	wb.mu.Lock()
	defer wb.mu.Unlock()

	target := wb.roundRobin % totalWeight
	wb.roundRobin++

	currentWeight := 0
	for _, vm := range availableVMs {
		weight := wb.getVMWeight(vm)
		currentWeight += weight
		if target < currentWeight {
			log.Printf("🎯 WEIGHTED SELECTION: %s selected for %s.%s (weight: %d)",
				vm.ClientID, collection.Database, collection.Collection, weight)
			return vm, nil
		}
	}

	// Fallback
	return availableVMs[0], nil
}

func (wb *WeightedBalancer) getVMWeight(vm *VMInstance) int {
	// Weight based on max collections capacity
	weight := vm.Capabilities.MaxCollections

	// Adjust based on current load
	currentLoad := len(vm.AssignedColls)
	availableCapacity := weight - currentLoad

	if availableCapacity <= 0 {
		return 0 // VM is at capacity
	}

	return availableCapacity
}

func (wb *WeightedBalancer) UpdateVMMetrics(clientID string, metrics *VMLoadMetrics) {
	// Weighted balancer doesn't need real-time metrics
}

func (wb *WeightedBalancer) GetVMHealth(clientID string) VMStatus {
	return VMStatusHealthy
}

// CapabilityBasedBalancer selects VMs based on their capabilities and preferences
type CapabilityBasedBalancer struct {
	mu sync.RWMutex
}

func NewCapabilityBasedBalancer() *CapabilityBasedBalancer {
	return &CapabilityBasedBalancer{}
}

func (cb *CapabilityBasedBalancer) SelectVM(collection *CollectionInfo, availableVMs []*VMInstance) (*VMInstance, error) {
	if len(availableVMs) == 0 {
		return nil, fmt.Errorf("no available VMs")
	}

	// Filter VMs by database preferences
	preferredVMs := cb.filterByDatabasePreference(collection, availableVMs)
	if len(preferredVMs) == 0 {
		preferredVMs = availableVMs // Use all if no preference match
	}

	// Sort by capability score
	sort.Slice(preferredVMs, func(i, j int) bool {
		scoreI := cb.calculateCapabilityScore(collection, preferredVMs[i])
		scoreJ := cb.calculateCapabilityScore(collection, preferredVMs[j])
		return scoreI > scoreJ // Higher score is better
	})

	selectedVM := preferredVMs[0]
	log.Printf("🎯 CAPABILITY SELECTION: %s selected for %s.%s (capability score: %.2f)",
		selectedVM.ClientID, collection.Database, collection.Collection,
		cb.calculateCapabilityScore(collection, selectedVM))

	return selectedVM, nil
}

func (cb *CapabilityBasedBalancer) filterByDatabasePreference(collection *CollectionInfo, vms []*VMInstance) []*VMInstance {
	var preferred []*VMInstance

	for _, vm := range vms {
		// Skip if database is excluded
		excluded := false
		for _, excludedDB := range vm.Capabilities.ExcludedDatabases {
			if excludedDB == collection.Database {
				excluded = true
				break
			}
		}
		if excluded {
			continue
		}

		// Check if database is preferred (empty means all are acceptable)
		if len(vm.Capabilities.PreferredDatabases) == 0 {
			preferred = append(preferred, vm)
			continue
		}

		for _, preferredDB := range vm.Capabilities.PreferredDatabases {
			if preferredDB == collection.Database {
				preferred = append(preferred, vm)
				break
			}
		}
	}

	return preferred
}

func (cb *CapabilityBasedBalancer) calculateCapabilityScore(collection *CollectionInfo, vm *VMInstance) float64 {
	score := 0.0

	// Available capacity score
	currentLoad := len(vm.AssignedColls)
	availableCapacity := vm.Capabilities.MaxCollections - currentLoad
	if availableCapacity > 0 {
		score += float64(availableCapacity) / float64(vm.Capabilities.MaxCollections) * 50
	}

	// Transport preference score
	if collection.EstimatedSizeMB > 1000 && vm.Capabilities.SupportsTCP {
		score += 20 // Prefer TCP for large collections
	}

	// Memory capacity score
	if vm.Capabilities.MemoryLimitMB > 0 {
		memoryScore := float64(vm.Capabilities.MemoryLimitMB) / 1024.0 // Convert to GB
		score += memoryScore * 5
	}

	// Concurrency score
	score += float64(vm.Capabilities.MaxConcurrency) * 2

	return score
}

func (cb *CapabilityBasedBalancer) UpdateVMMetrics(clientID string, metrics *VMLoadMetrics) {
	// Capability balancer doesn't need real-time metrics
}

func (cb *CapabilityBasedBalancer) GetVMHealth(clientID string) VMStatus {
	return VMStatusHealthy
}

// RoundRobinBalancer implements simple round-robin distribution
type RoundRobinBalancer struct {
	mu    sync.Mutex
	index int
}

func NewRoundRobinBalancer() *RoundRobinBalancer {
	return &RoundRobinBalancer{}
}

func (rb *RoundRobinBalancer) SelectVM(collection *CollectionInfo, availableVMs []*VMInstance) (*VMInstance, error) {
	if len(availableVMs) == 0 {
		return nil, fmt.Errorf("no available VMs")
	}

	rb.mu.Lock()
	defer rb.mu.Unlock()

	selected := availableVMs[rb.index%len(availableVMs)]
	rb.index++

	log.Printf("🎯 ROUND-ROBIN SELECTION: %s selected for %s.%s",
		selected.ClientID, collection.Database, collection.Collection)

	return selected, nil
}

func (rb *RoundRobinBalancer) UpdateVMMetrics(clientID string, metrics *VMLoadMetrics) {
	// Round-robin doesn't need metrics
}

func (rb *RoundRobinBalancer) GetVMHealth(clientID string) VMStatus {
	return VMStatusHealthy
}

// GeographicBalancer selects VMs based on geographic preferences
type GeographicBalancer struct {
	mu                sync.RWMutex
	regionPreferences map[string]string // Database -> Preferred Region
	vmRegions         map[string]string // VM ID -> Region
}

func NewGeographicBalancer() *GeographicBalancer {
	return &GeographicBalancer{
		regionPreferences: make(map[string]string),
		vmRegions:         make(map[string]string),
	}
}

func (gb *GeographicBalancer) SetRegionPreference(database, region string) {
	gb.mu.Lock()
	defer gb.mu.Unlock()
	gb.regionPreferences[database] = region
}

func (gb *GeographicBalancer) RegisterVMRegion(clientID, region string) {
	gb.mu.Lock()
	defer gb.mu.Unlock()
	gb.vmRegions[clientID] = region
}

func (gb *GeographicBalancer) SelectVM(collection *CollectionInfo, availableVMs []*VMInstance) (*VMInstance, error) {
	if len(availableVMs) == 0 {
		return nil, fmt.Errorf("no available VMs")
	}

	gb.mu.RLock()
	preferredRegion, hasPreference := gb.regionPreferences[collection.Database]
	gb.mu.RUnlock()

	if !hasPreference {
		// No preference, use first available
		return availableVMs[0], nil
	}

	// Find VMs in preferred region
	for _, vm := range availableVMs {
		gb.mu.RLock()
		vmRegion := gb.vmRegions[vm.ClientID]
		gb.mu.RUnlock()

		if vmRegion == preferredRegion {
			log.Printf("🎯 GEOGRAPHIC SELECTION: %s selected for %s.%s (region: %s)",
				vm.ClientID, collection.Database, collection.Collection, preferredRegion)
			return vm, nil
		}
	}

	// Fallback to first available if no regional match
	log.Printf("⚠️  GEOGRAPHIC FALLBACK: No VM in region %s, using %s for %s.%s",
		preferredRegion, availableVMs[0].ClientID, collection.Database, collection.Collection)
	return availableVMs[0], nil
}

func (gb *GeographicBalancer) UpdateVMMetrics(clientID string, metrics *VMLoadMetrics) {
	// Geographic balancer doesn't need real-time metrics
}

func (gb *GeographicBalancer) GetVMHealth(clientID string) VMStatus {
	return VMStatusHealthy
}
