package adaptive

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"go-data-sync-http/pkg/models"
)

// ResourceThresholds defines thresholds for resource usage
type ResourceThresholds struct {
	CPUHigh     float64 // CPU usage above this triggers throttling
	CPULow      float64 // CPU usage below this allows scaling up
	MemoryHigh  float64 // Memory usage above this triggers throttling
	MemoryLow   float64 // Memory usage below this allows scaling up
	LatencyHigh float64 // Latency above this triggers throttling (ms)
	LatencyLow  float64 // Latency below this allows scaling up (ms)
	QueueHigh   int     // Queue depth above this triggers throttling
	QueueLow    int     // Queue depth below this allows scaling up
}

// DefaultThresholds returns sensible default thresholds
func DefaultThresholds() ResourceThresholds {
	return ResourceThresholds{
		CPUHigh:     75.0,  // 75% CPU (more conservative)
		CPULow:      25.0,  // 25% CPU (lower threshold for scale up)
		MemoryHigh:  80.0,  // 80% Memory (more conservative)
		MemoryLow:   30.0,  // 30% Memory (lower threshold for scale up)
		LatencyHigh: 500.0, // 500ms (more aggressive latency detection)
		LatencyLow:  50.0,  // 50ms (better responsiveness target)
		QueueHigh:   500,   // 500 items (detect backlog sooner)
		QueueLow:    50,    // 50 items (allow scale up with smaller queues)
	}
}

// AdaptiveController manages dynamic parallelism and batch size adjustments
type AdaptiveController struct {
	thresholds       ResourceThresholds
	currentConfig    *models.AdaptiveConfig
	vmTelemetry      *models.TelemetryData
	cloudTelemetry   *models.TelemetryData
	mutex            sync.RWMutex
	lastAdjustment   time.Time
	adjustmentDelay  time.Duration
	learningHistory  []ConfigAdjustment
	maxHistory       int
	minFetchParallel int
	maxFetchParallel int
	minPushParallel  int
	maxPushParallel  int
	minBatchSize     int
	maxBatchSize     int
	learningEngine   *AdaptiveLearningEngine
}

// ConfigAdjustment represents a historical configuration change
type ConfigAdjustment struct {
	Timestamp      time.Time
	OldConfig      models.AdaptiveConfig
	NewConfig      models.AdaptiveConfig
	VMTelemetry    models.TelemetryData
	CloudTelemetry models.TelemetryData
	Reason         string
	Effectiveness  float64 // Measured after adjustment (0-1, higher is better)
}

// NewAdaptiveController creates a new adaptive controller
func NewAdaptiveController() *AdaptiveController {
	return &AdaptiveController{
		thresholds: DefaultThresholds(),
		currentConfig: &models.AdaptiveConfig{
			FetchParallelism: 4,   // Start with moderate parallelism
			PushParallelism:  2,   // Conservative push parallelism
			BatchSize:        100, // Moderate batch size
			BackPressure:     false,
			ThrottleDelay:    0,
			MaxQueueSize:     1000,
			AdaptedAt:        time.Now(),
			Reason:           "initial_configuration",
		},
		adjustmentDelay:  time.Second * 30, // Wait 30s between adjustments
		learningHistory:  make([]ConfigAdjustment, 0),
		maxHistory:       50, // Keep last 50 adjustments
		minFetchParallel: 1,
		maxFetchParallel: 16,
		minPushParallel:  1,
		maxPushParallel:  8,
		minBatchSize:     10,
		maxBatchSize:     1000,
		learningEngine:   NewAdaptiveLearningEngine(),
	}
}

// UpdateVMTelemetry updates VM Sync telemetry data
func (ac *AdaptiveController) UpdateVMTelemetry(telemetry *models.TelemetryData) {
	ac.mutex.Lock()
	defer ac.mutex.Unlock()
	ac.vmTelemetry = telemetry
}

// UpdateCloudTelemetry updates Cloud Sync telemetry data
func (ac *AdaptiveController) UpdateCloudTelemetry(telemetry *models.TelemetryData) {
	ac.mutex.Lock()
	defer ac.mutex.Unlock()
	ac.cloudTelemetry = telemetry
}

// GetCurrentConfig returns the current adaptive configuration
func (ac *AdaptiveController) GetCurrentConfig() *models.AdaptiveConfig {
	ac.mutex.RLock()
	defer ac.mutex.RUnlock()

	// Return a copy to prevent external modification
	config := *ac.currentConfig
	return &config
}

// ShouldAdjust determines if configuration should be adjusted
func (ac *AdaptiveController) ShouldAdjust() bool {
	ac.mutex.RLock()
	defer ac.mutex.RUnlock()

	// Don't adjust too frequently
	if time.Since(ac.lastAdjustment) < ac.adjustmentDelay {
		return false
	}

	// Need both VM and Cloud telemetry
	if ac.vmTelemetry == nil || ac.cloudTelemetry == nil {
		return false
	}

	return true
}

// AnalyzeAndAdjust analyzes current telemetry and adjusts configuration if needed
func (ac *AdaptiveController) AnalyzeAndAdjust(ctx context.Context) (*models.AdaptiveConfig, bool) {
	if !ac.ShouldAdjust() {
		return ac.GetCurrentConfig(), false
	}

	ac.mutex.Lock()
	defer ac.mutex.Unlock()

	// Store old config for learning
	oldConfig := *ac.currentConfig

	// Check for environment changes
	if ac.vmTelemetry != nil && ac.learningEngine != nil {
		if changed, reason, err := ac.learningEngine.DetectEnvironmentChange(ctx, ac.vmTelemetry); err == nil && changed {
			log.Printf("Environment change detected: %s", reason)
		}
	}

	// Try to get ML prediction first
	var newConfig *models.AdaptiveConfig
	var mlConfidence float64
	var usedML bool

	if ac.vmTelemetry != nil && ac.learningEngine != nil {
		if mlConfig, confidence, err := ac.learningEngine.PredictOptimalConfiguration(ctx, ac.vmTelemetry); err == nil && confidence > 0.6 {
			newConfig = mlConfig
			mlConfidence = confidence
			usedML = true
			log.Printf("Using ML prediction with confidence %.2f", confidence)
		}
	}

	// Fallback to traditional calculation if ML prediction is not confident enough
	if newConfig == nil {
		newConfig = ac.calculateOptimalConfig()
		usedML = false
	}

	// Apply learning from history (traditional approach)
	reasons := []string{}
	if !usedML && ac.vmTelemetry != nil {
		ac.applyLearning(newConfig, &reasons)
	}

	// Check if configuration actually changed
	if ac.configsEqual(&oldConfig, newConfig) {
		return &oldConfig, false
	}

	// Update current config
	newConfig.AdaptedAt = time.Now()
	if usedML {
		newConfig.Reason = fmt.Sprintf("ml_prediction_%.2f", mlConfidence)
	} else {
		newConfig.Reason = "adaptive_adjustment"
		if len(reasons) > 0 {
			newConfig.Reason += ": " + reasons[0]
		}
	}

	// Record the adjustment
	adjustment := ConfigAdjustment{
		Timestamp:     time.Now(),
		OldConfig:     oldConfig,
		NewConfig:     *newConfig,
		Reason:        newConfig.Reason,
		Effectiveness: 0.5, // Default, will be updated
	}

	if ac.vmTelemetry != nil {
		adjustment.VMTelemetry = *ac.vmTelemetry
	}
	if ac.cloudTelemetry != nil {
		adjustment.CloudTelemetry = *ac.cloudTelemetry
	}

	// Add to history
	ac.addToHistory(adjustment)

	// Update current configuration
	ac.currentConfig = newConfig
	ac.lastAdjustment = time.Now()

	log.Printf("Adaptive configuration changed: %s", newConfig.Reason)
	log.Printf("New config: Fetch=%d, Push=%d, Batch=%d, BackPressure=%v",
		newConfig.FetchParallelism, newConfig.PushParallelism,
		newConfig.BatchSize, newConfig.BackPressure)

	return newConfig, true
}

// calculateOptimalConfig determines the optimal configuration based on current telemetry
func (ac *AdaptiveController) calculateOptimalConfig() *models.AdaptiveConfig {
	config := *ac.currentConfig
	config.AdaptedAt = time.Now()
	reasons := []string{}

	// Log current telemetry for diagnostics
	log.Printf("🔍 ADAPTIVE ANALYSIS: Current telemetry - VM: CPU=%.1f%%, Mem=%.1f%%, Latency=%.1fms",
		ac.getVMCPU(), ac.getVMMemory(), ac.getVMLatency())
	log.Printf("🔍 ADAPTIVE ANALYSIS: Current config - Fetch=%d, Push=%d, Batch=%d",
		config.FetchParallelism, config.PushParallelism, config.BatchSize)

	// Analyze VM Sync resource usage
	vmOverloaded := ac.isVMOverloaded()
	cloudOverloaded := ac.isCloudOverloaded()

	log.Printf("🔍 ADAPTIVE ANALYSIS: VM overloaded=%v, Cloud overloaded=%v", vmOverloaded, cloudOverloaded)

	// Apply back-pressure if VM is overloaded
	if vmOverloaded {
		config.BackPressure = true
		config.ThrottleDelay = ac.calculateThrottleDelay()
		config.MaxQueueSize = int(float64(config.MaxQueueSize) * 0.7) // Reduce queue size

		// Reduce parallelism to ease VM load
		config.FetchParallelism = ac.clamp(config.FetchParallelism-1, ac.minFetchParallel, ac.maxFetchParallel)
		config.PushParallelism = ac.clamp(config.PushParallelism-1, ac.minPushParallel, ac.maxPushParallel)
		config.BatchSize = ac.clamp(int(float64(config.BatchSize)*0.8), ac.minBatchSize, ac.maxBatchSize)

		reasons = append(reasons, "vm_overloaded")
		log.Printf("⚠️  ADAPTIVE ACTION: Reducing load due to VM overload")
	} else if cloudOverloaded {
		// Cloud is overloaded, reduce its workload
		config.FetchParallelism = ac.clamp(config.FetchParallelism-1, ac.minFetchParallel, ac.maxFetchParallel)
		config.BatchSize = ac.clamp(int(float64(config.BatchSize)*0.9), ac.minBatchSize, ac.maxBatchSize)

		reasons = append(reasons, "cloud_overloaded")
		log.Printf("⚠️  ADAPTIVE ACTION: Reducing load due to Cloud overload")
	} else {
		// Both systems are healthy, can scale up if beneficial
		config.BackPressure = false
		config.ThrottleDelay = 0

		// ENHANCED: More intelligent scaling decisions
		canScale := ac.canScaleUp()
		shouldOptimize := ac.shouldOptimizeForThroughput()

		// Check if we can increase parallelism
		if canScale {
			oldFetch := config.FetchParallelism
			oldPush := config.PushParallelism
			oldBatch := config.BatchSize

			// Smart scaling: increase gradually based on system capacity
			scaleFactor := ac.calculateScaleFactor()

			config.FetchParallelism = ac.clamp(config.FetchParallelism+1, ac.minFetchParallel, ac.maxFetchParallel)
			config.PushParallelism = ac.clamp(config.PushParallelism+1, ac.minPushParallel, ac.maxPushParallel)
			config.BatchSize = ac.clamp(int(float64(config.BatchSize)*scaleFactor), ac.minBatchSize, ac.maxBatchSize)

			reasons = append(reasons, "scaling_up")
			log.Printf("📈 ADAPTIVE ACTION: Scaling up - Fetch %d→%d, Push %d→%d, Batch %d→%d (scale factor: %.2f)",
				oldFetch, config.FetchParallelism, oldPush, config.PushParallelism, oldBatch, config.BatchSize, scaleFactor)
		} else if shouldOptimize {
			// System is stable, apply minor optimizations
			oldBatch := config.BatchSize
			// Small batch size adjustment for throughput optimization
			config.BatchSize = ac.clamp(int(float64(config.BatchSize)*1.05), ac.minBatchSize, ac.maxBatchSize)

			reasons = append(reasons, "throughput_optimization")
			log.Printf("⚡ ADAPTIVE ACTION: Throughput optimization - Batch %d→%d", oldBatch, config.BatchSize)
		} else {
			log.Printf("➡️  ADAPTIVE ACTION: System healthy and stable - maintaining current configuration")
			reasons = append(reasons, "stable_optimal")
		}
	}

	// Apply learning from history
	ac.applyLearning(&config, &reasons)

	// Set reason
	if len(reasons) > 0 {
		config.Reason = reasons[0] // Use primary reason
	} else {
		config.Reason = "no_change_needed"
	}

	log.Printf("🎯 ADAPTIVE RESULT: %s - New config: Fetch=%d, Push=%d, Batch=%d",
		config.Reason, config.FetchParallelism, config.PushParallelism, config.BatchSize)

	return &config
}

// isVMOverloaded checks if VM Sync is overloaded
func (ac *AdaptiveController) isVMOverloaded() bool {
	if ac.vmTelemetry == nil {
		return false
	}

	t := ac.vmTelemetry
	return t.CPUUsage > ac.thresholds.CPUHigh ||
		t.MemoryUsage > ac.thresholds.MemoryHigh ||
		t.SyncLatency > ac.thresholds.LatencyHigh ||
		t.QueueDepth > ac.thresholds.QueueHigh
}

// isCloudOverloaded checks if Cloud Sync is overloaded
func (ac *AdaptiveController) isCloudOverloaded() bool {
	if ac.cloudTelemetry == nil {
		return false
	}

	t := ac.cloudTelemetry
	return t.CPUUsage > ac.thresholds.CPUHigh ||
		t.MemoryUsage > ac.thresholds.MemoryHigh ||
		t.SyncLatency > ac.thresholds.LatencyHigh ||
		t.QueueDepth > ac.thresholds.QueueHigh
}

// canScaleUp checks if it's safe to scale up
func (ac *AdaptiveController) canScaleUp() bool {
	if ac.vmTelemetry == nil || ac.cloudTelemetry == nil {
		return false
	}

	// FIXED: More intelligent scale-up logic instead of requiring both systems to be at low thresholds
	// Check if systems are healthy (not overloaded) rather than requiring them to be at low usage
	vmHealthy := ac.vmTelemetry.CPUUsage < ac.thresholds.CPUHigh &&
		ac.vmTelemetry.MemoryUsage < ac.thresholds.MemoryHigh &&
		ac.vmTelemetry.SyncLatency < ac.thresholds.LatencyHigh &&
		ac.vmTelemetry.QueueDepth < ac.thresholds.QueueHigh

	cloudHealthy := ac.cloudTelemetry.CPUUsage < ac.thresholds.CPUHigh &&
		ac.cloudTelemetry.MemoryUsage < ac.thresholds.MemoryHigh &&
		ac.cloudTelemetry.SyncLatency < ac.thresholds.LatencyHigh &&
		ac.cloudTelemetry.QueueDepth < ac.thresholds.QueueHigh

	// Additional intelligent conditions for scaling up
	canScaleVM := vmHealthy && ac.vmTelemetry.CPUUsage < (ac.thresholds.CPUHigh*0.8) // 80% of high threshold
	canScaleCloud := cloudHealthy && ac.cloudTelemetry.CPUUsage < (ac.thresholds.CPUHigh*0.8)

	// ENHANCED: Scale up if either system has capacity AND neither is overloaded
	// This prevents the progressive degradation we observed
	hasCapacity := canScaleVM || canScaleCloud
	bothHealthy := vmHealthy && cloudHealthy

	// Additional check: Don't scale up too aggressively if we just scaled down
	timeSinceLastAdjustment := time.Since(ac.lastAdjustment)
	cooldownPeriod := time.Second * 45 // 45 second cooldown
	recentAdjustment := timeSinceLastAdjustment < cooldownPeriod

	canScale := hasCapacity && bothHealthy && !recentAdjustment

	log.Printf("🔍 SCALE-UP ANALYSIS: VM_CPU=%.1f%%(<%%.1f), Cloud_CPU=%.1f%%(<%%.1f), CanScaleVM=%v, CanScaleCloud=%v, BothHealthy=%v, RecentAdj=%v → CanScale=%v",
		ac.vmTelemetry.CPUUsage, ac.thresholds.CPUHigh*0.8,
		ac.cloudTelemetry.CPUUsage, ac.thresholds.CPUHigh*0.8,
		canScaleVM, canScaleCloud, bothHealthy, recentAdjustment, canScale)

	return canScale
}

// calculateThrottleDelay calculates appropriate throttle delay based on overload severity
func (ac *AdaptiveController) calculateThrottleDelay() int {
	if ac.vmTelemetry == nil {
		return 100 // Default 100ms
	}

	// Calculate overload factor (0-1)
	cpuOverload := math.Max(0, (ac.vmTelemetry.CPUUsage-ac.thresholds.CPUHigh)/(100-ac.thresholds.CPUHigh))
	memOverload := math.Max(0, (ac.vmTelemetry.MemoryUsage-ac.thresholds.MemoryHigh)/(100-ac.thresholds.MemoryHigh))
	latencyOverload := math.Max(0, (ac.vmTelemetry.SyncLatency-ac.thresholds.LatencyHigh)/ac.thresholds.LatencyHigh)

	maxOverload := math.Max(cpuOverload, math.Max(memOverload, latencyOverload))

	// Scale delay from 50ms to 2000ms based on overload severity
	delay := 50 + int(maxOverload*1950)
	return delay
}

// applyLearning applies insights from historical adjustments
func (ac *AdaptiveController) applyLearning(config *models.AdaptiveConfig, reasons *[]string) {
	// Find similar historical situations and their effectiveness
	if len(ac.learningHistory) < 5 {
		return // Need more history to learn
	}

	// Safety check: ensure we have valid telemetry data for comparison
	if ac.vmTelemetry == nil {
		return // Cannot compare without current telemetry
	}

	// Look for patterns in recent history
	historyLength := len(ac.learningHistory)
	startIndex := historyLength - 10
	if startIndex < 0 {
		startIndex = 0
	}
	recentHistory := ac.learningHistory[startIndex:]
	for _, adjustment := range recentHistory {
		if adjustment.Effectiveness > 0.8 { // High effectiveness
			// If similar conditions, bias toward that configuration
			if ac.situationsSimilar(&adjustment.VMTelemetry, ac.vmTelemetry) {
				// Apply successful pattern
				config.FetchParallelism = adjustment.NewConfig.FetchParallelism
				config.PushParallelism = adjustment.NewConfig.PushParallelism
				config.BatchSize = adjustment.NewConfig.BatchSize
				*reasons = append(*reasons, "learned_pattern")
				break
			}
		}
	}
}

// situationsSimilar checks if two telemetry situations are similar
func (ac *AdaptiveController) situationsSimilar(t1, t2 *models.TelemetryData) bool {
	// Defensive nil checking
	if t1 == nil || t2 == nil {
		return false
	}

	cpuSimilar := math.Abs(t1.CPUUsage-t2.CPUUsage) < 20
	memSimilar := math.Abs(t1.MemoryUsage-t2.MemoryUsage) < 20
	latencySimilar := math.Abs(t1.SyncLatency-t2.SyncLatency) < 500

	return cpuSimilar && memSimilar && latencySimilar
}

// configsEqual checks if two configurations are equal
func (ac *AdaptiveController) configsEqual(c1, c2 *models.AdaptiveConfig) bool {
	return c1.FetchParallelism == c2.FetchParallelism &&
		c1.PushParallelism == c2.PushParallelism &&
		c1.BatchSize == c2.BatchSize &&
		c1.BackPressure == c2.BackPressure &&
		c1.ThrottleDelay == c2.ThrottleDelay &&
		c1.MaxQueueSize == c2.MaxQueueSize
}

// addToHistory adds a configuration adjustment to the learning history
func (ac *AdaptiveController) addToHistory(adjustment ConfigAdjustment) {
	ac.learningHistory = append(ac.learningHistory, adjustment)

	// Keep only recent history
	if len(ac.learningHistory) > ac.maxHistory {
		ac.learningHistory = ac.learningHistory[1:]
	}
}

// Helper functions for telemetry access with safe defaults
func (ac *AdaptiveController) getVMCPU() float64 {
	if ac.vmTelemetry == nil {
		return 0.0
	}
	return ac.vmTelemetry.CPUUsage
}

func (ac *AdaptiveController) getVMMemory() float64 {
	if ac.vmTelemetry == nil {
		return 0.0
	}
	return ac.vmTelemetry.MemoryUsage
}

func (ac *AdaptiveController) getVMLatency() float64 {
	if ac.vmTelemetry == nil {
		return 0.0
	}
	return ac.vmTelemetry.SyncLatency
}

// clamp ensures a value is within specified bounds
func (ac *AdaptiveController) clamp(value, min, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

// shouldOptimizeForThroughput determines if system should optimize for throughput
func (ac *AdaptiveController) shouldOptimizeForThroughput() bool {
	if ac.vmTelemetry == nil || ac.cloudTelemetry == nil {
		return false
	}

	// Optimize if both systems are stable and not under pressure
	vmStable := ac.vmTelemetry.CPUUsage < (ac.thresholds.CPUHigh*0.6) && // Below 60% of high threshold
		ac.vmTelemetry.MemoryUsage < (ac.thresholds.MemoryHigh*0.7) && // Below 70% of high threshold
		ac.vmTelemetry.SyncLatency < (ac.thresholds.LatencyHigh*0.5) // Below 50% of high threshold

	cloudStable := ac.cloudTelemetry.CPUUsage < (ac.thresholds.CPUHigh*0.6) &&
		ac.cloudTelemetry.MemoryUsage < (ac.thresholds.MemoryHigh*0.7) &&
		ac.cloudTelemetry.SyncLatency < (ac.thresholds.LatencyHigh*0.5)

	// Also check that we haven't made recent adjustments
	timeSinceLastAdjustment := time.Since(ac.lastAdjustment)
	stablePeriod := time.Second * 60 // 1 minute of stability

	return vmStable && cloudStable && timeSinceLastAdjustment > stablePeriod
}

// calculateScaleFactor determines how aggressively to scale based on system conditions
func (ac *AdaptiveController) calculateScaleFactor() float64 {
	if ac.vmTelemetry == nil || ac.cloudTelemetry == nil {
		return 1.1 // Conservative default
	}

	// Calculate available capacity (0-1, where 1 = lots of capacity)
	vmCapacity := 1.0 - (ac.vmTelemetry.CPUUsage / ac.thresholds.CPUHigh)
	cloudCapacity := 1.0 - (ac.cloudTelemetry.CPUUsage / ac.thresholds.CPUHigh)

	// Use the minimum capacity to be conservative
	minCapacity := math.Min(vmCapacity, cloudCapacity)

	// Scale factor between 1.05 (conservative) and 1.25 (aggressive)
	scaleFactor := 1.05 + (minCapacity * 0.20)

	// Clamp the scale factor
	if scaleFactor < 1.05 {
		scaleFactor = 1.05
	}
	if scaleFactor > 1.25 {
		scaleFactor = 1.25
	}

	return scaleFactor
}

// measureEffectiveness calculates the effectiveness of recent configuration changes
func (ac *AdaptiveController) measureEffectiveness() float64 {
	if ac.vmTelemetry == nil || ac.cloudTelemetry == nil {
		return 0.5 // Neutral score if no telemetry
	}

	// Calculate effectiveness based on multiple factors
	effectiveness := 0.0

	// Factor 1: Resource utilization efficiency (30%)
	vmResourceScore := ac.calculateResourceEfficiency(ac.vmTelemetry)
	cloudResourceScore := ac.calculateResourceEfficiency(ac.cloudTelemetry)
	resourceScore := (vmResourceScore + cloudResourceScore) / 2.0
	effectiveness += resourceScore * 0.3

	// Factor 2: Performance score (40%)
	performanceScore := ac.calculatePerformanceScore()
	effectiveness += performanceScore * 0.4

	// Factor 3: Stability score (20%)
	stabilityScore := ac.calculateStabilityScore()
	effectiveness += stabilityScore * 0.2

	// Factor 4: Trend improvement (10%)
	trendScore := ac.calculateTrendScore()
	effectiveness += trendScore * 0.1

	// Clamp between 0 and 1
	if effectiveness < 0 {
		effectiveness = 0
	}
	if effectiveness > 1 {
		effectiveness = 1
	}

	return effectiveness
}

// calculateResourceEfficiency calculates how efficiently resources are being used
func (ac *AdaptiveController) calculateResourceEfficiency(telemetry *models.TelemetryData) float64 {
	if telemetry == nil {
		return 0.5
	}

	// Ideal resource usage is around 50-70% for good efficiency
	cpuEfficiency := ac.calculateUsageEfficiency(telemetry.CPUUsage, 50, 70)
	memEfficiency := ac.calculateUsageEfficiency(telemetry.MemoryUsage, 40, 60)

	// Latency efficiency (lower is better)
	latencyEfficiency := 1.0 - math.Min(1.0, telemetry.SyncLatency/ac.thresholds.LatencyHigh)

	return (cpuEfficiency + memEfficiency + latencyEfficiency) / 3.0
}

// calculateUsageEfficiency calculates efficiency score for a usage metric
func (ac *AdaptiveController) calculateUsageEfficiency(usage, idealMin, idealMax float64) float64 {
	if usage >= idealMin && usage <= idealMax {
		return 1.0 // Perfect efficiency
	}

	if usage < idealMin {
		// Underutilized - score based on how close to ideal
		return 0.5 + (usage/idealMin)*0.5
	}

	// Overutilized - score decreases as usage increases
	overuse := (usage - idealMax) / (100 - idealMax)
	return math.Max(0.1, 1.0-overuse)
}

// calculatePerformanceScore calculates overall system performance score
func (ac *AdaptiveController) calculatePerformanceScore() float64 {
	if ac.vmTelemetry == nil {
		return 0.5
	}

	// Performance factors
	throughputScore := 1.0 // Would be calculated based on actual throughput metrics
	latencyScore := 1.0 - math.Min(1.0, ac.vmTelemetry.SyncLatency/ac.thresholds.LatencyHigh)
	queueScore := 1.0 - math.Min(1.0, float64(ac.vmTelemetry.QueueDepth)/float64(ac.thresholds.QueueHigh))

	return (throughputScore + latencyScore + queueScore) / 3.0
}

// calculateStabilityScore measures system stability
func (ac *AdaptiveController) calculateStabilityScore() float64 {
	// Check how stable the system has been (fewer adjustments = more stable)
	recentAdjustments := 0
	cutoff := time.Now().Add(-5 * time.Minute)

	for _, adj := range ac.learningHistory {
		if adj.Timestamp.After(cutoff) {
			recentAdjustments++
		}
	}

	// Score decreases with more frequent adjustments
	return math.Max(0.1, 1.0-float64(recentAdjustments)/10.0)
}

// calculateTrendScore measures improvement trends
func (ac *AdaptiveController) calculateTrendScore() float64 {
	// Simple trend analysis - compare recent performance
	if len(ac.learningHistory) < 3 {
		return 0.5 // Neutral if not enough data
	}

	// Get average effectiveness of last 3 adjustments
	recentCount := math.Min(3, float64(len(ac.learningHistory)))
	sum := 0.0
	for i := len(ac.learningHistory) - int(recentCount); i < len(ac.learningHistory); i++ {
		sum += ac.learningHistory[i].Effectiveness
	}
	avg := sum / recentCount

	return avg
}

// UpdateEffectiveness updates the effectiveness of a recent adjustment
func (ac *AdaptiveController) UpdateEffectiveness(effectiveness float64) {
	ac.mutex.Lock()
	defer ac.mutex.Unlock()

	if len(ac.learningHistory) > 0 {
		// Update the most recent adjustment's effectiveness
		lastIdx := len(ac.learningHistory) - 1
		ac.learningHistory[lastIdx].Effectiveness = effectiveness
		log.Printf("Updated adjustment effectiveness: %.2f", effectiveness)

		// Learn from this observation
		adjustment := ac.learningHistory[lastIdx]
		outcome := "success"
		if effectiveness < 0.5 {
			outcome = "failure"
		} else if effectiveness < 0.7 {
			outcome = "partial"
		}

		// Use VM telemetry for learning (primary source)
		telemetryData := &adjustment.VMTelemetry
		if telemetryData.NodeID == "" && adjustment.CloudTelemetry.NodeID != "" {
			// Fallback to cloud telemetry if VM telemetry is not available
			telemetryData = &adjustment.CloudTelemetry
		}

		if telemetryData.NodeID != "" && ac.learningEngine != nil {
			ctx := context.Background()
			if err := ac.learningEngine.LearnFromObservation(ctx, telemetryData, &adjustment.NewConfig, effectiveness, outcome); err != nil {
				log.Printf("Failed to learn from observation: %v", err)
			}
		}
	}
}

// GetStats returns controller statistics
func (ac *AdaptiveController) GetStats() map[string]interface{} {
	ac.mutex.RLock()
	defer ac.mutex.RUnlock()

	stats := map[string]interface{}{
		"currentConfig":  ac.currentConfig,
		"lastAdjustment": ac.lastAdjustment,
		"historySize":    len(ac.learningHistory),
		"vmTelemetry":    ac.vmTelemetry,
		"cloudTelemetry": ac.cloudTelemetry,
		"thresholds":     ac.thresholds,
	}

	// Add learning engine statistics
	if ac.learningEngine != nil {
		learningStats := ac.learningEngine.GetLearningStats()
		stats["learning_engine"] = learningStats
	}

	return stats
}
