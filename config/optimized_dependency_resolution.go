package config

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/gruntwork-io/terragrunt/options"
	"github.com/gruntwork-io/terragrunt/pkg/log"
	"github.com/hashicorp/go-multierror"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/gocty"
	"golang.org/x/sync/errgroup"
)

// DependencyBatch represents a group of dependencies that can be resolved together
type DependencyBatch struct {
	Target       string
	Dependencies []*Dependency
}

// OptimizedDependencyResolver provides enhanced dependency resolution with batching and caching
type OptimizedDependencyResolver struct {
	maxConcurrency int
	batchTimeout   time.Duration
	stats          DependencyResolverStats
	mu             sync.RWMutex
}

// DependencyResolverStats tracks resolver performance metrics
type DependencyResolverStats struct {
	TotalDependencies int64
	BatchedTargets    int64
	CacheHits         int64
	CacheMisses       int64
	TotalTime         time.Duration
	AvgTime           time.Duration
	Errors            int64
	mu                sync.RWMutex
}

// NewOptimizedDependencyResolver creates a new optimized dependency resolver
func NewOptimizedDependencyResolver() *OptimizedDependencyResolver {
	return &OptimizedDependencyResolver{
		maxConcurrency: runtime.NumCPU(), // default: runtime.NumCPU()
		batchTimeout:   5 * time.Second,
	}
}

// NewOptimizedDependencyResolverWithOptions creates a new optimized dependency resolver with configurable options
func NewOptimizedDependencyResolverWithOptions(opts *options.TerragruntOptions) *OptimizedDependencyResolver {
	maxConcurrency := opts.MaxDependencyWorkers
	if maxConcurrency == 0 {
		maxConcurrency = runtime.NumCPU() // Auto-detect: CPU cores
	}
	
	return &OptimizedDependencyResolver{
		maxConcurrency: maxConcurrency,
		batchTimeout:   5 * time.Second,
	}
}

// SetMaxConcurrency sets the maximum number of concurrent dependency resolutions
func (odr *OptimizedDependencyResolver) SetMaxConcurrency(max int) *OptimizedDependencyResolver {
	odr.maxConcurrency = max
	return odr
}

// SetBatchTimeout sets the timeout for batching operations
func (odr *OptimizedDependencyResolver) SetBatchTimeout(timeout time.Duration) *OptimizedDependencyResolver {
	odr.batchTimeout = timeout
	return odr
}

// ResolveDependenciesOptimized resolves dependencies with batching and enhanced concurrency
func (odr *OptimizedDependencyResolver) ResolveDependenciesOptimized(ctx *ParsingContext, l log.Logger, dependencyConfigs []Dependency) (*cty.Value, error) {
	startTime := time.Now()
	defer func() {
		odr.updateStats(time.Since(startTime), int64(len(dependencyConfigs)))
	}()

	if len(dependencyConfigs) == 0 {
		return &cty.NilVal, nil
	}

	// Group dependencies by target to batch processing
	batches := odr.groupDependenciesByTarget(dependencyConfigs)
	l.Debugf("Grouped %d dependencies into %d batches", len(dependencyConfigs), len(batches))

	// dependencyMap stores the final results
	dependencyMap := make(map[string]cty.Value)
	var mapMutex sync.Mutex

	// Use errgroup for controlled concurrency
	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(odr.maxConcurrency)

	// Process each batch concurrently
	for target, batch := range batches {
		target, batch := target, batch // Capture for goroutine
		g.Go(func() error {
			// Convert context back to ParsingContext for processBatch
			parsingCtx := ctx // ctx is already *ParsingContext
			return odr.processBatch(parsingCtx, l, target, batch, &dependencyMap, &mapMutex)
		})
	}

	// Wait for all batches to complete
	if err := g.Wait(); err != nil {
		odr.recordError()
		return nil, err
	}

	// Convert the final map to cty.Value
	convertedOutput, err := gocty.ToCtyValue(dependencyMap, generateTypeFromValuesMap(dependencyMap))
	if err != nil {
		return nil, fmt.Errorf("failed to convert dependency map to cty.Value: %w", err)
	}

	return &convertedOutput, nil
}

// groupDependenciesByTarget groups dependencies that target the same config
func (odr *OptimizedDependencyResolver) groupDependenciesByTarget(dependencies []Dependency) map[string][]*Dependency {
	batches := make(map[string][]*Dependency)

	for i := range dependencies {
		dep := &dependencies[i]
		target := dep.ConfigPath.AsString()
		batches[target] = append(batches[target], dep)
	}

	return batches
}

// processBatch processes a batch of dependencies that share the same target
func (odr *OptimizedDependencyResolver) processBatch(ctx *ParsingContext, l log.Logger, target string, dependencies []*Dependency, resultMap *map[string]cty.Value, mutex *sync.Mutex) error {
	// Get outputs once for the target
	var sharedOutputs *cty.Value
	var outputError error

	// Find a dependency that needs outputs to determine how to fetch them
	var sampleDep *Dependency
	for _, dep := range dependencies {
		if dep.shouldGetOutputs(ctx) {
			sampleDep = dep
			break
		}
	}

	if sampleDep != nil {
		// Use the existing output resolution logic but only once per target
		sharedOutputs, outputError = odr.resolveTargetOutputs(ctx, l, *sampleDep)
		if outputError != nil {
			// Handle the error according to the original logic
			if shouldUseMockOutputs(ctx, *sampleDep, outputError) {
				sharedOutputs = sampleDep.MockOutputs
				outputError = nil
			}
		}
	}

	// Apply the resolved outputs to all dependencies in the batch
	var batchErrors *multierror.Error
	for _, dep := range dependencies {
		if err := odr.processSingleDependency(ctx, l, dep, sharedOutputs, resultMap, mutex); err != nil {
			batchErrors = multierror.Append(batchErrors, fmt.Errorf("failed to process dependency %s: %w", dep.Name, err))
		}
	}

	return batchErrors.ErrorOrNil()
}

// resolveTargetOutputs resolves outputs for a specific target
func (odr *OptimizedDependencyResolver) resolveTargetOutputs(ctx *ParsingContext, l log.Logger, dep Dependency) (*cty.Value, error) {
	// This delegates to the existing getTerragruntOutput logic
	outputVal, isEmpty, err := getTerragruntOutput(ctx, l, dep)
	if err != nil {
		return nil, err
	}

	if isEmpty && dep.MockOutputs != nil {
		return dep.MockOutputs, nil
	}

	return outputVal, nil
}

// processSingleDependency processes a single dependency within a batch
func (odr *OptimizedDependencyResolver) processSingleDependency(ctx *ParsingContext, l log.Logger, dep *Dependency, sharedOutputs *cty.Value, resultMap *map[string]cty.Value, mutex *sync.Mutex) error {
	// Set the rendered outputs
	if sharedOutputs != nil {
		dep.RenderedOutputs = sharedOutputs
	}

	// Build the dependency encoding map
	dependencyEncodingMap := make(map[string]cty.Value)

	if dep.RenderedOutputs != nil {
		dependencyEncodingMap["outputs"] = *dep.RenderedOutputs
	}

	if dep.Inputs != nil {
		dependencyEncodingMap["inputs"] = *dep.Inputs
	}

	// Convert to cty.Value
	dependencyEncodingMapEncoded, err := gocty.ToCtyValue(dependencyEncodingMap, generateTypeFromValuesMap(dependencyEncodingMap))
	if err != nil {
		return fmt.Errorf("failed to encode dependency %s: %w", dep.Name, err)
	}

	// Store in result map (thread-safe)
	mutex.Lock()
	(*resultMap)[dep.Name] = dependencyEncodingMapEncoded
	mutex.Unlock()

	return nil
}

// shouldUseMockOutputs determines if mock outputs should be used based on error
func shouldUseMockOutputs(ctx *ParsingContext, dep Dependency, err error) bool {
	// This implements the logic from the original getTerragruntOutputIfAppliedElseConfiguredDefault
	return dep.MockOutputs != nil && !dep.shouldGetOutputs(ctx)
}

// updateStats updates performance statistics
func (odr *OptimizedDependencyResolver) updateStats(duration time.Duration, depCount int64) {
	odr.stats.mu.Lock()
	defer odr.stats.mu.Unlock()

	odr.stats.TotalDependencies += depCount
	odr.stats.TotalTime += duration

	total := odr.stats.TotalDependencies
	if total > 0 {
		odr.stats.AvgTime = odr.stats.TotalTime / time.Duration(total)
	}
}

// recordError increments the error counter
func (odr *OptimizedDependencyResolver) recordError() {
	odr.stats.mu.Lock()
	odr.stats.Errors++
	odr.stats.mu.Unlock()
}

// GetStats returns current resolver statistics
func (odr *OptimizedDependencyResolver) GetStats() DependencyResolverStats {
	odr.stats.mu.RLock()
	defer odr.stats.mu.RUnlock()
	return odr.stats
}

// ClearStats resets all statistics
func (odr *OptimizedDependencyResolver) ClearStats() {
	odr.stats.mu.Lock()
	odr.stats = DependencyResolverStats{}
	odr.stats.mu.Unlock()
}

// Global optimized dependency resolver instance
var globalDependencyResolver *OptimizedDependencyResolver
var resolverInitOnce sync.Once

// GetGlobalDependencyResolver returns the global dependency resolver instance
func GetGlobalDependencyResolver() *OptimizedDependencyResolver {
	resolverInitOnce.Do(func() {
		globalDependencyResolver = NewOptimizedDependencyResolver()
	})
	return globalDependencyResolver
}

// OptimizedDependencyBlocksToCtyValue is an enhanced version of dependencyBlocksToCtyValue
func OptimizedDependencyBlocksToCtyValue(ctx *ParsingContext, l log.Logger, dependencyConfigs []Dependency) (*cty.Value, error) {
	// Create a resolver with configurable options from the parsing context
	resolver := NewOptimizedDependencyResolverWithOptions(ctx.TerragruntOptions)
	return resolver.ResolveDependenciesOptimized(ctx, l, dependencyConfigs)
}
