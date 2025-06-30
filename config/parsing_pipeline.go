package config

import (
	"context"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/gruntwork-io/terragrunt/options"
	"github.com/gruntwork-io/terragrunt/pkg/log"
)

// ConfigParsingPipeline implements a multi-stage pipeline for config parsing
type ConfigParsingPipeline struct {
	discoveryWorkers int
	parseWorkers     int
	resolveWorkers   int
	bufferSize       int
	stats            PipelineStats
}

// PipelineStats tracks pipeline performance metrics
type PipelineStats struct {
	TotalConfigs     int64
	DiscoveryTime    time.Duration
	ParsingTime      time.Duration
	ResolutionTime   time.Duration
	TotalTime        time.Duration
	Errors           int64
	mu               sync.RWMutex
}

// ConfigDiscoveryResult represents a discovered config file
type ConfigDiscoveryResult struct {
	Path  string
	Error error
}

// ConfigParseResult represents a parsed config
type ConfigParseResult struct {
	Path   string
	Config *TerragruntConfig
	Error  error
}

// ConfigResolutionResult represents a fully resolved config
type ConfigResolutionResult struct {
	Path   string
	Config *TerragruntConfig
	Error  error
}

// NewConfigParsingPipeline creates a new config parsing pipeline
func NewConfigParsingPipeline() *ConfigParsingPipeline {
	cpuCount := runtime.NumCPU()
	return &ConfigParsingPipeline{
		discoveryWorkers: cpuCount,
		parseWorkers:     cpuCount * 2,
		resolveWorkers:   cpuCount,
		bufferSize:       100,
	}
}

// SetWorkerCounts allows customizing the number of workers for each stage
func (cpp *ConfigParsingPipeline) SetWorkerCounts(discovery, parse, resolve int) *ConfigParsingPipeline {
	cpp.discoveryWorkers = discovery
	cpp.parseWorkers = parse
	cpp.resolveWorkers = resolve
	return cpp
}

// SetBufferSize sets the buffer size for pipeline channels
func (cpp *ConfigParsingPipeline) SetBufferSize(size int) *ConfigParsingPipeline {
	cpp.bufferSize = size
	return cpp
}

// ProcessConfigsPipeline processes configs through a multi-stage pipeline
func (cpp *ConfigParsingPipeline) ProcessConfigsPipeline(ctx context.Context, l log.Logger, rootPath string, opts *options.TerragruntOptions) ([]*TerragruntConfig, error) {
	startTime := time.Now()
	defer func() {
		cpp.updateTotalTime(time.Since(startTime))
	}()

	// Stage 1: Discovery Pipeline
	discoveryStart := time.Now()
	configPaths := make(chan ConfigDiscoveryResult, cpp.bufferSize)
	go cpp.runDiscoveryStage(ctx, l, rootPath, opts, configPaths)
	cpp.updateDiscoveryTime(time.Since(discoveryStart))

	// Stage 2: Parsing Pipeline
	parseStart := time.Now()
	parsedConfigs := make(chan ConfigParseResult, cpp.bufferSize)
	go cpp.runParsingStage(ctx, l, opts, configPaths, parsedConfigs)
	cpp.updateParsingTime(time.Since(parseStart))

	// Stage 3: Resolution Pipeline
	resolutionStart := time.Now()
	resolvedConfigs := make(chan ConfigResolutionResult, cpp.bufferSize)
	go cpp.runResolutionStage(ctx, l, parsedConfigs, resolvedConfigs)
	cpp.updateResolutionTime(time.Since(resolutionStart))

	// Collect results
	var allConfigs []*TerragruntConfig
	var errors []error

	for result := range resolvedConfigs {
		if result.Error != nil {
			errors = append(errors, result.Error)
			cpp.recordError()
		} else {
			allConfigs = append(allConfigs, result.Config)
			cpp.recordProcessedConfig()
		}
	}

	if len(errors) > 0 {
		// Return first error for now, could be enhanced to return all errors
		return allConfigs, errors[0]
	}

	return allConfigs, nil
}

// runDiscoveryStage runs the config discovery stage
func (cpp *ConfigParsingPipeline) runDiscoveryStage(ctx context.Context, l log.Logger, rootPath string, opts *options.TerragruntOptions, output chan<- ConfigDiscoveryResult) {
	defer close(output)

	// Use concurrent discovery
	discovery := NewConcurrentFileDiscovery(opts).SetMaxWorkers(cpp.discoveryWorkers)
	configFiles, err := discovery.FindConfigFilesInPathConcurrent(rootPath)

	if err != nil {
		output <- ConfigDiscoveryResult{Error: err}
		return
	}

	// Send discovered config paths to parsing stage
	for _, configPath := range configFiles {
		select {
		case output <- ConfigDiscoveryResult{Path: configPath}:
		case <-ctx.Done():
			return
		}
	}
}

// runParsingStage runs the config parsing stage
func (cpp *ConfigParsingPipeline) runParsingStage(ctx context.Context, l log.Logger, opts *options.TerragruntOptions, input <-chan ConfigDiscoveryResult, output chan<- ConfigParseResult) {
	defer close(output)

	var wg sync.WaitGroup
	
	// Start parsing workers
	for i := 0; i < cpp.parseWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cpp.parseWorker(ctx, l, opts, input, output)
		}()
	}

	wg.Wait()
}

// parseWorker is a worker that parses individual configs
func (cpp *ConfigParsingPipeline) parseWorker(ctx context.Context, l log.Logger, opts *options.TerragruntOptions, input <-chan ConfigDiscoveryResult, output chan<- ConfigParseResult) {
	for {
		select {
		case discovery, ok := <-input:
			if !ok {
				return
			}
			
			if discovery.Error != nil {
				output <- ConfigParseResult{Error: discovery.Error}
				continue
			}

			// Create parsing context from the context.Context
			parsingCtx := &ParsingContext{
				Context:           ctx,
				TerragruntOptions: opts,
			}
			
			// Try to use persistent cache first
			if persistentCache, err := GetGlobalPersistentCache(opts); err == nil {
				if stat, err := os.Stat(discovery.Path); err == nil {
					if cached, found := persistentCache.Get(discovery.Path, stat.ModTime()); found {
						output <- ConfigParseResult{
							Path:   discovery.Path,
							Config: cached,
						}
						continue
					}
				}
			}

			// Parse the config
			config, err := ParseConfigFile(parsingCtx, l, discovery.Path, nil)
			
			// Cache the result if successful
			if err == nil && config != nil {
				if persistentCache, cacheErr := GetGlobalPersistentCache(opts); cacheErr == nil {
					if stat, statErr := os.Stat(discovery.Path); statErr == nil {
						persistentCache.Set(discovery.Path, config, stat.ModTime())
					}
				}
			}

			output <- ConfigParseResult{
				Path:   discovery.Path,
				Config: config,
				Error:  err,
			}

		case <-ctx.Done():
			return
		}
	}
}

// runResolutionStage runs the dependency resolution stage
func (cpp *ConfigParsingPipeline) runResolutionStage(ctx context.Context, l log.Logger, input <-chan ConfigParseResult, output chan<- ConfigResolutionResult) {
	defer close(output)

	var wg sync.WaitGroup
	
	// Start resolution workers
	for i := 0; i < cpp.resolveWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cpp.resolutionWorker(ctx, l, input, output)
		}()
	}

	wg.Wait()
}

// resolutionWorker resolves dependencies for parsed configs
func (cpp *ConfigParsingPipeline) resolutionWorker(ctx context.Context, l log.Logger, input <-chan ConfigParseResult, output chan<- ConfigResolutionResult) {
	for {
		select {
		case parseResult, ok := <-input:
			if !ok {
				return
			}
			
			if parseResult.Error != nil {
				output <- ConfigResolutionResult{
					Path:  parseResult.Path,
					Error: parseResult.Error,
				}
				continue
			}

			// For now, just pass through the parsed config
			// In a full implementation, this stage would resolve dependencies
			// between configs and populate any missing dependency outputs
			output <- ConfigResolutionResult{
				Path:   parseResult.Path,
				Config: parseResult.Config,
			}

		case <-ctx.Done():
			return
		}
	}
}

// updateDiscoveryTime updates discovery timing stats
func (cpp *ConfigParsingPipeline) updateDiscoveryTime(duration time.Duration) {
	cpp.stats.mu.Lock()
	cpp.stats.DiscoveryTime += duration
	cpp.stats.mu.Unlock()
}

// updateParsingTime updates parsing timing stats
func (cpp *ConfigParsingPipeline) updateParsingTime(duration time.Duration) {
	cpp.stats.mu.Lock()
	cpp.stats.ParsingTime += duration
	cpp.stats.mu.Unlock()
}

// updateResolutionTime updates resolution timing stats
func (cpp *ConfigParsingPipeline) updateResolutionTime(duration time.Duration) {
	cpp.stats.mu.Lock()
	cpp.stats.ResolutionTime += duration
	cpp.stats.mu.Unlock()
}

// updateTotalTime updates total timing stats
func (cpp *ConfigParsingPipeline) updateTotalTime(duration time.Duration) {
	cpp.stats.mu.Lock()
	cpp.stats.TotalTime = duration
	cpp.stats.mu.Unlock()
}

// recordProcessedConfig increments the processed config counter
func (cpp *ConfigParsingPipeline) recordProcessedConfig() {
	cpp.stats.mu.Lock()
	cpp.stats.TotalConfigs++
	cpp.stats.mu.Unlock()
}

// recordError increments the error counter
func (cpp *ConfigParsingPipeline) recordError() {
	cpp.stats.mu.Lock()
	cpp.stats.Errors++
	cpp.stats.mu.Unlock()
}

// GetStats returns current pipeline statistics
func (cpp *ConfigParsingPipeline) GetStats() PipelineStats {
	cpp.stats.mu.RLock()
	defer cpp.stats.mu.RUnlock()
	return cpp.stats
}

// ClearStats resets all statistics
func (cpp *ConfigParsingPipeline) ClearStats() {
	cpp.stats.mu.Lock()
	cpp.stats = PipelineStats{}
	cpp.stats.mu.Unlock()
}

// Global pipeline instance
var globalPipeline *ConfigParsingPipeline
var pipelineInitOnce sync.Once

// GetGlobalConfigPipeline returns the global config parsing pipeline
func GetGlobalConfigPipeline() *ConfigParsingPipeline {
	pipelineInitOnce.Do(func() {
		globalPipeline = NewConfigParsingPipeline()
	})
	return globalPipeline
}