package config

import (
	"crypto/sha256"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/gruntwork-io/terragrunt/internal/cache"
	"github.com/gruntwork-io/terragrunt/pkg/log"
	"github.com/gruntwork-io/terragrunt/util"
)

// EnhancedIncludeCache provides global caching for include configurations
type EnhancedIncludeCache struct {
	cache    *cache.Cache[*TerragruntConfig]
	mu       sync.RWMutex
	stats    IncludeCacheStats
	enabled  bool
}

// IncludeCacheStats tracks cache performance metrics
type IncludeCacheStats struct {
	Hits        int64
	Misses      int64
	Errors      int64
	TotalTime   time.Duration
	AvgTime     time.Duration
	mu          sync.RWMutex
}

// NewEnhancedIncludeCache creates a new enhanced include cache
func NewEnhancedIncludeCache() *EnhancedIncludeCache {
	return &EnhancedIncludeCache{
		cache:   cache.NewCache[*TerragruntConfig]("enhanced-include-cache"),
		enabled: true,
	}
}

// SetEnabled allows enabling/disabling the cache
func (eic *EnhancedIncludeCache) SetEnabled(enabled bool) *EnhancedIncludeCache {
	eic.enabled = enabled
	return eic
}

// ParseIncludedConfigCached parses an include config with caching
func (eic *EnhancedIncludeCache) ParseIncludedConfigCached(ctx *ParsingContext, l log.Logger, includeConfig *IncludeConfig) (*TerragruntConfig, error) {
	if !eic.enabled {
		return parseIncludedConfig(ctx, l, includeConfig)
	}
	
	startTime := time.Now()
	defer func() {
		eic.updateStats(time.Since(startTime))
	}()
	
	// Generate cache key based on include path and relevant context
	cacheKey := eic.generateIncludeCacheKey(ctx, includeConfig)
	
	// Try cache first
	eic.mu.RLock()
	if cached, found := eic.cache.Get(ctx, cacheKey); found && eic.isValidCacheEntry(ctx, includeConfig, cached) {
		eic.mu.RUnlock()
		eic.recordHit()
		l.Debugf("Include cache HIT for %s", includeConfig.Path)
		return cached, nil
	}
	eic.mu.RUnlock()
	
	// Cache miss - parse the config
	eic.recordMiss()
	l.Debugf("Include cache MISS for %s", includeConfig.Path)
	
	config, err := parseIncludedConfigOriginal(ctx, l, includeConfig)
	if err != nil {
		eic.recordError()
		return nil, err
	}
	
	// Store in cache
	eic.mu.Lock()
	eic.cache.Put(ctx, cacheKey, config)
	eic.mu.Unlock()
	
	return config, nil
}

// generateIncludeCacheKey creates a cache key that includes relevant context
func (eic *EnhancedIncludeCache) generateIncludeCacheKey(ctx *ParsingContext, includeConfig *IncludeConfig) string {
	hasher := sha256.New()
	
	// Include the canonical path
	canonicalPath, _ := util.CanonicalPath(includeConfig.Path, "")
	hasher.Write([]byte(canonicalPath))
	
	// Include relevant context that affects parsing
	hasher.Write([]byte(ctx.TerragruntOptions.WorkingDir))
	hasher.Write([]byte(ctx.TerragruntOptions.TerragruntConfigPath))
	
	// Include partial parse flag as it affects parsing behavior
	if len(ctx.PartialParseDecodeList) > 0 {
		hasher.Write([]byte("partial"))
	}
	
	// Include decode list as it affects what gets parsed
	for _, decode := range ctx.PartialParseDecodeList {
		hasher.Write([]byte(string(decode)))
	}
	
	// Include file modification time for cache invalidation
	if util.FileExists(canonicalPath) {
		if stat, err := os.Stat(canonicalPath); err == nil {
			hasher.Write([]byte(stat.ModTime().String()))
		}
	}
	
	return fmt.Sprintf("include-%x", hasher.Sum(nil))[:32]
}

// isValidCacheEntry validates that a cached entry is still valid
func (eic *EnhancedIncludeCache) isValidCacheEntry(ctx *ParsingContext, includeConfig *IncludeConfig, cached *TerragruntConfig) bool {
	// Basic validation
	if cached == nil {
		return false
	}
	
	// Check if include file still exists and hasn't been modified
	canonicalPath, _ := util.CanonicalPath(includeConfig.Path, "")
	if !util.FileExists(canonicalPath) {
		return false
	}
	
	// Additional validation could be added here based on specific requirements
	return true
}

// recordHit increments the cache hit counter
func (eic *EnhancedIncludeCache) recordHit() {
	eic.stats.mu.Lock()
	eic.stats.Hits++
	eic.stats.mu.Unlock()
}

// recordMiss increments the cache miss counter
func (eic *EnhancedIncludeCache) recordMiss() {
	eic.stats.mu.Lock()
	eic.stats.Misses++
	eic.stats.mu.Unlock()
}

// recordError increments the error counter
func (eic *EnhancedIncludeCache) recordError() {
	eic.stats.mu.Lock()
	eic.stats.Errors++
	eic.stats.mu.Unlock()
}

// updateStats updates timing statistics
func (eic *EnhancedIncludeCache) updateStats(duration time.Duration) {
	eic.stats.mu.Lock()
	eic.stats.TotalTime += duration
	total := eic.stats.Hits + eic.stats.Misses
	if total > 0 {
		eic.stats.AvgTime = eic.stats.TotalTime / time.Duration(total)
	}
	eic.stats.mu.Unlock()
}

// GetStats returns current cache statistics
func (eic *EnhancedIncludeCache) GetStats() IncludeCacheStats {
	eic.stats.mu.RLock()
	defer eic.stats.mu.RUnlock()
	return eic.stats
}

// Clear removes all cached entries by recreating the cache
func (eic *EnhancedIncludeCache) Clear() {
	eic.mu.Lock()
	eic.cache = cache.NewCache[*TerragruntConfig]("enhanced-include-cache")
	eic.mu.Unlock()
	
	// Reset stats
	eic.stats.mu.Lock()
	eic.stats = IncludeCacheStats{}
	eic.stats.mu.Unlock()
}

// GetHitRatio returns the cache hit ratio as a percentage
func (eic *EnhancedIncludeCache) GetHitRatio() float64 {
	eic.stats.mu.RLock()
	defer eic.stats.mu.RUnlock()
	
	total := eic.stats.Hits + eic.stats.Misses
	if total == 0 {
		return 0.0
	}
	
	return float64(eic.stats.Hits) / float64(total) * 100.0
}

// Global enhanced include cache instance
var globalIncludeCache *EnhancedIncludeCache
var includeCacheInitOnce sync.Once

// GetGlobalIncludeCache returns the global include cache instance
func GetGlobalIncludeCache() *EnhancedIncludeCache {
	includeCacheInitOnce.Do(func() {
		globalIncludeCache = NewEnhancedIncludeCache()
	})
	return globalIncludeCache
}

// Enhanced version of parseIncludedConfig that uses caching
func parseIncludedConfigWithCaching(ctx *ParsingContext, l log.Logger, includeConfig *IncludeConfig) (*TerragruntConfig, error) {
	includeCache := GetGlobalIncludeCache()
	return includeCache.ParseIncludedConfigCached(ctx, l, includeConfig)
}