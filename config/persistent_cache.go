package config

import (
	"context"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gruntwork-io/terragrunt/internal/cache"
	"github.com/gruntwork-io/terragrunt/options"
	"github.com/gruntwork-io/terragrunt/util"
)

// PersistentConfigCache provides file-based caching for parsed configurations
type PersistentConfigCache struct {
	cacheDir       string
	memoryCache    *cache.Cache[*TerragruntConfig]
	mu             sync.RWMutex
	maxCacheSize   int64
	defaultTTL     time.Duration
	enabled        bool
}

// CacheEntry represents a cached config with metadata
type CacheEntry struct {
	Config    *TerragruntConfig
	CreatedAt time.Time
	ModTime   time.Time
	Hash      string
	Path      string
}

// NewPersistentConfigCache creates a new persistent cache instance
func NewPersistentConfigCache(opts *options.TerragruntOptions) (*PersistentConfigCache, error) {
	cacheDir := filepath.Join(opts.WorkingDir, ".terragrunt", "cache", "configs")
	
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}
	
	memCache := cache.NewCache[*TerragruntConfig]("persistent-config-cache")
	
	return &PersistentConfigCache{
		cacheDir:     cacheDir,
		memoryCache:  memCache,
		maxCacheSize: 100 * 1024 * 1024, // 100MB default limit
		defaultTTL:   24 * time.Hour,     // 24 hour default TTL
		enabled:      true,
	}, nil
}

// SetEnabled allows enabling/disabling the cache
func (pcc *PersistentConfigCache) SetEnabled(enabled bool) *PersistentConfigCache {
	pcc.enabled = enabled
	return pcc
}

// SetMaxSize sets the maximum cache size in bytes
func (pcc *PersistentConfigCache) SetMaxSize(size int64) *PersistentConfigCache {
	pcc.maxCacheSize = size
	return pcc
}

// SetTTL sets the default time-to-live for cache entries
func (pcc *PersistentConfigCache) SetTTL(ttl time.Duration) *PersistentConfigCache {
	pcc.defaultTTL = ttl
	return pcc
}

// Get retrieves a config from cache if valid
func (pcc *PersistentConfigCache) Get(configPath string, currentModTime time.Time) (*TerragruntConfig, bool) {
	if !pcc.enabled {
		return nil, false
	}
	
	pcc.mu.RLock()
	defer pcc.mu.RUnlock()
	
	// Try memory cache first
	cacheKey := pcc.generateCacheKey(configPath)
	if cached, found := pcc.memoryCache.Get(context.Background(), cacheKey); found {
		return cached, true
	}
	
	// Try disk cache
	return pcc.getFromDisk(configPath, currentModTime)
}

// Set stores a config in both memory and disk cache
func (pcc *PersistentConfigCache) Set(configPath string, config *TerragruntConfig, modTime time.Time) error {
	if !pcc.enabled {
		return nil
	}
	
	pcc.mu.Lock()
	defer pcc.mu.Unlock()
	
	// Store in memory cache
	cacheKey := pcc.generateCacheKey(configPath)
	pcc.memoryCache.Put(context.Background(), cacheKey, config)
	
	// Store in disk cache
	return pcc.saveToDisk(configPath, config, modTime)
}

// Clear removes all cached entries
func (pcc *PersistentConfigCache) Clear() error {
	pcc.mu.Lock()
	defer pcc.mu.Unlock()
	
	// Clear memory cache
	pcc.memoryCache = cache.NewCache[*TerragruntConfig]("persistent-config-cache")
	
	// Clear disk cache
	return os.RemoveAll(pcc.cacheDir)
}

// getFromDisk retrieves and validates a config from disk cache
func (pcc *PersistentConfigCache) getFromDisk(configPath string, currentModTime time.Time) (*TerragruntConfig, bool) {
	cacheFile := pcc.getCacheFilePath(configPath)
	
	if !util.FileExists(cacheFile) {
		return nil, false
	}
	
	file, err := os.Open(cacheFile)
	if err != nil {
		return nil, false
	}
	defer file.Close()
	
	var entry CacheEntry
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&entry); err != nil {
		// Invalid cache entry, remove it
		os.Remove(cacheFile)
		return nil, false
	}
	
	// Validate cache entry
	if !pcc.isValidCacheEntry(&entry, configPath, currentModTime) {
		os.Remove(cacheFile)
		return nil, false
	}
	
	// Cache hit - add to memory cache for faster future access
	cacheKey := pcc.generateCacheKey(configPath)
	pcc.memoryCache.Put(context.Background(), cacheKey, entry.Config)
	
	return entry.Config, true
}

// saveToDisk stores a config entry to disk
func (pcc *PersistentConfigCache) saveToDisk(configPath string, config *TerragruntConfig, modTime time.Time) error {
	cacheFile := pcc.getCacheFilePath(configPath)
	
	// Create directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(cacheFile), 0755); err != nil {
		return err
	}
	
	// Create cache entry
	entry := CacheEntry{
		Config:    config,
		CreatedAt: time.Now(),
		ModTime:   modTime,
		Hash:      pcc.generateContentHash(configPath),
		Path:      configPath,
	}
	
	// Write to temporary file first, then rename for atomic operation
	tmpFile := cacheFile + ".tmp"
	file, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	defer file.Close()
	
	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(entry); err != nil {
		os.Remove(tmpFile)
		return err
	}
	
	// Atomic rename
	return os.Rename(tmpFile, cacheFile)
}

// isValidCacheEntry checks if a cache entry is still valid
func (pcc *PersistentConfigCache) isValidCacheEntry(entry *CacheEntry, configPath string, currentModTime time.Time) bool {
	// Check if entry is too old
	if time.Since(entry.CreatedAt) > pcc.defaultTTL {
		return false
	}
	
	// Check if file has been modified
	if !entry.ModTime.Equal(currentModTime) {
		return false
	}
	
	// Check if content hash still matches
	currentHash := pcc.generateContentHash(configPath)
	if entry.Hash != currentHash {
		return false
	}
	
	return true
}

// generateCacheKey creates a cache key for the given config path
func (pcc *PersistentConfigCache) generateCacheKey(configPath string) string {
	hasher := sha256.New()
	hasher.Write([]byte(configPath))
	return fmt.Sprintf("%x", hasher.Sum(nil))[:16]
}

// generateContentHash creates a hash of the config file content
func (pcc *PersistentConfigCache) generateContentHash(configPath string) string {
	content, err := os.ReadFile(configPath)
	if err != nil {
		return ""
	}
	
	hasher := sha256.New()
	hasher.Write(content)
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

// getCacheFilePath returns the full path to the cache file for a config
func (pcc *PersistentConfigCache) getCacheFilePath(configPath string) string {
	cacheKey := pcc.generateCacheKey(configPath)
	return filepath.Join(pcc.cacheDir, cacheKey+".cache")
}

// CleanupExpired removes expired cache entries
func (pcc *PersistentConfigCache) CleanupExpired() error {
	pcc.mu.Lock()
	defer pcc.mu.Unlock()
	
	return filepath.Walk(pcc.cacheDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		
		if !info.IsDir() && filepath.Ext(path) == ".cache" {
			// Check if file is expired
			if time.Since(info.ModTime()) > pcc.defaultTTL {
				os.Remove(path)
			}
		}
		
		return nil
	})
}

// GetCacheStats returns statistics about the cache usage
func (pcc *PersistentConfigCache) GetCacheStats() map[string]interface{} {
	pcc.mu.RLock()
	defer pcc.mu.RUnlock()
	
	stats := make(map[string]interface{})
	
	// Memory cache stats
	stats["memory_cache_enabled"] = pcc.enabled
	
	// Disk cache stats
	var totalSize int64
	var fileCount int
	
	filepath.Walk(pcc.cacheDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			totalSize += info.Size()
			fileCount++
		}
		return nil
	})
	
	stats["disk_cache_size_bytes"] = totalSize
	stats["disk_cache_file_count"] = fileCount
	stats["cache_directory"] = pcc.cacheDir
	
	return stats
}

// Global persistent cache instance
var globalPersistentCache *PersistentConfigCache
var cacheInitOnce sync.Once

// GetGlobalPersistentCache returns the global persistent cache instance
func GetGlobalPersistentCache(opts *options.TerragruntOptions) (*PersistentConfigCache, error) {
	var initErr error
	
	cacheInitOnce.Do(func() {
		globalPersistentCache, initErr = NewPersistentConfigCache(opts)
	})
	
	return globalPersistentCache, initErr
}