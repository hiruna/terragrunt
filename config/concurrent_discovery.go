package config

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/gruntwork-io/terragrunt/options"
	"github.com/gruntwork-io/terragrunt/util"
)

// ConcurrentFileDiscovery handles parallel file discovery for terragrunt configs
type ConcurrentFileDiscovery struct {
	opts              *options.TerragruntOptions
	maxWorkers        int
	maxDirectoryDepth int
}

// NewConcurrentFileDiscovery creates a new concurrent file discovery instance
func NewConcurrentFileDiscovery(opts *options.TerragruntOptions) *ConcurrentFileDiscovery {
	// Use configured values or auto-detect defaults
	maxWorkers := opts.MaxDiscoveryWorkers
	if maxWorkers == 0 {
		maxWorkers = runtime.NumCPU() * 2 // Auto-detect: CPU cores * 2
	}
	
	maxDepth := opts.MaxDirectoryDepth
	if maxDepth == 0 {
		maxDepth = 20 // Default to prevent infinite recursion
	}
	
	return &ConcurrentFileDiscovery{
		opts:              opts,
		maxWorkers:        maxWorkers,
		maxDirectoryDepth: maxDepth,
	}
}

// SetMaxWorkers allows customizing the number of worker goroutines
func (cfd *ConcurrentFileDiscovery) SetMaxWorkers(workers int) *ConcurrentFileDiscovery {
	cfd.maxWorkers = workers
	return cfd
}

// SetMaxDepth allows customizing the maximum directory traversal depth
func (cfd *ConcurrentFileDiscovery) SetMaxDepth(depth int) *ConcurrentFileDiscovery {
	cfd.maxDirectoryDepth = depth
	return cfd
}

// dirJob represents a directory to be scanned
type dirJob struct {
	path  string
	depth int
}

// configResult represents the result of scanning a directory
type configResult struct {
	configs []string
	subdirs []dirJob
	err     error
}

// FindConfigFilesInPathConcurrent performs concurrent file discovery
func (cfd *ConcurrentFileDiscovery) FindConfigFilesInPathConcurrent(rootPath string) ([]string, error) {
	jobs := make(chan dirJob, 100)
	results := make(chan configResult, 100)

	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < cfd.maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cfd.worker(jobs, results)
		}()
	}

	// Start with root directory
	go func() {
		defer close(jobs)
		jobs <- dirJob{path: rootPath, depth: 0}

		// Process results and queue subdirectories
		var pendingJobs int = 1
		for result := range results {
			pendingJobs--

			if result.err != nil {
				continue
			}

			// Queue subdirectories if within depth limit
			for _, subdir := range result.subdirs {
				if subdir.depth < cfd.maxDirectoryDepth {
					jobs <- subdir
					pendingJobs++
				}
			}

			// Close results channel when all jobs are processed
			if pendingJobs == 0 {
				close(results)
				break
			}
		}
	}()

	// Collect all config files
	var allConfigs []string
	var mu sync.Mutex

	go func() {
		for result := range results {
			if result.err == nil && len(result.configs) > 0 {
				mu.Lock()
				allConfigs = append(allConfigs, result.configs...)
				mu.Unlock()
			}
		}
	}()

	wg.Wait()

	return allConfigs, nil
}

// worker processes directory scan jobs
func (cfd *ConcurrentFileDiscovery) worker(jobs <-chan dirJob, results chan<- configResult) {
	for job := range jobs {
		result := cfd.scanDirectory(job)
		results <- result
	}
}

// scanDirectory scans a single directory for config files and subdirectories
func (cfd *ConcurrentFileDiscovery) scanDirectory(job dirJob) configResult {
	var configs []string
	var subdirs []dirJob

	// Check if this is a valid Terragrunt module directory
	isModule, err := isTerragruntModuleDir(job.path, cfd.opts)
	if err != nil {
		return configResult{err: err}
	}

	if !isModule {
		return configResult{} // Skip this directory
	}

	// Look for config files in this directory
	for _, configFile := range append(DefaultTerragruntConfigPaths, filepath.Base(cfd.opts.TerragruntConfigPath)) {
		if !filepath.IsAbs(configFile) {
			configFile = util.JoinPath(job.path, configFile)
		}

		if !util.IsDir(configFile) && util.FileExists(configFile) {
			configs = append(configs, configFile)
			break // Found a config file, no need to check others
		}
	}

	// Find subdirectories to process
	entries, err := os.ReadDir(job.path)
	if err != nil {
		return configResult{err: err}
	}

	for _, entry := range entries {
		if entry.IsDir() {
			subdirPath := filepath.Join(job.path, entry.Name())
			subdirs = append(subdirs, dirJob{
				path:  subdirPath,
				depth: job.depth + 1,
			})
		}
	}

	return configResult{
		configs: configs,
		subdirs: subdirs,
	}
}

// FindConfigFilesInPathConcurrent is a convenience function that uses the concurrent discovery
func FindConfigFilesInPathConcurrent(rootPath string, opts *options.TerragruntOptions) ([]string, error) {
	discovery := NewConcurrentFileDiscovery(opts)
	return discovery.FindConfigFilesInPathConcurrent(rootPath)
}
