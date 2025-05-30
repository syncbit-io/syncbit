package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"syncbit/internal/cache"
	"syncbit/internal/core/types"
	"syncbit/internal/runner"
	"syncbit/internal/transport"
)

type HTTPDownloadHandlerOption func(*HTTPDownloadHandler)

func HTTPDownloadHandlerWithLimiter(limiter *types.RateLimiter) HTTPDownloadHandlerOption {
	return func(t *HTTPDownloadHandler) {
		t.limiter = limiter
	}
}

// HTTPDownloadHandlerWithCache sets a cache for the handler
func HTTPDownloadHandlerWithCache(cache *cache.Cache) HTTPDownloadHandlerOption {
	return func(h *HTTPDownloadHandler) {
		h.cache = cache
	}
}

// HTTPDownloadHandler is a handler that downloads a file from a URL.
type HTTPDownloadHandler struct {
	localBasePath string // local base path
	localJobPath  string // local job path -> localBasePath/localJobPath
	baseUrl       string // base url of the remote file path
	remotePath    string // remote file path -> baseUrl/remotePath -> localBasePath/localJobPath/remotePath
	limiter       *types.RateLimiter
	cache         *cache.Cache // Cache for storing/retrieving files
}

// NewHTTPDownloadHandler creates a new HTTPDownloadHandler for downloading files from a URL.
func NewHTTPDownloadHandler(
	localBasePath, localJobPath,
	baseUrl, remotePath string,
	opts ...HTTPDownloadHandlerOption,
) *HTTPDownloadHandler {

	t := &HTTPDownloadHandler{
		localBasePath: localBasePath,
		localJobPath:  localJobPath,
		baseUrl:       baseUrl,
		remotePath:    remotePath,
		limiter:       types.DefaultRateLimiter(),
	}

	for _, opt := range opts {
		opt(t)
	}

	return t
}

func (t *HTTPDownloadHandler) Run(ctx context.Context, self *runner.Job) error {
	u, err := url.JoinPath(t.baseUrl, t.remotePath)
	if err != nil {
		return err
	}

	// Create cache key for this file using the full URL
	cacheKey := fmt.Sprintf("http/%s", t.remotePath)

	filePath := filepath.Dir(t.remotePath)
	fileName := filepath.Base(t.remotePath)
	destPath := filepath.Join(t.localBasePath, t.localJobPath, filePath)

	if err := os.MkdirAll(destPath, 0755); err != nil {
		return err
	}

	destFile := filepath.Join(destPath, fileName)

	// Try cache first if available
	if t.cache != nil {
		// Extract dataset and filepath from cache key
		parts := filepath.SplitList(cacheKey)
		if len(parts) >= 2 {
			dataset := parts[0]
			filePath := filepath.Join(parts[1:]...)

			// Check if file exists in cache
			if t.cache.HasFileByPath(dataset, filePath) {
				// Create destination file
				fileHandle, err := os.Create(destFile)
				if err != nil {
					return err
				}
				defer fileHandle.Close()

				// Get file from cache and estimate size for progress tracking
				if data, err := t.cache.GetFileByPath(dataset, filePath); err == nil {
					self.Tracker().SetTotal(int64(len(data)))
					self.Tracker().SetCurrent(0)

					// Create cache reader with progress tracking
					cacheReader := t.cache.NewCacheReader(dataset, filePath, types.Bytes(len(data)),
						types.RWWithIOWriter(fileHandle),
						types.RWWithReadLimiter(t.limiter),
						types.RWWithReaderCallback(func(n int64) {
							self.Tracker().IncCurrent(n)
						}),
					)

					_, err = cacheReader.Transfer(ctx)
					if err == nil {
						return nil // Cache hit successful
					}
					// Continue to HTTP download on cache transfer failure
				}
			}
		}
	}

	fileHandle, err := os.Create(destFile)
	if err != nil {
		return err
	}
	defer fileHandle.Close()

	ht := transport.NewHTTPTransfer(
		transport.HTTPWithClient(transport.DefaultHTTPClient()),
	)

	htCallback := func(resp *http.Response) error {
		defer resp.Body.Close()
		self.Tracker().SetTotal(resp.ContentLength)
		self.Tracker().SetCurrent(0)

		// Extract dataset and filepath from cache key for writing to cache
		parts := filepath.SplitList(cacheKey)
		if t.cache != nil && len(parts) >= 2 {
			dataset := parts[0]
			filePath := filepath.Join(parts[1:]...)

			// Create cache writer with progress tracking that also writes to file
			cacheWriter := t.cache.NewCacheWriter(dataset, filePath, types.Bytes(resp.ContentLength),
				types.RWWithIOReader(resp.Body),
				types.RWWithReadLimiter(t.limiter),
				types.RWWithReaderCallback(func(n int64) {
					self.Tracker().IncCurrent(n)
				}),
			)

			// Transfer from response body to cache (which handles file storage)
			_, err := cacheWriter.Transfer(ctx)
			if err == nil {
				// Copy from cache to destination file
				if data, getErr := t.cache.GetFileByPath(dataset, filePath); getErr == nil {
					_, writeErr := fileHandle.Write(data)
					return writeErr
				}
			}
			return err
		} else {
			// No cache available, write directly to file
			rw := types.NewReaderWriter(
				types.RWWithIOWriter(fileHandle),
				types.RWWithIOReader(resp.Body),
				types.RWWithReadLimiter(t.limiter),
				types.RWWithReaderCallback(func(n int64) {
					self.Tracker().IncCurrent(n)
				}),
			)

			_, err := rw.Transfer(ctx)
			return err
		}
	}

	return ht.Get(ctx, u, htCallback)
}
