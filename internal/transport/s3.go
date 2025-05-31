package transport

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"syncbit/internal/core/types"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// S3Provider implements the Provider interface for AWS S3
type S3Transfer struct {
	cfg      types.TransferConfig
	session  *session.Session
	s3Client *s3.S3
	limiter  *types.RateLimiter
}

// NewS3Transfer creates a new S3 transfer instance
func NewS3Transfer(s3Client *s3.S3, s3Session *session.Session, cfg types.TransferConfig) (*S3Transfer, error) {
	// Create rate limiter based on transfer config
	var limiter *types.RateLimiter
	if cfg.RateLimit > 0 {
		// Use configured rate limit
		limiter = types.NewRateLimiter(types.Bytes(cfg.RateLimit))
	} else {
		// No rate limit configured, use default
		limiter = types.DefaultRateLimiter()
	}

	return &S3Transfer{
		cfg:      cfg,
		session:  s3Session,
		s3Client: s3Client,
		limiter:  limiter,
	}, nil
}

// ListFiles returns a list of all files in the S3 bucket
func (t *S3Transfer) ListFiles(ctx context.Context, bucket string) ([]string, error) {
	var files []string

	// List objects in the bucket
	err := t.s3Client.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, obj := range page.Contents {
			files = append(files, *obj.Key)
		}
		return !lastPage
	})

	if err != nil {
		return nil, err
	}

	return files, nil
}

// GetFile gets metadata for a specific file
func (t *S3Transfer) GetFile(ctx context.Context, bucket, path string) (*types.FileInfo, error) {
	// Get object metadata from S3
	result, err := t.s3Client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return nil, err
	}

	// Create file info
	file := &types.FileInfo{
		Path:    path,
		ModTime: *result.LastModified,
		// Status:  types.FileStatusPending,
	}

	if result.ContentLength != nil {
		file.Size = types.Bytes(*result.ContentLength)
	}

	if etag, ok := result.Metadata["Etag"]; ok && etag != nil {
		file.ETag = *etag
	} else if result.ETag != nil {
		// S3 ETags for single-part uploads are MD5 sums enclosed in quotes
		file.ETag = strings.Trim(*result.ETag, "\"")
	}

	return file, nil
}

// DownloadFile downloads a file from S3 to the local filesystem
func (t *S3Transfer) DownloadFile(ctx context.Context, bucket, path, destPath string) (*types.FileInfo, error) {
	// Create parent directories if they don't exist
	dir := filepath.Dir(destPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Get file metadata
	file, err := t.GetFile(ctx, bucket, path)
	if err != nil {
		return nil, err
	}

	// Create destination file
	fileHandle, err := os.Create(destPath)
	if err != nil {
		return nil, err
	}
	defer fileHandle.Close()

	rw := types.NewReaderWriter(
		types.RWWithReadLimiter(t.limiter),
		types.RWWithIOWriter(fileHandle),
	)

	downloader := s3manager.NewDownloader(
		t.session,
		func(d *s3manager.Downloader) {
			d.Concurrency = 1
			d.PartSize = 1024 * 1024 * 1024
			d.BufferProvider = s3ReadFromProvider{rw.WriterReadFromAdapter(ctx)}
		},
	)

	_, err = downloader.Download(rw.WriterAt(ctx), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
	})

	return file, err
}

// s3ReadFromProvider implements s3manager.WriterReadFromProvider for S3 downloads.
type s3ReadFromProvider struct {
	types.WriterReadFromProvider
}

func (s s3ReadFromProvider) GetReadFrom(w io.Writer) (s3manager.WriterReadFrom, func()) {
	return s.WriterReadFromProvider.GetReadFrom(w)
}
