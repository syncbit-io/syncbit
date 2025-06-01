package provider

import (
	"context"
	"fmt"

	"syncbit/internal/core/types"
	"syncbit/internal/transport"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3Provider implements the Provider interface for AWS S3
// Provides authentication and connection services for S3 access
type S3Provider struct {
	id       string
	cfg      *types.ProviderConfig
	session  *session.Session
	s3Client *s3.S3
	transfer *transport.S3Transfer
}

// NewS3Provider creates a new S3 provider
func NewS3Provider(cfg types.ProviderConfig, transferCfg types.TransferConfig) (Provider, error) {
	// Create AWS session
	var sess *session.Session
	var err error

	switch cfg.Profile {
	case "":
		// Use default credentials/region
		sessionConfig := aws.Config{}
		if cfg.Region != "" {
			sessionConfig.Region = aws.String(cfg.Region)
		}
		sess, err = session.NewSessionWithOptions(session.Options{
			Config: sessionConfig,
		})
	default:
		// Use specific profile
		sessionConfig := aws.Config{}
		if cfg.Region != "" {
			sessionConfig.Region = aws.String(cfg.Region)
		}
		sess, err = session.NewSessionWithOptions(session.Options{
			Profile: cfg.Profile,
			Config:  sessionConfig,
		})
	}

	if err != nil {
		return nil, err
	}

	s3Client := s3.New(sess)
	transfer, err := transport.NewS3Transfer(s3Client, sess, transferCfg)
	if err != nil {
		return nil, err
	}

	return &S3Provider{
		id:       cfg.ID,
		cfg:      &cfg,
		session:  sess,
		s3Client: s3Client,
		transfer: transfer,
	}, nil
}

// GetName returns the name of the provider
func (p *S3Provider) GetName() string {
	if p.cfg.Name != "" {
		return p.cfg.Name
	}
	return p.cfg.Type
}

// GetID returns the unique ID of the provider
func (p *S3Provider) GetID() string {
	return p.id
}

// GetSession returns the AWS session for this provider
func (p *S3Provider) GetSession() *session.Session {
	return p.session
}

// GetS3Client returns the S3 client for this provider
func (p *S3Provider) GetS3Client() *s3.S3 {
	return p.s3Client
}

// ListFilesInBucket returns a list of all files in the specified S3 bucket
func (p *S3Provider) ListFilesInBucket(ctx context.Context, bucket string) ([]string, error) {
	return p.transfer.ListFiles(ctx, bucket)
}

// GetFileFromBucket gets metadata for a specific file in the specified bucket
func (p *S3Provider) GetFileFromBucket(ctx context.Context, bucket, path string) (*types.FileInfo, error) {
	return p.transfer.GetFile(ctx, bucket, path)
}

// Download downloads a file from S3 using the provided ReaderWriter
func (p *S3Provider) Download(ctx context.Context, repo, revision, filePath string, cacheWriter *types.ReaderWriter) error {
	// For S3 provider, repo should be the bucket name
	bucket := repo

	// The S3 transport already handles complex multipart downloads
	// We need to create a temporary ReaderWriter that writes to the cache writer
	// For now, use a simpler approach similar to HTTP providers

	// Get the object from S3
	result, err := p.s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filePath),
	})
	if err != nil {
		return fmt.Errorf("get S3 object: %w", err)
	}
	defer result.Body.Close()

	// Create a new ReaderWriter that combines the S3 response with the cache writer
	opts := []types.RWOption{
		types.RWWithIOReader(result.Body),
		types.RWWithIOWriter(cacheWriter.WriterAt(ctx)),
	}

	downloadRW := types.NewReaderWriter(opts...)

	// Transfer data from S3 response through to cache
	_, err = downloadRW.Transfer(ctx)
	return err
}

func init() {
	RegisterProviderFactory("s3", NewS3Provider)
}
