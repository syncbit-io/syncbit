package provider

import (
	"context"
	"fmt"

	"syncbit/internal/config"
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
	cfg      *config.ProviderConfig
	session  *session.Session
	s3Client *s3.S3
	transfer *transport.S3Transfer
}

// NewS3Provider creates a new S3 provider
func NewS3Provider(cfg config.ProviderConfig, transferCfg config.TransferConfig) (Provider, error) {
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
	transfer, err := transport.NewS3Transfer(s3Client, sess)
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

// Provider interface methods - these are deprecated in favor of bucket-specific methods

// ListFiles - deprecated, returns error directing to use ListFilesInBucket
func (p *S3Provider) ListFiles(ctx context.Context) ([]string, error) {
	return nil, fmt.Errorf("ListFiles is deprecated - use ListFilesInBucket(ctx, bucket) instead")
}

// GetFile - deprecated, returns error directing to use GetFileFromBucket
func (p *S3Provider) GetFile(ctx context.Context, path string) (*types.FileInfo, error) {
	return nil, fmt.Errorf("GetFile is deprecated - use GetFileFromBucket(ctx, bucket, path) instead")
}

// DownloadFile is not used in the job-based architecture
func (p *S3Provider) DownloadFile(ctx context.Context, path string, destPath string) (*types.FileInfo, error) {
	return nil, fmt.Errorf("DownloadFile not supported in job-based architecture - use S3DownloadHandler instead")
}

func init() {
	RegisterProviderFactory("s3", NewS3Provider)
}
