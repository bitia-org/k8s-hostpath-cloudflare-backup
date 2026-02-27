package r2

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Credentials holds Cloudflare R2 authentication details.
type Credentials struct {
	AccountID      string `json:"account_id"`
	AccessKeyID    string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	Bucket         string `json:"bucket"`
}

// ObjectInfo describes an object in R2.
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// Client wraps a minio client configured for Cloudflare R2.
type Client struct {
	mc      *minio.Client
	bucket  string
	verbose bool
}

// LoadCredentials reads and validates R2 credentials from a JSON file.
func LoadCredentials(path string) (*Credentials, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading credentials file: %w", err)
	}

	var creds Credentials
	if err := json.Unmarshal(data, &creds); err != nil {
		return nil, fmt.Errorf("parsing credentials JSON: %w", err)
	}

	if err := creds.validate(); err != nil {
		return nil, err
	}
	return &creds, nil
}

func (c *Credentials) validate() error {
	if c.AccountID == "" {
		return fmt.Errorf("credentials: account_id is required")
	}
	if c.AccessKeyID == "" {
		return fmt.Errorf("credentials: access_key_id is required")
	}
	if c.SecretAccessKey == "" {
		return fmt.Errorf("credentials: secret_access_key is required")
	}
	if c.Bucket == "" {
		return fmt.Errorf("credentials: bucket is required")
	}
	return nil
}

// New creates an R2 client from the given credentials.
func New(creds *Credentials, verbose bool) (*Client, error) {
	endpoint := fmt.Sprintf("%s.r2.cloudflarestorage.com", creds.AccountID)

	mc, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(creds.AccessKeyID, creds.SecretAccessKey, ""),
		Secure: true,
	})
	if err != nil {
		return nil, fmt.Errorf("creating R2 client: %w", err)
	}

	return &Client{mc: mc, bucket: creds.Bucket, verbose: verbose}, nil
}

// Upload sends a local file to R2 under the given key.
func (c *Client) Upload(ctx context.Context, archivePath, key string) error {
	c.logf("Uploading %s -> r2://%s/%s", archivePath, c.bucket, key)

	info, err := c.mc.FPutObject(ctx, c.bucket, key, archivePath, minio.PutObjectOptions{
		ContentType: "application/gzip",
	})
	if err != nil {
		return fmt.Errorf("uploading %s: %w", key, err)
	}

	c.logf("Uploaded %s (%d bytes)", key, info.Size)
	return nil
}

// Download fetches an object from R2 and saves it to destPath.
func (c *Client) Download(ctx context.Context, key, destPath string) error {
	c.logf("Downloading r2://%s/%s -> %s", c.bucket, key, destPath)

	if err := c.mc.FGetObject(ctx, c.bucket, key, destPath, minio.GetObjectOptions{}); err != nil {
		return fmt.Errorf("downloading %s: %w", key, err)
	}

	c.logf("Downloaded %s", key)
	return nil
}

// ListByPrefix returns objects whose key starts with prefix, sorted by LastModified descending (newest first).
func (c *Client) ListByPrefix(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	c.logf("Listing objects with prefix %q in bucket %s", prefix, c.bucket)

	var objects []ObjectInfo
	for obj := range c.mc.ListObjects(ctx, c.bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("listing objects: %w", obj.Err)
		}
		objects = append(objects, ObjectInfo{
			Key:          obj.Key,
			Size:         obj.Size,
			LastModified: obj.LastModified,
		})
	}

	sort.Slice(objects, func(i, j int) bool {
		return objects[i].LastModified.After(objects[j].LastModified)
	})

	c.logf("Found %d object(s) with prefix %q", len(objects), prefix)
	return objects, nil
}

// Delete removes a single object from R2.
func (c *Client) Delete(ctx context.Context, key string) error {
	c.logf("Deleting r2://%s/%s", c.bucket, key)

	if err := c.mc.RemoveObject(ctx, c.bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("deleting %s: %w", key, err)
	}
	return nil
}

// Rotate keeps only the keepLast newest objects matching prefix and deletes the rest.
// Returns the keys that were deleted.
func (c *Client) Rotate(ctx context.Context, prefix string, keepLast int) ([]string, error) {
	if keepLast <= 0 {
		return nil, nil
	}

	objects, err := c.ListByPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}

	if len(objects) <= keepLast {
		return nil, nil
	}

	toDelete := objects[keepLast:]
	var deleted []string
	for _, obj := range toDelete {
		if err := c.Delete(ctx, obj.Key); err != nil {
			return deleted, fmt.Errorf("rotating %s: %w", obj.Key, err)
		}
		deleted = append(deleted, obj.Key)
	}

	c.logf("Rotated prefix %q: kept %d, deleted %d", prefix, keepLast, len(deleted))
	return deleted, nil
}

func (c *Client) logf(format string, args ...interface{}) {
	if c.verbose {
		log.Printf("[r2] "+format, args...)
	}
}
