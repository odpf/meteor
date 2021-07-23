package googlecloudstorage

import (
	"context"
	"errors"
	"fmt"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/common"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"google.golang.org/protobuf/types/known/timestamppb"

	"cloud.google.com/go/storage"
	"github.com/mitchellh/mapstructure"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/utils"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	metadataSource = "googlecloudstorage"
)

type Config struct {
	ProjectID          string `mapstructure:"project_id" validate:"required"`
	ServiceAccountJSON string `mapstructure:"service_account_json"`
}

type Extractor struct {
	logger plugins.Logger
}

func New(logger plugins.Logger) extractor.BucketExtractor {
	return &Extractor{
		logger: logger,
	}
}

func (e *Extractor) Extract(configMap map[string]interface{}) (result []meta.Bucket, err error) {
	e.logger.Info("extracting kafka metadata...")
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return result, extractor.InvalidConfigError{}
	}
	err = e.validateConfig(config)
	if err != nil {
		return
	}

	ctx := context.Background()
	client, err := e.createClient(ctx, config)
	if err != nil {
		return
	}
	result, err = e.getMetadata(ctx, client, config.ProjectID)
	if err != nil {
		return
	}

	return
}

func (e *Extractor) getMetadata(ctx context.Context, client *storage.Client, projectID string) ([]meta.Bucket, error) {
	e.logger.Info(fmt.Sprintf("Extracting buckets metadata for %s", projectID))
	it := client.Buckets(ctx, projectID)
	var results []meta.Bucket

	for {
		bucket, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		e.logger.Info(fmt.Sprintf("Extracting blobs metadata for %s", bucket.Name))
		blobs, err := e.getBlobs(ctx, bucket.Name, client, projectID)
		if err != nil {
			return nil, err
		}
		results = append(results, e.mapBucket(bucket, projectID, blobs))
	}

	return results, nil
}

func (e *Extractor) getBlobs(ctx context.Context, bucketName string, client *storage.Client, projectID string) (*facets.Blobs, error) {
	it := client.Bucket(bucketName).Objects(ctx, nil)
	var blobs []*facets.Blob

	object, err := it.Next()
	for err == nil {
		blobs = append(blobs, e.mapObject(object, projectID))
		object, err = it.Next()
	}
	if err == iterator.Done {
		err = nil
	}

	blobsResult := &facets.Blobs{
		Blobs: blobs,
	}
	return blobsResult, err
}

func (e *Extractor) mapBucket(b *storage.BucketAttrs, projectID string, blobs *facets.Blobs) meta.Bucket {
	return meta.Bucket{
		Urn:         fmt.Sprintf("%s/%s", projectID, b.Name),
		Name:        b.Name,
		Location:    b.Location,
		StorageType: b.StorageClass,
		Blobs:       blobs,
		Source:      metadataSource,
		Timestamps: &common.Timestamp{
			CreatedAt: timestamppb.New(b.Created),
		},
		Tags: &facets.Tags{
			Tags: b.Labels,
		},
	}
}

func (e *Extractor) mapObject(blob *storage.ObjectAttrs, projectID string) *facets.Blob {
	return &facets.Blob{
		Urn:       fmt.Sprintf("%s/%s/%s", projectID, blob.Bucket, blob.Name),
		Name:      blob.Name,
		Size:      blob.Size,
		DeletedAt: timestamppb.New(blob.Deleted),
		ExpiredAt: timestamppb.New(blob.RetentionExpirationTime),
		Ownership: &facets.Ownership{
			Owners: []*facets.Owner{
				{Name: blob.Owner},
			},
		},
		Timestamps: &common.Timestamp{
			CreatedAt: timestamppb.New(blob.Created),
			UpdatedAt: timestamppb.New(blob.Updated),
		},
	}
}

func (e *Extractor) createClient(ctx context.Context, config Config) (*storage.Client, error) {
	if config.ServiceAccountJSON == "" {
		e.logger.Info("credentials are not specified, creating google cloud storage client using Default Credentials...")
		return storage.NewClient(ctx)
	}

	return storage.NewClient(ctx, option.WithCredentialsJSON([]byte(config.ServiceAccountJSON)))
}

func (e *Extractor) getConfig(configMap map[string]interface{}) (config Config, err error) {
	err = mapstructure.Decode(configMap, &config)
	if err != nil {
		return
	}

	return
}

func (e *Extractor) validateConfig(config Config) (err error) {
	if config.ProjectID == "" {
		return errors.New("project_id is required")
	}

	return
}
