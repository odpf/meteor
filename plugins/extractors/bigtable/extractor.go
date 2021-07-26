package bigtable

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/odpf/meteor/core"
	"github.com/odpf/meteor/proto/odpf/meta/facets"

	"cloud.google.com/go/bigtable"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/utils"
)

type Config struct {
	ProjectID string `mapstructure:"project_id" validate:"required"`
}

type Extractor struct {
	logger plugins.Logger
}

type InstancesFetcher interface {
	Instances(context.Context) ([]*bigtable.InstanceInfo, error)
}

var (
	instanceAdminClientCreator = createInstanceAdminClient
	instanceInfoGetter         = getInstancesInfo
)

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {
	e.logger.Info("extracting bigtable metadata...")

	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return extractor.InvalidConfigError{}
	}

	instanceAdminClient, err := instanceAdminClientCreator(ctx, config)
	if err != nil {
		return
	}
	instanceNames, err := instanceInfoGetter(ctx, instanceAdminClient)
	if err != nil {
		return
	}
	result, err := e.getTablesInfo(ctx, instanceNames, config.ProjectID)
	if err != nil {
		return
	}
	out <- result
	return
}

func getInstancesInfo(ctx context.Context, client InstancesFetcher) (instanceNames []string, err error) {
	instanceInfos, err := client.Instances(ctx)
	if err != nil {
		return
	}
	for i := 0; i < len(instanceInfos); i++ {
		instanceNames = append(instanceNames, instanceInfos[i].Name)
	}
	return instanceNames, nil
}

func (e *Extractor) getTablesInfo(ctx context.Context, instances []string, projectID string) (results []meta.Table, err error) {
	for _, instance := range instances {
		adminClient, err := e.createAdminClient(ctx, instance, projectID)
		if err != nil {
			return nil, err
		}
		tables, _ := adminClient.Tables(ctx)
		wg := sync.WaitGroup{}
		for _, table := range tables {
			wg.Add(1)
			go func(table string) {
				tableInfo, err := adminClient.TableInfo(ctx, table)
				if err != nil {
					return
				}
				customProps := make(map[string]string)
				familyInfoBytes, _ := json.Marshal(tableInfo.FamilyInfos)
				customProps["column_family"] = string(familyInfoBytes)
				results = append(results, meta.Table{
					Urn:    fmt.Sprintf("%s.%s.%s", projectID, instance, table),
					Name:   table,
					Source: "bigtable",
					Custom: &facets.Custom{
						CustomProperties: customProps,
					},
				})
				wg.Done()
			}(table)
		}
		wg.Wait()
	}
	return
}

func createInstanceAdminClient(ctx context.Context, config Config) (*bigtable.InstanceAdminClient, error) {
	return bigtable.NewInstanceAdminClient(ctx, config.ProjectID)
}

func (e *Extractor) createAdminClient(ctx context.Context, instance string, projectID string) (*bigtable.AdminClient, error) {
	return bigtable.NewAdminClient(ctx, projectID, instance)
}

// Register the extractor to catalog
func init() {
	if err := extractor.Catalog.Register("bigtable", func() core.Extractor {
		return &Extractor{
			logger: plugins.Log,
		}
	}); err != nil {
		panic(err)
	}
}
