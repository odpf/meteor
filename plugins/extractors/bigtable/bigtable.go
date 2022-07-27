package bigtable

import (
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/json"
	"fmt"
	"sync"

	"github.com/odpf/meteor/models"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/registry"

	"cloud.google.com/go/bigtable"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

// Config holds the configurations for the bigtable extractor
type Config struct {
	ProjectID string `mapstructure:"project_id" validate:"required"`
}

var sampleConfig = `
project_id: google-project-id`

// InstancesFetcher is an interface for fetching instances
type InstancesFetcher interface {
	Instances(context.Context) ([]*bigtable.InstanceInfo, error)
}

var (
	instanceAdminClientCreator = createInstanceAdminClient
	instanceInfoGetter         = getInstancesInfo
)

// Extractor used to extract bigtable metadata
type Extractor struct {
	config        Config
	logger        log.Logger
	instanceNames []string
}

func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

// Info returns the brief information about the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Compressed, high-performance, proprietary data storage system.",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"gcp", "extractor"},
	}
}

// Validate validates the configuration
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	client, err := instanceAdminClientCreator(ctx, config)
	if err != nil {
		return
	}
	e.instanceNames, err = instanceInfoGetter(ctx, client)
	if err != nil {
		return
	}

	return
}

//Extract checks if the extractor is configured and
// if so, then extracts the metadata and
// returns the assets.
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	err = e.getTablesInfo(ctx, emit)
	if err != nil {
		return
	}

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

func (e *Extractor) getTablesInfo(ctx context.Context, emit plugins.Emit) (err error) {
	for _, instance := range e.instanceNames {
		adminClient, err := e.createAdminClient(ctx, instance, e.config.ProjectID)
		if err != nil {
			return err
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
				familyInfoBytes, _ := json.Marshal(tableInfo.FamilyInfos)
				asset := v1beta2.Asset{
					Urn:     fmt.Sprintf("%s.%s.%s", e.config.ProjectID, instance, table),
					Name:    table,
					Service: "bigtable",
					Type:    "table",
					Attributes: utils.TryParseMapToProto(map[string]interface{}{
						"column_family": string(familyInfoBytes),
					}),
				}
				emit(models.NewRecord(&asset))

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
	if err := registry.Extractors.Register("bigtable", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
