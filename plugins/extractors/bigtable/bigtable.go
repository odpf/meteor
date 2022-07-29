package bigtable

import (
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/json"
	"fmt"
	"sync"

	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/registry"

	"cloud.google.com/go/bigtable"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

const (
	service = "bigtable"
)

// Config holds the configurations for the bigtable extractor
type Config struct {
	ProjectID string `mapstructure:"project_id" validate:"required"`
}

var info = plugins.Info{
	Description: "Compressed, high-performance, proprietary data storage system.",
	Summary:     summary,
	Tags:        []string{"gcp", "extractor"},
	SampleConfig: `
	project_id: google-project-id`,
}

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
	plugins.BaseExtractor
	config        Config
	logger        log.Logger
	instanceNames []string
}

func New(logger log.Logger) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	client, err := instanceAdminClientCreator(ctx, e.config)
	if err != nil {
		return
	}
	e.instanceNames, err = instanceInfoGetter(ctx, client)
	if err != nil {
		return
	}

	return
}

// Extract checks if the extractor is configured and
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
				emit(models.NewRecord(&assetsv1beta1.Table{
					Resource: &commonv1beta1.Resource{
						Urn:     models.NewURN(service, e.config.ProjectID, "table", fmt.Sprintf("%s.%s", instance, table)),
						Name:    table,
						Service: service,
						Type:    "table",
					},
					Properties: &facetsv1beta1.Properties{
						Attributes: utils.TryParseMapToProto(map[string]interface{}{
							"column_family": string(familyInfoBytes),
						}),
					},
				}))

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
