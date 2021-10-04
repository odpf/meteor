package elastic

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"reflect"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

type Config struct {
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Host     string `mapstructure:"host" validate:"required"`
}

var sampleConfig = `
 user: "elastic"
 password: "changeme"
 host: elastic_server`

// Extractor manages the extraction of data from elastic
type Extractor struct {
	config Config
	logger log.Logger
	client *elasticsearch.Client
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

// Info returns the brief information about the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Search engine based on the Lucene library.",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"oss", "extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	//build config
	err = utils.BuildConfig(configMap, &e.config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	//build elasticsearch client
	cfg := elasticsearch.Config{
		Addresses: []string{
			e.config.Host,
		},
		Username: e.config.User,
		Password: e.config.Password,
	}
	if e.client, err = elasticsearch.NewClient(cfg); err != nil {
		return errors.Wrap(err, "failed to create client")
	}

	return
}

// Extract extracts the data from the elastic server
// and collected through the emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	res, err := e.client.Cluster.Health(
		e.client.Cluster.Health.WithLevel("indices"),
	)
	if err != nil {
		return errors.Wrap(err, "failed to fetch cluster information")
	}
	var r map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&r)
	if err != nil {
		return
	}
	x := reflect.ValueOf(r["indices"]).MapRange()
	for x.Next() {
		indexName := x.Key().String()
		docProperties, err1 := e.listIndexInfo(x.Key().String())
		if err1 != nil {
			err = err1
			return
		}
		var columns []*facets.Column
		for i := range docProperties {
			columns = append(columns, &facets.Column{
				Name:     i,
				DataType: docProperties[i].(map[string]interface{})["type"].(string),
			})
		}
		countRes, err1 := e.client.Search(
			e.client.Search.WithIndex(x.Key().String()),
		)
		if err1 != nil {
			err = err1
			return
		}
		var t map[string]interface{}
		err = json.NewDecoder(countRes.Body).Decode(&t)
		if err != nil {
			res.Body.Close()
			return
		}
		docCount := len(t["hits"].(map[string]interface{})["hits"].([]interface{}))

		emit(models.NewRecord(&assets.Table{
			Resource: &common.Resource{
				Urn:  fmt.Sprintf("%s.%s", "elasticsearch", indexName),
				Name: indexName,
			},
			Schema: &facets.Columns{
				Columns: columns,
			},
			Profile: &assets.TableProfile{
				TotalRows: int64(docCount),
			},
		}))
	}
	return
}

// listIndexInfo returns the properties of the index
func (e *Extractor) listIndexInfo(index string) (result map[string]interface{}, err error) {
	var r map[string]interface{}
	res, err := e.client.Indices.GetMapping(
		e.client.Indices.GetMapping.WithIndex(index),
	)
	if err != nil {
		err = errors.Wrap(err, "failed to retrieve index")
		return
	}
	err = json.NewDecoder(res.Body).Decode(&r)
	if err != nil {
		res.Body.Close()
		return
	}
	result = r[index].(map[string]interface{})["mappings"].(map[string]interface{})["properties"].(map[string]interface{})
	res.Body.Close()
	return
}

// init registers the extractor to catalog
func init() {
	if err := registry.Extractors.Register("elastic", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
