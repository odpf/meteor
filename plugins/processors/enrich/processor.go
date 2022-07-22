package enrich

import (
	"context"
	_ "embed"

	"github.com/odpf/meteor/models"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

// Processor work in a list of data
type Processor struct {
	config map[string]interface{}
	logger log.Logger
}

// New create a new processor
func New(logger log.Logger) *Processor {
	return &Processor{
		logger: logger,
	}
}

var sampleConfig = `
 # Enrichment configuration
 # fieldA: valueA
 # fieldB: valueB`

// Info returns the plugin information
func (p *Processor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Append custom fields to records",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"processor", "transform"},
	}
}

// Validate validates the plugin configuration
func (p *Processor) Validate(configMap map[string]interface{}) (err error) {
	return nil
}

// Process processes the data
func (p *Processor) Init(ctx context.Context, config map[string]interface{}) (err error) {
	p.config = config
	return
}

// Process processes the data
func (p *Processor) Process(ctx context.Context, src models.Record) (dst models.Record, err error) {
	result, err := p.process(src)
	if err != nil {
		return src, err
	}

	return models.NewRecord(result), nil
}

func (p *Processor) process(record models.Record) (*v1beta2.Asset, error) {
	data := record.Data()
	p.logger.Debug("enriching record", "record", data.Urn)
	customProps := utils.GetCustomProperties(data)

	// update custom properties using value from config
	for key, value := range p.config {
		stringVal, ok := value.(string)
		if ok {
			customProps[key] = stringVal
		}
	}

	// save custom properties
	result, err := utils.SetCustomProperties(data, customProps)
	if err != nil {
		return data, err
	}

	return result, nil
}

func init() {
	if err := registry.Processors.Register("enrich", func() plugins.Processor {
		return New(plugins.GetLog())
	}); err != nil {
		return
	}
}
