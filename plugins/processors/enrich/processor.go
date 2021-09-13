package enrich

import (
	"context"
	//
	_ "embed"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
)

//go:embed README.md
var summary string

// Processor work in a list of data
type Processor struct{}

// New create a new processor
func New() *Processor {
	return new(Processor)
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
		Tags:         []string{"processor"},
	}
}

// Validate validates the plugin configuration
func (p *Processor) Validate(configMap map[string]interface{}) (err error) {
	return nil
}

// Process processes the data
func (p *Processor) Process(ctx context.Context, config map[string]interface{}, in <-chan models.Record, out chan<- models.Record) (err error) {
	for record := range in {
		result, err := p.process(record.Data(), config)
		if err != nil {
			return err
		}

		out <- models.NewRecord(result)
	}

	return
}

func (p *Processor) process(data interface{}, config map[string]interface{}) (interface{}, error) {
	customProps := utils.GetCustomProperties(data)
	if customProps == nil {
		return data, nil
	}

	// update custom properties using value from config
	for key, value := range config {
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
		return New()
	}); err != nil {
		return
	}
}
