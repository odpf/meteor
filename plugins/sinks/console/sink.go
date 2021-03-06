package console

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

type Sink struct {
	logger log.Logger
}

func New() plugins.Syncer {
	return new(Sink)
}

func (s *Sink) Info() plugins.Info {
	return plugins.Info{
		Description:  "Log to standard output",
		SampleConfig: "",
		Summary:      summary,
		Tags:         []string{"log", "sink"},
	}
}

func (s *Sink) Validate(configMap map[string]interface{}) (err error) {
	return nil
}

func (s *Sink) Init(ctx context.Context, config map[string]interface{}) (err error) {
	return
}

func (s *Sink) Sink(ctx context.Context, batch []models.Record) (err error) {
	for _, record := range batch {
		if err := s.process(record.Data()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sink) Close() (err error) { return }

func (s *Sink) process(value interface{}) error {
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return err
	}
	fmt.Println(string(jsonBytes))
	return nil
}

func init() {
	if err := registry.Sinks.Register("console", func() plugins.Syncer {
		return &Sink{
			logger: plugins.GetLog(),
		}
	}); err != nil {
		panic(err)
	}
}
