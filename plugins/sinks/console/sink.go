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

var info = plugins.Info{
	Description:  "Log to standard output",
	SampleConfig: "",
	Summary:      summary,
	Tags:         []string{"log", "sink"},
}

type Sink struct {
	plugins.BasePlugin
	logger log.Logger
}

func New(logger log.Logger) plugins.Syncer {
	s := &Sink{
		logger: logger,
	}
	s.BasePlugin = plugins.NewBasePlugin(info, nil)

	return s
}

func (s *Sink) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = s.BasePlugin.Init(ctx, config); err != nil {
		return err
	}
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
