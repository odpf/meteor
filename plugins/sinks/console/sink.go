package console

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
)

type Sink struct {
	logger log.Logger
}

func New() plugins.Syncer {
	return new(Sink)
}

func (s *Sink) ValidateConfig(configMap map[string]interface{}) (err error) {
	return
}

func (s *Sink) Sink(ctx context.Context, config map[string]interface{}, out <-chan interface{}) (err error) {
	for val := range out {
		if err := s.process(val); err != nil {
			return err
		}
	}
	return nil
}

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
	}, ""); err != nil {
		panic(err)
	}
}
