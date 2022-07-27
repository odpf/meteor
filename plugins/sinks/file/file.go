package file

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"strings"

	"github.com/odpf/meteor/models"
	assetsv1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	ndjson "github.com/scizorman/go-ndjson"
	"gopkg.in/yaml.v3"
)

//go:embed README.md
var summary string

type Config struct {
	Overwrite bool   `mapstructure:"overwrite" default:"true"`
	Path      string `mapstructure:"path" validate:"required"`
	Format    string `mapstructure:"format" validate:"required"`
}

var sampleConfig = `
path: ./output-filename.txt
format: ndjson
`

type Sink struct {
	logger log.Logger
	config Config
	format string
	File   *os.File
}

func New() plugins.Syncer {
	return new(Sink)
}

func (s *Sink) Info() plugins.Info {
	return plugins.Info{
		Description:  "save output to a file",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"file", "json", "yaml", "sink"},
	}
}

func (s *Sink) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

func (s *Sink) Init(ctx context.Context, config map[string]interface{}) (err error) {
	if err := utils.BuildConfig(config, &s.config); err != nil {
		return plugins.InvalidConfigError{Type: "sink", PluginName: "file"}
	}

	if err := s.validateFilePath(s.config.Path); err != nil {
		return err
	}

	s.format = s.config.Format
	if s.config.Overwrite {
		s.File, err = os.Create(s.config.Path)
		return err
	}
	s.File, err = os.OpenFile(s.config.Path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		return err
	}
	return
}

func (s *Sink) Sink(ctx context.Context, batch []models.Record) (err error) {
	var data []*assetsv1beta2.Asset
	for _, record := range batch {
		data = append(data, record.Data())
	}
	if s.format == "ndjson" {
		err := s.ndjsonOut(data)
		if err != nil {
			return err
		}
		return nil
	}
	err = s.yamlOut(data)
	if err != nil {
		return err
	}
	return nil
}

func (s *Sink) Close() (err error) {
	// return s.File.Close()
	return nil
}

func (s *Sink) ndjsonOut(data []*assetsv1beta2.Asset) error {
	jsnBy, err := ndjson.Marshal(data)
	if err != nil {
		return err
	}
	err = s.writeBytes(jsnBy)
	return err
}

func (s *Sink) yamlOut(data []*assetsv1beta2.Asset) error {
	ymlByte, err := yaml.Marshal(data)
	if err != nil {
		return err
	}
	err = s.writeBytes(ymlByte)
	return err
}

func (s *Sink) writeBytes(b []byte) error {
	_, err := s.File.Write(b)
	if err != nil {
		return err
	}
	return nil
}

func (s *Sink) validateFilePath(path string) error {
	dirs := strings.Split(path, "/")
	filename := dirs[len(dirs)-1]
	format := strings.Split(filename, ".")
	if len(format) != 2 {
		return fmt.Errorf("invalid filename")
	}
	return nil
}

func init() {
	if err := registry.Sinks.Register("file", func() plugins.Syncer {
		return &Sink{
			logger: plugins.GetLog(),
		}
	}); err != nil {
		panic(err)
	}
}
