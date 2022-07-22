package file_test

import (
	"context"
	_ "embed"
	"testing"

	"github.com/odpf/meteor/models"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	f "github.com/odpf/meteor/plugins/sinks/file"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/anypb"
)

//go:embed README.md
var summary string

var validConfig = map[string]interface{}{
	"path":   "./test-dir/sample.ndjson",
	"format": "ndjson",
}

func TestValidate(t *testing.T) {
	t.Run("should return error on invalid config", func(t *testing.T) {
		invalidConfig := map[string]interface{}{}
		fileSink := f.New()
		err := fileSink.Validate(invalidConfig)
		assert.Error(t, err)
	})
}

func TestInit(t *testing.T) {
	t.Run("should return InvalidConfigError on invalid config", func(t *testing.T) {
		invalidConfig := map[string]interface{}{}
		fileSink := f.New()
		err := fileSink.Init(context.TODO(), invalidConfig)
		assert.Equal(t, plugins.InvalidConfigError{Type: "sink", PluginName: "file"}, err)
	})
	t.Run("should return error on filename missing", func(t *testing.T) {
		invalidConfig := map[string]interface{}{
			"path":   "./some-dir",
			"format": "ndjson",
		}
		fileSink := f.New()
		err := fileSink.Init(context.TODO(), invalidConfig)
		assert.Error(t, err)
	})
	t.Run("should return no error on valid config", func(t *testing.T) {
		fileSink := f.New()
		err := fileSink.Init(context.TODO(), validConfig)
		assert.NoError(t, err)
	})
}

func TestMain(t *testing.T) {
	t.Run("should return no error with for valid ndjson config", func(t *testing.T) {
		assert.NoError(t, sinkValidSetup(t, validConfig))
	})
	t.Run("should return no error with for valid yaml config", func(t *testing.T) {
		config := map[string]interface{}{
			"path":   "./test-dir/sample.yaml",
			"format": "yaml",
		}
		assert.NoError(t, sinkValidSetup(t, config))

	})
	t.Run("should return error for invalid directory in yaml", func(t *testing.T) {
		config := map[string]interface{}{
			"path":   "./test-dir/some-dir/sample.yaml",
			"format": "yaml",
		}
		assert.Error(t, sinkInvalidPath(t, config))
	})
	t.Run("should return error for invalid directory in ndjson", func(t *testing.T) {
		config := map[string]interface{}{
			"path":   "./test-dir/some-dir/sample.ndjson",
			"format": "ndjson",
		}
		assert.Error(t, sinkInvalidPath(t, config))
	})
}

func TestInfo(t *testing.T) {
	info := f.New().Info()
	assert.Equal(t, summary, info.Summary)
}

func sinkInvalidPath(t *testing.T, config map[string]interface{}) error {
	fileSink := f.New()
	return fileSink.Init(context.TODO(), config)
}

func sinkValidSetup(t *testing.T, config map[string]interface{}) error {
	fileSink := f.New()
	err := fileSink.Init(context.TODO(), config)
	assert.NoError(t, err)
	err = fileSink.Sink(context.TODO(), getExpectedVal(t))
	assert.NoError(t, err)
	return fileSink.Close()
}

func getExpectedVal(t *testing.T) []models.Record {
	table1, err := anypb.New(&v1beta2.Table{
		Columns: []*v1beta2.Column{
			{
				Name:     "SomeStr",
				DataType: "text",
			},
		},
		Profile: &v1beta2.TableProfile{
			TotalRows: 1,
		},
	})
	if err != nil {
		t.Fatal("error creating Any struct for test: %w", err)
	}
	table2, err := anypb.New(&v1beta2.Table{
		Columns: []*v1beta2.Column{
			{
				Name:     "SomeStr",
				DataType: "text",
			},
		},
		Profile: &v1beta2.TableProfile{
			TotalRows: 1,
		},
	})
	if err != nil {
		t.Fatal("error creating Any struct for test: %w", err)
	}
	return []models.Record{
		models.NewRecord(&v1beta2.Asset{
			Urn:  "elasticsearch.index1",
			Name: "index1",
			Type: "table",
			Data: table1,
		}),
		models.NewRecord(&v1beta2.Asset{
			Urn:  "elasticsearch.index2",
			Name: "index2",
			Type: "table",
			Data: table2,
		}),
	}
}
