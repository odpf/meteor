//go:build plugins
// +build plugins

package file_test

import (
	"context"
	_ "embed"
	"testing"

	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	f "github.com/odpf/meteor/plugins/sinks/file"
	testUtils "github.com/odpf/meteor/test/utils"
	"github.com/stretchr/testify/assert"
)

var validConfig = map[string]interface{}{
	"path":   "./test-dir/sample.ndjson",
	"format": "ndjson",
}

func TestValidate(t *testing.T) {
	t.Run("should return error on invalid config", func(t *testing.T) {
		invalidConfig := map[string]interface{}{}
		fileSink := f.New(testUtils.Logger)
		err := fileSink.Validate(plugins.Config{RawConfig: invalidConfig})
		assert.Error(t, err)
	})
}

func TestInit(t *testing.T) {
	t.Run("should return InvalidConfigError on invalid config", func(t *testing.T) {
		invalidConfig := map[string]interface{}{}
		fileSink := f.New(testUtils.Logger)
		err := fileSink.Init(context.TODO(), plugins.Config{RawConfig: invalidConfig})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
	t.Run("should return error if file is not found ", func(t *testing.T) {
		invalidConfig := map[string]interface{}{
			"path":   "./some-dir",
			"format": "ndjson",
		}
		fileSink := f.New(testUtils.Logger)
		err := fileSink.Init(context.TODO(), plugins.Config{RawConfig: invalidConfig})
		assert.Error(t, err)
	})
	t.Run("should return no error on valid config", func(t *testing.T) {
		fileSink := f.New(testUtils.Logger)
		err := fileSink.Init(context.TODO(), plugins.Config{RawConfig: validConfig})
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

func sinkInvalidPath(t *testing.T, config map[string]interface{}) error {
	fileSink := f.New(testUtils.Logger)
	return fileSink.Init(context.TODO(), plugins.Config{RawConfig: config})
}

func sinkValidSetup(t *testing.T, config map[string]interface{}) error {
	fileSink := f.New(testUtils.Logger)
	err := fileSink.Init(context.TODO(), plugins.Config{RawConfig: config})
	assert.NoError(t, err)
	err = fileSink.Sink(context.TODO(), getExpectedVal())
	assert.NoError(t, err)
	return fileSink.Close()
}

func getExpectedVal() []models.Record {
	return []models.Record{
		models.NewRecord(&assetsv1beta1.Table{
			Resource: &commonv1beta1.Resource{
				Urn:  "elasticsearch.index1",
				Name: "index1",
				Type: "table",
			},
			Schema: &facetsv1beta1.Columns{
				Columns: []*facetsv1beta1.Column{
					{
						Name:     "SomeInt",
						DataType: "long",
					},
					{
						Name:     "SomeStr",
						DataType: "text",
					},
				},
			},
			Profile: &assetsv1beta1.TableProfile{
				TotalRows: 1,
			},
		}),
		models.NewRecord(&assetsv1beta1.Table{
			Resource: &commonv1beta1.Resource{
				Urn:  "elasticsearch.index2",
				Name: "index2",
				Type: "table",
			},
			Schema: &facetsv1beta1.Columns{
				Columns: []*facetsv1beta1.Column{
					{
						Name:     "SomeInt",
						DataType: "long",
					},
					{
						Name:     "SomeStr",
						DataType: "text",
					},
				},
			},
			Profile: &assetsv1beta1.TableProfile{
				TotalRows: 1,
			},
		}),
	}
}
