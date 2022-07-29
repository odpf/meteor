package plugins_test

import (
	"testing"

	"github.com/odpf/meteor/plugins"
	"github.com/stretchr/testify/assert"
)

func TestNewBaseExtractor(t *testing.T) {
	t.Run("should assign info and return base plugin", func(t *testing.T) {
		info := plugins.Info{
			Description:  "test-description",
			SampleConfig: "sample-config",
			Summary:      "test-summary",
			Tags:         []string{"test", "plugin"},
		}
		actual := plugins.NewBaseExtractor(info, nil)

		assert.Equal(t, info, actual.Info())
	})
}

func TestBaseExtractorValidate(t *testing.T) {
	t.Run("should return ErrEmptyURNScope if Config.URNScope is empty", func(t *testing.T) {
		basePlugin := plugins.NewBaseExtractor(plugins.Info{}, nil)
		err := basePlugin.Validate(plugins.Config{URNScope: ""})

		assert.ErrorIs(t, err, plugins.ErrEmptyURNScope)
	})

	t.Run("should return InvalidConfigError if config is invalid", func(t *testing.T) {
		invalidConfig := struct {
			FieldA string `validate:"required"`
		}{}

		basePlugin := plugins.NewBaseExtractor(plugins.Info{}, &invalidConfig)
		err := basePlugin.Validate(plugins.Config{
			URNScope:  "test-scope",
			RawConfig: map[string]interface{}{},
		})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should return no error if config is valid", func(t *testing.T) {
		validConfig := struct {
			FieldA string `validate:"required"`
		}{}

		basePlugin := plugins.NewBaseExtractor(plugins.Info{}, &validConfig)
		err := basePlugin.Validate(plugins.Config{
			URNScope: "test-scope",
			RawConfig: map[string]interface{}{
				"FieldA": "test-value",
			},
		})

		assert.NoError(t, err)
	})
}
