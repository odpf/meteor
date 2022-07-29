package plugins_test

import (
	"testing"

	"github.com/odpf/meteor/plugins"
	"github.com/stretchr/testify/assert"
)

func TestNewBasePlugin(t *testing.T) {
	t.Run("should assign info and return base plugin", func(t *testing.T) {
		info := plugins.Info{
			Description:  "test-description",
			SampleConfig: "sample-config",
			Summary:      "test-summary",
			Tags:         []string{"test", "plugin"},
		}
		actual := plugins.NewBasePlugin(info, nil)

		assert.Equal(t, info, actual.Info())
	})
}

func TestBasePluginInfo(t *testing.T) {
	t.Run("should return info", func(t *testing.T) {
		info := plugins.Info{
			Description:  "test-description",
			SampleConfig: "sample-config",
			Summary:      "test-summary",
			Tags:         []string{"test", "plugin"},
		}
		basePlugin := plugins.NewBasePlugin(info, nil)
		actual := basePlugin.Info()

		assert.Equal(t, info, actual)
	})
}

func TestBasePluginValidate(t *testing.T) {
	t.Run("should return InvalidConfigError if config is invalid", func(t *testing.T) {
		invalidConfig := struct {
			FieldA string `validate:"required"`
		}{}

		basePlugin := plugins.NewBasePlugin(plugins.Info{}, &invalidConfig)
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

		basePlugin := plugins.NewBasePlugin(plugins.Info{}, &validConfig)
		err := basePlugin.Validate(plugins.Config{
			URNScope: "test-scope",
			RawConfig: map[string]interface{}{
				"FieldA": "test-value",
			},
		})

		assert.NoError(t, err)
	})
}

func TestBasePluginInit(t *testing.T) {
	t.Run("should return InvalidConfigError if config is invalid", func(t *testing.T) {
		invalidConfig := struct {
			FieldA string `validate:"required"`
		}{}

		basePlugin := plugins.NewBasePlugin(plugins.Info{}, &invalidConfig)
		err := basePlugin.Validate(plugins.Config{
			URNScope:  "test-scope",
			RawConfig: map[string]interface{}{},
		})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should return populate config and return no error if config is valid", func(t *testing.T) {
		validConfig := struct {
			FieldA string `validate:"required"`
		}{}

		basePlugin := plugins.NewBasePlugin(plugins.Info{}, &validConfig)
		err := basePlugin.Validate(plugins.Config{
			URNScope: "test-scope",
			RawConfig: map[string]interface{}{
				"FieldA": "test-value",
			},
		})

		assert.NoError(t, err)
		assert.Equal(t, "test-value", validConfig.FieldA)
	})
}
