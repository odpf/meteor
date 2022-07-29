package plugins

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/mcuadros/go-defaults"
	"github.com/mitchellh/mapstructure"
	"github.com/odpf/meteor/models"
)

var validate *validator.Validate

func init() {
	validate = validator.New()
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		configName := strings.SplitN(fld.Tag.Get("mapstructure"), ",", 2)[0]

		if configName == "-" {
			return ""
		}
		return configName
	})
}

// BuildConfig builds a config struct from a map
func buildConfig(configMap map[string]interface{}, c interface{}) (err error) {
	defaults.SetDefaults(c)

	if err = mapstructure.Decode(configMap, c); err != nil {
		return err
	}
	if err = validate.Struct(c); err == nil {
		return nil
	}

	var validationErr validator.ValidationErrors
	if errors.As(err, &validationErr) {
		var configErrors []ConfigError
		for _, fieldErr := range validationErr {
			key := strings.TrimPrefix(fieldErr.Namespace(), "Config.")
			configErrors = append(configErrors, ConfigError{
				Key:     key,
				Message: fieldErr.Error(),
			})
		}
		return InvalidConfigError{
			Errors: configErrors,
		}
	}

	return err
}

func BigQueryURN(projectID, datasetID, tableID string) string {
	fqn := fmt.Sprintf("%s:%s.%s", projectID, datasetID, tableID)
	return models.NewURN("bigquery", projectID, "table", fqn)
}
