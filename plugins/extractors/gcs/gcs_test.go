//go:build plugins
// +build plugins

package gcs_test

import (
	"context"
	"testing"

	"github.com/odpf/meteor/test/utils"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/gcs"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	t.Run("should return error if no project_id in config", func(t *testing.T) {
		err := gcs.New(utils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: "test",
			RawConfig: map[string]interface{}{
				"wrong-config": "sample-project",
			},
		})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
}
