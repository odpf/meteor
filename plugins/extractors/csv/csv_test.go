//go:build plugins
// +build plugins

package csv_test

import (
	"context"
	"testing"

	"github.com/odpf/meteor/test/utils"

	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/csv"
	"github.com/odpf/meteor/test/mocks"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	t.Run("should return error if fileName and directory both are empty", func(t *testing.T) {
		config := map[string]interface{}{}
		err := csv.New(utils.Logger).Init(
			context.TODO(),
			plugins.Config{
				URNScope:  "test-csv",
				RawConfig: config,
			})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
}

func TestExtract(t *testing.T) {
	t.Run("should extract data if path is a file", func(t *testing.T) {
		ctx := context.TODO()
		extr := csv.New(utils.Logger)
		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-csv",
			RawConfig: map[string]interface{}{
				"path": "./testdata/test.csv",
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		expected := []models.Record{
			models.NewRecord(&assetsv1beta1.Table{
				Resource: &commonv1beta1.Resource{
					Urn:     "urn:csv:test-csv:file:test.csv",
					Name:    "test.csv",
					Service: "csv",
					Type:    "table",
				},
				Schema: &facetsv1beta1.Columns{
					Columns: []*facetsv1beta1.Column{
						{Name: "name"},
						{Name: "age"},
						{Name: "phone"},
					},
				},
			}),
		}

		assert.Equal(t, expected, emitter.Get())
	})

	t.Run("should extract data from all files if path is a dir", func(t *testing.T) {
		ctx := context.TODO()
		extr := csv.New(utils.Logger)
		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-csv",
			RawConfig: map[string]interface{}{
				"path": "./testdata",
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		expected := []models.Record{
			models.NewRecord(&assetsv1beta1.Table{
				Resource: &commonv1beta1.Resource{
					Urn:     "urn:csv:test-csv:file:test-2.csv",
					Name:    "test-2.csv",
					Service: "csv",
					Type:    "table",
				},
				Schema: &facetsv1beta1.Columns{
					Columns: []*facetsv1beta1.Column{
						{Name: "order"},
						{Name: "transaction_id"},
						{Name: "total_price"},
					},
				},
			}),
			models.NewRecord(&assetsv1beta1.Table{
				Resource: &commonv1beta1.Resource{
					Urn:     "urn:csv:test-csv:file:test.csv",
					Name:    "test.csv",
					Service: "csv",
					Type:    "table",
				},
				Schema: &facetsv1beta1.Columns{
					Columns: []*facetsv1beta1.Column{
						{Name: "name"},
						{Name: "age"},
						{Name: "phone"},
					},
				},
			}),
		}
		assert.Equal(t, expected, emitter.Get())
	})
}
