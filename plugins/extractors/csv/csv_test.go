//go:build plugins
// +build plugins

package csv_test

import (
	"context"
	"testing"

	"github.com/odpf/meteor/test/utils"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/odpf/meteor/models"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
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
			config)
		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should extract data if path is a file", func(t *testing.T) {
		ctx := context.TODO()
		extr := csv.New(utils.Logger)
		err := extr.Init(ctx, map[string]interface{}{
			"path": "./testdata/test.csv",
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)
		table, err := anypb.New(&v1beta2.Table{
			Columns: []*v1beta2.Column{
				{Name: "name"},
				{Name: "age"},
				{Name: "phone"},
			},
		})
		if err != nil {
			t.Fatal("error creating Any struct for test: %w", err)
		}
		expected := []models.Record{
			models.NewRecord(&v1beta2.Asset{
				Urn:     "test.csv",
				Name:    "test.csv",
				Service: "csv",
				Type:    "table",
				Data:    table,
			}),
		}

		assert.Equal(t, expected, emitter.Get())
	})

	t.Run("should extract data from all files if path is a dir", func(t *testing.T) {
		ctx := context.TODO()
		extr := csv.New(utils.Logger)
		err := extr.Init(ctx, map[string]interface{}{
			"path": "./testdata",
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)
		table1, err := anypb.New(&v1beta2.Table{
			Columns: []*v1beta2.Column{
				{Name: "order"},
				{Name: "transaction_id"},
				{Name: "total_price"},
			},
		})
		if err != nil {
			t.Fatal("error creating Any struct for test: %w", err)
		}
		table2, err := anypb.New(&v1beta2.Table{
			Columns: []*v1beta2.Column{
				{Name: "name"},
				{Name: "age"},
				{Name: "phone"},
			},
		})
		if err != nil {
			t.Fatal("error creating Any struct for test: %w", err)
		}
		expected := []models.Record{
			models.NewRecord(&v1beta2.Asset{
				Urn:     "test-2.csv",
				Name:    "test-2.csv",
				Service: "csv",
				Type:    "table",
				Data:    table1,
			}),
			models.NewRecord(&v1beta2.Asset{
				Urn:     "test.csv",
				Name:    "test.csv",
				Service: "csv",
				Type:    "table",
				Data:    table2,
			}),
		}
		assert.Equal(t, expected, emitter.Get())
	})
}
