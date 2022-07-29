package plugins_test

import (
	"testing"

	"github.com/odpf/meteor/plugins"
	"github.com/stretchr/testify/assert"
)

func TestBigQueryURN(t *testing.T) {
	t.Run("should create bigquery URN", func(t *testing.T) {
		project := "my-project"
		dataset := "my-dataset"
		table := "my-table"

		actual := plugins.BigQueryURN(project, dataset, table)
		expected := "urn:bigquery:my-project:table:my-project:my-dataset.my-table"

		assert.Equal(t, expected, actual)
	})
}
