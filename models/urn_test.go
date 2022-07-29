package models_test

import (
	"fmt"
	"testing"

	"github.com/odpf/meteor/models"
	"github.com/stretchr/testify/assert"
)

func TestNewURN(t *testing.T) {
	testCases := []struct {
		service  string
		scope    string
		kind     string
		id       string
		expected string
	}{
		{
			"metabase", "main-dashboard", "collection", "123",
			"urn:metabase:main-dashboard:collection:123",
		},
		{
			"bigquery", "p-godata-id", "table", "p-godata-id:mydataset.mytable",
			"urn:bigquery:p-godata-id:table:p-godata-id:mydataset.mytable",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("should return expected urn (#%d)", i+1), func(t *testing.T) {
			actual := models.NewURN(tc.service, tc.scope, tc.kind, tc.id)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
