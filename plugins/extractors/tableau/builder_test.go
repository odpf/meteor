//go:build plugins
// +build plugins

package tableau

import (
	"testing"

	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	testutils "github.com/odpf/meteor/test/utils"
	"github.com/stretchr/testify/assert"
)

func TestBuildLineageResource(t *testing.T) {
	t.Run("building bigquery DatabaseServer resource from interface", func(t *testing.T) {
		table := &Table{
			ID:       "id_table_1",
			Name:     "table1",
			FullName: "[database_server.access_data].[table1]",
			Schema:   "access_data",
			Database: Database{
				"id":             "db_server",
				"name":           "database_server",
				"connectionType": "bigquery",
				"hostName":       "",
				"port":           -1,
				"service":        "",
			},
		}
		e := New(testutils.Logger)
		res, err := e.buildLineageResources(table)

		expectedResource := &commonv1beta1.Resource{
			Urn:     "urn:bigquery:database_server:table:database_server:access_data.table1",
			Type:    "table",
			Service: table.Database["connectionType"].(string),
		}

		assert.Nil(t, err)
		assert.Equal(t, expectedResource, res)
	})

	t.Run("building other DatabaseServer resource from interface", func(t *testing.T) {
		table := &Table{
			ID:     "id_table_1",
			Name:   "table1",
			Schema: "",
			Database: Database{
				"id":             "db_server",
				"name":           "database_server",
				"connectionType": "postgres",
				"hostName":       "localhost",
				"port":           5432,
				"service":        "service",
			},
		}
		e := New(testutils.Logger)
		res, err := e.buildLineageResources(table)

		expectedResource := &commonv1beta1.Resource{
			Urn:     "urn:postgres:localhost:5432:table:database_server.table1",
			Type:    "table",
			Service: table.Database["connectionType"].(string),
		}

		assert.Nil(t, err)
		assert.Equal(t, expectedResource, res)
	})

	t.Run("building CloudFile resource from interface", func(t *testing.T) {
		table := &Table{
			ID:     "id_table",
			Name:   "table_name",
			Schema: "",
			Database: Database{
				"id":             "db_cloud_file",
				"name":           "database_cloud_file",
				"connectionType": "gcs",
				"provider":       "gcs",
			},
		}

		e := New(testutils.Logger)
		res, err := e.buildLineageResources(table)

		expectedResource := &commonv1beta1.Resource{
			Urn:     "urn:gcs:gcs:bucket:database_cloud_file/table_name",
			Type:    "bucket",
			Service: table.Database["connectionType"].(string),
		}

		assert.Nil(t, err)
		assert.Equal(t, expectedResource, res)
	})

	t.Run("building File resource from interface", func(t *testing.T) {
		table := &Table{
			ID:     "table_id",
			Name:   "table_name",
			Schema: "schema",
			Database: Database{
				"id":             "db_file",
				"name":           "database_file",
				"connectionType": "file",
				"filePath":       "/this/is/file",
			},
		}

		e := New(testutils.Logger)
		res, err := e.buildLineageResources(table)

		expectedResource := &commonv1beta1.Resource{
			Urn:     "urn:file:/this/is/file:bucket:database_file.table_name",
			Type:    "bucket",
			Service: table.Database["connectionType"].(string),
		}

		assert.Nil(t, err)
		assert.Equal(t, expectedResource, res)
	})

	t.Run("building WebDataConnector resource from interface", func(t *testing.T) {
		table := &Table{
			ID:     "table_id",
			Name:   "table_name",
			Schema: "schema",
			Database: Database{
				"id":             "db_wdc",
				"name":           "database_wdc",
				"connectionType": "web_data_connector",
				"connectorUrl":   "http://link_to_connector",
			},
		}

		e := New(testutils.Logger)
		res, err := e.buildLineageResources(table)

		expectedResource := &commonv1beta1.Resource{
			Urn:     "urn:web_data_connector:http://link_to_connector:table:database_wdc.table_name",
			Type:    "table",
			Service: table.Database["connectionType"].(string),
		}

		assert.Nil(t, err)
		assert.Equal(t, expectedResource, res)
	})

	t.Run("building Unknown resource from interface", func(t *testing.T) {
		table := &Table{
			Name: "table_name",
			Database: Database{
				"id":             "an_id",
				"name":           "a_name",
				"connectionType": "conn_type",
			},
		}

		e := New(testutils.Logger)
		res, err := e.buildLineageResources(table)
		assert.EqualError(t, err, "cannot build lineage resource, database structure unknown")
		assert.Nil(t, res)

	})
}

func TestBuildLineage(t *testing.T) {
	upstreamTables := []*Table{
		{
			ID:   "table_id_1",
			Name: "table_name_1",
			Database: Database{
				"id":             "db_1",
				"name":           "database_1",
				"connectionType": "postgres",
				"hostName":       "localhost",
				"port":           5432,
			},
		},
		{
			ID:   "table_id_2",
			Name: "table_name_2",
			Database: Database{
				"id":   "db_2",
				"name": "database_2",

				"connectionType": "gcs",
				"provider":       "gcs",
			},
		},
	}

	testDataWorkbook := Workbook{
		UpstreamTables: upstreamTables,
	}

	expectedLineage := &facetsv1beta1.Lineage{
		Upstreams: []*commonv1beta1.Resource{
			{
				Urn:     "urn:postgres:localhost:5432:table:database_1.table_name_1",
				Type:    "table",
				Service: upstreamTables[0].Database["connectionType"].(string),
			},
			{
				Urn:     "urn:gcs:gcs:bucket:database_2/table_name_2",
				Type:    "bucket",
				Service: upstreamTables[1].Database["connectionType"].(string),
			},
		},
	}

	e := New(testutils.Logger)
	assert.Equal(t, expectedLineage, e.buildLineage(testDataWorkbook.UpstreamTables))
}
