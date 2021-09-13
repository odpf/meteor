//+build integration

package grafana_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/grafana"
	"github.com/odpf/meteor/test"
	"github.com/stretchr/testify/assert"
)

var testServer *httptest.Server

func TestMain(m *testing.M) {
	testServer = NewTestServer()

	// run tests
	code := m.Run()

	testServer.Close()
	os.Exit(code)
}

func TestExtract(t *testing.T) {
	t.Run("should return error if for empty base_url in config", func(t *testing.T) {
		err := grafana.New(test.Logger).Extract(context.TODO(), map[string]interface{}{
			"base_url": "",
			"api_key":  "qwerty123",
		}, make(chan interface{}))

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should return error if for empty api_key in config", func(t *testing.T) {
		err := grafana.New(test.Logger).Extract(context.TODO(), map[string]interface{}{
			"base_url": testServer.URL,
			"api_key":  "",
		}, make(chan interface{}))

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should extract grafana metadata into meta dashboard", func(t *testing.T) {
		extractor := grafana.New(test.Logger)

		expectedData := []assets.Dashboard{
			{
				Resource: &common.Resource{
					Urn:     "grafana.HzK8qNW7z",
					Name:    "new-dashboard-copy",
					Service: "grafana",
					Url:     fmt.Sprintf("%s/d/HzK8qNW7z/new-dashboard-copy", testServer.URL),
				},
				Description: "",
				Charts: []*assets.Chart{
					{
						Urn:             "HzK8qNW7z.2",
						Name:            "Panel Title",
						Type:            "timeseries",
						Source:          "grafana",
						Description:     "",
						Url:             fmt.Sprintf("%s/d/HzK8qNW7z/new-dashboard-copy?viewPanel=2", testServer.URL),
						DataSource:      "",
						RawQuery:        "",
						DashboardUrn:    "grafana.HzK8qNW7z",
						DashboardSource: "grafana",
					},
				},
			},
			{
				Resource: &common.Resource{
					Urn:     "grafana.5WsKOvW7z",
					Name:    "test-dashboard-updated",
					Service: "grafana",
					Url:     fmt.Sprintf("%s/d/5WsKOvW7z/test-dashboard-updated", testServer.URL),
				},
				Description: "this is description for testing",
				Charts: []*assets.Chart{
					{
						Urn:             "5WsKOvW7z.4",
						Name:            "Panel Random",
						Type:            "table",
						Source:          "grafana",
						Description:     "random description for this panel",
						Url:             fmt.Sprintf("%s/d/5WsKOvW7z/test-dashboard-updated?viewPanel=4", testServer.URL),
						DataSource:      "postgres",
						RawQuery:        "SELECT\n  urn,\n  created_at AS \"time\"\nFROM resources\nORDER BY 1",
						DashboardUrn:    "grafana.5WsKOvW7z",
						DashboardSource: "grafana",
					},
					{
						Urn:             "5WsKOvW7z.2",
						Name:            "Panel Title",
						Type:            "timeseries",
						Source:          "grafana",
						Description:     "",
						Url:             fmt.Sprintf("%s/d/5WsKOvW7z/test-dashboard-updated?viewPanel=2", testServer.URL),
						DataSource:      "",
						RawQuery:        "",
						DashboardUrn:    "grafana.5WsKOvW7z",
						DashboardSource: "grafana",
					},
				},
			},
		}

		out := make(chan interface{})
		go func() {
			err := extractor.Extract(context.TODO(), map[string]interface{}{
				"base_url": testServer.URL,
				"api_key":  "qwerty123",
			}, out)
			close(out)

			assert.NoError(t, err)
		}()

		var actualData []assets.Dashboard
		for d := range out {
			dashboard, ok := d.(assets.Dashboard)
			if !ok {
				t.Fatal(errors.New("invalid metadata format"))
			}

			actualData = append(actualData, dashboard)
		}

		assert.EqualValues(t, expectedData, actualData)
	})
}

func NewTestServer() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/search", func(res http.ResponseWriter, req *http.Request) {
		res.Write([]byte(`[
		  {
			"id": 1,
			"uid": "HzK8qNW7z",
			"title": "New dashboard Copy",
			"uri": "db/new-dashboard-copy",
			"url": "/d/HzK8qNW7z/new-dashboard-copy",
			"slug": "",
			"type": "dash-db",
			"tags": [],
			"isStarred": false,
			"sortMeta": 0
		  },
		  {
			"id": 3,
			"uid": "5WsKOvW7z",
			"title": "Test dashboard updated",
			"uri": "db/test-dashboard-updated",
			"url": "/d/5WsKOvW7z/test-dashboard-updated",
			"slug": "",
			"type": "dash-db",
			"tags": [
			  "dev",
			  "test"
			],
			"isStarred": false,
			"folderId": 2,
			"folderUid": "cR0NOvW7k",
			"folderTitle": "testing-testing",
			"folderUrl": "/dashboards/f/cR0NOvW7k/testing-testing",
			"sortMeta": 0
		  }
		]`,
		),
		)
	})
	mux.HandleFunc("/api/datasources", func(res http.ResponseWriter, req *http.Request) {
		res.Write([]byte(`[
				{
				  "id": 1,
				  "uid": "Dkd9hvWnz",
				  "orgId": 1,
				  "name": "PostgreSQL",
				  "type": "postgres",
				  "typeName": "PostgreSQL",
				  "typeLogoUrl": "public/app/plugins/datasource/postgres/img/postgresql_logo.svg",
				  "access": "proxy",
				  "url": "host.docker.internal:5432",
				  "password": "",
				  "user": "meteor",
				  "database": "random",
				  "basicAuth": false,
				  "isDefault": true,
				  "jsonData": {
					"postgresVersion": 903,
					"sslmode": "disable",
					"tlsAuth": false,
					"tlsAuthWithCACert": false,
					"tlsConfigurationMethod": "file-path",
					"tlsSkipVerify": true
				  },
				  "readOnly": false
				},
				{
				  "id": 2,
				  "uid": "Pa4J0vZnk",
				  "orgId": 1,
				  "name": "PostgreSQL-1",
				  "type": "postgres",
				  "typeName": "PostgreSQL",
				  "typeLogoUrl": "public/app/plugins/datasource/postgres/img/postgresql_logo.svg",
				  "access": "proxy",
				  "url": "host.docker.internal:5432",
				  "password": "",
				  "user": "meteor",
				  "database": "random",
				  "basicAuth": false,
				  "isDefault": false,
				  "jsonData": {
					"postgresVersion": 903,
					"sslmode": "disable",
					"tlsAuth": false,
					"tlsAuthWithCACert": false,
					"tlsConfigurationMethod": "file-path",
					"tlsSkipVerify": true
				  },
				  "readOnly": false
				}
			]`,
		))
	})
	mux.HandleFunc("/api/dashboards/uid/5WsKOvW7z", func(res http.ResponseWriter, req *http.Request) {
		res.Write([]byte(`{
			"meta": {
			  "type": "db",
			  "canSave": true,
			  "canEdit": true,
			  "canAdmin": true,
			  "canStar": true,
			  "slug": "test-dashboard-updated",
			  "url": "/d/5WsKOvW7z/test-dashboard-updated",
			  "expires": "0001-01-01T00:00:00Z",
			  "created": "2021-07-22T07:32:57Z",
			  "updated": "2021-07-23T04:36:20Z",
			  "updatedBy": "admin",
			  "createdBy": "admin",
			  "version": 12,
			  "hasAcl": false,
			  "isFolder": false,
			  "folderId": 2,
			  "folderUid": "cR0NOvW7k",
			  "folderTitle": "testing-testing",
			  "folderUrl": "/dashboards/f/cR0NOvW7k/testing-testing",
			  "provisioned": false,
			  "provisionedExternalId": ""
			},
			"dashboard": {
			  "annotations": {
			  },
			  "description": "this is description for testing",
			  "editable": true,
			  "gnetId": null,
			  "graphTooltip": 0,
			  "id": 3,
			  "links": [],
			  "panels": [
				{
				  "datasource": "PostgreSQL-1",
				  "description": "random description for this panel",
				  "fieldConfig": {
					"defaults": {
					  "color": {
						"mode": "thresholds"
					  },
					  "custom": {
						"align": "auto",
						"displayMode": "auto"
					  },
					  "mappings": [],
					  "thresholds": {
						"mode": "absolute",
						"steps": [
						  {
							"color": "green",
							"value": null
						  },
						  {
							"color": "red",
							"value": 80
						  }
						]
					  }
					},
					"overrides": []
				  },
				  "gridPos": {
					"h": 8,
					"w": 12,
					"x": 0,
					"y": 0
				  },
				  "id": 4,
				  "options": {
					"showHeader": true
				  },
				  "pluginVersion": "8.0.6",
				  "targets": [
					{
					  "format": "table",
					  "group": [],
					  "hide": false,
					  "metricColumn": "none",
					  "rawQuery": true,
					  "rawSql": "SELECT\n  urn,\n  created_at AS \"time\"\nFROM resources\nORDER BY 1",
					  "refId": "A",
					  "select": [
						[
						  {
							"params": [
							  "id"
							],
							"type": "column"
						  }
						]
					  ],
					  "table": "resources",
					  "timeColumn": "created_at",
					  "timeColumnType": "timestamptz",
					  "where": []
					}
				  ],
				  "title": "Panel Random",
				  "type": "table"
				},
				{
				  "datasource": null,
				  "description": "",
				  "fieldConfig": {
					"defaults": {
					  "color": {
						"mode": "palette-classic"
					  },
					  "custom": {
						"axisLabel": "",
						"axisPlacement": "auto",
						"barAlignment": 0,
						"drawStyle": "line",
						"fillOpacity": 0,
						"gradientMode": "none",
						"hideFrom": {
						  "legend": false,
						  "tooltip": false,
						  "viz": false
						},
						"lineInterpolation": "linear",
						"lineWidth": 1,
						"pointSize": 5,
						"scaleDistribution": {
						  "type": "linear"
						},
						"showPoints": "auto",
						"spanNulls": false,
						"stacking": {
						  "group": "A",
						  "mode": "none"
						},
						"thresholdsStyle": {
						  "mode": "off"
						}
					  },
					  "mappings": [],
					  "thresholds": {
						"mode": "absolute",
						"steps": [
						  {
							"color": "green",
							"value": null
						  },
						  {
							"color": "red",
							"value": 80
						  }
						]
					  }
					},
					"overrides": []
				  },
				  "gridPos": {
					"h": 9,
					"w": 12,
					"x": 0,
					"y": 8
				  },
				  "id": 2,
				  "options": {
					"legend": {
					  "calcs": [],
					  "displayMode": "list",
					  "placement": "bottom"
					},
					"tooltip": {
					  "mode": "single"
					}
				  },
				  "targets": [
					{
					  "queryType": "randomWalk",
					  "refId": "A"
					}
				  ],
				  "title": "Panel Title",
				  "type": "timeseries"
				}
			  ],
			  "refresh": "",
			  "schemaVersion": 30,
			  "style": "dark",
			  "tags": [
				"test",
				"dev"
			  ],
			  "templating": {
				"list": []
			  },
			  "time": {
				"from": "now-6h",
				"to": "now"
			  },
			  "timepicker": {},
			  "timezone": "",
			  "title": "Test dashboard updated",
			  "uid": "5WsKOvW7z",
			  "version": 12
			}
		  }`,
		))
	})
	mux.HandleFunc("/api/dashboards/uid/HzK8qNW7z", func(res http.ResponseWriter, req *http.Request) {
		res.Write([]byte(`{
			"meta": {
			  "type": "db",
			  "canSave": true,
			  "canEdit": true,
			  "canAdmin": true,
			  "canStar": true,
			  "slug": "new-dashboard-copy",
			  "url": "/d/HzK8qNW7z/new-dashboard-copy",
			  "expires": "0001-01-01T00:00:00Z",
			  "created": "2021-07-22T04:10:07Z",
			  "updated": "2021-07-22T04:10:07Z",
			  "updatedBy": "admin",
			  "createdBy": "admin",
			  "version": 1,
			  "hasAcl": false,
			  "isFolder": false,
			  "folderId": 0,
			  "folderUid": "",
			  "folderTitle": "General",
			  "folderUrl": "",
			  "provisioned": false,
			  "provisionedExternalId": ""
			},
			"dashboard": {
			  "annotations": {
				"list": [
				  {
					"builtIn": 1,
					"datasource": "-- Grafana --",
					"enable": true,
					"hide": true,
					"iconColor": "rgba(0, 211, 255, 1)",
					"name": "Annotations \u0026 Alerts",
					"type": "dashboard"
				  }
				]
			  },
			  "editable": true,
			  "gnetId": null,
			  "graphTooltip": 0,
			  "hideControls": false,
			  "id": 1,
			  "links": [],
			  "panels": [
				{
				  "datasource": "-- Grafana --",
				  "fieldConfig": {
					"defaults": {
					  "color": {
						"mode": "palette-classic"
					  },
					  "custom": {
						"axisLabel": "",
						"axisPlacement": "auto",
						"barAlignment": 0,
						"drawStyle": "line",
						"fillOpacity": 0,
						"gradientMode": "none",
						"hideFrom": {
						  "legend": false,
						  "tooltip": false,
						  "viz": false
						},
						"lineInterpolation": "linear",
						"lineWidth": 1,
						"pointSize": 5,
						"scaleDistribution": {
						  "type": "linear"
						},
						"showPoints": "auto",
						"spanNulls": false,
						"stacking": {
						  "group": "A",
						  "mode": "none"
						},
						"thresholdsStyle": {
						  "mode": "off"
						}
					  },
					  "mappings": [],
					  "thresholds": {
						"mode": "absolute",
						"steps": [
						  {
							"color": "green",
							"value": null
						  },
						  {
							"color": "red",
							"value": 80
						  }
						]
					  }
					},
					"overrides": []
				  },
				  "gridPos": {
					"h": 9,
					"w": 12,
					"x": 0,
					"y": 0
				  },
				  "id": 2,
				  "options": {
					"legend": {
					  "calcs": [],
					  "displayMode": "list",
					  "placement": "bottom"
					},
					"tooltip": {
					  "mode": "single"
					}
				  },
				  "targets": [
					{
					  "queryType": "randomWalk",
					  "refId": "A"
					}
				  ],
				  "title": "Panel Title",
				  "type": "timeseries"
				}
			  ],
			  "refresh": "",
			  "schemaVersion": 30,
			  "style": "dark",
			  "tags": [],
			  "templating": {
				"list": []
			  },
			  "time": {
				"from": "now-5m",
				"to": "now"
			  },
			  "timepicker": {},
			  "timezone": "",
			  "title": "New dashboard Copy",
			  "uid": "HzK8qNW7z",
			  "version": 1
			}
		  }`,
		))
	})
	return httptest.NewServer(mux)
}
