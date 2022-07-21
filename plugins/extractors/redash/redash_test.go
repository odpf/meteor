//go:build plugins
// +build plugins

package redash_test

import (
	"context"
	"fmt"
	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/redash"
	"github.com/odpf/meteor/test/mocks"
	"github.com/odpf/meteor/test/utils"
	util "github.com/odpf/meteor/utils"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

var testServer *httptest.Server

func TestMain(m *testing.M) {
	testServer = NewTestServer()

	// run tests
	code := m.Run()

	testServer.Close()
	os.Exit(code)
}

// TestInit tests the configs
func TestInit(t *testing.T) {
	t.Run("should return error if for empty base_url in config", func(t *testing.T) {
		err := redash.New(utils.Logger).Init(context.TODO(), map[string]interface{}{
			"base_url": "",
			"api_key":  "checkAPI",
		})
		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should return error if for empty api_key in config", func(t *testing.T) {
		err := redash.New(utils.Logger).Init(context.TODO(), map[string]interface{}{
			"base_url": testServer.URL,
			"api_key":  "",
		})

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}

// TestExtract tests that the extractor returns the expected result
func TestExtract(t *testing.T) {
	t.Run("should return dashboard model", func(t *testing.T) {
		expectedData := []models.Record{
			models.NewRecord(&assetsv1beta1.Dashboard{
				Resource: &commonv1beta1.Resource{
					Urn:     fmt.Sprintf("redash::%s/dashboard/421", testServer.URL),
					Name:    "firstDashboard",
					Service: "redash",
					Type:    "dashboard",
				},
				Charts: nil,
				Properties: &facetsv1beta1.Properties{
					Attributes: util.TryParseMapToProto(map[string]interface{}{
						"user_id": 1,
						"version": 1,
						"slug":    "new-dashboard-copy",
					}),
				},
			}),
			models.NewRecord(&assetsv1beta1.Dashboard{
				Resource: &commonv1beta1.Resource{
					Urn:     fmt.Sprintf("redash::%s/dashboard/634", testServer.URL),
					Name:    "secondDashboard",
					Service: "redash",
					Type:    "dashboard",
				},
				Charts: nil,
				Properties: &facetsv1beta1.Properties{
					Attributes: util.TryParseMapToProto(map[string]interface{}{
						"user_id": 1,
						"version": 2,
						"slug":    "test-dashboard-updated",
					}),
				},
			}),
		}

		ctx := context.TODO()
		extractor := redash.New(utils.Logger)
		err := extractor.Init(ctx, map[string]interface{}{
			"base_url": testServer.URL,
			"api_key":  "checkAPI",
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extractor.Extract(ctx, emitter.Push)

		assert.NoError(t, err)
		assert.Equal(t, expectedData, emitter.Get())
	})
}

func NewTestServer() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/dashboards", func(res http.ResponseWriter, req *http.Request) {
		res.Write([]byte(`
			{
				"count": 2,
				"page": 1,
				"page_size": 25,
				"results": [
					{
						"tags": [],
						"is_archived": false,
						"updated_at": "2022-06-29T10:29:26.865Z",
						"is_favorite": false,
						"user": {
							"auth_type": "password",
							"is_disabled": false,
							"updated_at": "2022-06-29T10:46:55.810Z",
							"profile_image_url": "https://www.gravatar.com/avatar/75d23af433e0cea4c0e45a56dba18b30?s=40&d=identicon",
							"is_invitation_pending": false,
							"groups": [
								1,
								2
							],
							"id": 1,
							"name": "admin",
							"created_at": "2022-06-29T10:29:06.784Z",
							"disabled_at": null,
							"is_email_verified": true,
							"active_at": "2022-06-29T10:46:50Z",
							"email": "admin@gmail.com"
						},
						"layout": [],
						"is_draft": true,
						"id": 421,
						"user_id": 1,
						"name": "firstDashboard",
						"created_at": "2022-06-29T10:29:26.865Z",
						"slug": "new-dashboard-copy",
						"version": 1,
						"widgets": null,
						"dashboard_filters_enabled": false
					},
					{
						"tags": [],
						"is_archived": false,
						"updated_at": "2022-06-29T10:29:26.865Z",
						"is_favorite": false,
						"user": {
							"auth_type": "password",
							"is_disabled": false,
							"updated_at": "2022-06-29T10:46:55.810Z",
							"profile_image_url": "https://www.gravatar.com/avatar/75d23af433e0cea4c0e45a56dba18b30?s=40&d=identicon",
							"is_invitation_pending": false,
							"groups": [
								1,
								2
							],
							"id": 1,
							"name": "admin",
							"created_at": "2022-06-29T10:29:06.784Z",
							"disabled_at": null,
							"is_email_verified": true,
							"active_at": "2022-06-29T10:46:50Z",
							"email": "admin@gmail.com"
						},
						"layout": [],
						"is_draft": true,
						"id": 634,
						"user_id": 1,
						"name": "secondDashboard",
						"created_at": "2022-06-29T10:29:26.865Z",
						"slug": "test-dashboard-updated",
						"version": 2,
						"widgets": null,
						"dashboard_filters_enabled": false
					}
				]
			}
		`,
		),
		)
	})
	return httptest.NewServer(mux)
}
