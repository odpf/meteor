package stencil_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins/sinks/stencil"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/odpf/meteor/plugins"
	testUtils "github.com/odpf/meteor/test/utils"
	"github.com/stretchr/testify/assert"
)

var (
	host        = "http://stencil.com"
	namespaceID = "test-namespace"
	schemaID    = "schema-name"
)

// sample metadata
var (
	url = fmt.Sprintf("%s/v1beta1/namespaces/%s/schemas/%s", host, namespaceID, schemaID)
)

func TestInit(t *testing.T) {
	t.Run("should return InvalidConfigError on invalid config", func(t *testing.T) {
		invalidConfigs := []map[string]interface{}{
			{
				"host":        "",
				"namespaceId": "",
				"schemaId":    "",
			},
		}
		for i, config := range invalidConfigs {
			t.Run(fmt.Sprintf("test invalid config #%d", i+1), func(t *testing.T) {
				stencilSink := stencil.New(newMockHTTPClient(config, http.MethodPost, url, stencil.JsonSchema{}), testUtils.Logger)
				err := stencilSink.Init(context.TODO(), config)

				assert.Equal(t, plugins.InvalidConfigError{Type: plugins.PluginTypeSink}, err)
			})
		}
	})
}

func TestSink(t *testing.T) {
	t.Run("should return error if stencil host returns error", func(t *testing.T) {
		stencilError := `{"code": 0,"message": "string","details": [{"typeUrl": "string","value": "string"}]}`

		errMessage := "error sending data: stencil returns 404: {\"code\": 0,\"message\": \"string\",\"details\": [{\"typeUrl\": \"string\",\"value\": \"string\"}]}"
		// setup mock client
		url := fmt.Sprintf("%s/v1beta1/namespaces/%s/schemas/%s", host, namespaceID, schemaID)
		client := newMockHTTPClient(map[string]interface{}{}, http.MethodPost, url, stencil.JsonSchema{})
		client.SetupResponse(404, stencilError)
		ctx := context.TODO()

		stencilSink := stencil.New(client, testUtils.Logger)
		err := stencilSink.Init(ctx, map[string]interface{}{
			"host":        host,
			"namespaceId": namespaceID,
			"schemaId":    schemaID,
		})
		if err != nil {
			t.Fatal(err)
		}

		data := &assetsv1beta1.Table{Resource: &commonv1beta1.Resource{}}
		err = stencilSink.Sink(ctx, []models.Record{models.NewRecord(data)})
		assert.Equal(t, errMessage, err.Error())
	})

	t.Run("should return RetryError if stencil returns certain status code", func(t *testing.T) {
		for _, code := range []int{500, 501, 502, 503, 504, 505} {
			t.Run(fmt.Sprintf("%d status code", code), func(t *testing.T) {
				url := fmt.Sprintf("%s/v1beta1/namespaces/%s/schemas/%s", host, namespaceID, schemaID)
				client := newMockHTTPClient(map[string]interface{}{}, http.MethodPost, url, stencil.JsonSchema{})
				client.SetupResponse(code, `{"reason":"internal server error"}`)
				ctx := context.TODO()

				stencilSink := stencil.New(client, testUtils.Logger)
				err := stencilSink.Init(ctx, map[string]interface{}{
					"host":        host,
					"namespaceId": namespaceID,
					"schemaId":    schemaID,
				})
				if err != nil {
					t.Fatal(err)
				}

				data := &assetsv1beta1.Table{Resource: &commonv1beta1.Resource{}}
				err = stencilSink.Sink(ctx, []models.Record{models.NewRecord(data)})
				assert.True(t, errors.Is(err, plugins.RetryError{}))
			})
		}
	})

	successTestCases := []struct {
		description string
		data        *assetsv1beta1.Table
		config      map[string]interface{}
		expected    stencil.JsonSchema
	}{
		{
			description: "should create the right request to stencil",
			data: &assetsv1beta1.Table{
				Resource: &commonv1beta1.Resource{
					Name:    "stencil",
					Type:    "table",
					Service: "bigquery",
				},
				Schema: &facetsv1beta1.Columns{
					Columns: []*facetsv1beta1.Column{
						{
							Name:        "id",
							Description: "It is the ID",
							DataType:    "INT",
							IsNullable:  true,
						},
						{
							Name:        "user_id",
							Description: "It is the user ID",
							DataType:    "STRING",
							IsNullable:  false,
						},
						{
							Name:        "description",
							Description: "It is the description",
							DataType:    "STRING",
							IsNullable:  true,
						},
					},
				},
			},
			config: map[string]interface{}{
				"host":        host,
				"namespaceId": namespaceID,
				"schemaId":    schemaID,
			},
			expected: stencil.JsonSchema{
				Id:     fmt.Sprintf("%s/%s.%s.json", host, namespaceID, schemaID),
				Schema: "https://json-schema.org/draft/2020-12/schema",
				Title:  "stencil",
				Type:   "table",
				Properties: map[string]stencil.Property{
					"id": {
						Type:        []stencil.JsonType{stencil.JsonTypeNumber, stencil.JsonTypeNull},
						Description: "It is the ID",
					},
					"user_id": {
						Type:        []stencil.JsonType{stencil.JsonTypeString},
						Description: "It is the user ID",
					},
					"description": {
						Type:        []stencil.JsonType{stencil.JsonTypeString, stencil.JsonTypeNull},
						Description: "It is the description",
					},
				},
			},
		},
		//{
		//	description: "should send owners if data has ownership",
		//	data: &assetsv1beta1.Table{
		//		Resource: &commonv1beta1.Resource{
		//			Name: "stencil",
		//			Type: "table",
		//		},
		//		Profile: nil,
		//		Schema: &facetsv1beta1.Columns{
		//			Columns: []*facetsv1beta1.Column{
		//				{
		//					Name:        "",
		//					Description: "",
		//					DataType:    "",
		//					IsNullable:  false,
		//				},
		//			},
		//		},
		//		Properties: nil,
		//	},
		//	config: map[string]interface{}{
		//		"host":        host,
		//		"namespaceId": namespaceID,
		//		"schemaId":    schemaID,
		//	},
		//	expected: stencil.JsonSchema{
		//		Id:         "",
		//		Schema:     "",
		//		Title:      "",
		//		Type:       "",
		//		Properties: nil,
		//	},
		//},
		//{
		//	description: "should send headers if get populated in config",
		//	data: &assetsv1beta1.Table{
		//		Resource: &commonv1beta1.Resource{
		//			Name: "stencil",
		//			Type: "table",
		//		},
		//		Profile: nil,
		//		Schema: &facetsv1beta1.Columns{
		//			Columns: []*facetsv1beta1.Column{
		//				{
		//					Name:        "",
		//					Description: "",
		//					DataType:    "",
		//					IsNullable:  false,
		//				},
		//			},
		//		},
		//		Properties: nil,
		//	},
		//	config: map[string]interface{}{
		//		"host":        host,
		//		"namespaceId": namespaceID,
		//		"schemaId":    schemaID,
		//		"headers": map[string]string{
		//			"Key1": "value11, value12",
		//			"Key2": "value2",
		//		},
		//	},
		//	expected: stencil.JsonSchema{
		//		Id:         "",
		//		Schema:     "",
		//		Title:      "",
		//		Type:       "",
		//		Properties: nil,
		//	},
		//},
	}

	for _, tc := range successTestCases {
		t.Run(tc.description, func(t *testing.T) {
			payload := stencil.JsonSchema{
				Id:         tc.expected.Id,
				Schema:     tc.expected.Schema,
				Title:      tc.expected.Title,
				Type:       tc.expected.Type,
				Properties: tc.expected.Properties,
			}

			client := newMockHTTPClient(tc.config, http.MethodPost, url, payload)
			client.SetupResponse(200, "")
			ctx := context.TODO()

			stencilSink := stencil.New(client, testUtils.Logger)
			err := stencilSink.Init(ctx, tc.config)
			if err != nil {
				t.Fatal(err)
			}

			err = stencilSink.Sink(ctx, []models.Record{models.NewRecord(tc.data)})
			assert.NoError(t, err)

			client.Assert(t)
		})
	}

}

type mockHTTPClient struct {
	URL            string
	Method         string
	Headers        map[string]string
	RequestPayload stencil.JsonSchema
	ResponseJSON   string
	ResponseStatus int
	req            *http.Request
}

func newMockHTTPClient(config map[string]interface{}, method, url string, payload stencil.JsonSchema) *mockHTTPClient {
	headersMap := map[string]string{}
	if headersItf, ok := config["headers"]; ok {
		headersMap = headersItf.(map[string]string)
	}
	return &mockHTTPClient{
		Method:         method,
		URL:            url,
		Headers:        headersMap,
		RequestPayload: payload,
	}
}

func (m *mockHTTPClient) SetupResponse(statusCode int, json string) {
	m.ResponseStatus = statusCode
	m.ResponseJSON = json
}

func (m *mockHTTPClient) Do(req *http.Request) (res *http.Response, err error) {
	m.req = req

	res = &http.Response{
		// default values
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		StatusCode:    m.ResponseStatus,
		Request:       req,
		Header:        make(http.Header),
		ContentLength: int64(len(m.ResponseJSON)),
		Body:          ioutil.NopCloser(bytes.NewBufferString(m.ResponseJSON)),
	}

	return
}

func (m *mockHTTPClient) Assert(t *testing.T) {
	assert.Equal(t, m.Method, m.req.Method)
	actualURL := fmt.Sprintf(
		"%s://%s%s",
		m.req.URL.Scheme,
		m.req.URL.Host,
		m.req.URL.Path,
	)
	assert.Equal(t, m.URL, actualURL)

	headersMap := map[string]string{}
	for hdrKey, hdrVals := range m.req.Header {
		headersMap[hdrKey] = strings.Join(hdrVals, ",")
	}
	assert.Equal(t, m.Headers, headersMap)
	var bodyBytes = []byte("")
	if m.req.Body != nil {
		var err error
		bodyBytes, err = ioutil.ReadAll(m.req.Body)
		if err != nil {
			t.Error(err)
		}
	}

	expectedBytes, err := json.Marshal(m.RequestPayload)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, string(expectedBytes), string(bodyBytes))
}
