package stencil_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins/sinks/stencil"

	"github.com/odpf/meteor/plugins"
	testUtils "github.com/odpf/meteor/test/utils"
	"github.com/stretchr/testify/assert"
)

var (
	host        = "https://stencil.com"
	namespaceID = "test-namespace"
	tableURN    = "test-table-urn"
	url         = fmt.Sprintf("%s/v1beta1/namespaces/%s/schemas/%s", host, namespaceID, tableURN)
)

func TestInit(t *testing.T) {
	t.Run("should return InvalidConfigError on invalid config", func(t *testing.T) {
		invalidConfigs := []map[string]interface{}{
			{
				"host":         "",
				"namespace_id": "",
				"format":       "",
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
		url := fmt.Sprintf("%s/v1beta1/namespaces/%s/schemas/%s", host, namespaceID, tableURN)
		client := newMockHTTPClient(map[string]interface{}{}, http.MethodPost, url, stencil.JsonSchema{})
		client.SetupResponse(404, stencilError)
		ctx := context.TODO()

		stencilSink := stencil.New(client, testUtils.Logger)
		err := stencilSink.Init(ctx, map[string]interface{}{
			"host":         host,
			"namespace_id": namespaceID,
			"format":       "json",
		})
		if err != nil {
			t.Fatal(err)
		}

		data := &assetsv1beta1.Table{Resource: &commonv1beta1.Resource{Urn: tableURN}}
		err = stencilSink.Sink(ctx, []models.Record{models.NewRecord(data)})
		assert.Equal(t, errMessage, err.Error())
	})

	t.Run("should return RetryError if stencil returns certain status code", func(t *testing.T) {
		for _, code := range []int{500, 501, 502, 503, 504, 505} {
			t.Run(fmt.Sprintf("%d status code", code), func(t *testing.T) {
				url := fmt.Sprintf("%s/v1beta1/namespaces/%s/schemas/%s", host, namespaceID, tableURN)
				client := newMockHTTPClient(map[string]interface{}{}, http.MethodPost, url, stencil.JsonSchema{})
				client.SetupResponse(code, `{"reason":"internal server error"}`)
				ctx := context.TODO()

				stencilSink := stencil.New(client, testUtils.Logger)
				err := stencilSink.Init(ctx, map[string]interface{}{
					"host":         host,
					"namespace_id": namespaceID,
					"format":       "json",
				})
				if err != nil {
					t.Fatal(err)
				}

				data := &assetsv1beta1.Table{Resource: &commonv1beta1.Resource{Urn: tableURN}}
				err = stencilSink.Sink(ctx, []models.Record{models.NewRecord(data)})
				assert.True(t, errors.Is(err, plugins.RetryError{}))
			})
		}
	})

	successJsonTestCases := []struct {
		description string
		data        *v1beta2.Asset
		config      map[string]interface{}
		expected    stencil.JsonSchema
	}{
		{
			description: "should create the right request from json schema to stencil when bigquery is the service",
			data: &assetsv1beta1.Table{
				Resource: &commonv1beta1.Resource{
					Urn:     tableURN,
					Name:    "table-name",
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
							Name:        "email_id",
							Description: "It is the email ID",
							IsNullable:  true,
						},
						{
							Name:        "description",
							Description: "It is the description",
							DataType:    "STRING",
							IsNullable:  true,
						},
						{
							Name:        "is_active",
							Description: "It shows user regularity",
							DataType:    "BOOLEAN",
							IsNullable:  false,
						},
						{
							Name:        "address",
							Description: "It shows user address",
							DataType:    "RECORD",
							IsNullable:  false,
						},
						{
							Name:        "range",
							Description: "It is the range",
							DataType:    "BYTES",
							IsNullable:  false,
						},
					},
				},
			},
			config: map[string]interface{}{
				"host":         host,
				"namespace_id": namespaceID,
				"format":       "json",
			},
			expected: stencil.JsonSchema{
				Id:     fmt.Sprintf("%s.json", tableURN),
				Schema: "https://json-schema.org/draft/2020-12/schema",
				Title:  "table-name",
				Type:   "object",
				Properties: map[string]stencil.JsonProperty{
					"id": {
						Type:        []stencil.JsonType{stencil.JsonTypeNumber, stencil.JsonTypeNull},
						Description: "It is the ID",
					},
					"user_id": {
						Type:        []stencil.JsonType{stencil.JsonTypeString},
						Description: "It is the user ID",
					},
					"email_id": {
						Type:        []stencil.JsonType{stencil.JsonTypeString, stencil.JsonTypeNull},
						Description: "It is the email ID",
					},
					"description": {
						Type:        []stencil.JsonType{stencil.JsonTypeString, stencil.JsonTypeNull},
						Description: "It is the description",
					},
					"is_active": {
						Type:        []stencil.JsonType{stencil.JsonTypeBoolean},
						Description: "It shows user regularity",
					},
					"address": {
						Type:        []stencil.JsonType{stencil.JsonTypeObject},
						Description: "It shows user address",
					},
					"range": {
						Type:        []stencil.JsonType{stencil.JsonTypeArray},
						Description: "It is the range",
					},
				},
			},
		},
		{
			description: "should create the right request from json schema to stencil when postgres is the service",
			data: &assetsv1beta1.Table{
				Resource: &commonv1beta1.Resource{
					Urn:     tableURN,
					Name:    "table-name",
					Service: "postgres",
				},
				Schema: &facetsv1beta1.Columns{
					Columns: []*facetsv1beta1.Column{
						{
							Name:        "id",
							Description: "It is the ID",
							DataType:    "integer",
							IsNullable:  true,
						},
						{
							Name:        "user_id",
							Description: "It is the user ID",
							DataType:    "varchar",
							IsNullable:  false,
						},
						{
							Name:        "email_id",
							Description: "It is the email ID",
							IsNullable:  true,
						},
						{
							Name:        "description",
							Description: "It is the description",
							DataType:    "varchar",
							IsNullable:  true,
						},
						{
							Name:        "is_active",
							Description: "It shows user regularity",
							DataType:    "boolean",
							IsNullable:  false,
						},
						{
							Name:        "range",
							Description: "It is the range",
							DataType:    "bytea",
							IsNullable:  false,
						},
					},
				},
			},
			config: map[string]interface{}{
				"host":         host,
				"namespace_id": namespaceID,
				"format":       "json",
			},
			expected: stencil.JsonSchema{
				Id:     fmt.Sprintf("%s.json", tableURN),
				Schema: "https://json-schema.org/draft/2020-12/schema",
				Title:  "table-name",
				Type:   "object",
				Properties: map[string]stencil.JsonProperty{
					"id": {
						Type:        []stencil.JsonType{stencil.JsonTypeNumber, stencil.JsonTypeNull},
						Description: "It is the ID",
					},
					"user_id": {
						Type:        []stencil.JsonType{stencil.JsonTypeString},
						Description: "It is the user ID",
					},
					"email_id": {
						Type:        []stencil.JsonType{stencil.JsonTypeString, stencil.JsonTypeNull},
						Description: "It is the email ID",
					},
					"description": {
						Type:        []stencil.JsonType{stencil.JsonTypeString, stencil.JsonTypeNull},
						Description: "It is the description",
					},
					"is_active": {
						Type:        []stencil.JsonType{stencil.JsonTypeBoolean},
						Description: "It shows user regularity",
					},
					"range": {
						Type:        []stencil.JsonType{stencil.JsonTypeArray},
						Description: "It is the range",
					},
				},
			},
		},
		{
			description: "should return correct schema request with valid config",
			data: &assetsv1beta1.Table{
				Resource: &commonv1beta1.Resource{
					Urn:     tableURN,
					Name:    "table-name",
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
					},
				},
			},
			config: map[string]interface{}{
				"host":         host,
				"namespace_id": namespaceID,
				"format":       "json",
			},
			expected: stencil.JsonSchema{
				Id:     fmt.Sprintf("%s.json", tableURN),
				Schema: "https://json-schema.org/draft/2020-12/schema",
				Title:  "table-name",
				Type:   "object",
				Properties: map[string]stencil.JsonProperty{
					"id": {
						Type:        []stencil.JsonType{stencil.JsonTypeNumber, stencil.JsonTypeNull},
						Description: "It is the ID",
					},
					"user_id": {
						Type:        []stencil.JsonType{stencil.JsonTypeString},
						Description: "It is the user ID",
					},
				},
			},
		},
	}

	for _, tc := range successJsonTestCases {
		t.Run(tc.description, func(t *testing.T) {
			payload := stencil.JsonSchema{
				Id:         tc.expected.Id,
				Schema:     tc.expected.Schema,
				Title:      tc.expected.Title,
				Type:       tc.expected.Type,
				Properties: tc.expected.Properties,
			}

			client := newMockHTTPClient(tc.config, http.MethodPost, url, payload)
			client.SetupResponse(http.StatusCreated, "")
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

	successAvroTestCases := []struct {
		description string
		data        *v1beta2.Asset
		config      map[string]interface{}
		expected    stencil.AvroSchema
	}{
		{
			description: "should create the right request from avro schema to stencil when bigquery is the service",
			data: &assetsv1beta1.Table{
				Resource: &commonv1beta1.Resource{
					Urn:     tableURN,
					Name:    "table-name",
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
							IsNullable:  true,
						},
						{
							Name:        "distance",
							Description: "It is the user distance from source",
							DataType:    "FLOAT",
							IsNullable:  true,
						},
						{
							Name:        "is_active",
							Description: "It shows user regularity",
							DataType:    "BOOLEAN",
							IsNullable:  false,
						},
						{
							Name:        "address",
							Description: "It shows user address",
							DataType:    "RECORD",
							IsNullable:  false,
						},
						{
							Name:        "range",
							Description: "It is the range",
							DataType:    "BYTES",
							IsNullable:  false,
						},
					},
				},
			},
			config: map[string]interface{}{
				"host":         host,
				"namespace_id": namespaceID,
				"format":       "avro",
			},
			expected: stencil.AvroSchema{
				Type:      "record",
				Namespace: namespaceID,
				Name:      "table-name",
				Fields: []stencil.AvroFields{
					{
						Name: "id",
						Type: []stencil.AvroType{stencil.AvroTypeInteger, stencil.AvroTypeNull},
					},
					{
						Name: "user_id",
						Type: []stencil.AvroType{stencil.AvroTypeString},
					},
					{
						Name: "description",
						Type: []stencil.AvroType{stencil.AvroTypeString, stencil.AvroTypeNull},
					},
					{
						Name: "distance",
						Type: []stencil.AvroType{stencil.AvroTypeFloat, stencil.AvroTypeNull},
					},
					{
						Name: "is_active",
						Type: []stencil.AvroType{stencil.AvroTypeBoolean},
					},
					{
						Name: "address",
						Type: []stencil.AvroType{stencil.AvroTypeRecord},
					},
					{
						Name: "range",
						Type: []stencil.AvroType{stencil.AvroTypeBytes},
					},
				},
			},
		},
		{
			description: "should create the right request to stencil when postgres is the service",
			data: &assetsv1beta1.Table{
				Resource: &commonv1beta1.Resource{
					Urn:     tableURN,
					Name:    "table-name",
					Type:    "table",
					Service: "postgres",
				},
				Schema: &facetsv1beta1.Columns{
					Columns: []*facetsv1beta1.Column{
						{
							Name:        "id",
							Description: "It is the ID",
							DataType:    "integer",
							IsNullable:  true,
						},
						{
							Name:        "user_id",
							Description: "It is the user ID",
							DataType:    "varchar",
							IsNullable:  false,
						},
						{
							Name:        "description",
							Description: "It is the description",
							IsNullable:  true,
						},
						{
							Name:        "is_active",
							Description: "It shows user regularity",
							DataType:    "boolean",
							IsNullable:  false,
						},
						{
							Name:        "range",
							Description: "It is the range",
							DataType:    "bytea",
							IsNullable:  false,
						},
					},
				},
			},
			config: map[string]interface{}{
				"host":         host,
				"namespace_id": namespaceID,
				"schema_id":    tableURN,
				"format":       "avro",
			},
			expected: stencil.AvroSchema{
				Type:      "record",
				Namespace: namespaceID,
				Name:      "table-name",
				Fields: []stencil.AvroFields{
					{
						Name: "id",
						Type: []stencil.AvroType{stencil.AvroTypeInteger, stencil.AvroTypeNull},
					},
					{
						Name: "user_id",
						Type: []stencil.AvroType{stencil.AvroTypeString},
					},
					{
						Name: "description",
						Type: []stencil.AvroType{stencil.AvroTypeString, stencil.AvroTypeNull},
					},
					{
						Name: "is_active",
						Type: []stencil.AvroType{stencil.AvroTypeBoolean},
					},
					{
						Name: "range",
						Type: []stencil.AvroType{stencil.AvroTypeArray},
					},
				},
			},
		},
	}

	for _, tc := range successAvroTestCases {
		t.Run(tc.description, func(t *testing.T) {
			payload := stencil.AvroSchema{
				Type:      tc.expected.Type,
				Namespace: tc.expected.Namespace,
				Name:      tc.expected.Name,
				Fields:    tc.expected.Fields,
			}

			client := newMockHTTPClient(tc.config, http.MethodPost, url, payload)
			client.SetupResponse(http.StatusCreated, "")
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
	RequestPayload interface{}
	ResponseJSON   string
	ResponseStatus int
	req            *http.Request
}

func newMockHTTPClient(config map[string]interface{}, method, url string, payload interface{}) *mockHTTPClient {
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
