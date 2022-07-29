//go:build plugins
// +build plugins

package compass_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	testUtils "github.com/odpf/meteor/test/utils"
	"github.com/odpf/meteor/utils"

	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/sinks/compass"
	"github.com/stretchr/testify/assert"
)

var (
	host = "http://compass.com"
)

// sample metadata
var (
	url = fmt.Sprintf("%s/v1beta1/assets", host)
)

func TestInit(t *testing.T) {
	t.Run("should return InvalidConfigError on invalid config", func(t *testing.T) {
		invalidConfigs := []map[string]interface{}{
			{
				"host": "",
			},
		}
		for i, config := range invalidConfigs {
			t.Run(fmt.Sprintf("test invalid config #%d", i+1), func(t *testing.T) {
				compassSink := compass.New(newMockHTTPClient(config, http.MethodPatch, url, compass.RequestPayload{}), testUtils.Logger)
				err := compassSink.Init(context.TODO(), plugins.Config{RawConfig: config})

				assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
			})
		}
	})
}

func TestSink(t *testing.T) {
	t.Run("should return error if compass host returns error", func(t *testing.T) {
		compassError := `{"reason":"no asset found"}`
		errMessage := "error sending data: compass returns 404: {\"reason\":\"no asset found\"}"

		// setup mock client
		url := fmt.Sprintf("%s/v1beta1/assets", host)
		client := newMockHTTPClient(map[string]interface{}{}, http.MethodPatch, url, compass.RequestPayload{})
		client.SetupResponse(404, compassError)
		ctx := context.TODO()

		compassSink := compass.New(client, testUtils.Logger)
		err := compassSink.Init(ctx, plugins.Config{RawConfig: map[string]interface{}{
			"host": host,
		}})
		if err != nil {
			t.Fatal(err)
		}

		data := &assetsv1beta1.Topic{Resource: &commonv1beta1.Resource{}}
		err = compassSink.Sink(ctx, []models.Record{models.NewRecord(data)})
		assert.Equal(t, errMessage, err.Error())
	})

	t.Run("should return RetryError if compass returns certain status code", func(t *testing.T) {
		for _, code := range []int{500, 501, 502, 503, 504, 505} {
			t.Run(fmt.Sprintf("%d status code", code), func(t *testing.T) {
				url := fmt.Sprintf("%s/v1beta1/assets", host)
				client := newMockHTTPClient(map[string]interface{}{}, http.MethodPatch, url, compass.RequestPayload{})
				client.SetupResponse(code, `{"reason":"internal server error"}`)
				ctx := context.TODO()

				compassSink := compass.New(client, testUtils.Logger)
				err := compassSink.Init(ctx, plugins.Config{RawConfig: map[string]interface{}{
					"host": host,
				}})
				if err != nil {
					t.Fatal(err)
				}

				data := &assetsv1beta1.Topic{Resource: &commonv1beta1.Resource{}}
				err = compassSink.Sink(ctx, []models.Record{models.NewRecord(data)})
				assert.True(t, errors.Is(err, plugins.RetryError{}))
			})
		}
	})

	t.Run("should return error for various invalid labels", func(t *testing.T) {
		testData := &assetsv1beta1.User{
			Resource: &commonv1beta1.Resource{
				Urn:         "my-topic-urn",
				Name:        "my-topic",
				Service:     "kafka",
				Type:        "topic",
				Description: "topic information",
			},
			Properties: &facetsv1beta1.Properties{
				Attributes: utils.TryParseMapToProto(map[string]interface{}{
					"attrA": "valueAttrA",
					"attrB": "valueAttrB",
				}),
				Labels: map[string]string{
					"labelA": "valueLabelA",
					"labelB": "valueLabelB",
				},
			},
		}
		testPayload := compass.RequestPayload{
			Asset: compass.Asset{
				URN:         "my-topic-urn",
				Name:        "my-topic",
				Service:     "kafka",
				Type:        "topic",
				Description: "topic information",
			},
		}
		invalidConfigs := []map[string]interface{}{
			{
				"host": host,
				"labels": map[string]string{
					"foo": "$properties.attributes",
				},
			},
			{
				"host": host,
				"labels": map[string]string{
					"foo": "$properties.attributes.12",
				},
			},
			{
				"host": host,
				"labels": map[string]string{
					"foo": "$properties.attributes.attrC",
				},
			},
			{
				"host": host,
				"labels": map[string]string{
					"foo": "$invalid.attributes.attrC",
				},
			},
			{
				"host": host,
				"labels": map[string]string{
					"bar": "$properties.labels.labelC",
				},
			},
		}
		for _, c := range invalidConfigs {
			client := newMockHTTPClient(c, http.MethodPatch, url, testPayload)
			client.SetupResponse(200, "")
			ctx := context.TODO()
			compassSink := compass.New(client, testUtils.Logger)
			err := compassSink.Init(ctx, plugins.Config{RawConfig: c})
			if err != nil {
				t.Fatal(err)
			}
			err = compassSink.Sink(ctx, []models.Record{models.NewRecord(testData)})
			assert.Error(t, err)
			fmt.Println(err)
		}

	})

	successTestCases := []struct {
		description string
		data        models.Metadata
		config      map[string]interface{}
		expected    compass.RequestPayload
	}{
		{
			description: "should create the right request to compass",
			data: &assetsv1beta1.User{
				Resource: &commonv1beta1.Resource{
					Urn:         "my-topic-urn",
					Name:        "my-topic",
					Service:     "kafka",
					Type:        "topic",
					Description: "topic information",
				},
			},
			config: map[string]interface{}{
				"host": host,
			},
			expected: compass.RequestPayload{
				Asset: compass.Asset{
					URN:         "my-topic-urn",
					Name:        "my-topic",
					Service:     "kafka",
					Type:        "topic",
					Description: "topic information",
				},
			},
		},
		{
			description: "should build compass labels if labels is defined in config",
			data: &assetsv1beta1.Topic{
				Resource: &commonv1beta1.Resource{
					Urn:         "my-topic-urn",
					Name:        "my-topic",
					Service:     "kafka",
					Type:        "topic",
					Description: "topic information",
				},
				Properties: &facetsv1beta1.Properties{
					Attributes: utils.TryParseMapToProto(map[string]interface{}{
						"attrA": "valueAttrA",
						"attrB": "valueAttrB",
					}),
					Labels: map[string]string{
						"labelA": "valueLabelA",
						"labelB": "valueLabelB",
					},
				},
			},
			config: map[string]interface{}{
				"host": host,
				"labels": map[string]string{
					"foo": "$properties.attributes.attrB",
					"bar": "$properties.labels.labelA",
				},
			},
			expected: compass.RequestPayload{
				Asset: compass.Asset{
					URN:         "my-topic-urn",
					Name:        "my-topic",
					Service:     "kafka",
					Type:        "topic",
					Description: "topic information",
					Labels: map[string]string{
						"foo": "valueAttrB",
						"bar": "valueLabelA",
					},
				},
			},
		},
		{
			description: "should send upstreams if data has upstreams",
			data: &assetsv1beta1.Topic{
				Resource: &commonv1beta1.Resource{
					Urn:         "my-topic-urn",
					Name:        "my-topic",
					Service:     "kafka",
					Type:        "topic",
					Description: "topic information",
				},
				Lineage: &facetsv1beta1.Lineage{
					Upstreams: []*commonv1beta1.Resource{
						{
							Urn:     "urn-1",
							Type:    "type-a",
							Service: "kafka",
						},
						{
							Urn:     "urn-2",
							Type:    "type-b",
							Service: "bigquery",
						},
					},
				},
			},
			config: map[string]interface{}{
				"host": host,
			},
			expected: compass.RequestPayload{
				Asset: compass.Asset{
					URN:         "my-topic-urn",
					Name:        "my-topic",
					Service:     "kafka",
					Type:        "topic",
					Description: "topic information",
				},
				Upstreams: []compass.LineageRecord{
					{
						URN:     "urn-1",
						Type:    "type-a",
						Service: "kafka",
					},
					{
						URN:     "urn-2",
						Type:    "type-b",
						Service: "bigquery",
					},
				},
			},
		},
		{
			description: "should send downstreams if data has downstreams",
			data: &assetsv1beta1.Topic{
				Resource: &commonv1beta1.Resource{
					Urn:         "my-topic-urn",
					Name:        "my-topic",
					Service:     "kafka",
					Type:        "topic",
					Description: "topic information",
				},
				Lineage: &facetsv1beta1.Lineage{
					Downstreams: []*commonv1beta1.Resource{
						{
							Urn:     "urn-1",
							Type:    "type-a",
							Service: "kafka",
						},
						{
							Urn:     "urn-2",
							Type:    "type-b",
							Service: "bigquery",
						},
					},
				},
			},
			config: map[string]interface{}{
				"host": host,
			},
			expected: compass.RequestPayload{
				Asset: compass.Asset{
					URN:         "my-topic-urn",
					Name:        "my-topic",
					Service:     "kafka",
					Type:        "topic",
					Description: "topic information",
				},
				Downstreams: []compass.LineageRecord{
					{
						URN:     "urn-1",
						Type:    "type-a",
						Service: "kafka",
					},
					{
						URN:     "urn-2",
						Type:    "type-b",
						Service: "bigquery",
					},
				},
			},
		},
		{
			description: "should send owners if data has ownership",
			data: &assetsv1beta1.Topic{
				Resource: &commonv1beta1.Resource{
					Urn:         "my-topic-urn",
					Name:        "my-topic",
					Service:     "kafka",
					Type:        "topic",
					Description: "topic information",
				},
				Ownership: &facetsv1beta1.Ownership{
					Owners: []*facetsv1beta1.Owner{
						{
							Urn:   "urn-1",
							Name:  "owner-a",
							Role:  "role-a",
							Email: "email-1",
						},
						{
							Urn:   "urn-2",
							Name:  "owner-b",
							Role:  "role-b",
							Email: "email-2",
						},
						{
							Urn:   "urn-3",
							Name:  "owner-c",
							Role:  "role-c",
							Email: "email-3",
						},
					},
				},
			},
			config: map[string]interface{}{
				"host": host,
			},
			expected: compass.RequestPayload{
				Asset: compass.Asset{
					URN:         "my-topic-urn",
					Name:        "my-topic",
					Service:     "kafka",
					Type:        "topic",
					Description: "topic information",
					Owners: []compass.Owner{
						{
							URN:   "urn-1",
							Name:  "owner-a",
							Role:  "role-a",
							Email: "email-1",
						},
						{
							URN:   "urn-2",
							Name:  "owner-b",
							Role:  "role-b",
							Email: "email-2",
						},
						{
							URN:   "urn-3",
							Name:  "owner-c",
							Role:  "role-c",
							Email: "email-3",
						},
					},
				},
			},
		},
		{
			description: "should send headers if get populated in config",
			data: &assetsv1beta1.Topic{
				Resource: &commonv1beta1.Resource{
					Urn:         "my-topic-urn",
					Name:        "my-topic",
					Service:     "kafka",
					Type:        "topic",
					Description: "topic information",
				},
			},
			config: map[string]interface{}{
				"host": host,
				"headers": map[string]string{
					"Key1": "value11, value12",
					"Key2": "value2",
				},
			},
			expected: compass.RequestPayload{
				Asset: compass.Asset{
					URN:         "my-topic-urn",
					Name:        "my-topic",
					Service:     "kafka",
					Type:        "topic",
					Description: "topic information",
				},
			},
		},
	}

	for _, tc := range successTestCases {
		t.Run(tc.description, func(t *testing.T) {
			tc.expected.Asset.Data = tc.data
			payload := compass.RequestPayload{
				Asset:       tc.expected.Asset,
				Upstreams:   tc.expected.Upstreams,
				Downstreams: tc.expected.Downstreams,
			}

			client := newMockHTTPClient(tc.config, http.MethodPatch, url, payload)
			client.SetupResponse(200, "")
			ctx := context.TODO()

			compassSink := compass.New(client, testUtils.Logger)
			err := compassSink.Init(ctx, plugins.Config{RawConfig: tc.config})
			if err != nil {
				t.Fatal(err)
			}

			err = compassSink.Sink(ctx, []models.Record{models.NewRecord(tc.data)})
			assert.NoError(t, err)

			client.Assert(t)
		})
	}
}

type mockHTTPClient struct {
	URL            string
	Method         string
	Headers        map[string]string
	RequestPayload compass.RequestPayload
	ResponseJSON   string
	ResponseStatus int
	req            *http.Request
}

func newMockHTTPClient(config map[string]interface{}, method, url string, payload compass.RequestPayload) *mockHTTPClient {
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
