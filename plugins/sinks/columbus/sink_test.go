package columbus_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"testing"

	"github.com/odpf/meteor/core/sink"
	"github.com/odpf/meteor/plugins/sinks/columbus"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"github.com/stretchr/testify/assert"
)

var (
	host = "http://columbus.com"
)

func TestSink(t *testing.T) {
	// sample metadata
	var (
		topic = meta.Topic{
			Urn:  "my-topic-urn",
			Name: "my-topic",
			Ownership: &facets.Ownership{
				Owners: []*facets.Owner{
					{Name: "admin-A"},
				},
			},
		}
		requestPayload = []interface{}{topic} // columbus requires payload to be in a list
		columbusType   = "my-type"
		url            = fmt.Sprintf("%s/v1/types/%s/records", host, columbusType)
	)

	t.Run("should return invalid config error if host is empty", func(t *testing.T) {
		invalids := []map[string]interface{}{
			{
				"host": "",
				"type": "columbus-type",
			},
			{
				"host": host,
				"type": "",
			},
		}
		for i, config := range invalids {
			t.Run(fmt.Sprintf("test invalid config #%d", i+1), func(t *testing.T) {
				columbusSink := columbus.New(newMockHttpClient(http.MethodGet, url, requestPayload))
				err := columbusSink.Sink(context.TODO(), config, make(<-chan interface{}))

				assert.Equal(t, sink.InvalidConfigError{}, err)
			})
		}
	})

	t.Run("should create the right request to columbus", func(t *testing.T) {
		client := newMockHttpClient(http.MethodPut, url, requestPayload)
		client.SetupResponse(200, "")

		in := make(chan interface{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			columbusSink := columbus.New(client)
			columbusSink.Sink(context.TODO(), map[string]interface{}{
				"host": host,
				"type": columbusType,
			}, in)

			client.Assert(t)

			wg.Done()
		}()

		in <- topic
		close(in)
		wg.Wait()
	})

	t.Run("should return error if columbus host returns error", func(t *testing.T) {
		columbusError := `{"reason":"no such type: \"my-type\""}`
		expectedErr := errors.New("columbus returns 404: {\"reason\":\"no such type: \\\"my-type\\\"\"}")

		// setup mock client
		url := fmt.Sprintf("%s/v1/types/my-type/records", host)
		client := newMockHttpClient(http.MethodPut, url, requestPayload)
		client.SetupResponse(404, columbusError)

		in := make(chan interface{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			columbusSink := columbus.New(client)
			err := columbusSink.Sink(context.TODO(), map[string]interface{}{
				"host": host,
				"type": "my-type",
			}, in)

			assert.Equal(t, expectedErr, err)
			client.Assert(t)

			wg.Done()
		}()

		in <- topic
		wg.Wait()
	})

	t.Run("should return no error if columbus returns 200", func(t *testing.T) {
		// setup mock client
		client := newMockHttpClient(http.MethodPut, url, requestPayload)
		client.SetupResponse(200, `{"success": true}`)

		in := make(chan interface{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			columbusSink := columbus.New(client)
			err := columbusSink.Sink(context.TODO(), map[string]interface{}{
				"host": host,
				"type": "my-type",
			}, in)

			assert.NoError(t, err)
			client.Assert(t)

			wg.Done()
		}()

		in <- topic
		close(in)
		wg.Wait()
	})
}

type mockHttpClient struct {
	URL            string
	Method         string
	Data           interface{}
	ResponseJSON   string
	ResponseStatus int
	req            *http.Request
}

func newMockHttpClient(method, url string, data interface{}) *mockHttpClient {
	return &mockHttpClient{
		Method: method,
		URL:    url,
		Data:   data,
	}
}

func (m *mockHttpClient) SetupResponse(statusCode int, json string) {
	m.ResponseStatus = statusCode
	m.ResponseJSON = json
}

func (m *mockHttpClient) Do(req *http.Request) (res *http.Response, err error) {
	m.req = req

	res = &http.Response{
		// default values
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		StatusCode:    m.ResponseStatus,
		Request:       req,
		Header:        make(http.Header, 0),
		ContentLength: int64(len(m.ResponseJSON)),
		Body:          ioutil.NopCloser(bytes.NewBufferString(m.ResponseJSON)),
	}

	return
}

func (m *mockHttpClient) Assert(t *testing.T) {
	assert.Equal(t, m.Method, m.req.Method)
	actualURL := fmt.Sprintf(
		"%s://%s%s",
		m.req.URL.Scheme,
		m.req.URL.Host,
		m.req.URL.Path,
	)
	assert.Equal(t, m.URL, actualURL)

	dataJsonBytes, err := json.Marshal(m.Data)
	if err != nil {
		t.Error(err)
	}

	var bodyBytes = []byte("")
	if m.req.Body != nil {
		bodyBytes, err = ioutil.ReadAll(m.req.Body)
		if err != nil {
			t.Error(err)
		}
	}

	assert.Equal(t, string(dataJsonBytes), string(bodyBytes))
}
