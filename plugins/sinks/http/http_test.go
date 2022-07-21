package http_test

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"
	"testing"

	"github.com/alecthomas/assert"
	"github.com/dnaeon/go-vcr/v2/recorder"
	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	"github.com/odpf/meteor/plugins"
	h "github.com/odpf/meteor/plugins/sinks/http"
	testutils "github.com/odpf/meteor/test/utils"
	"github.com/pkg/errors"
)

//go:embed README.md
var summary string

var success_code int = 200

func TestSink(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		httpSink := h.New(&http.Client{}, testutils.Logger)
		config := map[string]interface{}{
			"success_code": success_code,
			"method":       "POST",
			"headers": map[string]string{
				"Content-Type": "application/json",
				"Accept":       "application/json",
			},
		}
		err := httpSink.Init(context.TODO(), config)
		assert.Equal(t, err, plugins.InvalidConfigError{Type: plugins.PluginTypeSink, PluginName: "http"})
	})

	t.Run("should return no error for valid config, without optional values", func(t *testing.T) {
		httpSink := h.New(&http.Client{}, testutils.Logger)
		config := map[string]interface{}{
			"url":    "http://sitename.com",
			"method": "POST",
		}
		err := httpSink.Init(context.TODO(), config)
		assert.NoError(t, err)
	})

	t.Run("should return error for status code when not success", func(t *testing.T) {
		r, err := recorder.New(fmt.Sprintf("fixtures/response_%d", 404))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			err := r.Stop()
			if err != nil {
				t.Fatal(err)
			}
		}()
		httpSink := h.New(&http.Client{Transport: r}, testutils.Logger)
		config := map[string]interface{}{
			"success_code": success_code,
			"url":          "http://127.0.0.1:54927",
			"method":       "PATCH",
			"headers": map[string]string{
				"Content-Type": "application/json",
				"Accept":       "application/json",
			},
		}
		err = httpSink.Init(context.TODO(), config)
		assert.NoError(t, err)
		defer httpSink.Close()
		err = httpSink.Sink(context.TODO(), getExpectedVal())
		assert.Error(t, err)

		// change value of url in config
		config["url"] = "https://random-incorrect-url.odpf.com"
		err = httpSink.Init(context.TODO(), config)
		assert.NoError(t, err)
		err = httpSink.Sink(context.TODO(), getExpectedVal())
		assert.Error(t, err)

		// change value of method in config
		config["method"] = "RANDOM"
		config["url"] = "http://127.0.0.1:54927"
		err = httpSink.Init(context.TODO(), config)
		assert.NoError(t, err)
		err = httpSink.Sink(context.TODO(), getExpectedVal())
		assert.Error(t, err)
	})

	t.Run("should return retry error for error code 5xx", func(t *testing.T) {
		port := 54931
		for _, code := range []int{500, 501, 502, 503, 504, 505} {
			t.Run(fmt.Sprintf("should retry for status code %d", code), func(t *testing.T) {
				r, err := recorder.New(fmt.Sprintf("fixtures/response_%d", code))
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					err := r.Stop()
					if err != nil {
						t.Fatal(err)
					}
				}()
				httpSink := h.New(&http.Client{Transport: r}, testutils.Logger)
				config := map[string]interface{}{
					"success_code": success_code,
					"url":          fmt.Sprintf("http://127.0.0.1:%d", port),
					"method":       "POST",
					"headers": map[string]string{
						"Content-Type": "application/json",
						"Accept":       "application/json",
					},
				}
				err = httpSink.Init(context.TODO(), config)
				assert.NoError(t, err)
				defer httpSink.Close()
				err = httpSink.Sink(context.TODO(), getExpectedVal())
				assert.True(t, errors.Is(err, plugins.RetryError{}))
				port += 2
			})
		}
	})

	t.Run("should return no error for correct status code in response", func(t *testing.T) {
		r, err := recorder.New("fixtures/response")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			err := r.Stop()
			if err != nil {
				t.Fatal(err)
			}
		}()
		httpSink := h.New(&http.Client{Transport: r}, testutils.Logger)
		config := map[string]interface{}{
			"success_code": success_code,
			"url":          "http://127.0.0.1:54943",
			"method":       "POST",
			"headers": map[string]string{
				"Content-Type": "application/json",
				"Accept":       "application/json",
			},
		}
		err = httpSink.Init(context.TODO(), config)
		assert.NoError(t, err)
		defer httpSink.Close()
		err = httpSink.Sink(context.TODO(), getExpectedVal())
		assert.NoError(t, err)
	})
}

func getExpectedVal() []models.Record {
	return []models.Record{
		models.NewRecord(&assetsv1beta1.Table{
			Resource: &commonv1beta1.Resource{
				Urn:  "elasticsearch.index1",
				Name: "index1",
				Type: "table",
			},
			Schema: &facetsv1beta1.Columns{
				Columns: []*facetsv1beta1.Column{
					{
						Name:     "SomeInt",
						DataType: "long",
					},
					{
						Name:     "SomeStr",
						DataType: "text",
					},
				},
			},
			Profile: &assetsv1beta1.TableProfile{
				TotalRows: 1,
			},
		}),
		models.NewRecord(&assetsv1beta1.Table{
			Resource: &commonv1beta1.Resource{
				Urn:  "elasticsearch.index2",
				Name: "index2",
				Type: "table",
			},
			Schema: &facetsv1beta1.Columns{
				Columns: []*facetsv1beta1.Column{
					{
						Name:     "SomeInt",
						DataType: "long",
					},
					{
						Name:     "SomeStr",
						DataType: "text",
					},
				},
			},
			Profile: &assetsv1beta1.TableProfile{
				TotalRows: 1,
			},
		}),
	}
}

func TestInfo(t *testing.T) {
	info := h.New(&http.Client{}, testutils.Logger).Info()
	assert.Equal(t, summary, info.Summary)
}
