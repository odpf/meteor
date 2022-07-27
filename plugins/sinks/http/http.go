package http

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
)

//go:embed README.md
var summary string

type Config struct {
	URL         string            `mapstructure:"url" validate:"required"`
	Headers     map[string]string `mapstructure:"headers"`
	Method      string            `mapstructure:"method" validate:"required"`
	SuccessCode int               `mapstructure:"success_code" default:"200"`
}

var sampleConfig = `
# The url (hostname and route) of the http service
url: https://compass.com/route
# Additional HTTP headers, multiple headers value are separated by a comma
headers:
	X-Other-Header: value1, value2
`

type httpClient interface {
	Do(*http.Request) (*http.Response, error)
}

type Sink struct {
	client httpClient
	config Config
	logger log.Logger
}

func New(c httpClient, logger log.Logger) plugins.Syncer {
	sink := &Sink{client: c, logger: logger}
	return sink
}

func (s *Sink) Info() plugins.Info {
	return plugins.Info{
		Description:  "Send metadata to http service",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"http", "sink"},
	}
}

func (s *Sink) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &s.config)
}

func (s *Sink) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	if err = s.Validate(configMap); err != nil {
		return plugins.InvalidConfigError{Type: plugins.PluginTypeSink, PluginName: "http"}
	}
	return
}

func (s *Sink) Sink(ctx context.Context, batch []models.Record) (err error) {
	for _, record := range batch {
		metadata := record.Data()
		s.logger.Info("sinking record to http", "record", metadata.Urn)
		payload, err := json.Marshal(metadata)
		if err != nil {
			return errors.Wrap(err, "failed to build http payload")
		}
		if err = s.send(payload); err != nil {
			return errors.Wrap(err, "error sending data")
		}

		s.logger.Info("successfully sinked record to http", "record", metadata.Urn)
	}

	return
}

func (s *Sink) Close() (err error) { return }

func (s *Sink) send(payloadBytes []byte) (err error) {
	// send request
	req, err := http.NewRequest(s.config.Method, s.config.URL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return
	}

	for hdrKey, hdrVal := range s.config.Headers {
		hdrVals := strings.Split(hdrVal, ",")
		for _, val := range hdrVals {
			req.Header.Add(hdrKey, val)
		}
	}

	res, err := s.client.Do(req)
	if err != nil {
		return
	}
	if res.StatusCode == s.config.SuccessCode {
		return
	}

	var bodyBytes []byte
	bodyBytes, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	err = fmt.Errorf("http returns %d: %v", res.StatusCode, string(bodyBytes))

	switch code := res.StatusCode; {
	case code >= 500:
		return plugins.NewRetryError(err)
	default:
		return err
	}
}

func init() {
	if err := registry.Sinks.Register("http", func() plugins.Syncer {
		return New(&http.Client{}, plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
