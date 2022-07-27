package compass

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
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
)

//go:embed README.md
var summary string

type Config struct {
	Host    string            `mapstructure:"host" validate:"required"`
	Headers map[string]string `mapstructure:"headers"`
	Labels  map[string]string `mapstructure:"labels"`
}

var sampleConfig = `
# The hostname of the compass service
host: https://compass.com
# Additional HTTP headers send to compass, multiple headers value are separated by a comma
headers:
	Compass-User-Email: meteor@odpf.io
	X-Other-Header: value1, value2
# The labels to pass as payload label of the patch api
labels:
	myCustom: $properties.attributes.myCustomField
	sampleLabel: $properties.labels.sampleLabelField
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
		Description:  "Send asset to compass http service",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"http", "sink"},
	}
}

func (s *Sink) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

func (s *Sink) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	if err = utils.BuildConfig(configMap, &s.config); err != nil {
		return plugins.InvalidConfigError{Type: plugins.PluginTypeSink}
	}

	return
}

func (s *Sink) Sink(ctx context.Context, batch []models.Record) (err error) {
	for _, record := range batch {
		asset := record.Data()
		s.logger.Info("sinking record to compass", "record", asset.GetUrn())

		compassPayload, err := s.buildCompassPayload(asset)
		if err != nil {
			return errors.Wrap(err, "failed to build compass payload")
		}
		if err = s.send(compassPayload); err != nil {
			return errors.Wrap(err, "error sending data")
		}

		s.logger.Info("successfully sinked record to compass", "record", asset.GetUrn())
	}

	return
}

func (s *Sink) Close() (err error) { return }

func (s *Sink) send(record RequestPayload) (err error) {
	payloadBytes, err := json.Marshal(record)
	if err != nil {
		return
	}

	// send request
	url := fmt.Sprintf("%s/v1beta1/assets", s.config.Host)
	req, err := http.NewRequest(http.MethodPatch, url, bytes.NewBuffer(payloadBytes))
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
	if res.StatusCode == 200 {
		return
	}

	var bodyBytes []byte
	bodyBytes, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	err = fmt.Errorf("compass returns %d: %v", res.StatusCode, string(bodyBytes))

	switch code := res.StatusCode; {
	case code >= 500:
		return plugins.NewRetryError(err)
	default:
		return err
	}
}

func (s *Sink) buildCompassPayload(asset *v1beta2.Asset) (RequestPayload, error) {
	labels, err := s.buildLabels(asset)
	if err != nil {
		return RequestPayload{}, errors.Wrap(err, "failed to build labels")
	}

	upstreams, downstreams := s.buildLineage(asset)
	owners := s.buildOwners(asset)
	record := RequestPayload{
		Asset: Asset{
			URN:         asset.GetUrn(),
			Type:        asset.GetType(),
			Name:        asset.GetName(),
			Service:     asset.GetService(),
			Description: asset.GetDescription(),
			Owners:      owners,
			Data:        asset,
			Labels:      labels,
		},
		Upstreams:   upstreams,
		Downstreams: downstreams,
	}

	return record, nil
}

func (s *Sink) buildLineage(asset *v1beta2.Asset) (upstreams, downstreams []LineageRecord) {
	lineage := asset.GetLineage()
	if lineage == nil {
		return
	}

	for _, upstream := range lineage.Upstreams {
		upstreams = append(upstreams, LineageRecord{
			URN:     upstream.Urn,
			Type:    upstream.Type,
			Service: upstream.Service,
		})
	}
	for _, downstream := range lineage.Downstreams {
		downstreams = append(downstreams, LineageRecord{
			URN:     downstream.Urn,
			Type:    downstream.Type,
			Service: downstream.Service,
		})
	}

	return
}

func (s *Sink) buildOwners(asset *v1beta2.Asset) (owners []Owner) {
	for _, ownerProto := range asset.GetOwners() {
		owners = append(owners, Owner{
			URN:   ownerProto.Urn,
			Name:  ownerProto.Name,
			Role:  ownerProto.Role,
			Email: ownerProto.Email,
		})
	}
	return
}

func (s *Sink) buildLabels(asset *v1beta2.Asset) (labels map[string]string, err error) {
	if s.config.Labels == nil {
		return
	}

	labels = map[string]string{}
	for key, template := range s.config.Labels {
		var value string
		value, err = s.buildLabelValue(template, asset)
		if err != nil {
			err = errors.Wrapf(err, "could not find \"%s\"", template)
			return
		}

		labels[key] = value
	}

	return
}

func (s *Sink) buildLabelValue(template string, asset *v1beta2.Asset) (value string, err error) {
	fields := strings.Split(template, ".")
	if len(fields) < 3 {
		err = errors.New("label template has to be at least nested 3 levels")
		return
	}

	switch fields[0] {
	case "$properties":
		value, err = s.getLabelValueFromProperties(fields[1], fields[2], asset)
		if err != nil {
			err = errors.Wrapf(err, "error getting label value from $properties")
		}
		return
	}

	err = errors.New("invalid label template format")
	return
}

func (s *Sink) getLabelValueFromProperties(field1 string, field2 string, asset *v1beta2.Asset) (value string, err error) {
	switch field1 {
	case "attributes":
		attr := utils.GetCustomProperties(asset)
		v, ok := attr[field2]
		if !ok {
			err = fmt.Errorf("could not find \"%s\" field on attributes", field2)
			return
		}
		value, ok = v.(string)
		if !ok {
			err = fmt.Errorf("\"%s\" field is not a string", field2)
			return
		}
		return
	case "labels":
		labels := asset.GetLabels()
		if labels == nil {
			err = errors.New("could not find labels field")
			return
		}
		var ok bool
		value, ok = labels[field2]
		if !ok {
			err = fmt.Errorf("could not find \"%s\" from labels", field2)
			return
		}

		return
	}

	err = errors.New("invalid label template format")
	return
}

func init() {
	if err := registry.Sinks.Register("compass", func() plugins.Syncer {
		return New(&http.Client{}, plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
