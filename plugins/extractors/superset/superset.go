package superset

import (
	"bytes"
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
)

//go:embed README.md
var summary string

// Config holds the set of configuration for the superset extractor
type Config struct {
	Username string `mapstructure:"username" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
	Host     string `mapstructure:"host" validate:"required"`
	Provider string `mapstructure:"provider" validate:"required"`
}

var sampleConfig = `
username: meteor_tester
password: meteor_pass_1234
host: http://localhost:3000
provider: db`

// Extractor manages the extraction of data
// from the superset server
type Extractor struct {
	config      Config
	accessToken string
	csrfToken   string
	logger      log.Logger
	client      *http.Client
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

// Info returns the brief information of the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Dashboard list from Superset server.",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"oss", "extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

// Init initializes the extractor
func (e *Extractor) Init(_ context.Context, configMap map[string]interface{}) (err error) {
	// build and validate config
	if err = utils.BuildConfig(configMap, &e.config); err != nil {
		return plugins.InvalidConfigError{}
	}
	e.client = &http.Client{
		Timeout: 4 * time.Second,
	}
	// get access token for further api calls in superset
	if e.accessToken, err = e.getAccessToken(); err != nil {
		return errors.Wrap(err, "failed to get access token")
	}

	// get csrf token for secure api calls in superset
	if e.csrfToken, err = e.getCsrfToken(); err != nil {
		return errors.Wrap(err, "failed to get csrf token")
	}
	return
}

// Extract collects metadata of each dashboard through emitter
func (e *Extractor) Extract(_ context.Context, emit plugins.Emit) (err error) {
	dashboards, err := e.getDashboardsList()
	if err != nil {
		return errors.Wrap(err, "failed to get dashboard list")
	}
	for _, dashboard := range dashboards {
		data, err := e.buildDashboard(dashboard.ID)
		if err != nil {
			return errors.Wrap(err, "failed to build dashbaord")
		}
		emit(models.NewRecord(data))
	}
	return
}

// buildDashboard builds a dashboard from superset server
func (e *Extractor) buildDashboard(id int) (data *assetsv1beta1.Dashboard, err error) {
	var dashboard Dashboard
	chart, err := e.getChartsList(id)
	if err != nil {
		err = errors.Wrap(err, "failed to get chart list")
		return
	}
	data = &assetsv1beta1.Dashboard{
		Resource: &commonv1beta1.Resource{
			Urn:     fmt.Sprintf("superset.%s", dashboard.DashboardTitle),
			Name:    dashboard.DashboardTitle,
			Service: "superset",
			Url:     dashboard.URL,
			Type:    "dashboard",
		},
		Charts: chart,
	}
	return
}

// getDashboardsList gets a list of dashboards from superset server
func (e *Extractor) getDashboardsList() (dashboards []Dashboard, err error) {
	type response struct {
		Result []Dashboard `json:"result"`
	}
	var data response
	if err = e.makeRequest("GET", e.config.Host+"/api/v1/dashboard", nil, &data); err != nil {
		err = errors.Wrap(err, "failed to get dashboard")
		return
	}
	return data.Result, nil
}

// getChartsList gets a list of charts from superset server
func (e *Extractor) getChartsList(id int) (charts []*assetsv1beta1.Chart, err error) {
	type responseChart struct {
		Result []Chart `json:"result"`
	}
	var data responseChart
	if err = e.makeRequest("GET",
		fmt.Sprintf("%s/api/v1/dashboard/%d/charts", e.config.Host, id), nil, &data); err != nil {
		err = errors.Wrap(err, "failed to get list of chart details")
		return
	}
	var tempCharts []*assetsv1beta1.Chart
	for _, res := range data.Result {
		var tempChart assetsv1beta1.Chart
		tempChart.Name = res.SliceName
		tempChart.Source = "superset"
		tempChart.Description = res.Description
		tempChart.Url = res.SliceUrl
		tempChart.DataSource = res.Datasource
		tempChart.DashboardUrn = "dashboard:" + strconv.Itoa(id)
		tempCharts = append(tempCharts, &tempChart)
	}
	return tempCharts, nil
}

//getAccessToken authenticate and get a JWT access token
func (e *Extractor) getAccessToken() (accessToken string, err error) {
	payload := map[string]interface{}{
		"username": e.config.Username,
		"password": e.config.Password,
		"provider": e.config.Provider,
	}
	type responseToken struct {
		Token string `json:"access_token"`
	}
	var data responseToken
	if err = e.makeRequest("POST", e.config.Host+"/api/v1/security/login", payload, &data); err != nil {
		return "", errors.Wrap(err, "failed to fetch access token")
	}
	return data.Token, nil
}

// getCsrfToken fetch the CSRF token
func (e *Extractor) getCsrfToken() (csrfToken string, err error) {
	type responseCsrfToken struct {
		CsrfToken string `json:"result"`
	}
	var data responseCsrfToken
	if err = e.makeRequest("GET", e.config.Host+"/api/v1/security/csrf_token/", nil, &data); err != nil {
		return "", errors.Wrap(err, "failed to fetch csrf token")
	}
	return data.CsrfToken, nil
}

// makeRequest helper function to avoid rewriting a request
func (e *Extractor) makeRequest(method, url string, payload interface{}, data interface{}) (err error) {
	jsonifyPayload, err := json.Marshal(payload)
	if err != nil {
		return errors.Wrap(err, "failed to encode the payload JSON")
	}
	body := bytes.NewBuffer(jsonifyPayload)
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}
	var bearer = "Bearer " + e.accessToken
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", bearer)
	req.Header.Set("X-CSRFToken", e.csrfToken)
	req.Header.Set("Referer", url)

	res, err := e.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to generate response")
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return errors.Wrapf(err, "response failed with status code: %d", res.StatusCode)
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return errors.Wrap(err, "failed to read response body")
	}
	if err = json.Unmarshal(b, &data); err != nil {
		return errors.Wrapf(err, "failed to parse: %s", string(b))
	}
	return
}

// init register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("superset", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
