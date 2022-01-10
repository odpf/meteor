package grafana

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"net/http"

	"github.com/pkg/errors"

	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

// Config hold 
type Config struct {
	BaseURL string `mapstructure:"base_url" validate:"required"`
	APIKey  string `mapstructure:"api_key" validate:"required"`
}

var sampleConfig = `
base_url: grafana_server
api_key: your_api_key`

// Extractor manages the communication with the Grafana Server
type Extractor struct {
	client *Client
	config Config
	logger log.Logger
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

// Info returns the brief information about the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Dashboard list from Grafana server.",
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
func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	// build config
	err = utils.BuildConfig(configMap, &e.config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	// build client
	e.client = NewClient(&http.Client{}, e.config)

	return
}

// Extract checks if the extractor is configured and
// if so, then it extracts the assets from the extractor.
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	uids, err := e.client.SearchAllDashboardUIDs()
	if err != nil {
		return errors.Wrap(err, "failed to fetch dashboards")
	}
	dashboardDetails, err := e.client.GetAllDashboardDetails(uids)
	if err != nil {
		return errors.Wrap(err, "failed to fetch dashboard details")
	}

	for _, dashboardDetail := range dashboardDetails {
		dashboard := e.grafanaDashboardToMeteorDashboard(dashboardDetail)
		emit(models.NewRecord(dashboard))
	}

	return
}

// grafanaDashboardToMeteorDashboard converts a grafana dashboard to a meteor dashboard
func (e *Extractor) grafanaDashboardToMeteorDashboard(dashboard DashboardDetail) *assetsv1beta1.Dashboard {
	charts := make([]*assetsv1beta1.Chart, len(dashboard.Dashboard.Panels))
	for i, panel := range dashboard.Dashboard.Panels {
		c := e.grafanaPanelToMeteorChart(panel, dashboard.Dashboard.UID, dashboard.Meta.URL)
		charts[i] = &c
	}
	return &assetsv1beta1.Dashboard{
		Resource: &commonv1beta1.Resource{
			Urn:         fmt.Sprintf("grafana.%s", dashboard.Dashboard.UID),
			Name:        dashboard.Meta.Slug,
			Service:     "grafana",
			Url:         dashboard.Meta.URL,
			Description: dashboard.Dashboard.Description,
		},
		Charts: charts,
	}
}

// grafanaPanelToMeteorChart converts a grafana panel to a meteor chart
func (e *Extractor) grafanaPanelToMeteorChart(panel Panel, dashboardUID string, metaURL string) assetsv1beta1.Chart {
	var rawQuery string
	if len(panel.Targets) > 0 {
		rawQuery = panel.Targets[0].RawSQL
	}
	return assetsv1beta1.Chart{
		Urn:             fmt.Sprintf("%s.%d", dashboardUID, panel.ID),
		Name:            panel.Title,
		Type:            panel.Type,
		Source:          "grafana",
		Description:     panel.Description,
		DataSource:      panel.DataSource,
		RawQuery:        rawQuery,
		Url:             fmt.Sprintf("%s?viewPanel=%d", metaURL, panel.ID),
		DashboardUrn:    fmt.Sprintf("grafana.%s", dashboardUID),
		DashboardSource: "grafana",
	}
}

// init registers the extractor to catalog
func init() {
	if err := registry.Extractors.Register("grafana", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
