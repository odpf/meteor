package tableau

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"

	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//go:embed README.md
var summary string

var sampleConfig = `
host: https://server.tableau.com
version: 3.12
identifier: my-tableau
username: meteor_user
password: xxxxxxxxxx
sitename: testdev550928
`

// Config that holds a set of configuration for tableau extractor
type Config struct {
	Host       string `mapstructure:"host" validate:"required"`
	Version    string `mapstructure:"version" validate:"required"` // float as string
	Identifier string `mapstructure:"identifier" validate:"required"`
	Username   string `mapstructure:"username"`
	Password   string `mapstructure:"password" validate:"required_with=Username"`
	AuthToken  string `mapstructure:"auth_token" validate:"required_without=Username"`
	SiteID     string `mapstructure:"site_id" validate:"required_without=Username"`
	Sitename   string `mapstructure:"sitename"`
}

// Extractor manages the extraction of data
// from tableau server
type Extractor struct {
	config     Config
	logger     log.Logger
	httpClient *http.Client
	client     Client
}

// Option provides extension abstraction to Extractor constructor
type Option func(*Extractor)

// WithHTTPClient assign custom http client to the Extractor constructor
func WithHTTPClient(hcl *http.Client) Option {
	return func(e *Extractor) {
		e.httpClient = hcl
	}
}

// New returns pointer to an initialized Extractor Object
func New(logger log.Logger, opts ...Option) *Extractor {
	e := &Extractor{
		logger: logger,
	}

	for _, opt := range opts {
		opt(e)
	}

	e.client = NewClient(e.httpClient)
	return e
}

// Info returns the brief information of the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Dashboard list from Tableau server",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"oss", "extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	// build and validate config
	err = utils.BuildConfig(configMap, &e.config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	err = e.client.Init(ctx, e.config)
	if err != nil {
		return
	}

	return nil
}

// Extract collects metadata from the source. The metadata is collected through the out channel
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	projects, err := e.client.GetAllProjects(ctx)
	if err != nil {
		err = errors.Wrap(err, "failed to fetch list of projects")
		return
	}

	for _, proj := range projects {
		workbooks, errC := e.client.GetDetailedWorkbooksByProjectName(ctx, proj.Name)
		if errC != nil {
			e.logger.Warn("failed to fetch list of workbook", "err", errC.Error())
			continue
		}
		for _, wb := range workbooks {
			dashboard, errB := e.buildDashboard(wb)
			if errB != nil {
				e.logger.Error("failed to build dashboard", "data", wb, "err", errB.Error())
				continue
			}
			emit(models.NewRecord(dashboard))
		}
	}
	return
}

func (e *Extractor) buildDashboard(wb *Workbook) (data *assetsv1beta1.Dashboard, err error) {
	lineages := e.buildLineage(wb.UpstreamTables)
	dashboardURN := models.DashboardURN("tableau", e.config.Identifier, fmt.Sprintf("workbook/%s", wb.ID))
	data = &assetsv1beta1.Dashboard{
		Resource: &commonv1beta1.Resource{
			Urn:         dashboardURN,
			Name:        wb.Name,
			Service:     "tableau",
			Type:        "dashboard",
			Description: wb.Description,
		},
		Charts: e.buildCharts(dashboardURN, wb, lineages),
		Properties: &facetsv1beta1.Properties{
			Attributes: utils.TryParseMapToProto(map[string]interface{}{
				"id":           wb.ID,
				"name":         wb.Name,
				"project_name": wb.ProjectName,
				"uri":          wb.URI,
				"owner_id":     wb.Owner.ID,
				"owner_name":   wb.Owner.Name,
				"owner_email":  wb.Owner.Email,
			}),
		},
		Ownership: &facetsv1beta1.Ownership{
			Owners: []*facetsv1beta1.Owner{
				{
					Urn:   wb.Owner.Email,
					Name:  wb.Owner.Name,
					Email: wb.Owner.Email,
				},
			},
		},
		Lineage: lineages,
		Timestamps: &commonv1beta1.Timestamp{
			CreateTime: timestamppb.New(wb.CreatedAt),
			UpdateTime: timestamppb.New(wb.UpdatedAt),
		},
	}
	return
}

func (e *Extractor) buildCharts(dashboardURN string, wb *Workbook, lineages *facetsv1beta1.Lineage) (charts []*assetsv1beta1.Chart) {
	for _, sh := range wb.Sheets {
		chartURN := models.DashboardURN("tableau", e.config.Identifier, fmt.Sprintf("sheet/%s", sh.ID))
		charts = append(charts, &assetsv1beta1.Chart{
			Urn:          chartURN,
			Name:         sh.Name,
			DashboardUrn: dashboardURN,
			Source:       "tableau",
			Properties: &facetsv1beta1.Properties{
				Attributes: utils.TryParseMapToProto(map[string]interface{}{
					"id":   sh.ID,
					"name": sh.Name,
				}),
			},
			Timestamps: &commonv1beta1.Timestamp{
				CreateTime: timestamppb.New(sh.CreatedAt),
				UpdateTime: timestamppb.New(sh.UpdatedAt),
			},
		})
	}
	return
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("tableau", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
