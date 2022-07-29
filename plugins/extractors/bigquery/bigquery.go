package bigquery

import (
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/json"
	"html/template"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/bigquery/auditlog"
	"github.com/odpf/meteor/plugins/extractors/bigquery/util"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//go:embed README.md
var summary string

// Config holds the set of configuration for the bigquery extractor
type Config struct {
	ProjectID            string   `mapstructure:"project_id" validate:"required"`
	ServiceAccountJSON   string   `mapstructure:"service_account_json"`
	TablePattern         string   `mapstructure:"table_pattern"`
	IncludeColumnProfile bool     `mapstructure:"include_column_profile"`
	MaxPreviewRows       int      `mapstructure:"max_preview_rows" default:"30"`
	IsCollectTableUsage  bool     `mapstructure:"collect_table_usage" default:"false"`
	UsagePeriodInDay     int64    `mapstructure:"usage_period_in_day" default:"7"`
	UsageProjectIDs      []string `mapstructure:"usage_project_ids"`
}

var sampleConfig = `
project_id: google-project-id
table_pattern: gofood.fact_
include_column_profile: true
service_account_json: |-
  {
    "type": "service_account",
    "private_key_id": "xxxxxxx",
    "private_key": "xxxxxxx",
    "client_email": "xxxxxxx",
    "client_id": "xxxxxxx",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "xxxxxxx",
    "client_x509_cert_url": "xxxxxxx"
  }
collect_table_usage: false
usage_period_in_day: 7`

var info = plugins.Info{
	Description:  "Big Query table metadata and metrics",
	SampleConfig: sampleConfig,
	Tags:         []string{"gcp", "table", "extractor"},
	Summary:      summary,
}

// Extractor manages the communication with the bigquery service
type Extractor struct {
	plugins.BaseExtractor
	logger    log.Logger
	client    *bigquery.Client
	config    Config
	galClient *auditlog.AuditLog
}

func New(logger log.Logger) *Extractor {
	galc := auditlog.New(logger)

	e := &Extractor{
		logger:    logger,
		galClient: galc,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	e.client, err = e.createClient(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create client")
	}

	if e.config.IsCollectTableUsage {
		errL := e.galClient.Init(ctx,
			auditlog.InitWithConfig(auditlog.Config{
				ProjectID:           e.config.ProjectID,
				ServiceAccountJSON:  e.config.ServiceAccountJSON,
				IsCollectTableUsage: e.config.IsCollectTableUsage,
				UsagePeriodInDay:    e.config.UsagePeriodInDay,
				UsageProjectIDs:     e.config.UsageProjectIDs,
			}),
		)
		if errL != nil {
			e.logger.Error("failed to create google audit log client", "err", errL)
		}
	}

	return
}

// Extract checks if the table is valid and extracts the table schema
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {

	// Fetch and iterate over datasets
	it := e.client.Datasets(ctx)
	for {
		ds, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return errors.Wrap(err, "failed to fetch dataset")
		}
		e.extractTable(ctx, ds, emit)
	}

	return
}

// Create big query client
func (e *Extractor) createClient(ctx context.Context) (*bigquery.Client, error) {
	if e.config.ServiceAccountJSON == "" {
		e.logger.Info("credentials are not specified, creating bigquery client using default credentials...")
		return bigquery.NewClient(ctx, e.config.ProjectID)
	}

	return bigquery.NewClient(ctx, e.config.ProjectID, option.WithCredentialsJSON([]byte(e.config.ServiceAccountJSON)))
}

// Create big query client
func (e *Extractor) extractTable(ctx context.Context, ds *bigquery.Dataset, emit plugins.Emit) {
	tb := ds.Tables(ctx)
	for {
		table, err := tb.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			e.logger.Error("failed to get table, skipping table", "err", err)
			continue
		}
		e.logger.Debug("extracting table", "table", table.FullyQualifiedName())
		tmd, err := table.Metadata(ctx)
		if err != nil {
			e.logger.Error("failed to fetch table metadata", "err", err, "table", table.FullyQualifiedName())
			continue
		}

		emit(models.NewRecord(e.buildTable(ctx, table, tmd)))
	}
}

// Build the bigquery table metadata
func (e *Extractor) buildTable(ctx context.Context, t *bigquery.Table, md *bigquery.TableMetadata) *assetsv1beta1.Table {
	var tableStats *auditlog.TableStats
	if e.config.IsCollectTableUsage {
		// Fetch and extract logs first to build a map
		var errL error
		tableStats, errL = e.galClient.Collect(ctx, t.TableID)
		if errL != nil {
			e.logger.Warn("error populating table stats usage", "error", errL)
		}
	}

	tableFQN := t.FullyQualifiedName()
	tableURN := util.TableURN(t.ProjectID, t.DatasetID, t.TableID)

	tableProfile := e.buildTableProfile(tableURN, tableStats)

	var partitionField string
	if md.TimePartitioning != nil {
		partitionField = md.TimePartitioning.Field
	}

	var preview *facetsv1beta1.Preview
	if md.Type == bigquery.RegularTable {
		var err error
		preview, err = e.buildPreview(ctx, t)
		if err != nil {
			e.logger.Warn("error building preview", "err", err, "table", tableFQN)
		}
	}

	return &assetsv1beta1.Table{
		Resource: &commonv1beta1.Resource{
			Urn:         tableURN,
			Name:        t.TableID,
			Type:        "table",
			Description: md.Description,
			Service:     "bigquery",
		},
		Schema: &facetsv1beta1.Columns{
			Columns: e.buildColumns(ctx, md),
		},
		Preview: preview,
		Properties: &facetsv1beta1.Properties{
			Attributes: utils.TryParseMapToProto(map[string]interface{}{
				"full_qualified_name": tableFQN,
				"dataset":             t.DatasetID,
				"project":             t.ProjectID,
				"type":                string(md.Type),
				"partition_field":     partitionField,
			}),
			Labels: md.Labels,
		},
		Profile: tableProfile,
		Timestamps: &commonv1beta1.Timestamp{
			CreateTime: timestamppb.New(md.CreationTime),
			UpdateTime: timestamppb.New(md.LastModifiedTime),
		},
	}
}

// Extract table schema
func (e *Extractor) buildColumns(ctx context.Context, tm *bigquery.TableMetadata) []*facetsv1beta1.Column {
	schema := tm.Schema
	var wg sync.WaitGroup

	wg.Add(len(schema))
	columns := make([]*facetsv1beta1.Column, len(schema))
	for i, b := range schema {
		index := i
		go func(s *bigquery.FieldSchema) {
			defer wg.Done()

			columns[index] = e.buildColumn(ctx, s, tm)
		}(b)
	}
	wg.Wait()

	return columns
}

func (e *Extractor) buildColumn(ctx context.Context, field *bigquery.FieldSchema, tm *bigquery.TableMetadata) (col *facetsv1beta1.Column) {
	col = &facetsv1beta1.Column{
		Name:        field.Name,
		Description: field.Description,
		DataType:    string(field.Type),
		IsNullable:  !(field.Required || field.Repeated),
		Properties: &facetsv1beta1.Properties{
			Attributes: utils.TryParseMapToProto(map[string]interface{}{
				"mode": e.getColumnMode(field),
			}),
		},
	}

	if e.config.IncludeColumnProfile {
		profile, err := e.getColumnProfile(ctx, field, tm)
		if err != nil {
			e.logger.Error("error fetching column's profile", "error", err)
		}
		col.Profile = profile
	}

	return
}

func (e *Extractor) buildPreview(ctx context.Context, t *bigquery.Table) (preview *facetsv1beta1.Preview, err error) {
	preview = &facetsv1beta1.Preview{
		Fields: []string{},
	}
	if e.config.MaxPreviewRows == 0 {
		return
	}

	rows := []interface{}{}
	totalRows := 0
	ri := t.Read(ctx)
	for totalRows < e.config.MaxPreviewRows {
		var row []bigquery.Value
		err = ri.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return
		}

		// populate row fields once
		if len(preview.Fields) < 1 {
			for _, schema := range ri.Schema {
				preview.Fields = append(preview.Fields, schema.Name)
			}
		}

		var temp []interface{}
		var jsonBytes []byte
		jsonBytes, err = json.Marshal(row)
		if err != nil {
			err = errors.Wrapf(err, "error marshalling \"%s\" to json", t.FullyQualifiedName())
			return
		}
		// sanitize unicode sequence
		// replace unicode null characters with "null" string to ensure downstream would not have issues dealing with unicode null characters
		jsonString := strings.ReplaceAll(string(jsonBytes), "\\u0000", "null")
		jsonBytes = []byte(jsonString)
		err = json.Unmarshal(jsonBytes, &temp)
		if err != nil {
			err = errors.Wrapf(err, "error marshalling \"%s\" to json", t.FullyQualifiedName())
			return
		}

		rows = append(rows, temp)

		totalRows++
	}

	preview.Rows, err = structpb.NewList(rows)
	if err != nil {
		err = errors.Wrap(err, "error creating preview list")
		return
	}

	return
}

func (e *Extractor) getColumnProfile(ctx context.Context, col *bigquery.FieldSchema, tm *bigquery.TableMetadata) (cp *facetsv1beta1.ColumnProfile, err error) {
	if col.Type == bigquery.BytesFieldType || col.Repeated || col.Type == bigquery.RecordFieldType {
		e.logger.Info("Skip profiling " + col.Name + " column")
		return
	}

	// build and run query
	query, err := e.buildColumnProfileQuery(col, tm)
	if err != nil {
		return nil, err
	}

	it, err := query.Read(ctx)
	if err != nil {
		return nil, err
	}

	// fetch first row for column profile result
	type Row struct {
		Min    string  `bigquery:"min"`
		Max    string  `bigquery:"max"`
		Avg    float64 `bigquery:"avg"`
		Med    float64 `bigquery:"med"`
		Unique int64   `bigquery:"unique"`
		Count  int64   `bigquery:"count"`
		Top    string  `bigquery:"top"`
	}
	var row Row
	err = it.Next(&row)
	if err != nil && err != iterator.Done {
		return
	}

	// map row data to column profile
	cp = &facetsv1beta1.ColumnProfile{
		Min:    row.Min,
		Max:    row.Max,
		Avg:    row.Avg,
		Med:    row.Med,
		Unique: row.Unique,
		Count:  row.Count,
		Top:    row.Top,
	}

	return
}

func (e *Extractor) buildColumnProfileQuery(col *bigquery.FieldSchema, tm *bigquery.TableMetadata) (query *bigquery.Query, err error) {
	queryTemplate := `SELECT
		COALESCE(CAST(MIN({{ .ColumnName }}) AS STRING), "") AS min,
		COALESCE(CAST(MAX({{ .ColumnName }}) AS STRING), "") AS max,
		COALESCE(AVG(SAFE_CAST(SAFE_CAST({{ .ColumnName }} AS STRING) AS FLOAT64)), 0.0) AS avg,
		COALESCE(SAFE_CAST(CAST(APPROX_QUANTILES({{ .ColumnName }}, 2)[OFFSET(1)] AS STRING) AS FLOAT64), 0.0) AS med,
		COALESCE(APPROX_COUNT_DISTINCT({{ .ColumnName }}),0) AS unique,
		COALESCE(COUNT({{ .ColumnName }}), 0) AS count,
		COALESCE(CAST(APPROX_TOP_COUNT({{ .ColumnName }}, 1)[OFFSET(0)].value AS STRING), "") AS top
	FROM
		{{ .TableName }}`
	data := map[string]interface{}{
		"ColumnName": col.Name,
		"TableName":  strings.ReplaceAll(tm.FullID, ":", "."),
	}
	temp := template.Must(template.New("query").Parse(queryTemplate))
	builder := &strings.Builder{}
	err = temp.Execute(builder, data)
	if err != nil {
		return
	}
	finalQuery := builder.String()
	query = e.client.Query(finalQuery)

	return
}

func (e *Extractor) getColumnMode(col *bigquery.FieldSchema) string {
	switch {
	case col.Repeated:
		return "REPEATED"
	case col.Required:
		return "REQUIRED"
	default:
		return "NULLABLE"
	}
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("bigquery", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
