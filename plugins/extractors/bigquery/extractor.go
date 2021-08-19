package bigquery

import (
	"context"
	"fmt"
	"html/template"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/common"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Config struct {
	ProjectID            string `mapstructure:"project_id" validate:"required"`
	ServiceAccountJSON   string `mapstructure:"service_account_json"`
	TablePattern         string `mapstructure:"table_pattern"`
	IncludeColumnProfile bool   `mapstructure:"include_column_profile"`
}

type Extractor struct {
	logger plugins.Logger
	client *bigquery.Client
	config Config
}

func (e *Extractor) Extract(ctx context.Context, config map[string]interface{}, out chan<- interface{}) (err error) {
	err = utils.BuildConfig(config, &e.config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	e.client, err = e.createClient(ctx)
	if err != nil {
		return
	}

	// Fetch and iterate over datesets
	it := e.client.Datasets(ctx)
	for {
		ds, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			e.logger.Error("failed to fetch, skipping dataset", "err", err)
			continue
		}
		e.extractTable(ctx, ds, out)
		break
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
func (e *Extractor) extractTable(ctx context.Context, ds *bigquery.Dataset, out chan<- interface{}) {
	tb := ds.Tables(ctx)
	for {
		table, err := tb.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			e.logger.Error("failed to scan, skipping table", "err", err)
			continue
		}
		tmd, err := table.Metadata(ctx)
		if err != nil {
			e.logger.Error("failed to fetch table's metadata, skipping table", "err", err)
			continue
		}

		out <- e.buildTable(ctx, table, tmd)
		break
	}
}

// Build the bigquery table metadata
func (e *Extractor) buildTable(ctx context.Context, t *bigquery.Table, md *bigquery.TableMetadata) meta.Table {
	return meta.Table{
		Urn:    fmt.Sprintf("%s:%s.%s", t.ProjectID, t.DatasetID, t.TableID),
		Name:   t.TableID,
		Source: "bigquery",
		Schema: &facets.Columns{
			Columns: e.buildColumns(ctx, md),
		},
		Custom: &facets.Custom{
			CustomProperties: map[string]string{
				"dataset": t.DatasetID,
				"project": t.ProjectID,
				"type":    string(md.Type),
			},
		},
		Tags: &facets.Tags{
			Tags: md.Labels,
		},
		Timestamps: &common.Timestamp{
			CreatedAt: timestamppb.New(md.CreationTime),
			UpdatedAt: timestamppb.New(md.LastModifiedTime),
		},
	}
}

// Extract table schema
func (e *Extractor) buildColumns(ctx context.Context, tm *bigquery.TableMetadata) []*facets.Column {
	schema := tm.Schema
	var wg sync.WaitGroup

	wg.Add(len(schema))
	columns := make([]*facets.Column, len(schema))
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

func (e *Extractor) buildColumn(ctx context.Context, field *bigquery.FieldSchema, tm *bigquery.TableMetadata) (col *facets.Column) {
	col = &facets.Column{
		Name:        field.Name,
		Description: field.Description,
		DataType:    string(field.Type),
		IsNullable:  !(field.Required || field.Repeated),
		Custom: &facets.Custom{
			CustomProperties: map[string]string{
				"mode": e.getColumnMode(field),
			},
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

func (e *Extractor) getColumnProfile(ctx context.Context, col *bigquery.FieldSchema, tm *bigquery.TableMetadata) (cp *facets.ColumnProfile, err error) {
	if col.Type == bigquery.BytesFieldType || col.Repeated || col.Type == bigquery.RecordFieldType {
		e.logger.Info("Skip profiling " + col.Name + " column")
		return
	}

	// build and run query
	query, err := e.buildColumnProfileQuery(col, tm)
	it, err := query.Read(ctx)
	if err != nil {
		return nil, err
	}

	// fetch first row for column profile result
	type Row struct {
		Min    string  `bigquery:"min"`
		Max    string  `bigquery:"max"`
		Avg    float32 `bigquery:"avg"`
		Med    float32 `bigquery:"med"`
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
	cp = &facets.ColumnProfile{
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
		return &Extractor{
			logger: plugins.Log,
		}
	}); err != nil {
		panic(err)
	}
}
