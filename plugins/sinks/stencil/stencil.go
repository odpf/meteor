package stencil

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
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
)

//go:embed README.md
var summary string

// Config holds the set of configuration options for the sink
type Config struct {
	Host        string `mapstructure:"host" validate:"required"`
	NamespaceID string `mapstructure:"namespace_id" validate:"required"`
	Format      string `mapstructure:"format" validate:"oneof=json avro" default:"json"`
}

var info = plugins.Info{
	Description: "Send metadata to stencil http service",
	Summary:     summary,
	Tags:        []string{"http", "sink"},
	SampleConfig: `
	# The hostname of the stencil service
	host: https://stencil.com
	# The namespace ID of the stencil service
	namespace_id: myNamespace
	# The schema format in which data will sink to stencil
	format: avro
	`,
}

// httpClient holds the set of methods require for creating request
type httpClient interface {
	Do(*http.Request) (*http.Response, error)
}

// Sink manages the sinking of data to Stencil
type Sink struct {
	plugins.BasePlugin
	client httpClient
	config Config
	logger log.Logger
}

// New returns a pointer to an initialized Sink Object
func New(c httpClient, logger log.Logger) plugins.Syncer {
	s := &Sink{
		logger: logger,
		client: c,
	}
	s.BasePlugin = plugins.NewBasePlugin(info, &s.config)

	return s
}

// Init initializes the sink
func (s *Sink) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = s.BasePlugin.Init(ctx, config); err != nil {
		return err
	}

	return
}

// Sink helps to sink record to stencil
func (s *Sink) Sink(_ context.Context, batch []models.Record) (err error) {
	var stencilPayload interface{}

	for _, record := range batch {
		metadata := record.Data()

		table, ok := metadata.(*assetsv1beta1.Table)
		if !ok {
			continue
		}
		s.logger.Info("sinking record to stencil", "record", table.GetResource().Urn)

		switch s.config.Format {
		case "avro":
			stencilPayload, err = s.buildAvroStencilPayload(table)
		case "json":
			stencilPayload, err = s.buildJsonStencilPayload(table)
		}

		if err != nil {
			return errors.Wrap(err, "failed to build stencil payload")
		}
		if err = s.send(table.Resource.Urn, stencilPayload); err != nil {
			return errors.Wrap(err, "error sending data")
		}

		s.logger.Info("successfully sinked record to stencil", "record", table.GetResource().Urn)
	}

	return
}

// Close will be called once after everything is done
func (s *Sink) Close() (err error) { return }

// buildJsonStencilPayload build json stencil payload
func (s *Sink) buildJsonStencilPayload(table *assetsv1beta1.Table) (JsonSchema, error) {
	resource := table.GetResource()
	jsonProperties := buildJsonProperties(table)

	record := JsonSchema{
		Id:         resource.GetUrn() + ".json",
		Schema:     "https://json-schema.org/draft/2020-12/schema",
		Title:      resource.GetName(),
		Type:       JsonTypeObject,
		Properties: jsonProperties,
	}

	return record, nil
}

// buildAvroStencilPayload build Json stencil payload
func (s *Sink) buildAvroStencilPayload(table *assetsv1beta1.Table) (AvroSchema, error) {
	resource := table.GetResource()
	avroFields := buildAvroFields(table)

	record := AvroSchema{
		Type:      "record",
		Namespace: s.config.NamespaceID,
		Name:      resource.GetName(),
		Fields:    avroFields,
	}

	return record, nil
}

// send helps to pass data to stencil
func (s *Sink) send(tableURN string, record interface{}) (err error) {
	payloadBytes, err := json.Marshal(record)
	if err != nil {
		return
	}

	// send request
	schemaID := strings.ReplaceAll(tableURN, "/", ".")
	url := fmt.Sprintf("%s/v1beta1/namespaces/%s/schemas/%s", s.config.Host, s.config.NamespaceID, schemaID)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return
	}

	if s.config.Format == "json" {
		req.Header.Add("X-Compatibility", "COMPATIBILITY_UNSPECIFIED")
	}

	res, err := s.client.Do(req)
	if err != nil {
		return
	}
	if res.StatusCode == http.StatusCreated {
		return
	}

	var bodyBytes []byte
	bodyBytes, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	err = fmt.Errorf("stencil returns %d: %v", res.StatusCode, string(bodyBytes))

	switch code := res.StatusCode; {
	case code >= 500:
		return plugins.NewRetryError(err)
	default:
		return err
	}
}

// buildJsonProperties builds the json schema properties
func buildJsonProperties(table *assetsv1beta1.Table) map[string]JsonProperty {
	columnRecord := make(map[string]JsonProperty)
	service := table.GetResource().GetService()
	schema := table.GetSchema()
	if schema == nil {
		return nil
	}
	columns := schema.GetColumns()
	if len(columns) == 0 {
		return nil
	}

	for _, column := range columns {
		dataType := typeToJsonSchemaType(service, column.DataType)
		columnType := []JsonType{dataType}

		if column.IsNullable {
			columnType = append(columnType, JsonTypeNull)
		}

		columnRecord[column.Name] = JsonProperty{
			Type:        columnType,
			Description: column.GetDescription(),
		}
	}

	return columnRecord
}

// typeToJsonSchemaType converts particular service type to Json type
func typeToJsonSchemaType(service string, columnType string) (dataType JsonType) {

	if service == "bigquery" {
		switch columnType {
		case "STRING", "DATE", "DATETIME", "TIME", "TIMESTAMP", "GEOGRAPHY":
			dataType = JsonTypeString
		case "INT64", "NUMERIC", "FLOAT64", "INT", "FLOAT", "BIGNUMERIC":
			dataType = JsonTypeNumber
		case "BYTES":
			dataType = JsonTypeArray
		case "BOOLEAN":
			dataType = JsonTypeBoolean
		case "RECORD":
			dataType = JsonTypeObject
		default:
			dataType = JsonTypeString
		}
	}
	if service == "postgres" {
		switch columnType {
		case "uuid", "integer", "decimal", "smallint", "bigint", "bit", "bit varying", "numeric", "real", "double precision", "cidr", "inet", "macaddr", "serial", "bigserial", "money":
			dataType = JsonTypeNumber
		case "varchar", "text", "character", "character varying", "date", "time", "timestamp", "interval", "point", "line", "path":
			dataType = JsonTypeString
		case "boolean":
			dataType = JsonTypeBoolean
		case "bytea", "integer[]", "character[]", "text[]":
			dataType = JsonTypeArray
		default:
			dataType = JsonTypeString
		}
	}

	return
}

// buildAvroFields builds the avro schema fields
func buildAvroFields(table *assetsv1beta1.Table) (fields []AvroFields) {
	service := table.GetResource().GetService()
	schema := table.GetSchema()
	if schema == nil {
		return
	}
	columns := schema.GetColumns()
	if len(columns) == 0 {
		return
	}

	for _, column := range columns {
		dataType := typeToAvroSchemaType(service, column.DataType)
		columnType := []AvroType{dataType}

		if column.IsNullable {
			columnType = []AvroType{dataType, AvroTypeNull}
		}

		fields = append(fields, AvroFields{
			Name: column.Name,
			Type: columnType,
		})
	}

	return fields
}

// typeToAvroSchemaType converts particular service type to avro type
func typeToAvroSchemaType(service string, columnType string) (dataType AvroType) {

	if service == "bigquery" {
		switch columnType {
		case "STRING", "DATE", "DATETIME", "TIME", "TIMESTAMP", "GEOGRAPHY":
			dataType = AvroTypeString
		case "INT64", "NUMERIC", "INT", "BIGNUMERIC":
			dataType = AvroTypeInteger
		case "FLOAT64", "FLOAT":
			dataType = AvroTypeFloat
		case "BYTES":
			dataType = AvroTypeBytes
		case "BOOLEAN":
			dataType = AvroTypeBoolean
		case "RECORD":
			dataType = AvroTypeRecord
		default:
			dataType = AvroTypeString
		}
	}
	if service == "postgres" {
		switch columnType {
		case "uuid", "integer", "decimal", "smallint", "bigint", "bit", "bit varying", "numeric", "real", "double precision", "cidr", "inet", "macaddr", "serial", "bigserial", "money":
			dataType = AvroTypeInteger
		case "varchar", "text", "character", "character varying", "date", "time", "timestamp", "interval", "point", "line", "path":
			dataType = AvroTypeString
		case "boolean":
			dataType = AvroTypeBoolean
		case "bytea", "integer[]", "character[]", "text[]":
			dataType = AvroTypeArray
		default:
			dataType = AvroTypeString
		}
	}

	return
}

// init register the sink to the catalog
func init() {
	if err := registry.Sinks.Register("stencil", func() plugins.Syncer {
		return New(&http.Client{}, plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
