package clickhouse

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"

	"github.com/pkg/errors"

	_ "github.com/ClickHouse/clickhouse-go" // clickhouse driver
	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

// Config holds the connection URL for the extractor
type Config struct {
	ConnectionURL string `mapstructure:"connection_url" validate:"required"`
}

var sampleConfig = `
connection_url: "tcp://localhost:3306?username=admin&password=pass123&debug=true"`

// Extractor manages the output stream
// and logger interface for the extractor
type Extractor struct {
	config Config
	logger log.Logger
	db     *sql.DB
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
		Description:  "Column-oriented DBMS for online analytical processing.",
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
	if err = utils.BuildConfig(configMap, &e.config); err != nil {
		return plugins.InvalidConfigError{}
	}

	if e.db, err = sql.Open("clickhouse", e.config.ConnectionURL); err != nil {
		return errors.Wrap(err, "failed to create a client")
	}

	return
}

//Extract checks if the extractor is configured and
// if the connection to the DB is successful
// and then starts the extraction process
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	err = e.extractTables(emit)
	if err != nil {
		return errors.Wrap(err, "failed to extract tables")
	}

	return
}

// extractTables extract tables from a given database
func (e *Extractor) extractTables(emit plugins.Emit) (err error) {
	res, err := e.db.Query("SELECT name, database FROM system.tables WHERE database not like 'system'")
	if err != nil {
		return errors.Wrap(err, "failed to execute query")
	}
	for res.Next() {
		var dbName, tableName string
		err = res.Scan(&tableName, &dbName)
		if err != nil {
			return
		}

		var columns []*facetsv1beta1.Column
		columns, err = e.getColumnsInfo(dbName, tableName)
		if err != nil {
			return
		}

		emit(models.NewRecord(&assetsv1beta1.Table{
			Resource: &commonv1beta1.Resource{
				Urn:  fmt.Sprintf("%s.%s", dbName, tableName),
				Name: tableName,
				Type: "table",
			}, Schema: &facetsv1beta1.Columns{
				Columns: columns,
			},
		}))
	}
	return
}

func (e *Extractor) getColumnsInfo(dbName string, tableName string) (result []*facetsv1beta1.Column, err error) {
	sqlStr := fmt.Sprintf("DESCRIBE TABLE %s.%s", dbName, tableName)

	rows, err := e.db.Query(sqlStr)
	if err != nil {
		err = errors.Wrapf(err, "failed to execute query %s", sqlStr)
		return
	}
	for rows.Next() {
		var colName, colDesc, dataType string
		var temp1, temp2, temp3, temp4 string
		err = rows.Scan(&colName, &dataType, &colDesc, &temp1, &temp2, &temp3, &temp4)
		if err != nil {
			return
		}
		result = append(result, &facetsv1beta1.Column{
			Name:        colName,
			DataType:    dataType,
			Description: colDesc,
		})
	}
	return result, nil
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("clickhouse", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
