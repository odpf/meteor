package snowflake

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"
	"net/http"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	"github.com/snowflakedb/gosnowflake"
	_ "github.com/snowflakedb/gosnowflake" // used to register the snowflake driver

	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
)

//go:embed README.md
var summary string

// Config holds the connection URL for the extractor
type Config struct {
	ConnectionURL string `mapstructure:"connection_url" validate:"required"`
}

var sampleConfig = `connection_url: "user:password@my_organization-my_account/mydb"`
var info = plugins.Info{
	Description:  "Table metadata from Snowflake server.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
}

// Extractor manages the extraction of data from snowflake
type Extractor struct {
	plugins.BaseExtractor
	logger        log.Logger
	config        Config
	httpTransport http.RoundTripper
	db            *sql.DB
	emit          plugins.Emit
}

// Option provides extension abstraction to Extractor constructor
type Option func(*Extractor)

// WithHTTPTransport assign custom http client to the Extractor constructor
func WithHTTPTransport(htr http.RoundTripper) Option {
	return func(e *Extractor) {
		e.httpTransport = htr
	}
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger, opts ...Option) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	for _, opt := range opts {
		opt(e)
	}

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	if e.httpTransport == nil {
		// create snowflake client
		if e.db, err = sql.Open("snowflake", e.config.ConnectionURL); err != nil {
			return fmt.Errorf("failed to create client: %w", err)
		}
		return
	}

	cfg, err := gosnowflake.ParseDSN(e.config.ConnectionURL)
	if err != nil {
		return fmt.Errorf("failed to parse dsn when creating client: %w", err)
	}
	cfg.Transporter = e.httpTransport
	connector := gosnowflake.NewConnector(&gosnowflake.SnowflakeDriver{}, *cfg)
	e.db = sql.OpenDB(connector)

	return
}

// Extract collects metadata of the database through emitter
func (e *Extractor) Extract(_ context.Context, emit plugins.Emit) (err error) {
	defer e.db.Close()
	e.emit = emit

	// Get list of databases
	dbs, err := e.db.Query("SHOW DATABASES;")
	if err != nil {
		return fmt.Errorf("failed to get the list of databases: %w", err)
	}

	// Iterate through all tables and databases
	for dbs.Next() {
		var createdOn, name, isDefault, isCurrent, origin, owner, comment, options string
		var retentionTime int

		if err = dbs.Scan(&createdOn, &name, &isDefault, &isCurrent, &origin, &owner, &comment, &options, &retentionTime); err != nil {
			return fmt.Errorf("failed to scan database %s: %w", name, err)
		}
		if err = e.extractTables(name); err != nil {
			return fmt.Errorf("failed to extract tables from %s: %w", name, err)
		}
	}

	return
}

// extractTables extracts tables from a given database
func (e *Extractor) extractTables(database string) (err error) {
	// extract tables
	_, err = e.db.Exec(fmt.Sprintf("USE %s;", database))
	if err != nil {
		return fmt.Errorf("failed to execute USE query on %s: %w", database, err)
	}
	rows, err := e.db.Query("SHOW TABLES;")
	if err != nil {
		return fmt.Errorf("failed to show tables for %s: %w", database, err)
	}

	// process each rows
	for rows.Next() {
		var createdOn, name, databaseName, schemaName, kind, comment, clusterBy, owner, autoClustering, changeTracking, isExternal string
		var bytes, rowsCount, retentionTime int

		if err = rows.Scan(&createdOn, &name, &databaseName, &schemaName, &kind, &comment, &clusterBy, &rowsCount,
			&bytes, &owner, &retentionTime, &autoClustering, &changeTracking, &isExternal); err != nil {
			return err
		}
		if err = e.processTable(database, name); err != nil {
			return err
		}
	}

	return
}

// processTable builds and push table to out channel
func (e *Extractor) processTable(database string, tableName string) (err error) {
	var columns []*facetsv1beta1.Column
	columns, err = e.extractColumns(database, tableName)
	if err != nil {
		return fmt.Errorf("failed to extract columns from %s.%s: %w", database, tableName, err)
	}

	// push table to channel
	e.emit(models.NewRecord(&assetsv1beta1.Table{
		Resource: &commonv1beta1.Resource{
			Urn:     models.NewURN("snowflake", e.UrnScope, "table", fmt.Sprintf("%s.%s", database, tableName)),
			Name:    tableName,
			Service: "snowflake",
			Type:    "table",
		},
		Schema: &facetsv1beta1.Columns{
			Columns: columns,
		},
	}))

	return
}

// extractColumns extracts columns from a given table
func (e *Extractor) extractColumns(database string, tableName string) (result []*facetsv1beta1.Column, err error) {
	// extract columns
	_, err = e.db.Exec(fmt.Sprintf("USE %s;", database))
	if err != nil {
		return nil, fmt.Errorf("failed to execute USE query on %s: %w", database, err)
	}
	sqlStr := `SELECT COLUMN_NAME,COMMENT,DATA_TYPE,IS_NULLABLE,IFNULL(CHARACTER_MAXIMUM_LENGTH,0)
			   FROM information_schema.columns
		       WHERE TABLE_NAME = ?
		       ORDER BY COLUMN_NAME ASC;`
	rows, err := e.db.Query(sqlStr, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to execute a query to extract columns metadata: %w", err)
	}

	for rows.Next() {
		var fieldName, fieldDesc, dataType, isNullableString sql.NullString
		var length int

		if err = rows.Scan(&fieldName, &fieldDesc, &dataType, &isNullableString, &length); err != nil {
			return nil, fmt.Errorf("failed to scan fields from query: %w", err)
		}
		result = append(result, &facetsv1beta1.Column{
			Name:        fieldName.String,
			DataType:    dataType.String,
			Description: fieldDesc.String,
			IsNullable:  e.isNullable(isNullableString.String),
			Length:      int64(length),
		})
	}

	return result, nil
}

// isNullable returns true if the string is "YES"
func (e *Extractor) isNullable(value string) bool {
	return value == "YES"
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("snowflake", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
