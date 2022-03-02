package presto

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	_ "github.com/prestodb/presto-go-client/presto" // presto driver
	"net/url"
	"strings"
)

//go:embed README.md
var summary string

// Config holds the set of configuration options for the extractor
type Config struct {
	ConnectionURL string `mapstructure:"connection_url" validate:"required"`
	Exclude       string `mapstructure:"exclude_catalog"`
}

var sampleConfig = `
connection_url: "http://user:pass@localhost:8080"
exclude_catalog: "memory,system,tpcds,tpch"`

// Extractor manages the extraction of data
type Extractor struct {
	logger log.Logger
	config Config
	client *sql.DB

	// These below values are used to recreate a connection for each catalog
	host     string
	username string
	password string
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
		Description:  "Table metadata from Presto server.",
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
	// Build and validate config received from recipe
	if err = utils.BuildConfig(configMap, &e.config); err != nil {
		return plugins.InvalidConfigError{}
	}

	// create presto client
	if e.client, err = sql.Open("presto", e.config.ConnectionURL); err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	if err = e.extractConnectionComponents(e.config.ConnectionURL); err != nil {
		err = fmt.Errorf("failed to split configs from connection string: %w", err)
		return
	}

	return
}

// Extract collects metadata of the database through emitter
func (e *Extractor) Extract(_ context.Context, emit plugins.Emit) (err error) {
	defer e.client.Close()

	catalogs, err := e.getCatalogs()
	for _, catalog := range catalogs {
		// Open a new connection to the given catalogs list to collect
		// schemas information

		db, err := e.connection(catalog)
		if err != nil {
			e.logger.Error("failed to connect, skipping catalog", "error", err)
			continue
		}

		dbs, err := e.getDatabases(db, catalog)
		if err != nil {
			return fmt.Errorf("failed to extract tables from %s: %w", catalog, err)
		}
		for _, database := range dbs {
			tables, err := e.getTables(db, catalog, database)
			if err != nil {
				e.logger.Error("failed to get tables, skipping database", "catalog", catalog, "error", err)
				continue
			}

			for _, table := range tables {
				result, err := e.processTable(db, catalog, database, table)
				if err != nil {
					e.logger.Error("failed to get table metadata, skipping table", "error", err)
					continue
				}
				// Publish metadata to channel
				emit(models.NewRecord(result))
			}
		}
	}
	return nil
}

func (e *Extractor) getCatalogs() (list []string, err error) {
	// Get list of catalogs
	catalogs, err := e.client.Query("SHOW CATALOGS")
	if err != nil {
		return nil, fmt.Errorf("failed to get the list of catalogs: %w", err)
	}

	var excludeList []string
	excludeList = append(excludeList, strings.Split(e.config.Exclude, ",")...)

	for catalogs.Next() {
		var catalog string
		if err = catalogs.Scan(&catalog); err != nil {
			return nil, fmt.Errorf("failed to scan schema from %s: %w", catalog, err)
		}
		if exclude(excludeList, catalog) {
			continue
		}
		list = append(list, catalog)
	}

	return list, err
}

func (e *Extractor) getDatabases(db *sql.DB, catalog string) (list []string, err error) {
	// Get list of databases
	showSchemasQuery := fmt.Sprintf("show schemas in %s", catalog)
	dbs, err := db.Query(showSchemasQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to get the list of schemas from %s: %w", catalog, err)
	}

	for dbs.Next() {
		var database string
		if err = dbs.Scan(&database); err != nil {
			return nil, fmt.Errorf("failed to scan %s.%s: %w", catalog, database, err)
		}
		list = append(list, database)
	}

	return list, nil
}

// getTables extracts tables from a given database
func (e *Extractor) getTables(db *sql.DB, catalog string, database string) (list []string, err error) {
	showTablesQuery := fmt.Sprintf("SHOW TABLES FROM %s.%s", catalog, database)
	rows, err := db.Query(showTablesQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to show tables for %s: %w", database, err)
	}

	// process each rows
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		list = append(list, tableName)
	}

	return list, err
}

// processTable builds and push table to out channel
func (e *Extractor) processTable(db *sql.DB, catalog string, database string, tableName string) (result *assetsv1beta1.Table, err error) {
	var columns []*facetsv1beta1.Column
	columns, err = e.extractColumns(db, catalog)
	if err != nil {
		return result, fmt.Errorf("failed to extract columns: %w", err)
	}

	// push table to channel
	result = &assetsv1beta1.Table{
		Resource: &commonv1beta1.Resource{
			Urn:     fmt.Sprintf("%s.%s.%s", catalog, database, tableName),
			Name:    tableName,
			Service: "presto",
		},
		Schema: &facetsv1beta1.Columns{
			Columns: columns,
		},
	}

	return
}

// extractColumns extracts columns from a given table
func (e *Extractor) extractColumns(db *sql.DB, catalog string) (result []*facetsv1beta1.Column, err error) {
	sqlStr := fmt.Sprintf(`SELECT column_name,data_type,
				is_nullable, comment
				FROM %s.information_schema.columns
				ORDER BY column_name ASC`, catalog)
	rows, err := db.Query(sqlStr)
	if err != nil {
		return nil, fmt.Errorf("failed to execute a query to extract columns metadata: %w", err)
	}

	for rows.Next() {
		var fieldName, dataType, isNullableString, comment sql.NullString
		err = rows.Scan(&fieldName, &dataType, &isNullableString, &comment)
		if err != nil {
			return nil, fmt.Errorf("failed to scan fields from query: %w", err)
		}

		result = append(result, &facetsv1beta1.Column{
			Name:        fieldName.String,
			DataType:    dataType.String,
			IsNullable:  isNullable(isNullableString.String),
			Description: comment.String,
		})
	}

	return result, nil
}

// isNullable returns true if the string is "YES"
func isNullable(value string) bool {
	return value == "YES"
}

// connection generates a connection string
func (e *Extractor) connection(catalog string) (db *sql.DB, err error) {
	var connStr string
	if len(e.password) != 0 {
		connStr = fmt.Sprintf("http://%s:%s@%s?catalog=%s", e.username, e.password, e.host, catalog)
	} else {
		connStr = fmt.Sprintf("http://%s@%s?catalog=%s", e.username, e.host, catalog)
	}

	return sql.Open("presto", connStr)
}

// extractConnectionComponents extracts the components from the connection URL
func (e *Extractor) extractConnectionComponents(connectionURL string) (err error) {
	connectionStr, err := url.Parse(connectionURL)
	if err != nil {
		err = fmt.Errorf("failed to parse connection url: %w", err)
		return
	}
	e.host = connectionStr.Host
	e.username = connectionStr.User.Username()
	e.password, _ = connectionStr.User.Password()

	return
}

// Exclude checks if the catalog is in the ignored catalogs
func exclude(names []string, catalog string) bool {
	for _, b := range names {
		if b == catalog {
			return true
		}
	}

	return false
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("presto", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
