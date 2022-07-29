package postgres

import (
	"context"
	"database/sql"
	_ "embed" // // used to print the embedded assets
	"fmt"
	"net/url"
	"strings"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/structpb"

	// used to register the postgres driver
	_ "github.com/lib/pq"
	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins/sqlutil"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

var defaultDBList = []string{"information_schema", "root", "postgres"}

// Config holds the set of configuration options for the extractor
type Config struct {
	ConnectionURL string `mapstructure:"connection_url" validate:"required"`
	Exclude       string `mapstructure:"exclude"`
}

var sampleConfig = `
connection_url: "postgres://admin:pass123@localhost:3306/postgres?sslmode=disable"
exclude: testDB,secondaryDB`

var info = plugins.Info{
	Description:  "Table metadata and metrics from Postgres SQL sever.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
}

// Extractor manages the extraction of data from the extractor
type Extractor struct {
	plugins.BaseExtractor
	excludedDbs map[string]bool
	logger      log.Logger
	config      Config
	client      *sql.DB

	// These below values are used to recreate a connection for each database
	host     string
	username string
	password string
	sslmode  string
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	// build excluded database list
	excludeList := append(defaultDBList, strings.Split(e.config.Exclude, ",")...)
	e.excludedDbs = sqlutil.BuildBoolMap(excludeList)

	// Create database connection
	e.client, err = sql.Open("postgres", e.config.ConnectionURL)
	if err != nil {
		return errors.Wrap(err, "failed to create connection")
	}

	if err = e.extractConnectionComponents(e.config.ConnectionURL); err != nil {
		err = errors.Wrap(err, "failed to split host from connection string")
		return
	}

	return
}

// Extract collects metadata from the source. Metadata is collected through the emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	defer e.client.Close()

	// Get list of databases
	dbs, err := sqlutil.FetchDBs(e.client, e.logger, "SELECT datname FROM pg_database WHERE datistemplate = false;")
	if err != nil {
		return errors.Wrap(err, "failed to fetch databases")
	}

	// Iterate through all tables and databases
	for _, database := range dbs {
		//skip dbs meant to be excluded
		if e.isExcludedDB(database) {
			continue
		}
		// Open a new connection to the given database to collect
		// tables information without this default database
		// information will be returned

		db, err := e.connection(database)
		if err != nil {
			e.logger.Error("failed to connect, skipping database", "error", err)
			continue
		}
		query := `SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
		ORDER BY table_name;`

		_, err = db.Exec(fmt.Sprintf("SET search_path TO %s, public;", database))
		if err != nil {
			e.logger.Error("failed to get tables, skipping database", "error", err)
			continue
		}
		tables, err := sqlutil.FetchTablesInDB(db, database, query)
		if err != nil {
			e.logger.Error("failed to get tables, skipping database", "error", err)
			continue
		}

		for _, table := range tables {
			result, err := e.getTableMetadata(db, database, table)
			if err != nil {
				e.logger.Error("failed to get table metadata, skipping table", "error", err)
				continue
			}
			// Publish metadata to channel
			emit(models.NewRecord(result))
		}
	}

	return nil
}

// Prepares the list of tables and the attached metadata
func (e *Extractor) getTableMetadata(db *sql.DB, dbName string, tableName string) (result *assetsv1beta1.Table, err error) {
	var columns []*facetsv1beta1.Column
	columns, err = e.getColumnMetadata(db, dbName, tableName)
	if err != nil {
		return result, nil
	}

	usrPrivilegeInfo, err := e.userPrivilegesInfo(db, dbName, tableName)
	if err != nil {
		return result, nil
	}

	result = &assetsv1beta1.Table{
		Resource: &commonv1beta1.Resource{
			Urn:     models.NewURN("postgres", e.UrnScope, "table", fmt.Sprintf("%s.%s", dbName, tableName)),
			Name:    tableName,
			Service: "postgres",
			Type:    "table",
		},
		Schema: &facetsv1beta1.Columns{
			Columns: columns,
		},
		Properties: &facetsv1beta1.Properties{
			Attributes: usrPrivilegeInfo,
		},
	}

	return
}

// Prepares the list of columns and the attached metadata
func (e *Extractor) getColumnMetadata(db *sql.DB, dbName string, tableName string) (result []*facetsv1beta1.Column, err error) {
	sqlStr := `SELECT COLUMN_NAME,DATA_TYPE,
				IS_NULLABLE,coalesce(CHARACTER_MAXIMUM_LENGTH,0)
				FROM information_schema.columns
				WHERE TABLE_NAME = '%s' ORDER BY COLUMN_NAME ASC;`
	rows, err := db.Query(fmt.Sprintf(sqlStr, tableName))
	if err != nil {
		err = errors.Wrap(err, "failed to fetch data from query")
		return
	}
	for rows.Next() {
		var fieldName, dataType, isNullableString string
		var length int
		if err = rows.Scan(&fieldName, &dataType, &isNullableString, &length); err != nil {
			e.logger.Error("failed to get fields", "error", err)
			continue
		}
		result = append(result, &facetsv1beta1.Column{
			Name:       fieldName,
			DataType:   dataType,
			IsNullable: isNullable(isNullableString),
			Length:     int64(length),
		})
	}
	return result, nil
}

func (e *Extractor) userPrivilegesInfo(db *sql.DB, dbName string, tableName string) (result *structpb.Struct, err error) {
	query := `SELECT grantee, string_agg(privilege_type, ',') 
	FROM information_schema.role_table_grants 
	WHERE table_name='%s' AND table_catalog='%s'
	GROUP BY grantee;`

	rows, err := db.Query(fmt.Sprintf(query, tableName, dbName))
	if err != nil {
		err = errors.Wrap(err, "failed to fetch data from query")
		return
	}

	var usrs []interface{}
	for rows.Next() {
		var grantee, privilege_type string

		if err = rows.Scan(&grantee, &privilege_type); err != nil {
			e.logger.Error("failed to get fields", "error", err)
			continue
		}

		usrs = append(usrs, map[string]interface{}{
			"user":            grantee,
			"privilege_types": ConvertStringListToInterface(strings.Split(privilege_type, ",")),
		})
	}
	grants := map[string]interface{}{
		"grants": usrs,
	}
	result = utils.TryParseMapToProto(grants)
	return
}

// Convert nullable string to a boolean
func isNullable(value string) bool {
	return value == "YES"
}

// connection generates a connection string
func (e *Extractor) connection(database string) (db *sql.DB, err error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s", e.username, e.password, e.host, database, e.sslmode)
	return sql.Open("postgres", connStr)
}

// extractConnectionComponents extracts the components from the connection URL
func (e *Extractor) extractConnectionComponents(connectionURL string) (err error) {
	connectionStr, err := url.Parse(connectionURL)
	if err != nil {
		err = errors.Wrap(err, "failed to parse connection url")
		return
	}
	e.host = connectionStr.Host
	e.username = connectionStr.User.Username()
	e.password, _ = connectionStr.User.Password()
	e.sslmode = connectionStr.Query().Get("sslmode")

	return
}

// isExcludedDB checks if the given db is in the list of excluded databases
func (e *Extractor) isExcludedDB(database string) bool {
	_, ok := e.excludedDbs[database]
	return ok
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("postgres", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}

func ConvertStringListToInterface(s []string) []interface{} {
	out := make([]interface{}, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}
