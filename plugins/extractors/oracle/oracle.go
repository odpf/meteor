package oracle

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"

	"github.com/pkg/errors"

	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	"github.com/odpf/meteor/plugins/sqlutil"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	_ "github.com/sijms/go-ora/v2"
)

var summary string

// Config holds the set of configuration options for the extractor
type Config struct {
	ConnectionURL string `mapstructure:"connection_url" validate:"required"`
}

var sampleConfig = `
connection_url: oracle://username:passwd@localhost:1521/xe`

// Extractor manages the extraction of data from the extractor
type Extractor struct {
	logger log.Logger
	config Config
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
		Description:  "Table metadata Oracle SQL Database.",
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
func (e *Extractor) Init(ctx context.Context, config map[string]interface{}) (err error) {
	// Build and validate config received from recipe
	if err := utils.BuildConfig(config, &e.config); err != nil {
		return plugins.InvalidConfigError{}
	}

	// Create database connection
	e.db, err = connection(e.config)
	if err != nil {
		return errors.Wrap(err, "failed to create connection")
	}

	return
}

// Extract collects metadata from the source. Metadata is collected through the emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	defer e.db.Close()

	// Get username
	userName, err := e.getUserName(e.db)
	if err != nil {
		e.logger.Error("failed to get the user name", "error", err)
		return
	}

	// Get DB name
	dbs, err := sqlutil.FetchDBs(e.db, e.logger, "select ora_database_name from dual")
	if err != nil {
		return
	}
	database := dbs[0]

	tableQuery := fmt.Sprintf(`SELECT object_name 
 		FROM all_objects
		WHERE object_type = 'TABLE'
		AND upper(owner) = upper('%s')`, userName)
	tables, err := sqlutil.FetchTablesInDB(e.db, database, tableQuery)
	if err != nil {
		e.logger.Error("failed to get tables, skipping database", "error", err)
		// continue
	}

	for _, table := range tables {
		result, err := e.getTableMetadata(e.db, database, table)
		if err != nil {
			e.logger.Error("failed to get table metadata, skipping table", "error", err)
			// continue
		}
		// Publish metadata to channel
		emit(models.NewRecord(result))
	}

	return nil
}

func (e *Extractor) getUserName(db *sql.DB) (userName string, err error) {
	sqlStr := `select user from dual`

	rows, err := db.Query(sqlStr)
	if err != nil {
		return
	}
	for rows.Next() {
		err = rows.Scan(&userName)
		if err != nil {
			return
		}
	}
	return userName, err
}

// Prepares the list of tables and the attached metadata
func (e *Extractor) getTableMetadata(db *sql.DB, dbName string, tableName string) (result *v1beta2.Asset, err error) {
	var columns []*facetsv1beta1.Column
	columns, err = e.getColumnMetadata(db, dbName, tableName)
	if err != nil {
		return result, nil
	}

	// get table row count
	sqlStr := `select count(*) from %s`
	rows, err := db.Query(fmt.Sprintf(sqlStr, tableName))
	var rowCount int64
	for rows.Next() {
		if err = rows.Scan(&rowCount); err != nil {
			e.logger.Error("failed to get fields", "error", err)
			continue
		}
	}

	result = &assetsv1beta1.Table{
		Resource: &commonv1beta1.Resource{
			Urn:     fmt.Sprintf("%s.%s", dbName, tableName),
			Name:    tableName,
			Service: "Oracle",
			Type:    "table",
		},
		Schema: &facetsv1beta1.Columns{
			Columns: columns,
		},
		Profile: &assetsv1beta1.TableProfile{
			TotalRows: rowCount,
		},
	}

	return
}

// Prepares the list of columns and the attached metadata
func (e *Extractor) getColumnMetadata(db *sql.DB, dbName string, tableName string) (result []*facetsv1beta1.Column, err error) {
	sqlStr := `select utc.column_name, utc.data_type, 
			decode(utc.char_used, 'C', utc.char_length, utc.data_length) as data_length,
			utc.nullable, nvl(ucc.comments, '') as col_comment
			from USER_TAB_COLUMNS utc
			INNER JOIN USER_COL_COMMENTS ucc ON
			utc.column_name = ucc.column_name AND
			utc.table_name = ucc.table_name
			WHERE utc.table_name ='%s'`

	rows, err := db.Query(fmt.Sprintf(sqlStr, tableName))
	if err != nil {
		err = errors.Wrap(err, "failed to fetch data from query")
		return
	}

	for rows.Next() {
		var fieldName, dataType, isNullableString string
		var fieldDesc sql.NullString
		var length int
		if err = rows.Scan(&fieldName, &dataType, &length, &isNullableString, &fieldDesc); err != nil {
			e.logger.Error("failed to get fields", "error", err)
			continue
		}

		result = append(result, &facetsv1beta1.Column{
			Name:        fieldName,
			DataType:    dataType,
			Description: fieldDesc.String,
			IsNullable:  isNullable(isNullableString),
			Length:      int64(length),
		})
	}
	return result, nil
}

// Convert nullable string to a boolean
func isNullable(value string) bool {
	return value == "Y"
}

// connection generates a connection string
func connection(cfg Config) (db *sql.DB, err error) {
	return sql.Open("oracle", cfg.ConnectionURL)
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("oracle", func() plugins.Extractor {
		return &Extractor{
			logger: plugins.GetLog(),
		}
	}); err != nil {
		panic(err)
	}
}
