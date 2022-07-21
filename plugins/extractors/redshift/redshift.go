package redshift

import (
	"context"
	_ "embed" // used to print the embedded assets

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/redshiftdataapiservice"
	"github.com/aws/aws-sdk-go/service/redshiftdataapiservice/redshiftdataapiserviceiface"
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

// Config holds the set of configuration for the metabase extractor
type Config struct {
	ClusterID string `mapstructure:"cluster_id" validate:"required"`
	DBName    string `mapstructure:"db_name" validate:"required"`
	DBUser    string `mapstructure:"db_user" validate:"required"`
	AWSRegion string `mapstructure:"aws_region" validate:"required"`
	Exclude   string `mapstructure:"exclude"`
}

var sampleConfig = `
cluster_id: cluster_test
db_name: testDB
db_user: testUser
aws_region: us-east-1
exclude: secondaryDB
`

// Option provides extension abstraction to Extractor constructor
type Option func(*Extractor)

// WithClient assign custom client to the Extractor constructor
func WithClient(redshiftClient redshiftdataapiserviceiface.RedshiftDataAPIServiceAPI) Option {
	return func(e *Extractor) {
		e.client = redshiftClient
	}
}

// Extractor manages the extraction of data
// from the redshift server
type Extractor struct {
	config Config
	logger log.Logger
	client redshiftdataapiserviceiface.RedshiftDataAPIServiceAPI
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger, opts ...Option) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	for _, opt := range opts {
		opt(e)
	}

	return e
}

// Info returns the brief information about the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Table metadata from Redshift server.",
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
func (e *Extractor) Init(_ context.Context, config map[string]interface{}) (err error) {
	// Build and validate config received from recipe
	if err = utils.BuildConfig(config, &e.config); err != nil {
		return plugins.InvalidConfigError{}
	}

	if e.client != nil {
		// Create session
		var sess = session.Must(session.NewSession())

		// Initialize the redshift client
		e.client = redshiftdataapiservice.New(sess, aws.NewConfig().WithRegion(e.config.AWSRegion))
	}

	return
}

// Extract collects metadata from the source. Metadata is collected through the emitter
func (e *Extractor) Extract(_ context.Context, emit plugins.Emit) (err error) {
	listDB, err := e.GetDBList()
	if err != nil {
		return err
	}

	for _, database := range listDB {
		tables, err := e.GetTables(database)
		if err != nil {
			e.logger.Error("failed to get tables, skipping database", "error", err)
			continue
		}

		for _, tableName := range tables {
			result, err := e.getTableMetadata(database, tableName)
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

// GetDBList returns the list of databases in a cluster
func (e *Extractor) GetDBList() (list []string, err error) {
	listDbOutput, err := e.client.ListDatabases(&redshiftdataapiservice.ListDatabasesInput{
		ClusterIdentifier: aws.String(e.config.ClusterID),
		Database:          aws.String(e.config.DBName),
		DbUser:            aws.String(e.config.DBUser),
		MaxResults:        nil,
		NextToken:         nil,
		SecretArn:         nil,
	})
	if err != nil {
		return nil, err
	}

	for _, db := range listDbOutput.Databases {
		list = append(list, aws.StringValue(db))
	}

	return list, nil
}

// GetTables return the list of tables name
func (e *Extractor) GetTables(dbName string) (list []string, err error) {
	listTbOutput, err := e.client.ListTables(&redshiftdataapiservice.ListTablesInput{
		ClusterIdentifier: aws.String(e.config.ClusterID),
		ConnectedDatabase: aws.String(dbName),
		Database:          aws.String(e.config.DBName),
		DbUser:            aws.String(e.config.DBUser),
		SchemaPattern:     nil,
		MaxResults:        nil,
		NextToken:         nil,
		SecretArn:         nil, // required when authenticating through secret manager
		TablePattern:      nil,
	})
	if err != nil {
		return nil, err
	}

	for _, table := range listTbOutput.Tables {
		list = append(list, aws.StringValue(table.Name))
	}

	return list, nil
}

// getTableMetadata prepares the list of tables and the attached metadata
func (e *Extractor) getTableMetadata(dbName string, tableName string) (result *v1beta2.Asset, err error) {
	var columns []*facetsv1beta1.Column
	colMetadata, err := e.GetColumn(dbName, tableName)
	if err != nil {
		return result, nil
	}
	columns, err = e.getColumnMetadata(colMetadata)
	if err != nil {
		return result, nil
	}

	result = &assetsv1beta1.Table{
		Resource: &commonv1beta1.Resource{
			Urn:     models.TableURN("redshift", e.config.AWSRegion, dbName, tableName),
			Name:    tableName,
			Type:    "table",
			Service: "redshift",
		},
		Schema: &facetsv1beta1.Columns{
			Columns: columns,
		},
	}

	return
}

// GetColumn returns the column metadata of particular table in a database
func (e *Extractor) GetColumn(dbName string, tableName string) (result []*redshiftdataapiservice.ColumnMetadata, err error) {
	descTable, err := e.client.DescribeTable(&redshiftdataapiservice.DescribeTableInput{
		ClusterIdentifier: aws.String(e.config.ClusterID),
		ConnectedDatabase: aws.String(e.config.DBName),
		Database:          aws.String(dbName),
		DbUser:            aws.String(e.config.DBName),
		Table:             aws.String(tableName),
		Schema:            nil,
		MaxResults:        nil,
		NextToken:         nil,
		SecretArn:         nil,
	})
	if err != nil {
		return nil, err
	}

	return descTable.ColumnList, nil
}

// getColumnMetadata prepares the list of columns and the attached metadata
func (e *Extractor) getColumnMetadata(columnMetadata []*redshiftdataapiservice.ColumnMetadata) (result []*facetsv1beta1.Column, err error) {
	var tempResults []*facetsv1beta1.Column
	for _, column := range columnMetadata {
		var tempResult facetsv1beta1.Column
		tempResult.Name = aws.StringValue(column.Name)
		tempResult.Description = aws.StringValue(column.Label)
		tempResult.DataType = aws.StringValue(column.TypeName)
		tempResult.IsNullable = isNullable(aws.Int64Value(column.Nullable))
		tempResult.Length = aws.Int64Value(column.Length)
		tempResults = append(tempResults, &tempResult)
	}
	return tempResults, nil
}

// Convert nullable int to a boolean
func isNullable(value int64) bool {
	return value == 1
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("redshift", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
