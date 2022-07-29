package mongodb

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"sort"

	"github.com/pkg/errors"

	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//go:embed README.md
var summary string

var defaultCollections = []string{
	"system.users",
	"system.version",
	"system.sessions",
	"startup_log",
}

// Config holds the connection URL for the extractor
type Config struct {
	ConnectionURL string `mapstructure:"connection_url" validate:"required"`
}

var sampleConfig = `
connection_url: "mongodb://admin:pass123@localhost:3306"`

var info = plugins.Info{
	Description:  "Collection metadata from MongoDB Server",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
}

// Extractor manages the communication with the mongo server
type Extractor struct {
	plugins.BaseExtractor
	// internal states
	client   *mongo.Client
	excluded map[string]bool
	logger   log.Logger
	config   Config
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	// build excluded list
	e.buildExcludedCollections()

	// setup client
	if e.client, err = createAndConnnectClient(ctx, e.config.ConnectionURL); err != nil {
		return errors.Wrap(err, "failed to create client")
	}

	return
}

// Extract collects metadata of each database through emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	databases, err := e.client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return errors.Wrap(err, "failed to list database names")
	}

	for _, dbName := range databases {
		database := e.client.Database(dbName)
		if err := e.extractCollections(ctx, database, emit); err != nil {
			return errors.Wrap(err, "failed to extract collections")
		}
	}

	return
}

// Extract and output collections from a single mongo database
func (e *Extractor) extractCollections(ctx context.Context, db *mongo.Database, emit plugins.Emit) (err error) {
	collections, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "failed to list collection names")
	}

	// we need to sort the collections for testing purpose
	// this ensures the returned collection list are in consistent order
	// or else test might fail
	sort.Strings(collections)
	for _, collectionName := range collections {
		// skip if collection is default mongo
		if e.isDefaultCollection(collectionName) {
			continue
		}

		table, err := e.buildTable(ctx, db, collectionName)
		if err != nil {
			return errors.Wrap(err, "failed to build table")
		}

		emit(models.NewRecord(table))
	}

	return
}

// Build table metadata model from a collection
func (e *Extractor) buildTable(ctx context.Context, db *mongo.Database, collectionName string) (table *assetsv1beta1.Table, err error) {
	// get total rows
	totalRows, err := db.Collection(collectionName).EstimatedDocumentCount(ctx)
	if err != nil {
		err = errors.Wrap(err, "failed to fetch total no of rows")
		return
	}

	table = &assetsv1beta1.Table{
		Resource: &commonv1beta1.Resource{
			Urn:     models.NewURN("mongodb", e.UrnScope, "collection", fmt.Sprintf("%s.%s", db.Name(), collectionName)),
			Name:    collectionName,
			Service: "mongodb",
			Type:    "table",
		},
		Profile: &assetsv1beta1.TableProfile{
			TotalRows: totalRows,
		},
	}

	return
}

// Build a map of excluded collections using list of collection names
func (e *Extractor) buildExcludedCollections() {
	excluded := make(map[string]bool)
	for _, collection := range defaultCollections {
		excluded[collection] = true
	}

	e.excluded = excluded
}

// Check if collection is default using stored map
func (e *Extractor) isDefaultCollection(collectionName string) bool {
	_, ok := e.excluded[collectionName]
	return ok
}

// Create mongo client and tries to connect
func createAndConnnectClient(ctx context.Context, uri string) (client *mongo.Client, err error) {
	clientOptions := options.Client().ApplyURI(uri)
	client, err = mongo.NewClient(clientOptions)
	if err != nil {
		return
	}
	err = client.Connect(ctx)
	if err != nil {
		return
	}

	return
}

func init() {
	if err := registry.Extractors.Register("mongodb", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
