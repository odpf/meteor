package redis

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"go.mongodb.org/mongo-driver/bson"
)

//go:embed README.md
var summary string

// Config hold the set of configuration for the extractor
type Config struct {
	UserID   string `mapstructure:"user_id" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
	Host     string `mapstructure:"host" validate:"required"`
	Database int `mapstructure:"database" validate:"required"`
}

var sampleConfig = `
host: localhost:27017
user_id: admin
password: "1234"`

// Extractor manages the communication with the redis server
type Extractor struct {
	// internal states
	pool   *redis.Pool
	excluded map[string]bool
	logger   log.Logger
	config   Config
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
		Description:  "Collection metadata from redis Server",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"oss", "extractor"},
	}
}

func (e *Extractor) newPool() *redis.Pool {
	return &redis.Pool{
		// Other pool configuration not shown in this example.
		Dial: func () (redis.Conn, error) {
			c, err := redis.Dial("tcp", e.config.Host,
				redis.DialUsername(e.config.UserID),
				redis.DialPassword(e.config.Password),
				redis.DialDatabase(e.config.Database),
			)
			//c, err := redis.Dial("tcp", e.config.Host)
			if err != nil {
				return nil, err
			}
			//if _, err := c.Do("AUTH", e.config.Password); err != nil {
			//	c.Close()
			//	return nil, err
			//}
			//if _, err := c.Do("SELECT", e.config.Database); err != nil {
			//	err := c.Close()
			//	if err != nil {
			//		return nil, err
			//	}
			//	return nil, err
			//}
			return c, nil
		},
	}

}
// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	err = utils.BuildConfig(configMap, &e.config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}


	// setup client
	//uri := fmt.Sprintf("redis.DialUsername(%s)%s:%s@%s/%s", e.config.UserID, e.config.Password, e.config.Host, e.config.Database)
	e.pool = e.newPool()
	if err != nil {
		return
	}

	return
}
//
// Extract extracts the data from the redis server
// and outputs the data to the out channel
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	//e.client.
	databases, err := e.pool.Dial()
	if err != nil {
		return
	}

	for _, dbName := range databases {
		database := e.pool.Database(dbName)
		if err := e.extractCollections(ctx, database, emit); err != nil {
			return err
		}
	}

	return
}
//
//// Extract and output collections from a single redis database
//func (e *Extractor) extractCollections(ctx context.Context, db *redis.Database, emit plugins.Emit) (err error) {
//	collections, err := db.ListCollectionNames(ctx, bson.D{})
//	if err != nil {
//		return
//	}
//
//	// we need to sort the collections for testing purpose
//	// this ensures the returned collection list are in consistent order
//	// or else test might fail
//	sort.Strings(collections)
//	for _, collectionName := range collections {
//		// skip if collection is default redis
//		if e.isDefaultCollection(collectionName) {
//			continue
//		}
//
//		table, err := e.buildTable(ctx, db, collectionName)
//		if err != nil {
//			return err
//		}
//
//		emit(models.NewRecord(table))
//	}
//
//	return
//}
//
//// Build table metadata model from a collection
//func (e *Extractor) buildTable(ctx context.Context, db *redis.Database, collectionName string) (table *assets.Table, err error) {
//	// get total rows
//	totalRows, err := db.Collection(collectionName).EstimatedDocumentCount(ctx)
//	if err != nil {
//		return
//	}
//
//	table = &assets.Table{
//		Resource: &common.Resource{
//			Urn:  fmt.Sprintf("%s.%s", db.Name(), collectionName),
//			Name: collectionName,
//		},
//		Profile: &assets.TableProfile{
//			TotalRows: totalRows,
//		},
//	}
//
//	return
//}
//
//// Build a map of excluded collections using list of collection names
//func (e *Extractor) buildExcludedCollections() {
//	excluded := make(map[string]bool)
//	for _, collection := range defaultCollections {
//		excluded[collection] = true
//	}
//
//	e.excluded = excluded
//}
//
//// Check if collection is default using stored map
//func (e *Extractor) isDefaultCollection(collectionName string) bool {
//	_, ok := e.excluded[collectionName]
//	return ok
//}
//
//// Create redis client and tries to connect
//func createAndConnnectClient(ctx context.Context, uri string) (client *redis.Client, err error) {
//	clientOptions := options.Client().ApplyURI(uri)
//	client, err = redis.NewClient(clientOptions)
//	if err != nil {
//		return
//	}
//	err = client.Connect(ctx)
//	if err != nil {
//		return
//	}
//
//	return
//}
//
//func init() {
//	if err := registry.Extractors.Register("redisdb", func() plugins.Extractor {
//		return New(plugins.GetLog())
//	}); err != nil {
//		panic(err)
//	}
//}

