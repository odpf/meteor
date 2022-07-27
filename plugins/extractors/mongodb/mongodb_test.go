//go:build plugins
// +build plugins

package mongodb_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/odpf/meteor/test/utils"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/odpf/meteor/models"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/mongodb"
	"github.com/odpf/meteor/test/mocks"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	testDB = "MeteorMongoExtractorTest"
	user   = "user"
	pass   = "abcd"
	port   = "27017"
)

var (
	host   = "127.0.0.1:" + port
	client *mongo.Client
)

func TestMain(m *testing.M) {
	ctx := context.TODO()

	// setup test
	opts := dockertest.RunOptions{
		Repository: "mongo",
		Tag:        "4.4.6",
		Env: []string{
			"MONGO_INITDB_ROOT_USERNAME=" + user,
			"MONGO_INITDB_ROOT_PASSWORD=" + pass,
		},
		ExposedPorts: []string{port},
		PortBindings: map[docker.Port][]docker.PortBinding{
			port: {
				{HostIP: "0.0.0.0", HostPort: port},
			},
		},
	}
	retryFn := func(resource *dockertest.Resource) (err error) {
		uri := fmt.Sprintf("mongodb://%s:%s@%s", user, pass, host)
		clientOptions := options.Client().ApplyURI(uri)
		client, err = mongo.NewClient(clientOptions)
		if err != nil {
			return
		}
		err = client.Connect(ctx)
		if err != nil {
			return
		}

		return client.Ping(ctx, nil)
	}
	purgeFn, err := utils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}

	if err := setup(ctx); err != nil {
		log.Fatal(err)
	}

	// run tests
	code := m.Run()

	// clean tests
	if err := client.Disconnect(ctx); err != nil {
		log.Fatal(err)
	}
	if err := purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func TestInit(t *testing.T) {
	t.Run("should return error for invalid", func(t *testing.T) {
		err := mongodb.New(utils.Logger).Init(context.TODO(), map[string]interface{}{
			"invalid_config": "invalid_config_value",
		})

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should extract and output tables metadata along with its columns", func(t *testing.T) {
		ctx := context.TODO()
		extr := mongodb.New(utils.Logger)

		err := extr.Init(ctx, map[string]interface{}{
			"connection_url": fmt.Sprintf("mongodb://%s:%s@%s", user, pass, host),
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)

		assert.NoError(t, err)
		assert.Equal(t, getExpected(t), emitter.Get())
	})
}

func setup(ctx context.Context) (err error) {
	// create and populate connections collection
	err = createCollection(ctx, "connections", []interface{}{
		bson.D{{Key: "name", Value: "Albert"}, {Key: "relation", Value: "mutual"}},
		bson.D{{Key: "name", Value: "Josh"}, {Key: "relation", Value: "following"}},
		bson.D{{Key: "name", Value: "Abish"}, {Key: "relation", Value: "follower"}},
	})
	if err != nil {
		return
	}

	// create and populate posts collection
	err = createCollection(ctx, "posts", []interface{}{
		bson.D{{Key: "title", Value: "World"}, {Key: "body", Value: "Hello World"}},
		bson.D{{Key: "title", Value: "Mars"}, {Key: "body", Value: "Hello Mars"}},
	})
	if err != nil {
		return
	}

	// create and populate stats collection
	err = createCollection(ctx, "stats", []interface{}{
		bson.D{{Key: "views", Value: "500"}, {Key: "likes", Value: "200"}},
	})
	if err != nil {
		return
	}

	return
}

func createCollection(ctx context.Context, collectionName string, data []interface{}) (err error) {
	collection := client.Database(testDB).Collection(collectionName)
	_, err = collection.InsertMany(ctx, data)
	return
}

func getExpected(t *testing.T) []models.Record {
	data1, err := anypb.New(&v1beta2.Table{
		Profile: &v1beta2.TableProfile{
			TotalRows: 3,
		},
	})
	if err != nil {
		t.Fatal(fmt.Println(err, "failed to build Any struct"))
	}
	data2, err := anypb.New(&v1beta2.Table{
		Profile: &v1beta2.TableProfile{
			TotalRows: 2,
		},
	})
	if err != nil {
		t.Fatal(fmt.Println(err, "failed to build Any struct"))
	}
	data3, err := anypb.New(&v1beta2.Table{
		Profile: &v1beta2.TableProfile{
			TotalRows: 1,
		},
	})
	if err != nil {
		t.Fatal(fmt.Println(err, "failed to build Any struct"))
	}

	return []models.Record{
		models.NewRecord(&v1beta2.Asset{
			Urn:  testDB + ".connections",
			Name: "connections",
			Type: "table",
			Data: data1,
		}),
		models.NewRecord(&v1beta2.Asset{
			Urn:  testDB + ".posts",
			Name: "posts",
			Type: "table",
			Data: data2,
		}),
		models.NewRecord(&v1beta2.Asset{
			Urn:  testDB + ".stats",
			Name: "stats",
			Type: "table",
			Data: data3,
		}),
	}
}
