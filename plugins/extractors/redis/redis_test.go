//+build integration

package redis_test

import (
	//"context"
	"fmt"
	//"github.com/go-redis/redis/v8"
	//"github.com/odpf/meteor/plugins/extractors/redis"
	"github.com/odpf/meteor/test"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"log"
	//"os"
	"testing"
	"github.com/gomodule/redigo/redis"
)

const (
	testDB = "MeteorRedisExtractorTest"
	user   = "user"
	pass   = "abcd"
	port   = "6379"
)

var (
	server  = "127.0.0.1:" + port
	client *redis.Pool
)

func TestMain(m *testing.M) {
	fmt.Println("ch 1")
	//ctx := context.TODO()

	// setup test
	opts := dockertest.RunOptions{
		Repository: "redis",
		Tag:        "latest",
		ExposedPorts: []string{port},
		PortBindings: map[docker.Port][]docker.PortBinding{
			port: {
				{HostIP: "0.0.0.0", HostPort: port},
			},
		},
	}
	fmt.Println("ch 2")

	retryFn := func(resource *dockertest.Resource) (err error) {
		fmt.Println("3")


		pool := &redis.Pool{
			// Other pool configuration not shown in this example.
			Dial: func () (redis.Conn, error) {
				c, err := redis.Dial("tcp", server, redis.DialUsername(), redis.DialPassword(), redis.DialDatabase())
				if err != nil {
					return nil, err
				}
				//if _, err := c.Do("AUTH", pass); err != nil {
				//	c.Close()
				//	return nil, err
				//}
				if _, err := c.Do("SELECT", 1); err != nil {
					c.Close()
					return nil, err
				}
				return c, nil
			},
		}

		// Then get a connection
		conn := pool.Get()
		defer conn.Close()

		// Test the connection
		pong, err := conn.Do("PING")
		if err != nil {
			log.Fatal("Can't connect to the Redis database")
		}
		fmt.Println(pong)

		return
	}
	purgeFn, err := test.CreateContainer(opts, retryFn)
	if err != nil {
		fmt.Println("ch 3")

		log.Fatal(err)
	}
	//
	//if err := setup(ctx); err != nil {
	//	fmt.Println("ch 4")
	//
	//	log.Fatal(err)
	//}

	// run tests
	//code := m.Run()
	//// clean tests
	//if err := client.Close(); err != nil {
	//	fmt.Println("ch 5")
	//
	//	log.Fatal(err)
	//}
	if err := purgeFn(); err != nil {
		fmt.Println("ch 6")

		log.Fatal(err)
	}
	//os.Exit(code)
}

//func TestInit(t *testing.T) {
//	t.Run("should return error for invalid", func(t *testing.T) {
//		err := redis.New(test.Logger).Init(context.TODO(), map[string]interface{}{
//			"password": pass,
//			"host":     host,
//		})
//
//		assert.Equal(t, plugins.InvalidConfigError{}, err)
//	})
//}
//
//func TestExtract(t *testing.T) {
//	t.Run("should extract and output tables metadata along with its columns", func(t *testing.T) {
//		ctx := context.TODO()
//		extr := redis.New(test.Logger)
//
//		err := extr.Init(ctx, map[string]interface{}{
//			"user_id":  user,
//			"password": pass,
//			"host":     host,
//		})
//		if err != nil {
//			t.Fatal(err)
//		}
//
//		emitter := mocks.NewEmitter()
//		err = extr.Extract(ctx, emitter.Push)
//
//		assert.NoError(t, err)
//		//assert.Equal(t, getExpected(), emitter.Get())
//	})
//}

//func setup(ctx context.Context) (err error) {
//fmt.Println("setup 1")
////client.Do(ctx,"SET", )
	// we can call set with a `Key` and a `Value`.
	//err = client.Set(ctx, "name", "Elliot", 0).Err()
	//// if there has been an error setting the value
	//// handle the error
	//if err != nil {
	//	fmt.Println("setup 1.1")
	//
	//	fmt.Println(err)
	//}
	//fmt.Println("setup 1.2")
	//
	//
	//val, err := client.Get(ctx, "name").Result()
	//if err != nil {
	//	fmt.Println("setup 1.3")
	//
	//	fmt.Println(err)
	//}
	//
	//fmt.Println(val)

	//err = client.Do(ctx, "SET", "mykey", "Hello from redigo!").Err()
	//if err != nil {
	//	panic(err)
	//}
	//
	//value := client.Do(ctx, "GET", "mykey").Name()
	////if err != nil {
	////	panic(err)
	////}
	//
	//fmt.Printf("%s \n", value)

	//create and populate connections collection
	//err = createCollection(ctx, "connections", []interface{}{
	//	bson.D{{Key: "name", Value: "Albert"}, {Key: "relation", Value: "mutual"}},
	//	bson.D{{Key: "name", Value: "Josh"}, {Key: "relation", Value: "following"}},
	//	bson.D{{Key: "name", Value: "Abish"}, {Key: "relation", Value: "follower"}},
	//})
	//if err != nil {
	//	return
	//}
	//
	//// create and populate posts collection
	//err = createCollection(ctx, "posts", []interface{}{
	//	bson.D{{Key: "title", Value: "World"}, {Key: "body", Value: "Hello World"}},
	//	bson.D{{Key: "title", Value: "Mars"}, {Key: "body", Value: "Hello Mars"}},
	//})
	//if err != nil {
	//	return
	//}
	//
	//// create and populate stats collection
	//err = createCollection(ctx, "stats", []interface{}{
	//	bson.D{{Key: "views", Value: "500"}, {Key: "likes", Value: "200"}},
	//})
	//if err != nil {
	//	return
	//}
//
//	return
//}

//func createCollection(ctx context.Context, collectionName string, data []interface{}) (err error) {
//	collection := client.Database(testDB).Collection(collectionName)
//	_, err = collection.InsertMany(ctx, data)
//	return
//}
//
//func getExpected() []models.Record {
//	return []models.Record{
//		models.NewRecord(&assets.Table{
//			Resource: &common.Resource{
//				Urn:  testDB + ".connections",
//				Name: "connections",
//			},
//			Profile: &assets.TableProfile{
//				TotalRows: 3,
//			},
//		}),
//		models.NewRecord(&assets.Table{
//			Resource: &common.Resource{
//				Urn:  testDB + ".posts",
//				Name: "posts",
//			},
//			Profile: &assets.TableProfile{
//				TotalRows: 2,
//			},
//		}),
//		models.NewRecord(&assets.Table{
//			Resource: &common.Resource{
//				Urn:  testDB + ".stats",
//				Name: "stats",
//			},
//			Profile: &assets.TableProfile{
//				TotalRows: 1,
//			},
//		}),
//	}
//}
