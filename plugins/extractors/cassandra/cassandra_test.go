//go:build plugins
// +build plugins

package cassandra_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/odpf/meteor/test/utils"

	"github.com/gocql/gocql"
	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/cassandra"
	"github.com/odpf/meteor/test/mocks"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

const (
	user     = "cassandra"
	pass     = "cassandra"
	port     = 9042
	host     = "127.0.0.1"
	keyspace = "cassandra_meteor_test"
)

var session *gocql.Session

func TestMain(m *testing.M) {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	// setup test
	opts := dockertest.RunOptions{
		Repository: "cassandra",
		Tag:        "3.11.11",
		Mounts: []string{
			fmt.Sprintf("%s/localConfig/cassandra.yaml:/etc/cassandra/cassandra.yaml", pwd),
		},
		ExposedPorts: []string{"9042"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9042": {
				{HostIP: "0.0.0.0", HostPort: "9042"},
			},
		},
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	retryFn := func(resource *dockertest.Resource) (err error) {
		//create a new session
		cluster := gocql.NewCluster(host)
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: "cassandra",
			Password: "cassandra",
		}
		cluster.Consistency = gocql.LocalQuorum
		cluster.ProtoVersion = 4
		cluster.Port = port
		session, err = cluster.CreateSession()
		if err != nil {
			return err
		}
		return nil
	}
	purgeFn, err := utils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}
	if err := setup(); err != nil {
		log.Fatal(err)
	}
	// run tests
	code := m.Run()

	// clean tests
	session.Close()
	if err := purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

// TestEmptyHosts tests that the extractor returns an error if no hosts are provided
func TestEmptyHosts(t *testing.T) {
	//connect to cassandra
	cluster := gocql.NewCluster("")
	cluster.Keyspace = ""
	cluster.Consistency = gocql.Quorum
	if session, err := cluster.CreateSession(); err == nil {
		session.Close()
		t.Error("expected err, got nil")
	}
}

// TestInit tests the configs
func TestInit(t *testing.T) {
	t.Run("should return error for invalid configs", func(t *testing.T) {
		err := cassandra.New(utils.Logger).Init(context.TODO(), map[string]interface{}{
			"password": pass,
			"host":     host,
		})

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}

// TestExtract tests that the extractor returns the expected result
func TestExtract(t *testing.T) {
	t.Run("should extract and output tables metadata along with its columns", func(t *testing.T) {
		ctx := context.TODO()
		extr := cassandra.New(utils.Logger)

		err := extr.Init(ctx, map[string]interface{}{
			"user_id":  user,
			"password": pass,
			"host":     host,
			"port":     port,
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)

		assert.NoError(t, err)
		assert.Equal(t, getExpected(), emitter.Get())
	})
}

// setup is a helper function to setup the test keyspace
func setup() (err error) {
	// create database, user and grant access
	err = execute([]string{
		fmt.Sprintf(`DROP KEYSPACE IF EXISTS %s`, keyspace),
		fmt.Sprintf(`CREATE KEYSPACE %s WITH REPLICATION={'class':'SimpleStrategy','replication_factor':1}`, keyspace),
		fmt.Sprintf(`CREATE ROLE IF NOT EXISTS '%s' WITH PASSWORD ='%s'`, user, pass),
		fmt.Sprintf(`GRANT ALL PERMISSIONS ON ALL KEYSPACES TO '%s'`, user),
	})
	if err != nil {
		return errors.Wrap(err, "fail to create database")
	}

	//create and populate tables
	err = execute([]string{
		fmt.Sprintf(`CREATE TABLE %s.applicant (applicantid int PRIMARY KEY, last_name text, first_name text);`, keyspace),
		fmt.Sprintf(`INSERT INTO %s.applicant (applicantid, last_name, first_name) VALUES (1, 'test1', 'test11');`, keyspace),
		fmt.Sprintf(`CREATE TABLE %s.jobs (jobid int PRIMARY KEY, job text, department text);`, keyspace),
		fmt.Sprintf(`INSERT INTO %s.jobs (jobid, job, department) VALUES (2, 'test2', 'test22');`, keyspace),
	})
	if err != nil {
		return errors.Wrap(err, "fail to populate database")
	}
	return
}

// execute is a helper function to execute a list of queries
func execute(queries []string) (err error) {
	for _, query := range queries {
		err = session.Query(query).Exec()
		if err != nil {
			return err
		}
	}
	return
}

// newExtractor returns a new extractor
func newExtractor() *cassandra.Extractor {
	return cassandra.New(utils.Logger)
}

// getExpected returns the expected result
func getExpected() []models.Record {
	return []models.Record{
		models.NewRecord(&assetsv1beta1.Table{
			Resource: &commonv1beta1.Resource{
				Urn:  keyspace + ".applicant",
				Name: "applicant",
				Type: "table",
			},
			Schema: &facetsv1beta1.Columns{
				Columns: []*facetsv1beta1.Column{
					{
						Name:     "applicantid",
						DataType: "int",
					},
					{
						Name:     "first_name",
						DataType: "text",
					},
					{
						Name:     "last_name",
						DataType: "text",
					},
				},
			},
		}),
		models.NewRecord(&assetsv1beta1.Table{
			Resource: &commonv1beta1.Resource{
				Urn:  keyspace + ".jobs",
				Name: "jobs",
				Type: "table",
			},
			Schema: &facetsv1beta1.Columns{
				Columns: []*facetsv1beta1.Column{
					{
						Name:     "department",
						DataType: "text",
					},
					{
						Name:     "job",
						DataType: "text",
					},
					{
						Name:     "jobid",
						DataType: "int",
					},
				},
			},
		}),
	}
}
