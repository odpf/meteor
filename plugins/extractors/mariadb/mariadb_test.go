//go:build plugins
// +build plugins

package mariadb_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/mariadb"
	"github.com/odpf/meteor/test/mocks"
	"github.com/odpf/meteor/test/utils"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

const (
	testDB   = "test_db"
	user     = "test_user"
	pass     = "pass"
	port     = "3306"
	urnScope = "test-mariadb"
)

var (
	host = "127.0.0.1:" + port
	db   *sql.DB
)

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository: "mariadb",
		Tag:        "10.6",
		Env: []string{
			"MARIADB_ALLOW_EMPTY_ROOT_PASSWORD=true",
		},
		ExposedPorts: []string{"3306"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"3306": {
				{HostIP: "0.0.0.0", HostPort: "3306"},
			},
		},
	}

	// Exponential backoff-retry for container to accept connections
	retryFn := func(r *dockertest.Resource) (err error) {
		db, err = sql.Open("mysql", fmt.Sprintf("root:@tcp(%s)/", host))
		if err != nil {
			return err
		}
		return db.Ping()
	}
	purgeFn, err := utils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}
	if err := setup(); err != nil {
		log.Fatal(err)
	}

	// Run tests
	code := m.Run()

	// Clean tests
	db.Close()
	if err := purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

// TestInit tests the configs
func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := mariadb.New(utils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"invalid_config": "invalid_config_value",
			}})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
}

// TestExtract tests that the extractor returns the expected result
func TestExtract(t *testing.T) {
	t.Run("should return mockdata we generated with mariadb", func(t *testing.T) {
		ctx := context.TODO()
		newExtractor := mariadb.New(utils.Logger)

		err := newExtractor.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"connection_url": fmt.Sprintf("%s:%s@tcp(%s)/", user, pass, host),
			}})
		if err != nil {

			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = newExtractor.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		var urns []string
		for _, record := range emitter.Get() {
			table := record.Data().(*assetsv1beta1.Table)
			urns = append(urns, table.Resource.Urn)
		}

		assert.Equal(t, []string{
			"urn:mariadb:test-mariadb:table:test_db.applicant",
			"urn:mariadb:test-mariadb:table:test_db.jobs",
		}, urns)
	})
}

// setup is a helper function to set up the test database
func setup() (err error) {
	var queries = []string{
		fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB),
		fmt.Sprintf("CREATE DATABASE %s", testDB),
		fmt.Sprintf("USE %s;", testDB),
		fmt.Sprintf(`CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s';`, user, pass),
		fmt.Sprintf(`GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%';`, user),
	}
	if err = execute(db, queries); err != nil {
		return
	}

	// create and populate tables
	var populate = []string{
		"CREATE TABLE applicant (applicant_id int, last_name varchar(255), first_name varchar(255));",
		"INSERT INTO applicant VALUES (1, 'test1', 'test11');",
		"CREATE TABLE jobs (job_id int, job varchar(255), department varchar(255));",
		"INSERT INTO jobs VALUES (2, 'test2', 'test22');",
	}
	if err = execute(db, populate); err != nil {
		return
	}
	return
}

// execute is a helper function to execute a list of queries
func execute(db *sql.DB, queries []string) (err error) {
	for _, query := range queries {
		if _, err = db.Exec(query); err != nil {
			return
		}
	}
	return
}
