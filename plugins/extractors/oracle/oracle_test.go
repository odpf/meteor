//go:build plugins
// +build plugins

package oracle_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/test/utils"

	"database/sql"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/oracle"
	"github.com/odpf/meteor/test/mocks"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	_ "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/assert"
)

var db *sql.DB

const (
	user      = "test_user"
	password  = "oracle"
	port      = "1521"
	defaultDB = "xe"
	sysUser   = "system"
	urnScope  = "test-oracle"
)

var host = "localhost:" + port

func TestMain(m *testing.M) {
	opts := dockertest.RunOptions{
		Repository:   "wnameless/oracle-xe-11g-r2",
		Tag:          "latest",
		Env:          []string{},
		ExposedPorts: []string{port, "1521"},
		PortBindings: map[docker.Port][]docker.PortBinding{"1521": {{HostIP: "0.0.0.0", HostPort: port}}},
	}

	// Exponential backoff-retry for container to be resy to accept connections
	retryFn := func(r *dockertest.Resource) (err error) {
		db, err = sql.Open("oracle", fmt.Sprintf("oracle://%s:%s@%s/%s", sysUser, password, host, defaultDB))
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

func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := oracle.New(utils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"password": "pass",
				"host":     host,
			}})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
}

func TestExtract(t *testing.T) {
	t.Run("should return mockdata we generated with oracle", func(t *testing.T) {
		ctx := context.TODO()
		extr := oracle.New(utils.Logger)

		err := extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"connection_url": fmt.Sprintf("oracle://%s:%s@%s/%s", user, password, host, defaultDB),
			}})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)

		assert.NoError(t, err)
		assert.Equal(t, getExpected(), emitter.Get())
	})
}

func setup() (err error) {
	// using system user to setup the oracle database
	var queries = []string{
		fmt.Sprintf("CREATE USER %s IDENTIFIED BY %s", user, password),
		fmt.Sprintf("GRANT CREATE SESSION TO %s", user),
		fmt.Sprintf("GRANT DBA TO %s", user),
	}
	err = execute(db, queries)

	userDB, err := sql.Open("oracle", fmt.Sprintf("oracle://%s:%s@%s/%s", user, password, host, defaultDB))
	if err != nil {
		return
	}
	defer userDB.Close()

	var createTables = []string{
		"CREATE TABLE employee (empid integer primary key, name varchar2(30) NOT NULL, salary number(10, 2))",
		"CREATE TABLE department (id integer primary key, title varchar(20) NOT NULL, budget float(26))",
		"COMMENT ON column department.title IS 'Department Name'",
	}

	var populateTables = []string{
		"INSERT INTO employee values(10, 'Sameer', 51000.0)",
		"INSERT INTO employee values(11, 'Jash', 45000.60)",
		"INSERT INTO employee values(12, 'Jay', 70000.11)",
		"INSERT INTO department values(1001, 'Sales', 10000.758)",
		"INSERT INTO department values(1002, 'Marketing', 150000.000)",
		"INSERT INTO department values(1003, 'Devlopment', 70000.5796)",
		"INSERT INTO department values(1004, 'Research', 90000.500)",
	}

	err = execute(userDB, createTables)
	err = execute(userDB, populateTables)

	return
}

func execute(db *sql.DB, queries []string) (err error) {
	for _, query := range queries {
		_, err = db.Exec(query)
		if err != nil {
			return
		}
	}
	return
}

func getExpected() []models.Record {
	return []models.Record{
		models.NewRecord(&assetsv1beta1.Table{
			Resource: &commonv1beta1.Resource{
				Urn:     "urn:oracle:test-oracle:table:XE.EMPLOYEE",
				Name:    "EMPLOYEE",
				Service: "oracle",
				Type:    "table",
			},
			Profile: &assetsv1beta1.TableProfile{
				TotalRows: 3,
			},
			Schema: &facetsv1beta1.Columns{
				Columns: []*facetsv1beta1.Column{
					{
						Name:     "EMPID",
						DataType: "NUMBER",
						Length:   22,
					},
					{
						Name:     "NAME",
						DataType: "VARCHAR2",
						Length:   30,
					},
					{
						Name:       "SALARY",
						DataType:   "NUMBER",
						IsNullable: true,
						Length:     22,
					},
				},
			},
		}),
		models.NewRecord(&assetsv1beta1.Table{
			Resource: &commonv1beta1.Resource{
				Urn:     "urn:oracle:test-oracle:table:XE.DEPARTMENT",
				Name:    "DEPARTMENT",
				Service: "oracle",
				Type:    "table",
			},
			Profile: &assetsv1beta1.TableProfile{
				TotalRows: 4,
			},
			Schema: &facetsv1beta1.Columns{
				Columns: []*facetsv1beta1.Column{
					{
						Name:     "ID",
						DataType: "NUMBER",
						Length:   22,
					},
					{
						Name:        "TITLE",
						Description: "Department Name",
						DataType:    "VARCHAR2",
						Length:      20,
					},
					{
						Name:       "BUDGET",
						DataType:   "FLOAT",
						IsNullable: true,
						Length:     22,
					},
				},
			},
		}),
	}
}
