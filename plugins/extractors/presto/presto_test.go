//go:build plugins
// +build plugins

package presto_test

import (
	"context"
	"database/sql"
	"fmt"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/presto"
	"github.com/odpf/meteor/test/mocks"
	"github.com/odpf/meteor/test/utils"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	_ "github.com/prestodb/presto-go-client/presto"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

const (
	user = "presto"
	port = "8888"
)

var (
	host = "localhost:" + port
	db   *sql.DB
)

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository:   "ahanaio/prestodb-sandbox",
		Tag:          "0.270",
		ExposedPorts: []string{"8080"},
		PortBindings: map[docker.Port][]docker.PortBinding{"8080": {{HostIP: "0.0.0.0", HostPort: port}}},
	}

	// dsn format - http[s]://user[:pass]@host[:port][?parameters]
	retryFn := func(r *dockertest.Resource) (err error) {
		dsn := fmt.Sprintf("http://presto@localhost:%s", port)
		db, err = sql.Open("presto", dsn)
		if err != nil {
			return err
		}
		_, err = db.Query("SELECT 1")
		return
	}
	purgeFn, err := utils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}

	// Run tests
	code := m.Run()

	// Clean tests
	if err = db.Close(); err != nil {
		log.Fatal(err)
	}
	if err = purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

// TestInit tests the configs
func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := presto.New(utils.Logger).Init(context.TODO(), map[string]interface{}{
			"invalid_config": "invalid_config_value",
		})
		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}

// TestExtract tests that the extractor returns the expected result
func TestExtract(t *testing.T) {
	t.Run("should return mock data we generated with presto", func(t *testing.T) {
		ctx := context.TODO()
		newExtractor := presto.New(utils.Logger)

		if err := newExtractor.Init(ctx, map[string]interface{}{
			"connection_url":  fmt.Sprintf("http://%s@%s", user, host),
			"exclude_catalog": "memory,jmx,tpcds,tpch", // only system catalog is not excluded
		}); err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err := newExtractor.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		var urns []string
		for _, record := range emitter.Get() {
			table := record.Data().(*v1beta2.Asset)
			urns = append(urns, table.Resource.Urn)

		}
		assert.Equal(t, 30, len(urns))
	})
}
