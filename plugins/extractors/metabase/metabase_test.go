//go:build plugins
// +build plugins

package metabase_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	testutils "github.com/odpf/meteor/test/utils"
	"github.com/pkg/errors"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/metabase"
	"github.com/odpf/meteor/test/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	host     = "https://my-metabase.com"
	urnScope = "test-metabase"
)

func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		client := new(mockClient)
		config := map[string]interface{}{
			"host":           "sample-host",
			"instance_label": "my-metabase",
		}
		err := metabase.New(client, testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope:  urnScope,
			RawConfig: config,
		})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
	t.Run("should authenticate with client if config is valid", func(t *testing.T) {
		config := map[string]interface{}{
			"host":           "sample-host",
			"instance_label": "my-metabase",
			"username":       "user",
			"password":       "sample-password",
		}

		client := new(mockClient)
		client.On("Authenticate", "sample-host", "user", "sample-password", "").Return(nil)

		err := metabase.New(client, testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope:  urnScope,
			RawConfig: config,
		})
		assert.NoError(t, err)
	})
	t.Run("should allow session_id to replace username and password", func(t *testing.T) {
		config := map[string]interface{}{
			"host":           "sample-host",
			"instance_label": "my-metabase",
			"session_id":     "sample-session",
		}

		client := new(mockClient)
		client.On("Authenticate", "sample-host", "", "", "sample-session").Return(nil)

		err := metabase.New(client, testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope:  urnScope,
			RawConfig: config,
		})
		assert.NoError(t, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should return dashboard model", func(t *testing.T) {
		dashboards := getDashboardList(t)
		dashboard_1 := getDashboard(t, 1)

		client := new(mockClient)
		client.On("Authenticate", host, "test-user", "test-pass", "").Return(nil)
		client.On("GetDashboards").Return(dashboards, nil)
		client.On("GetDashboard", 1).Return(dashboard_1, nil)
		client.On("GetTable", 2).Return(getTable(t, 2), nil).Once()
		client.On("GetDatabase", 2).Return(getDatabase(t, 2), nil).Once()
		client.On("GetTable", 5).Return(getTable(t, 5), nil).Once()
		client.On("GetDatabase", 3).Return(getDatabase(t, 3), nil).Once()
		defer client.AssertExpectations(t)

		emitter := mocks.NewEmitter()
		extr := metabase.New(client, plugins.GetLog())
		err := extr.Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"host":           host,
				"username":       "test-user",
				"password":       "test-pass",
				"instance_label": "my-metabase",
			}})
		if err != nil {
			t.Fatal(err)
		}

		err = extr.Extract(context.TODO(), emitter.Push)
		assert.NoError(t, err)

		actuals := emitter.GetAllData()
		testutils.AssertWithJSONFile(t, "./testdata/expected.json", actuals)
	})
}

func getDashboardList(t *testing.T) []metabase.Dashboard {
	var dashboards []metabase.Dashboard
	err := readFromFiles("./testdata/dashboards.json", &dashboards)
	if err != nil {
		t.Fatalf("error reading dashboards: %s", err.Error())
	}

	return dashboards
}

func getDashboard(t *testing.T, id int) metabase.Dashboard {
	var dashboard metabase.Dashboard
	filePath := fmt.Sprintf("./testdata/dashboard_%d.json", id)
	err := readFromFiles(filePath, &dashboard)
	if err != nil {
		t.Fatalf("error reading %s: %s", filePath, err.Error())
	}

	return dashboard
}

func getDatabase(t *testing.T, id int) metabase.Database {
	var database metabase.Database
	filePath := fmt.Sprintf("./testdata/database_%d.json", id)
	err := readFromFiles(filePath, &database)
	if err != nil {
		t.Fatalf("error reading %s: %s", filePath, err.Error())
	}

	return database
}

func getTable(t *testing.T, id int) metabase.Table {
	var table metabase.Table
	filePath := fmt.Sprintf("./testdata/table_%d.json", id)
	err := readFromFiles(filePath, &table)
	if err != nil {
		t.Fatalf("error reading %s: %s", filePath, err.Error())
	}

	return table
}

func readFromFiles(path string, data interface{}) error {
	file, err := os.Open(path)
	if err != nil {
		return errors.Wrapf(err, "error opening \"%s\"", path)
	}
	err = json.NewDecoder(file).Decode(&data)
	if err != nil {
		return errors.Wrapf(err, "error decoding \"%s\"", path)
	}

	return nil
}

type mockClient struct {
	mock.Mock
}

func (m *mockClient) Authenticate(host, username, password, sessionID string) error {
	args := m.Called(host, username, password, sessionID)
	return args.Error(0)
}

func (m *mockClient) GetDashboards() ([]metabase.Dashboard, error) {
	args := m.Called()
	return args.Get(0).([]metabase.Dashboard), args.Error(1)
}

func (m *mockClient) GetDashboard(id int) (metabase.Dashboard, error) {
	args := m.Called(id)
	return args.Get(0).(metabase.Dashboard), args.Error(1)
}

func (m *mockClient) GetDatabase(id int) (metabase.Database, error) {
	args := m.Called(id)
	return args.Get(0).(metabase.Database), args.Error(1)
}

func (m *mockClient) GetTable(id int) (metabase.Table, error) {
	args := m.Called(id)
	return args.Get(0).(metabase.Table), args.Error(1)
}
