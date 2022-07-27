package metabase_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/odpf/meteor/models"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/utils"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	host = "https://my-metabase.com"
)

func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		client := new(mockClient)
		config := map[string]interface{}{
			"host":           "sample-host",
			"instance_label": "my-metabase",
		}
		err := metabase.New(client, testutils.Logger).Init(context.TODO(), config)

		assert.Equal(t, plugins.InvalidConfigError{}, err)
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

		err := metabase.New(client, testutils.Logger).Init(context.TODO(), config)
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

		err := metabase.New(client, testutils.Logger).Init(context.TODO(), config)
		assert.NoError(t, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should return dashboard model", func(t *testing.T) {
		dashboards := getDashboardList(t)
		dashboard1 := getDashboard(t, 1)

		client := new(mockClient)
		client.On("Authenticate", host, "test-user", "test-pass", "").Return(nil)
		client.On("GetDashboards").Return(dashboards, nil)
		client.On("GetDashboard", 1).Return(dashboard1, nil)
		client.On("GetTable", 2).Return(getTable(t, 2), nil).Once()
		client.On("GetDatabase", 2).Return(getDatabase(t, 2), nil).Once()
		client.On("GetTable", 5).Return(getTable(t, 5), nil).Once()
		client.On("GetDatabase", 3).Return(getDatabase(t, 3), nil).Once()
		defer client.AssertExpectations(t)

		emitter := mocks.NewEmitter()
		extr := metabase.New(client, plugins.GetLog())
		err := extr.Init(context.TODO(), map[string]interface{}{
			"host":           host,
			"username":       "test-user",
			"password":       "test-pass",
			"instance_label": "my-metabase",
		})
		if err != nil {
			t.Fatal(err)
		}

		err = extr.Extract(context.TODO(), emitter.Push)
		assert.NoError(t, err)

		actuals := emitter.Get()
		//marshal, err := json.Marshal(actuals)
		//if err != nil {
		//	return
		//}
		//fmt.Println("marshal data ", string(marshal))
		//assertResults(t, getExpected(), actuals)
		//for i, j := range getExpected() {
		//	//assert.Equal(t, j, actuals[i])
		//
		//}
		assert.Equal(t, getExpected(), actuals)
		//testutils.AssertWithJSONFile(t, "./testdata/expected.json", actuals)
		//bytes, err := json.Marshal(getExpected()[0])
		//if err != nil {
		//	return
		//}
		//bytes1, err := json.Marshal(actuals[0])
		//if err != nil {
		//	return
		//}
		//
		//assert.Equal(t, string(bytes), string(bytes1))
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

func getExpected() []*v1beta2.Asset {
	chart, _ := anypb.New(&v1beta2.Dashboard{
		Charts: []*v1beta2.Chart{
			{
				Urn:          "metabase::my-metabase/card/1",
				Name:         "Orders, Filtered by Quantity",
				Source:       "metabase",
				Description:  "HELPFUL CHART DESC",
				DashboardUrn: "metabase::my-metabase/dashboard/1",
				Attributes: utils.TryParseMapToProto(map[string]interface{}{
					"id":                     1,
					"collection_id":          1,
					"creator_id":             1,
					"database_id":            1,
					"table_id":               2,
					"query_average_duration": 114,
					"display":                "table",
					"archived":               false,
				}),
				Lineage: &v1beta2.Lineage{
					Upstreams: []*v1beta2.Resource{
						{
							Urn:     "h2::zip:/app/metabase.jar!/sample-dataset.db/ORDERS",
							Service: "h2",
							Type:    "table",
						},
					},
				},
			},
			{
				Urn:          "metabase::my-metabase/card/2",
				Name:         "Exceptional Users",
				Source:       "metabase",
				Description:  "This shows only exceptional users.",
				DashboardUrn: "metabase::my-metabase/dashboard/1",
				Attributes: utils.TryParseMapToProto(map[string]interface{}{
					"id":                     2,
					"collection_id":          0,
					"creator_id":             1,
					"database_id":            2,
					"table_id":               0,
					"query_average_duration": 25,
					"display":                "table",
					"archived":               false,
				}),
				Lineage: &v1beta2.Lineage{
					Upstreams: []*v1beta2.Resource{
						{
							Urn:     "postgres::postgres:5432/postgres/user",
							Service: "postgres",
							Type:    "table",
						},
					},
				},
			},
			{
				Urn:          "metabase::my-metabase/card/3",
				Name:         "Users, Average of Total Followers and Cumulative sum of Total Likes, Filtered by Total Followers",
				Source:       "metabase",
				Description:  "Users, Average of Total Followers",
				DashboardUrn: "metabase::my-metabase/dashboard/1",
				Attributes: utils.TryParseMapToProto(map[string]interface{}{
					"id":                     3,
					"collection_id":          1,
					"creator_id":             1,
					"database_id":            2,
					"table_id":               5,
					"query_average_duration": 30,
					"display":                "table",
					"archived":               false,
				}),
				Lineage: &v1beta2.Lineage{
					Upstreams: []*v1beta2.Resource{
						{
							Urn:     "postgres::postgres:5432/postgres/user",
							Service: "postgres",
							Type:    "table",
						},
					},
				},
			},
			{
				Urn:          "metabase::my-metabase/card/4",
				Name:         "BCR",
				Source:       "metabase",
				DashboardUrn: "metabase::my-metabase/dashboard/1",
				Attributes: utils.TryParseMapToProto(map[string]interface{}{
					"archived":               false,
					"collection_id":          1,
					"creator_id":             1,
					"database_id":            2,
					"display":                "line",
					"id":                     4,
					"query_average_duration": 0,
					"table_id":               0,
				}),
				Lineage: &v1beta2.Lineage{
					Upstreams: []*v1beta2.Resource{
						{
							Urn:     "bigquery::sample-project/dataset_a/invoice",
							Service: "bigquery",
							Type:    "table",
						},
						{
							Urn:     "bigquery::project_a/dataset_b/user",
							Service: "bigquery",
							Type:    "table",
						},
					},
				},
			},
		},
	})

	expectedData := []*v1beta2.Asset{
		{
			Urn:         "metabase::my-metabase/dashboard/1",
			Name:        "Main",
			Service:     "metabase",
			Type:        "dashboard",
			Description: "HELPFUL DESCRIPTION",
			Data:        chart,
			Attributes: utils.TryParseMapToProto(map[string]interface{}{
				"id":            1,
				"collection_id": 1,
				"creator_id":    1,
			}),
			Owners: nil,
			Lineage: &v1beta2.Lineage{
				Upstreams: []*v1beta2.Resource{
					{
						Urn:     "h2::zip:/app/metabase.jar!/sample-dataset.db/ORDERS",
						Service: "h2",
						Type:    "table",
					},
					{
						Urn:     "postgres::postgres:5432/postgres/user",
						Service: "postgres",
						Type:    "table",
					},
					{
						Urn:     "bigquery::sample-project/dataset_a/invoice",
						Service: "bigquery",
						Type:    "table",
					},
					{
						Urn:     "bigquery::project_a/dataset_b/user",
						Service: "bigquery",
						Type:    "table",
					},
				},
			},
			CreateTime: &timestamppb.Timestamp{
				Seconds: 1635178240,
				Nanos:   371000000,
			},
			UpdateTime: &timestamppb.Timestamp{
				Seconds: 1635849178,
				Nanos:   786000000,
			},
		},
	}

	return expectedData
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

// This function compares two slices without concerning about the order
func assertResults(t *testing.T, expected []models.Record, result []models.Record) {
	assert.Len(t, result, len(expected))

	expectedMap := make(map[string]*v1beta2.Asset)
	for _, record := range expected {
		expectedAsset := record.Data()
		expectedMap[expectedAsset.Urn] = expectedAsset
	}

	for _, record := range result {
		actualAsset := record.Data()
		assert.Contains(t, expectedMap, actualAsset.Urn)
		assert.Equal(t, expectedMap[actualAsset.Urn], actualAsset)

		// delete entry to make sure there is no duplicate
		delete(expectedMap, actualAsset.Urn)
	}
}
