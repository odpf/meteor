//go:build plugins
// +build plugins

package optimus_test

import (
	"context"
	"testing"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/optimus"
	"github.com/odpf/meteor/test/mocks"
	testutils "github.com/odpf/meteor/test/utils"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	validConfig = map[string]interface{}{
		"host": "optimus:80",
	}
	urnScope = "test-optimus"
)

func TestInit(t *testing.T) {
	t.Run("should return error if config is invalid", func(t *testing.T) {
		extr := optimus.New(testutils.Logger, new(mockClient))
		err := extr.Init(context.TODO(), plugins.Config{
			URNScope:  urnScope,
			RawConfig: map[string]interface{}{},
		})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should hit optimus /ping to check connection if config is valid", func(t *testing.T) {
		var err error
		ctx := context.TODO()

		client := new(mockClient)
		client.On("Connect", ctx, validConfig["host"], 0).Return(nil)
		defer client.AssertExpectations(t)

		extr := optimus.New(testutils.Logger, client)
		err = extr.Init(ctx, plugins.Config{
			URNScope:  urnScope,
			RawConfig: validConfig,
		})
		assert.NoError(t, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should build Job models from Optimus", func(t *testing.T) {
		var err error
		ctx := context.TODO()

		client := new(mockClient)
		setupExtractExpectation(ctx, client)
		client.On("Close").Return(nil, nil).Once()
		defer client.AssertExpectations(t)

		extr := optimus.New(testutils.Logger, client)
		err = extr.Init(ctx, plugins.Config{URNScope: urnScope, RawConfig: validConfig})
		require.NoError(t, err)

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		actual := emitter.GetAllData()
		testutils.AssertWithJSONFile(t, "testdata/expected.json", actual)
	})
}

type mockClient struct {
	pb.NamespaceServiceClient
	pb.ProjectServiceClient
	pb.JobSpecificationServiceClient
	pb.JobRunServiceClient
	mock.Mock
}

func (c *mockClient) Connect(ctx context.Context, host string, maxSizeInMB int) (err error) {
	args := c.Called(ctx, host, maxSizeInMB)

	return args.Error(0)
}

func (c *mockClient) Close() error {
	args := c.Called()
	return args.Error(0)
}

func (c *mockClient) ListProjects(ctx context.Context, in *pb.ListProjectsRequest, opts ...grpc.CallOption) (*pb.ListProjectsResponse, error) {
	args := c.Called(ctx, in, opts)

	return args.Get(0).(*pb.ListProjectsResponse), args.Error(1)
}

func (c *mockClient) ListProjectNamespaces(ctx context.Context, in *pb.ListProjectNamespacesRequest, opts ...grpc.CallOption) (*pb.ListProjectNamespacesResponse, error) {
	args := c.Called(ctx, in, opts)

	return args.Get(0).(*pb.ListProjectNamespacesResponse), args.Error(1)
}

func (c *mockClient) ListJobSpecification(ctx context.Context, in *pb.ListJobSpecificationRequest, opts ...grpc.CallOption) (*pb.ListJobSpecificationResponse, error) {
	args := c.Called(ctx, in, opts)

	return args.Get(0).(*pb.ListJobSpecificationResponse), args.Error(1)
}

func (c *mockClient) GetJobTask(
	ctx context.Context,
	in *pb.GetJobTaskRequest,
	opts ...grpc.CallOption,
) (*pb.GetJobTaskResponse, error) {
	args := c.Called(ctx, in, opts)

	return args.Get(0).(*pb.GetJobTaskResponse), args.Error(1)
}

func setupExtractExpectation(ctx context.Context, client *mockClient) {
	client.On("Connect", ctx, validConfig["host"], 0).Return(nil).Once()

	client.On("ListProjects", ctx, &pb.ListProjectsRequest{}, mock.Anything).Return(&pb.ListProjectsResponse{
		Projects: []*pb.ProjectSpecification{
			{
				Name: "project-A",
				Config: map[string]string{
					"BAR": "foo",
				},
				Secrets: []*pb.ProjectSpecification_ProjectSecret{},
			},
			{
				Name: "project-B",
				Config: map[string]string{
					"FOO": "bar",
				},
				Secrets: []*pb.ProjectSpecification_ProjectSecret{},
			},
		},
	}, nil).Once()

	client.On("ListProjectNamespaces", ctx, &pb.ListProjectNamespacesRequest{
		ProjectName: "project-A",
	}, mock.Anything).Return(&pb.ListProjectNamespacesResponse{
		Namespaces: []*pb.NamespaceSpecification{
			{
				Name:   "namespace-A",
				Config: map[string]string{},
			},
		},
	}, nil).Once()

	client.On("ListProjectNamespaces", ctx, &pb.ListProjectNamespacesRequest{
		ProjectName: "project-B",
	}, mock.Anything).Return(&pb.ListProjectNamespacesResponse{
		Namespaces: []*pb.NamespaceSpecification{},
	}, nil).Once()

	client.On("ListJobSpecification", ctx, &pb.ListJobSpecificationRequest{
		ProjectName:   "project-A",
		NamespaceName: "namespace-A",
	}, mock.Anything).Return(&pb.ListJobSpecificationResponse{
		Jobs: []*pb.JobSpecification{
			{
				Version:       1,
				Name:          "job-A",
				Owner:         "john_doe@example.com",
				StartDate:     "2019-09-16",
				EndDate:       "",
				Interval:      "0 19 * * *",
				DependsOnPast: false,
				CatchUp:       false,
				TaskName:      "bq2bq",
				Config: []*pb.JobConfigItem{
					{
						Name:  "FOO_A_1",
						Value: "BAR_A_1",
					},
					{
						Name:  "FOO_A_2",
						Value: "BAR_A_2",
					},
				},
				WindowSize:       "48h",
				WindowOffset:     "24h",
				WindowTruncateTo: "d",
				Dependencies:     []*pb.JobDependency{},
				Assets: map[string]string{
					"query.sql": "SELECT * FROM projectA.datasetB.tableC",
				},
				Hooks:       []*pb.JobSpecHook{},
				Description: "sample description for job-A",
				Labels: map[string]string{
					"orchestrator": "optimus",
				},
				Behavior: &pb.JobSpecification_Behavior{
					Retry: &pb.JobSpecification_Behavior_Retry{
						Count: 0,
						Delay: &durationpb.Duration{
							Seconds: 0,
						},
						ExponentialBackoff: false,
					},
					Notify: []*pb.JobSpecification_Behavior_Notifiers{},
				},
			},
			{
				Version:       1,
				Name:          "job-B",
				Owner:         "jane_doe@example.com",
				StartDate:     "2021-01-01",
				EndDate:       "",
				Interval:      "0 19 1 * *",
				DependsOnPast: false,
				CatchUp:       true,
				TaskName:      "bq2bq",
				Config: []*pb.JobConfigItem{
					{
						Name:  "FOO_B_1",
						Value: "BAR_B_1",
					},
					{
						Name:  "FOO_B_2",
						Value: "BAR_B_2",
					},
				},
				WindowSize:       "720h",
				WindowOffset:     "-720h",
				WindowTruncateTo: "M",
				Dependencies:     []*pb.JobDependency{},
				Assets: map[string]string{
					"query.sql": "SELECT * FROM projectZ.datasetY.tableX",
				},
				Hooks:       []*pb.JobSpecHook{},
				Description: "sample description for job-B",
				Labels: map[string]string{
					"orchestrator": "optimus",
				},
				Behavior: &pb.JobSpecification_Behavior{
					Retry: &pb.JobSpecification_Behavior_Retry{
						Count: 0,
						Delay: &durationpb.Duration{
							Seconds: 0,
						},
						ExponentialBackoff: false,
					},
					Notify: []*pb.JobSpecification_Behavior_Notifiers{},
				},
			},
		},
	}, nil).Once()

	client.On("GetJobTask", ctx, &pb.GetJobTaskRequest{
		ProjectName:   "project-A",
		NamespaceName: "namespace-A",
		JobName:       "job-A",
	}, mock.Anything).Return(&pb.GetJobTaskResponse{
		Task: &pb.JobTask{
			Name:        "task-A",
			Description: "task's description",
			Image:       "task's image",
			Destination: &pb.JobTask_Destination{
				Destination: "bigquery://dst-project:dst-dataset.dst-table",
				Type:        "bigquery",
			},
			Dependencies: []*pb.JobTask_Dependency{
				{
					Dependency: "bigquery://src-project:src-dataset.src-table",
				},
			},
		},
	}, nil).Once()

	client.On("GetJobTask", ctx, &pb.GetJobTaskRequest{
		ProjectName:   "project-A",
		NamespaceName: "namespace-A",
		JobName:       "job-B",
	}, mock.Anything).Return(&pb.GetJobTaskResponse{
		Task: &pb.JobTask{
			Name:        "task-B",
			Description: "task's description B",
			Image:       "task's image B",
			Destination: &pb.JobTask_Destination{
				Destination: "bigquery://dst-b-project:dst-b-dataset.dst-b-table",
				Type:        "bigquery",
			},
			Dependencies: []*pb.JobTask_Dependency{
				{Dependency: "bigquery://src-b1-project:src-b1-dataset.src-b1-table"},
				{Dependency: "bigquery://src-b2-project:src-b2-dataset.src-b2-table"},
			},
		},
	}, nil).Once()
}
