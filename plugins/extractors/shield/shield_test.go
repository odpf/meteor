//go:build plugins
// +build plugins

package shield_test

import (
	"context"
	"testing"

	"github.com/odpf/meteor/plugins/extractors/shield"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/test/mocks"
	testutils "github.com/odpf/meteor/test/utils"
	sh "github.com/odpf/shield/proto/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

var (
	validConfig = map[string]interface{}{
		"host": "shield:80",
	}
	urnScope = "test-shield"
)

func TestInit(t *testing.T) {
	t.Run("should return error if config is invalid", func(t *testing.T) {
		extr := shield.New(testutils.Logger, new(mockClient))
		err := extr.Init(context.TODO(), plugins.Config{URNScope: urnScope, RawConfig: map[string]interface{}{}})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should hit shield /admin/ping to check connection if config is valid", func(t *testing.T) {
		var err error
		ctx := context.TODO()

		client := new(mockClient)
		client.On("Connect", ctx, validConfig["host"]).Return(nil)
		defer client.AssertExpectations(t)

		extr := shield.New(testutils.Logger, client)
		err = extr.Init(ctx, plugins.Config{URNScope: urnScope, RawConfig: validConfig})
		assert.NoError(t, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should extract user information from shield", func(t *testing.T) {
		var err error
		ctx := context.TODO()

		client := new(mockClient)
		setupExtractExpectation(ctx, client)
		client.On("Close").Return(nil, nil).Once()
		defer client.AssertExpectations(t)

		extr := shield.New(testutils.Logger, client)
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
	sh.ShieldServiceClient
	mock.Mock
}

func (c *mockClient) Connect(ctx context.Context, host string) (err error) {
	args := c.Called(ctx, host)

	return args.Error(0)
}

func (c *mockClient) Close() error {
	args := c.Called()

	return args.Error(0)
}

func (c *mockClient) ListUsers(ctx context.Context, in *sh.ListUsersRequest, opts ...grpc.CallOption) (*sh.ListUsersResponse, error) {
	args := c.Called(ctx, in, opts)

	return args.Get(0).(*sh.ListUsersResponse), args.Error(1)
}

func (c *mockClient) GetGroup(ctx context.Context, in *sh.GetGroupRequest, opts ...grpc.CallOption) (*sh.GetGroupResponse, error) {
	args := c.Called(ctx, in, opts)

	return args.Get(0).(*sh.GetGroupResponse), args.Error(1)
}

func (c *mockClient) GetRole(ctx context.Context, in *sh.GetRoleRequest, opts ...grpc.CallOption) (*sh.GetRoleResponse, error) {
	args := c.Called(ctx, in, opts)

	return args.Get(0).(*sh.GetRoleResponse), args.Error(1)
}

func setupExtractExpectation(ctx context.Context, client *mockClient) {
	client.On("Connect", ctx, validConfig["host"]).Return(nil).Once()

	client.On("ListUsers", ctx, &sh.ListUsersRequest{}, mock.Anything).Return(&sh.ListUsersResponse{
		Users: []*sh.User{
			{
				Id:       "user-A",
				Name:     "fullname-A",
				Slug:     "sample description for user-A",
				Email:    "user1@gojek.com",
				Metadata: nil,
				CreatedAt: &timestamppb.Timestamp{
					Seconds: 2400,
				},
				UpdatedAt: &timestamppb.Timestamp{
					Seconds: 2100,
				},
			},
			{
				Id:       "user-B",
				Name:     "fullname-B",
				Slug:     "sample description for user-B",
				Email:    "user2@gojek.com",
				Metadata: nil,
				CreatedAt: &timestamppb.Timestamp{
					Seconds: 1200,
				},
				UpdatedAt: &timestamppb.Timestamp{
					Seconds: 900,
				},
			},
		},
	}, nil).Once()

	client.On("GetRole", ctx, &sh.GetRoleRequest{
		Id: "user-A",
	}, mock.Anything).Return(&sh.GetRoleResponse{
		Role: &sh.Role{
			Id:   "user-A",
			Name: "role-A",
		},
	}, nil).Once()

	client.On("GetRole", ctx, &sh.GetRoleRequest{
		Id: "user-B",
	}, mock.Anything).Return(&sh.GetRoleResponse{
		Role: &sh.Role{
			Id:   "user-B",
			Name: "role-B",
		},
	}, nil).Once()

	client.On("GetGroup", ctx, &sh.GetGroupRequest{
		Id: "user-A",
	}, mock.Anything).Return(&sh.GetGroupResponse{
		Group: &sh.Group{
			Id:   "grpId-A",
			Name: "grpname-A",
		},
	}, nil).Once()

	client.On("GetGroup", ctx, &sh.GetGroupRequest{
		Id: "user-B",
	}, mock.Anything).Return(&sh.GetGroupResponse{
		Group: &sh.Group{
			Id:   "grpId-B",
			Name: "grpname-B",
		},
	}, nil).Once()
}
