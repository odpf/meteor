package shield

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"

	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	sh "github.com/odpf/shield/proto/v1beta1"
)

//go:embed README.md
var summary string

// Config holds the set of configuration for the shield extractor
type Config struct {
	Host string `mapstructure:"host" validate:"required"`
}

var sampleConfig = `
host: shield.com:80`

var info = plugins.Info{
	Description:  "Shield' users metadata",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"shield", "extractor"},
}

// Extractor manages the communication with the shield service
type Extractor struct {
	plugins.BaseExtractor
	logger log.Logger
	config Config
	client Client
}

func New(logger log.Logger, client Client) *Extractor {
	e := &Extractor{
		logger: logger,
		client: client,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	if err := e.client.Connect(ctx, e.config.Host); err != nil {
		return fmt.Errorf("error connecting to host: %w", err)
	}

	return
}

// Extract extracts the user information
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	defer e.client.Close()

	listUsers, err := e.client.ListUsers(ctx, &sh.ListUsersRequest{
		Fields: nil,
	})
	if err != nil {
		return fmt.Errorf("error fetching users: %w", err)
	}

	for _, user := range listUsers.Users {
		role, roleErr := e.client.GetRole(ctx, &sh.GetRoleRequest{Id: user.GetId()})
		if roleErr != nil {
			return fmt.Errorf("error fetching user roles: %w", err)
		}

		grp, grpErr := e.client.GetGroup(ctx, &sh.GetGroupRequest{Id: user.GetId()})
		if grpErr != nil {
			return fmt.Errorf("error fetching user groups: %w", err)
		}

		emit(models.NewRecord(&assetsv1beta1.User{
			Resource: &commonv1beta1.Resource{
				Urn:         models.NewURN(service, e.UrnScope, "user", user.GetId()),
				Name:        user.GetName(),
				Service:     service,
				Type:        "user",
				Description: user.GetSlug(),
			},
			Email:    user.GetEmail(),
			Username: user.GetId(),
			FullName: user.GetName(),
			Status:   "active",
			Memberships: []*assetsv1beta1.Membership{
				{
					GroupUrn: fmt.Sprintf("%s:%s", grp.Group.GetName(), grp.Group.GetId()),
					Role:     []string{role.Role.GetName()},
				},
			},
			Timestamps: &commonv1beta1.Timestamp{
				CreateTime: user.GetCreatedAt(),
				UpdateTime: user.GetUpdatedAt(),
			},
		}))
	}

	return nil
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("shield", func() plugins.Extractor {
		return New(plugins.GetLog(), newClient())
	}); err != nil {
		panic(err)
	}
}
