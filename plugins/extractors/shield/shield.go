package shield

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"

	"github.com/odpf/meteor/models"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	sh "github.com/odpf/shield/proto/v1beta1"
	"google.golang.org/protobuf/types/known/anypb"
)

//go:embed README.md
var summary string

// Config holds the set of configuration for the shield extractor
type Config struct {
	Host string `mapstructure:"host" validate:"required"`
}

var sampleConfig = `
host: shield.com:80`

// Extractor manages the communication with the shield service
type Extractor struct {
	logger log.Logger
	config Config
	client Client
}

func New(logger log.Logger, client Client) *Extractor {
	return &Extractor{
		logger: logger,
		client: client,
	}
}

// Info returns the detailed information about the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Shield' users metadata",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"shield", "extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	if err := utils.BuildConfig(configMap, &e.config); err != nil {
		return plugins.InvalidConfigError{}
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
		data, err := anypb.New(&v1beta2.User{
			Email:    user.GetEmail(),
			Username: user.GetId(),
			FullName: user.GetName(),
			Status:   "active",
			Memberships: []*v1beta2.Membership{
				{
					GroupUrn: fmt.Sprintf("%s:%s", grp.Group.GetName(), grp.Group.GetId()),
					Role:     []string{role.Role.GetName()},
				},
			},
		})
		if err != nil {
			err = fmt.Errorf("error creating Any struct: %w", err)
			return err
		}
		emit(models.NewRecord(&v1beta2.Asset{
			Urn:         fmt.Sprintf("%s::%s/%s", service, e.config.Host, user.GetId()),
			Name:        user.GetName(),
			Service:     service,
			Type:        "user",
			Description: user.GetSlug(),
			Data:        data,
			CreateTime:  user.GetCreatedAt(),
			UpdateTime:  user.GetUpdatedAt(),
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
