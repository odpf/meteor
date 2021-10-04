package github

import (
	"context"
	_ "embed" // used to print the embedded assets

	"github.com/google/go-github/v37/github"
	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"golang.org/x/oauth2"
)

//go:embed README.md
var summary string

// Config hold the set of configuration for the extractor
type Config struct {
	Org   string `mapstructure:"org" validate:"required"`
	Token string `mapstructure:"token" validate:"required"`
}

var sampleConfig = `
org: odpf
token: github_token`

// Extractor manages the extraction of data from the extractor
type Extractor struct {
	logger log.Logger
	config Config
	client *github.Client
}

// Info returns the brief information about the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "User list from Github organisation.",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"platform", "extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	err = utils.BuildConfig(configMap, &e.config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: e.config.Token},
	)
	tc := oauth2.NewClient(ctx, ts)
	e.client = github.NewClient(tc)

	return
}

// Extract extracts the data from the extractor
// The data is returned as a list of assets.Asset
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	users, _, err := e.client.Organizations.ListMembers(ctx, e.config.Org, nil)

	if err != nil {
		return err
	}
	for _, user := range users {
		usr, _, err := e.client.Users.Get(ctx, *user.Login)
		if err != nil {
			continue
		}
		emit(models.NewRecord(&assets.User{
			Resource: &common.Resource{
				Urn: usr.GetURL(),
			},
			Email:    usr.GetEmail(),
			Username: usr.GetLogin(),
			FullName: usr.GetName(),
			Status:   "active",
		}))
	}

	return nil
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("github", func() plugins.Extractor {
		return &Extractor{
			logger: plugins.GetLog(),
		}
	}); err != nil {
		panic(err)
	}
}
