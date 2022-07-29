package github

import (
	"context"
	_ "embed" // used to print the embedded assets

	"github.com/pkg/errors"

	"github.com/google/go-github/v37/github"
	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	"golang.org/x/oauth2"
)

//go:embed README.md
var summary string

// Config holds the set of configuration for the extractor
type Config struct {
	Org   string `mapstructure:"org" validate:"required"`
	Token string `mapstructure:"token" validate:"required"`
}

var sampleConfig = `
org: odpf
token: github_token`

var info = plugins.Info{
	Description:  "User list from Github organisation.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"platform", "extractor"},
}

// Extractor manages the extraction of data from the extractor
type Extractor struct {
	plugins.BaseExtractor
	logger log.Logger
	config Config
	client *github.Client
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
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
		return errors.Wrap(err, "failed to fetch organizations")
	}
	for _, user := range users {
		usr, _, err := e.client.Users.Get(ctx, *user.Login)
		if err != nil {
			e.logger.Error("failed to fetch user", "error", err)
			continue
		}
		emit(models.NewRecord(&assetsv1beta1.User{
			Resource: &commonv1beta1.Resource{
				Urn:     models.NewURN("github", e.UrnScope, "user", usr.GetNodeID()),
				Service: "github",
				Name:    usr.GetEmail(),
				Type:    "user",
				Url:     usr.GetURL(),
			},
			Email:    usr.GetEmail(),
			Username: usr.GetLogin(),
			FullName: usr.GetName(),
			Status:   "active",
		}))
	}

	return nil
}

// init registers the extractor to catalog
func init() {
	if err := registry.Extractors.Register("github", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
