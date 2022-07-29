package plugins

import (
	"context"
)

type BasePlugin struct {
	UrnScope  string
	RawConfig map[string]interface{}
	info      Info
	configRef interface{}
}

func NewBasePlugin(info Info, configRef interface{}) BasePlugin {
	return BasePlugin{
		info:      info,
		configRef: configRef,
	}
}

// Info returns plugin's information.
func (p *BasePlugin) Info() Info {
	return p.info
}

// Validate checks if the given options is valid for the plugin.
func (p *BasePlugin) Validate(config Config) error {
	return buildConfig(config.RawConfig, p.configRef)
}

// Init will be called once before running the plugin.
// This is where you want to initiate any client or test any connection to external service.
func (p *BasePlugin) Init(ctx context.Context, config Config) error {
	p.UrnScope = config.URNScope
	p.RawConfig = config.RawConfig

	return p.Validate(config)
}
