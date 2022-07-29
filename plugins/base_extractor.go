package plugins

import "context"

type BaseExtractor struct {
	BasePlugin
}

func NewBaseExtractor(info Info, configRef interface{}) BaseExtractor {
	return BaseExtractor{
		BasePlugin: NewBasePlugin(info, configRef),
	}
}

// Validate checks if the given options is valid for the plugin.
func (p *BaseExtractor) Validate(config Config) error {
	if config.URNScope == "" {
		return ErrEmptyURNScope
	}

	return p.BasePlugin.Validate(config)
}

// Init will be called once before running the plugin.
// This is where you want to initiate any client or test any connection to external service.
func (p *BaseExtractor) Init(ctx context.Context, config Config) error {
	p.UrnScope = config.URNScope
	p.RawConfig = config.RawConfig

	return p.Validate(config)
}
