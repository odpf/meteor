package agent

import (
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/recipe"
)

func recipeToPluginConfig(pr recipe.PluginRecipe) plugins.Config {
	return plugins.Config{
		URNScope:  pr.Scope,
		RawConfig: pr.Config,
	}
}
