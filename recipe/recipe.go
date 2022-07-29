package recipe

// Recipe contains the json data for a recipe
type Recipe struct {
	Name       string         `json:"name" yaml:"name" validate:"required"`
	Version    string         `json:"version" yaml:"version" validate:"required"`
	Source     PluginRecipe   `json:"source" yaml:"source" validate:"required"`
	Sinks      []PluginRecipe `json:"sinks" yaml:"sinks" validate:"required,min=1"`
	Processors []PluginRecipe `json:"processors" yaml:"processors"`
	Node       RecipeNode
}

// PluginRecipe contains the json data for a recipe that is being used for
// generating the plugins code for a recipe.
type PluginRecipe struct {
	Name   string                 `json:"name" yaml:"name" validate:"required"`
	Scope  string                 `json:"scope" yaml:"scope"`
	Config map[string]interface{} `json:"config" yaml:"config"`
	Node   PluginNode
}
