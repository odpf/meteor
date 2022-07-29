package recipe

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// RecipeNode contains the json data for a recipe node
type RecipeNode struct {
	Name       yaml.Node    `json:"name" yaml:"name"`
	Version    yaml.Node    `json:"version" yaml:"version"`
	Source     PluginNode   `json:"source" yaml:"source"`
	Sinks      []PluginNode `json:"sinks" yaml:"sinks"`
	Processors []PluginNode `json:"processors" yaml:"processors"`
}

// PluginNode contains the json data for a recipe node that is being used for
// generating the plugins code for a recipe.
type PluginNode struct {
	Name   yaml.Node            `json:"name" yaml:"name"`
	Type   yaml.Node            `json:"type" yaml:"type"`
	Scope  yaml.Node            `json:"scope" yaml:"scope"`
	Config map[string]yaml.Node `json:"config" yaml:"config"`
}

// decodeConfig decodes the plugins config
func (plug PluginNode) decodeConfig() (map[string]interface{}, error) {
	config := make(map[string]interface{})

	for key, val := range plug.Config {
		var configVal interface{}
		if err := val.Decode(&configVal); err != nil {
			return nil, fmt.Errorf("error decoding config :%w", err)
		}
		config[key] = configVal
	}

	return config, nil
}

// toRecipe passes the value from RecipeNode to Recipe
func (node RecipeNode) toRecipe() (recipe Recipe, err error) {
	// It supports both tags `name` and `type` for source
	// till `type` tag gets deprecated
	if node.Source.Name.IsZero() {
		node.Source.Name = node.Source.Type
	}
	sourceConfig, err := node.Source.decodeConfig()
	if err != nil {
		err = fmt.Errorf("error decoding source config :%w", err)
		return
	}
	processors, err := node.toProcessors()
	if err != nil {
		err = fmt.Errorf("error building processors :%w", err)
		return
	}
	sinks, err := node.toSinks()
	if err != nil {
		err = fmt.Errorf("error building sinks :%w", err)
		return
	}

	recipe = Recipe{
		Name:    node.Name.Value,
		Version: node.Version.Value,
		Source: PluginRecipe{
			Name:   node.Source.Name.Value,
			Scope:  node.Source.Scope.Value,
			Config: sourceConfig,
			Node:   node.Source,
		},
		Sinks:      sinks,
		Processors: processors,
		Node:       node,
	}

	return
}

// toProcessors passes the value of processor PluginNode to its PluginRecipe
func (node RecipeNode) toProcessors() (processors []PluginRecipe, err error) {
	for _, processor := range node.Processors {
		processorConfig, cfgErr := processor.decodeConfig()
		if cfgErr != nil {
			err = fmt.Errorf("error decoding processor config :%w", cfgErr)
			return
		}
		processors = append(processors, PluginRecipe{
			Name:   processor.Name.Value,
			Config: processorConfig,
			Node:   processor,
		})
	}
	return
}

// toSinks passes the value of sink PluginNode to its PluginRecipe
func (node RecipeNode) toSinks() (sinks []PluginRecipe, err error) {
	for _, sink := range node.Sinks {
		sinkConfig, cfgErr := sink.decodeConfig()
		if cfgErr != nil {
			err = fmt.Errorf("error decoding sink config :%w", cfgErr)
			return
		}
		sinks = append(sinks, PluginRecipe{
			Name:   sink.Name.Value,
			Config: sinkConfig,
			Node:   sink,
		})
	}
	return
}
