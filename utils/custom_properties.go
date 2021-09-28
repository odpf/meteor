package utils

import (
	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"google.golang.org/protobuf/types/known/structpb"
)

// GetCustomProperties returns custom properties of the given asset
func GetCustomProperties(metadata models.Metadata) map[string]interface{} {
	customProps := metadata.GetProperties()

	// if data's custom facet is nil, return new empty custom properties
	if customProps == nil {
		return make(map[string]interface{})
	}

	// return custom fields as map
	return parseToMap(customProps.Attributes)
}

// SetCustomProperties sets custom properties of the given asset
func SetCustomProperties(metadata models.Metadata, customFields map[string]interface{}) (models.Metadata, error) {
	properties, err := appendCustomFields(metadata, customFields)
	if err != nil {
		return metadata, err
	}

	switch metadata := metadata.(type) {
	case *assets.Table:
	case *assets.Topic:
	case *assets.Dashboard:
	case *assets.Bucket:
	case *assets.Group:
	case *assets.Job:
	case *assets.User:
		metadata.Properties = properties
	}

	return metadata, nil
}

func appendCustomFields(metadata models.Metadata, customFields map[string]interface{}) (*facets.Properties, error) {
	properties := metadata.GetProperties()
	if properties == nil {
		properties = &facets.Properties{
			Attributes: &structpb.Struct{},
		}
	}

	protoStruct, err := parseMapToProto(customFields)
	if err != nil {
		return properties, err
	}
	properties.Attributes = protoStruct

	return properties, err
}

func parseToMap(src *structpb.Struct) map[string]interface{} {
	if src == nil {
		return nil
	}

	return src.AsMap()
}

func parseMapToProto(src map[string]interface{}) (*structpb.Struct, error) {
	if src == nil {
		return nil, nil
	}

	return structpb.NewStruct(src)
}

// TryParseMapToProto parses given map to proto struct
func TryParseMapToProto(src map[string]interface{}) *structpb.Struct {
	res, err := parseMapToProto(src)
	if err != nil {
		panic(err)
	}

	return res
}
