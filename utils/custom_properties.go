package utils

import (
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/structpb"
)

// GetCustomProperties returns custom properties of the given asset
func GetCustomProperties(asset *v1beta2.Asset) map[string]interface{} {
	customProps := asset.Attributes

	// if data's custom facet is nil, return new empty custom properties
	if customProps == nil {
		return make(map[string]interface{})
	}

	// return custom fields as map
	return parseToMap(customProps)
}

// SetCustomProperties sets custom properties of the given asset
func SetCustomProperties(asset *v1beta2.Asset, customFields map[string]interface{}) (*v1beta2.Asset, error) {
	record, err := appendCustomFields(asset, customFields)
	if err != nil {
		return asset, errors.Wrap(err, "failed to append custom fields in asset")
	}
	asset.Attributes = record.Attributes
	return asset, nil
}

func appendCustomFields(asset *v1beta2.Asset, customFields map[string]interface{}) (*v1beta2.Asset, error) {
	record := asset
	if record.Attributes == nil {
		record.Attributes = &structpb.Struct{}
	}

	protoStruct, err := parseMapToProto(customFields)
	if err != nil {
		return record, errors.Wrap(err, "failed to parse map to proto")
	}
	record.Attributes = protoStruct

	return record, err
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
