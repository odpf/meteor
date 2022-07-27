package models

import (
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
)

// Record represents the metadata of a record
type Record struct {
	data *v1beta2.Asset
}

// NewRecord creates a new record
func NewRecord(data *v1beta2.Asset) Record {
	return Record{data: data}
}

// Data returns the record data
func (r Record) Data() *v1beta2.Asset {
	return r.data
}
