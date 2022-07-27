package tableau

import (
	"github.com/mitchellh/mapstructure"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/pkg/errors"
)

func (e *Extractor) buildLineage(tables []*Table) (lineage *v1beta2.Lineage) {
	upstreamLineages := []*v1beta2.Resource{}
	for _, t := range tables {
		res, err := e.buildLineageResources(t)
		if err != nil {
			e.logger.Warn("failed to build upstreams", "err", err.Error(), "table_id", t.ID, "table_name", t.Name)
			continue
		}
		upstreamLineages = append(upstreamLineages, res)
	}
	lineage = &v1beta2.Lineage{Upstreams: upstreamLineages}
	return
}

func (e *Extractor) buildLineageResources(t *Table) (resource *v1beta2.Resource, err error) {
	if t == nil {
		err = errors.New("no table found")
		return
	}
	var table = *t

	upstreamDB := t.Database
	if _, found := upstreamDB["hostName"]; found {
		// DatabaseServer
		var db DatabaseServer
		err = mapstructure.Decode(upstreamDB, &db)
		if err != nil {
			err = errors.Wrap(err, "error cast database to DatabaseServer struct")
			return
		}
		resource = db.CreateResource(table)
		return
	}
	if _, found := upstreamDB["provider"]; found {
		// CloudFile
		var db CloudFile
		err = mapstructure.Decode(upstreamDB, &db)
		if err != nil {
			err = errors.Wrap(err, "error cast database to CloudFile struct")
			return
		}
		resource = db.CreateResource(table)
		return
	}
	if _, found := upstreamDB["filePath"]; found {
		// File
		var db File
		err = mapstructure.Decode(upstreamDB, &db)
		if err != nil {
			err = errors.Wrap(err, "error cast database to File struct")
			return
		}
		resource = db.CreateResource(table)
		return
	}
	if _, found := upstreamDB["connectorUrl"]; found {
		// WebDataConnector
		var db WebDataConnector
		err = mapstructure.Decode(upstreamDB, &db)
		if err != nil {
			err = errors.Wrap(err, "error cast database to WebDataConnector struct")
			return
		}
		resource = db.CreateResource(table)
		return
	}
	err = errors.New("cannot build lineage resource, database structure unknown")
	return
}
