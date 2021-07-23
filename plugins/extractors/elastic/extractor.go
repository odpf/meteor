package elastic

import (
	"encoding/json"
	"errors"
	"reflect"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/mitchellh/mapstructure"
)

type Config struct {
	Host string `mapstructure:"host"`
}

type Extractor struct{}

func (e *Extractor) Extract(configMap map[string]interface{}) (result []map[string]interface{}, err error) {
	config, err := e.getConfig(configMap)
	if err != nil {
		return
	}
	err = e.validateConfig(config)
	if err != nil {
		return
	}
	cfg := elasticsearch.Config{
		Addresses: []string{
			config.Host,
		},
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return
	}
	result, err = e.listIndexes(client)
	if err != nil {
		return
	}
	return
}

func (e *Extractor) listIndexes(client *elasticsearch.Client) (result []map[string]interface{}, err error) {
	res, err := client.Cluster.Health(
		client.Cluster.Health.WithLevel("indices"),
	)
	if err != nil {
		return
	}
	var r map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&r)
	if err != nil {
		return
	}
	x := reflect.ValueOf(r["indices"]).MapRange()
	for x.Next() {
		row := make(map[string]interface{})
		row["index_name"] = x.Key().String()
		doc_properties, err1 := e.listIndexInfo(client, x.Key().String())
		if err1 != nil {
			err = err1
			return
		}
		row["document_properties"] = doc_properties
		countRes, err1 := client.Search(
			client.Search.WithIndex(x.Key().String()),
		)
		if err1 != nil {
			err = err1
			return
		}
		var t map[string]interface{}
		err = json.NewDecoder(countRes.Body).Decode(&t)
		if err != nil {
			res.Body.Close()
			return
		}
		row["document_count"] = len(t["hits"].(map[string]interface{})["hits"].([]interface{}))
		result = append(result, row)
	}
	return
}

func (e *Extractor) listIndexInfo(client *elasticsearch.Client, index string) (result map[string]interface{}, err error) {
	var r map[string]interface{}
	res, err := client.Indices.GetMapping(
		client.Indices.GetMapping.WithIndex(index),
	)
	if err != nil {
		return
	}
	err = json.NewDecoder(res.Body).Decode(&r)
	if err != nil {
		res.Body.Close()
		return
	}
	result = r[index].(map[string]interface{})["mappings"].(map[string]interface{})["properties"].(map[string]interface{})
	res.Body.Close()
	return
}

func (e *Extractor) getConfig(configMap map[string]interface{}) (config Config, err error) {
	err = mapstructure.Decode(configMap, &config)
	return
}

func (e *Extractor) validateConfig(config Config) (err error) {
	if config.Host == "" {
		return errors.New("atleast one host address is required")
	}
	return
}
