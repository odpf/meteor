package cmd

import (
	"log"

	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/extractors"
	"github.com/odpf/meteor/metrics"
	"github.com/odpf/meteor/processors"
	"github.com/odpf/meteor/recipes"
	"github.com/odpf/meteor/sinks"
)

func initRunner(config config.Config) *recipes.Runner {
	extractorStore := initExtractorStore()
	processorStore := initProcessorStore()
	sinkStore := initSinkStore()
	metricsMonitor := initMetricsMonitor(config)
	return recipes.NewRunner(
		extractorStore,
		processorStore,
		sinkStore,
		metricsMonitor,
	)
}
func initExtractorStore() *extractors.Store {
	store := extractors.NewStore()
	extractors.PopulateStore(store)
	return store
}
func initProcessorStore() *processors.Store {
	store := processors.NewStore()
	processors.PopulateStore(store)
	return store
}
func initSinkStore() *sinks.Store {
	store := sinks.NewStore()
	sinks.PopulateStore(store)
	return store
}
func initMetricsMonitor(c config.Config) *metrics.StatsdMonitor {
	if !c.StatsdEnabled {
		return nil
	}

	client, err := metrics.NewStatsdClient(c.StatsdHost)
	if err != nil {
		log.Fatal(err)
	}
	monitor := metrics.NewStatsdMonitor(client, c.StatsdPrefix)
	return monitor
}
