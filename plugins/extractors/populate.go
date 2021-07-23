package extractors

import (
	"net/http"

	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/bigquery"
	"github.com/odpf/meteor/plugins/extractors/github"
	"github.com/odpf/meteor/plugins/extractors/clickhouse"
	"github.com/odpf/meteor/plugins/extractors/googlecloudstorage"
	"github.com/odpf/meteor/plugins/extractors/grafana"
	"github.com/odpf/meteor/plugins/extractors/bigtable"
	"github.com/odpf/meteor/plugins/extractors/csv"
	"github.com/odpf/meteor/plugins/extractors/kafka"
	"github.com/odpf/meteor/plugins/extractors/mongodb"
	"github.com/odpf/meteor/plugins/extractors/mssql"
	"github.com/odpf/meteor/plugins/extractors/mysql"
	"github.com/odpf/meteor/plugins/extractors/postgres"
)

func PopulateFactory(factory *extractor.Factory, logger plugins.Logger) {
	// populate topic extractors
	factory.SetTopicExtractor("kafka", func() extractor.TopicExtractor {
		return kafka.New(logger)
	})

	// populate table extractors
	factory.SetTableExtractor("bigquery", func() extractor.TableExtractor {
		return bigquery.New(logger)
	})

	factory.SetTableExtractor("clickhouse", clickhouse.New)
	factory.SetTableExtractor("csv", func() extractor.TableExtractor {
		return csv.New(logger)
	})
	factory.SetTableExtractor("mysql", mysql.New)
	factory.SetTableExtractor("mssql", mssql.New)
	factory.SetTableExtractor("mongodb", mongodb.New)
	factory.SetTableExtractor("postgres", postgres.New)

	factory.SetUserExtractor("github", github.New)
	factory.SetBucketExtractor("googlecloudstorage", func() extractor.BucketExtractor {
		return googlecloudstorage.New(logger)
  })
	factory.SetDashboardExtractor("grafana", func() extractor.DashboardExtractor {
		return grafana.New(&http.Client{}, logger)
  })
	factory.SetTableExtractor("bigtable", func() extractor.TableExtractor {
		return bigtable.New(logger)
	})
}
