//go:build plugins
// +build plugins

package bigquery

import (
	"testing"

	"github.com/alecthomas/assert"
	"github.com/odpf/meteor/models"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins/extractors/bigquery/auditlog"
)

func TestBuildTableProfile(t *testing.T) {
	tableURN := models.TableURN("bigquery", "project1", "dataset1", "table1")
	t.Run("table profile usage related fields are empty if usage collection is disabled", func(t *testing.T) {

		var tableStats *auditlog.TableStats
		extr := &Extractor{
			config: Config{
				IsCollectTableUsage: false,
			},
		}

		tp := extr.buildTableProfile(tableURN, tableStats)

		assert.Empty(t, tp.UsageCount)
		assert.Empty(t, tp.Joins)
	})

	t.Run("table profile usage related fields are empty if table stats is nil", func(t *testing.T) {
		extr := &Extractor{
			config: Config{
				IsCollectTableUsage: true,
			},
		}

		tp := extr.buildTableProfile(tableURN, nil)

		assert.Empty(t, tp.UsageCount)
		assert.Empty(t, tp.Joins)
	})

	t.Run("table profile usage related fields are populated if table stats is not nil and usage collection is enabled", func(t *testing.T) {
		tableStats := &auditlog.TableStats{
			TableUsage: map[string]int64{
				models.TableURN("bigquery", "project1", "dataset1", "table1"): 5,
				models.TableURN("bigquery", "project2", "dataset1", "table1"): 3,
				models.TableURN("bigquery", "project3", "dataset1", "table1"): 1,
			},
			JoinDetail: map[string]map[string]auditlog.JoinDetail{
				models.TableURN("bigquery", "project1", "dataset1", "table1"): {
					models.TableURN("bigquery", "project2", "dataset1", "table1"): auditlog.JoinDetail{
						Usage: 1,
					},
					models.TableURN("bigquery", "project3", "dataset1", "table1"): auditlog.JoinDetail{
						Usage: 3,
					},
					models.TableURN("bigquery", "project4", "dataset1", "table1"): auditlog.JoinDetail{
						Usage: 1,
					},
				},
			},
		}

		extr := &Extractor{
			config: Config{
				IsCollectTableUsage: true,
			},
		}

		tp := extr.buildTableProfile(tableURN, tableStats)

		assert.EqualValues(t, 5, tp.UsageCount)
		assert.Contains(t, tp.Joins, &assetsv1beta1.Join{
			Urn:   models.TableURN("bigquery", "project2", "dataset1", "table1"),
			Count: 1,
		})
		assert.Contains(t, tp.Joins, &assetsv1beta1.Join{
			Urn:   models.TableURN("bigquery", "project3", "dataset1", "table1"),
			Count: 3,
		})
		assert.Contains(t, tp.Joins, &assetsv1beta1.Join{
			Urn:   models.TableURN("bigquery", "project4", "dataset1", "table1"),
			Count: 1,
		})
	})
}
