//go:build plugins
// +build plugins

package bigquery

import (
	"testing"

	"github.com/alecthomas/assert"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins/extractors/bigquery/auditlog"
	"github.com/odpf/meteor/plugins/extractors/bigquery/util"
)

func TestBuildTableProfile(t *testing.T) {
	tableURN := util.TableURN("project1", "dataset1", "table1")
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
				util.TableURN("project1", "dataset1", "table1"): 5,
				util.TableURN("project2", "dataset1", "table1"): 3,
				util.TableURN("project3", "dataset1", "table1"): 1,
			},
			JoinDetail: map[string]map[string]auditlog.JoinDetail{
				util.TableURN("project1", "dataset1", "table1"): {
					util.TableURN("project2", "dataset1", "table1"): auditlog.JoinDetail{
						Usage: 1,
					},
					util.TableURN("project3", "dataset1", "table1"): auditlog.JoinDetail{
						Usage: 3,
					},
					util.TableURN("project4", "dataset1", "table1"): auditlog.JoinDetail{
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
			Urn:   util.TableURN("project2", "dataset1", "table1"),
			Count: 1,
		})
		assert.Contains(t, tp.Joins, &assetsv1beta1.Join{
			Urn:   util.TableURN("project3", "dataset1", "table1"),
			Count: 3,
		})
		assert.Contains(t, tp.Joins, &assetsv1beta1.Join{
			Urn:   util.TableURN("project4", "dataset1", "table1"),
			Count: 1,
		})
	})
}
