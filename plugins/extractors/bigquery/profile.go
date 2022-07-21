package bigquery

import (
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins/extractors/bigquery/auditlog"
)

func (e *Extractor) buildTableProfile(tableURN string, tableStats *auditlog.TableStats) (tp *v1beta2.TableProfile) {
	var tableUsage int64
	var commonJoins []*v1beta2.TableCommonJoin
	var filterConditions []string

	if e.config.IsCollectTableUsage && tableStats != nil {
		// table usage
		tableUsage = tableStats.TableUsage[tableURN]

		// common join
		if jdMapping, exist := tableStats.JoinDetail[tableURN]; exist {
			for joinedTableURN, jd := range jdMapping {
				var joinConditions []string
				for jc := range jd.Conditions {
					joinConditions = append(joinConditions, jc)
				}
				commonJoins = append(commonJoins, &v1beta2.TableCommonJoin{
					Urn:        joinedTableURN,
					Count:      jd.Usage,
					Conditions: joinConditions,
				})
			}
		}

		// filter conditions
		if filterMapping, exist := tableStats.FilterConditions[tableURN]; exist {
			for filterExpression := range filterMapping {
				filterConditions = append(filterConditions, filterExpression)
			}
		}
	}

	tp = &v1beta2.TableProfile{
		UsageCount:  tableUsage,
		CommonJoins: commonJoins,
		Filters:     filterConditions,
	}

	return
}
