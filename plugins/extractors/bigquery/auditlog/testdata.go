package auditlog

import (
	"github.com/odpf/meteor/plugins/extractors/bigquery/util"
	loggingpb "google.golang.org/genproto/googleapis/cloud/bigquery/logging/v1"
)

var testDataRefTables1 = []string{
	util.TableURN("project1", "dataset1", "table1"),
	util.TableURN("project2", "dataset1", "table1"),
	util.TableURN("project3", "dataset1", "table1"),
}

var testDataRefTables2 = []string{
	util.TableURN("project1", "dataset1", "table1"),
	util.TableURN("project3", "dataset1", "table1"),
	util.TableURN("project4", "dataset1", "table1"),
}

var testDataRefTables3 = []string{
	util.TableURN("project1", "dataset1", "table1"),
	util.TableURN("project3", "dataset1", "table1"),
}

var testDataRefTables4 = []string{
	util.TableURN("project1", "dataset1", "table1"),
}

var testDataLogData1 = &LogData{
	&loggingpb.AuditData{
		JobCompletedEvent: &loggingpb.JobCompletedEvent{
			EventName: "",
			Job: &loggingpb.Job{
				JobConfiguration: &loggingpb.JobConfiguration{
					Configuration: &loggingpb.JobConfiguration_Query_{
						Query: &loggingpb.JobConfiguration_Query{
							Query: `
							SELECT
							t1.field1 AS field1,
							t2.field2 AS field2,
							t1.field3 AS field3,
							t3.field4 AS field4` +
								"FROM `project1.dataset1.table1` t1" +
								"JOIN `project2.dataset1.table1` t2 ON t1.somefield = t2.anotherfield " +
								"JOIN `project3.dataset1.table1` t3 ON t1.somefield = t3.yetanotherfield",
						},
					},
				},
				JobStatistics: &loggingpb.JobStatistics{
					ReferencedTables: []*loggingpb.TableName{
						{
							ProjectId: "project1",
							DatasetId: "dataset1",
							TableId:   "table1",
						}, {
							ProjectId: "project2",
							DatasetId: "dataset1",
							TableId:   "table1",
						}, {
							ProjectId: "project3",
							DatasetId: "dataset1",
							TableId:   "table1",
						},
					},
				},
			},
		},
	},
}

var testDataLogData2 = &LogData{
	&loggingpb.AuditData{
		JobCompletedEvent: &loggingpb.JobCompletedEvent{
			EventName: "",
			Job: &loggingpb.Job{
				JobConfiguration: &loggingpb.JobConfiguration{
					Configuration: &loggingpb.JobConfiguration_Query_{
						Query: &loggingpb.JobConfiguration_Query{
							Query: `
							WITH temp_table as 
							(SELECT
							t1.field1 AS field1,
							t2.field2 AS field2,
							t1.field3 AS field3,
							t3.field4 AS field4` +
								"FROM `project1.dataset1.table1` t1" +
								"JOIN `project3.dataset1.table1` t2 ON t1.somefield = t2.anotherfield " +
								"JOIN `project4.dataset1.table1` t3 ON t1.somefield = t3.yetanotherfield)" +
								`SELECT * FROM temp_table WHERE t1.field2 = 'valid';`,
						},
					},
				},
				JobStatistics: &loggingpb.JobStatistics{
					ReferencedTables: []*loggingpb.TableName{
						{
							ProjectId: "project1",
							DatasetId: "dataset1",
							TableId:   "table1",
						}, {
							ProjectId: "project3",
							DatasetId: "dataset1",
							TableId:   "table1",
						}, {
							ProjectId: "project4",
							DatasetId: "dataset1",
							TableId:   "table1",
						},
					},
				},
			},
		},
	},
}

var testDataLogData3 = &LogData{
	&loggingpb.AuditData{
		JobCompletedEvent: &loggingpb.JobCompletedEvent{
			EventName: "",
			Job: &loggingpb.Job{
				JobConfiguration: &loggingpb.JobConfiguration{
					Configuration: &loggingpb.JobConfiguration_Query_{
						Query: &loggingpb.JobConfiguration_Query{
							Query: `
							SELECT 
							*
							(SELECT order_id FROM FROM project1.dataset1.table1 WHERE column_1 IS TRUE)
							JOIN project3.dataset1.table1
							USING (somefield,anotherfield)`},
					},
				},
				JobStatistics: &loggingpb.JobStatistics{
					ReferencedTables: []*loggingpb.TableName{
						{
							ProjectId: "project1",
							DatasetId: "dataset1",
							TableId:   "table1",
						}, {
							ProjectId: "project3",
							DatasetId: "dataset1",
							TableId:   "table1",
						},
					},
				},
			},
		},
	},
}

var testDataLogData4 = &LogData{
	&loggingpb.AuditData{
		JobCompletedEvent: &loggingpb.JobCompletedEvent{
			EventName: "",
			Job: &loggingpb.Job{
				JobConfiguration: &loggingpb.JobConfiguration{
					Configuration: &loggingpb.JobConfiguration_Query_{
						Query: &loggingpb.JobConfiguration_Query{
							Query: "SELECT start_time FROM `project1`.dataset1.table1 where job_type=\"query\" and statement_type=\"insert\" order by start_time desc limit 1",
						},
					},
				},
				JobStatistics: &loggingpb.JobStatistics{
					ReferencedTables: []*loggingpb.TableName{
						{
							ProjectId: "project1",
							DatasetId: "dataset1",
							TableId:   "table1",
						},
					},
				},
			},
		},
	},
}

var testDataJoinDetail1234 = map[string]map[string]JoinDetail{
	util.TableURN("project1", "dataset1", "table1"): {
		util.TableURN("project2", "dataset1", "table1"): {
			Usage: 1,
			Conditions: map[string]bool{
				"ON t1.somefield = t2.anotherfield":    true,
				"ON t1.somefield = t3.yetanotherfield": true,
			},
		},
		util.TableURN("project3", "dataset1", "table1"): {
			Usage: 3,
			Conditions: map[string]bool{
				"ON t1.somefield = t2.anotherfield":    true,
				"ON t1.somefield = t3.yetanotherfield": true,
				"USING (somefield,anotherfield)":       true,
			},
		},

		util.TableURN("project4", "dataset1", "table1"): {
			Usage: 1,
			Conditions: map[string]bool{
				"ON t1.somefield = t2.anotherfield":    true,
				"ON t1.somefield = t3.yetanotherfield": true,
			},
		},
	},
	util.TableURN("project2", "dataset1", "table1"): {
		util.TableURN("project1", "dataset1", "table1"): {
			Usage: 1,
			Conditions: map[string]bool{
				"ON t1.somefield = t2.anotherfield":    true,
				"ON t1.somefield = t3.yetanotherfield": true,
			},
		},
		util.TableURN("project3", "dataset1", "table1"): {
			Usage: 1,
			Conditions: map[string]bool{
				"ON t1.somefield = t2.anotherfield":    true,
				"ON t1.somefield = t3.yetanotherfield": true,
			},
		},
	},
	util.TableURN("project3", "dataset1", "table1"): {
		util.TableURN("project1", "dataset1", "table1"): {
			Usage: 3,
			Conditions: map[string]bool{
				"ON t1.somefield = t2.anotherfield":    true,
				"ON t1.somefield = t3.yetanotherfield": true,
				"USING (somefield,anotherfield)":       true,
			},
		},
		util.TableURN("project2", "dataset1", "table1"): {
			Usage: 1,
			Conditions: map[string]bool{
				"ON t1.somefield = t2.anotherfield":    true,
				"ON t1.somefield = t3.yetanotherfield": true,
			},
		},
		util.TableURN("project4", "dataset1", "table1"): {
			Usage: 1,
			Conditions: map[string]bool{
				"ON t1.somefield = t2.anotherfield":    true,
				"ON t1.somefield = t3.yetanotherfield": true,
			},
		},
	},
	util.TableURN("project4", "dataset1", "table1"): {
		util.TableURN("project1", "dataset1", "table1"): {
			Usage: 1,
			Conditions: map[string]bool{
				"ON t1.somefield = t2.anotherfield":    true,
				"ON t1.somefield = t3.yetanotherfield": true,
			},
		},
		util.TableURN("project3", "dataset1", "table1"): {
			Usage: 1,
			Conditions: map[string]bool{
				"ON t1.somefield = t2.anotherfield":    true,
				"ON t1.somefield = t3.yetanotherfield": true,
			},
		},
	},
}

var testDataJoinUsage1234 = map[string]map[string]JoinDetail{
	util.TableURN("project1", "dataset1", "table1"): {
		util.TableURN("project2", "dataset1", "table1"): {
			Usage: 1,
		},
		util.TableURN("project3", "dataset1", "table1"): {
			Usage: 3,
		},

		util.TableURN("project4", "dataset1", "table1"): {
			Usage: 1,
		},
	},
	util.TableURN("project2", "dataset1", "table1"): {
		util.TableURN("project1", "dataset1", "table1"): {
			Usage: 1,
		},
		util.TableURN("project3", "dataset1", "table1"): {
			Usage: 1,
		},
	},
	util.TableURN("project3", "dataset1", "table1"): {
		util.TableURN("project1", "dataset1", "table1"): {
			Usage: 3,
		},
		util.TableURN("project2", "dataset1", "table1"): {
			Usage: 1,
		},
		util.TableURN("project4", "dataset1", "table1"): {
			Usage: 1,
		},
	},
	util.TableURN("project4", "dataset1", "table1"): {
		util.TableURN("project1", "dataset1", "table1"): {
			Usage: 1,
		},
		util.TableURN("project3", "dataset1", "table1"): {
			Usage: 1,
		},
	},
}

var testDataTableUsage1234 = map[string]int64{
	util.TableURN("project1", "dataset1", "table1"): 4,
	util.TableURN("project2", "dataset1", "table1"): 1,
	util.TableURN("project3", "dataset1", "table1"): 3,
	util.TableURN("project4", "dataset1", "table1"): 1,
}

var testDataFilterCondition1234 = map[string]map[string]bool{
	util.TableURN("project1", "dataset1", "table1"): {
		"WHERE column_1 IS TRUE":                                 true,
		"WHERE t1.field2 = 'valid'":                              true,
		"where job_type=\"query\" and statement_type=\"insert\"": true,
	},
	util.TableURN("project3", "dataset1", "table1"): {
		"WHERE column_1 IS TRUE":    true,
		"WHERE t1.field2 = 'valid'": true,
	},
	util.TableURN("project4", "dataset1", "table1"): {
		"WHERE t1.field2 = 'valid'": true,
	},
}
