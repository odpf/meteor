package auditlog

import (
	"testing"

	"github.com/odpf/meteor/plugins/extractors/bigquery/util"
	"github.com/stretchr/testify/assert"
	loggingpb "google.golang.org/genproto/googleapis/cloud/bigquery/logging/v1"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
)

func TestValidateAuditData(t *testing.T) {

	t.Run("return error if AuditData does not have JobCompletedEvent data", func(t *testing.T) {
		ld := &LogData{
			&loggingpb.AuditData{},
		}
		err := ld.validateAuditData()

		assert.EqualError(t, err, "can't found jobCompletedEvent field")
	})

	t.Run("return error if JobCompletedEvent does not have Job data", func(t *testing.T) {
		ld := &LogData{
			&loggingpb.AuditData{
				JobCompletedEvent: &loggingpb.JobCompletedEvent{
					EventName: "",
				},
			},
		}
		err := ld.validateAuditData()

		assert.EqualError(t, err, "can't found jobCompletedEvent.job field")
	})

	t.Run("return error if JobCompletedEvent.Job.JobStatistics is nil", func(t *testing.T) {
		ld := &LogData{
			&loggingpb.AuditData{
				JobCompletedEvent: &loggingpb.JobCompletedEvent{
					EventName: "",
					Job:       &loggingpb.Job{},
				},
			},
		}
		err := ld.validateAuditData()

		assert.EqualError(t, err, "job statistics is nil")
	})

	t.Run("return error if referencedTables is empty", func(t *testing.T) {
		ld := &LogData{
			&loggingpb.AuditData{
				JobCompletedEvent: &loggingpb.JobCompletedEvent{
					EventName: "",
					Job: &loggingpb.Job{
						JobStatistics: &loggingpb.JobStatistics{},
					},
				},
			},
		}
		err := ld.validateAuditData()

		assert.EqualError(t, err, "no referenced tables found")
	})

	t.Run("return error if JobCompletedEvent.Job does not have JobStatus info", func(t *testing.T) {
		ld := &LogData{
			&loggingpb.AuditData{
				JobCompletedEvent: &loggingpb.JobCompletedEvent{
					EventName: "",
					Job: &loggingpb.Job{
						JobStatistics: &loggingpb.JobStatistics{
							ReferencedTables: []*loggingpb.TableName{
								{
									ProjectId: "project_id",
								},
							},
						},
					},
				},
			},
		}
		err := ld.validateAuditData()

		assert.EqualError(t, err, "can't found jobCompletedEvent.job.jobStatus field")
	})

	t.Run("return error if JobCompletedEvent.Job.JobStatus's state is empty", func(t *testing.T) {
		ld := &LogData{
			&loggingpb.AuditData{
				JobCompletedEvent: &loggingpb.JobCompletedEvent{
					EventName: "",
					Job: &loggingpb.Job{
						JobStatistics: &loggingpb.JobStatistics{
							ReferencedTables: []*loggingpb.TableName{
								{
									ProjectId: "project_id",
								},
							},
						},
						JobStatus: &loggingpb.JobStatus{},
					},
				},
			},
		}
		err := ld.validateAuditData()

		assert.EqualError(t, err, "jobCompletedEvent.job.jobStatus.state is empty")
	})

	t.Run("return error if JobCompletedEvent.Job.JobStatus's state is not done", func(t *testing.T) {
		ld := &LogData{
			&loggingpb.AuditData{
				JobCompletedEvent: &loggingpb.JobCompletedEvent{
					EventName: "",
					Job: &loggingpb.Job{
						JobStatistics: &loggingpb.JobStatistics{
							ReferencedTables: []*loggingpb.TableName{
								{
									ProjectId: "project_id",
								},
							},
						},
						JobStatus: &loggingpb.JobStatus{
							State: "WORKING",
						},
					},
				},
			},
		}
		err := ld.validateAuditData()

		assert.EqualError(t, err, "job status state is not DONE")
	})

	t.Run("return error if JobCompletedEvent.Job.JobStatus error is not nil and has an error message", func(t *testing.T) {
		ld := &LogData{
			&loggingpb.AuditData{
				JobCompletedEvent: &loggingpb.JobCompletedEvent{
					EventName: "",
					Job: &loggingpb.Job{
						JobStatistics: &loggingpb.JobStatistics{
							ReferencedTables: []*loggingpb.TableName{
								{
									ProjectId: "project_id",
								},
							},
						},
						JobStatus: &loggingpb.JobStatus{
							State: "DONE",
							Error: &statuspb.Status{
								Message: "error parsing the data",
							},
						},
					},
				},
			},
		}
		err := ld.validateAuditData()

		assert.EqualError(t, err, "job status has error: error parsing the data")
	})

	t.Run("return nil if AuditData's Job is DONE and no error", func(t *testing.T) {
		ld := &LogData{
			&loggingpb.AuditData{
				JobCompletedEvent: &loggingpb.JobCompletedEvent{
					EventName: "",
					Job: &loggingpb.Job{
						JobStatistics: &loggingpb.JobStatistics{
							ReferencedTables: []*loggingpb.TableName{
								{
									ProjectId: "project_id",
								},
							},
						},
						JobStatus: &loggingpb.JobStatus{
							State: "DONE",
						},
					},
				},
			},
		}
		err := ld.validateAuditData()

		assert.Nil(t, err)
	})
}

func TestGetReferencedTablesURN(t *testing.T) {
	t.Run("return empty slice if JobStatistics is nil", func(t *testing.T) {
		ld := &LogData{
			&loggingpb.AuditData{
				JobCompletedEvent: &loggingpb.JobCompletedEvent{
					EventName: "",
					Job:       &loggingpb.Job{},
				},
			},
		}
		rts := ld.GetReferencedTablesURN()

		assert.Empty(t, rts)
	})

	t.Run("return slice of urns if referenced tables exists if JobStatistics", func(t *testing.T) {
		rts := testDataLogData1.GetReferencedTablesURN()

		expectedRefTablesURN := []string{
			util.TableURN("project1", "dataset1", "table1"),
			util.TableURN("project2", "dataset1", "table1"),
			util.TableURN("project3", "dataset1", "table1"),
		}
		assert.EqualValues(t, expectedRefTablesURN, rts)
	})
}
