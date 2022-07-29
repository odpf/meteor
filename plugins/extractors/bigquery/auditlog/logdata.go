package auditlog

import (
	"github.com/odpf/meteor/plugins/extractors/bigquery/util"
	"github.com/pkg/errors"
	loggingpb "google.golang.org/genproto/googleapis/cloud/bigquery/logging/v1"
)

type LogData struct {
	*loggingpb.AuditData
}

func (ld *LogData) GetReferencedTablesURN() (refTablesURN []string) {
	refTablesURN = []string{}
	var stats *loggingpb.JobStatistics
	if stats = ld.GetJobCompletedEvent().GetJob().GetJobStatistics(); stats == nil {
		return
	}
	for _, rt := range stats.ReferencedTables {
		tableURN := util.TableURN(rt.ProjectId, rt.DatasetId, rt.TableId)
		refTablesURN = append(refTablesURN, tableURN)
	}
	return
}

func (ld *LogData) GetQuery() (sqlQuery string, err error) {

	if jobConfig := ld.GetJobCompletedEvent().GetJob().GetJobConfiguration(); jobConfig == nil {
		err = errors.New("jobConfiguration is nil")
		return
	}
	if jobConfigQuery := ld.GetJobCompletedEvent().GetJob().GetJobConfiguration().GetQuery(); jobConfigQuery == nil {
		err = errors.New("jobConfiguration_Query_ is nil")
		return
	}
	sqlQuery = ld.GetJobCompletedEvent().GetJob().GetJobConfiguration().GetQuery().GetQuery()
	if sqlQuery == "" {
		err = errors.New("sql query is empty")
	}
	return
}

func (ld *LogData) validateAuditData() (err error) {
	if ld.GetJobCompletedEvent() == nil {
		err = errors.New("can't found jobCompletedEvent field")
		return
	}

	job := ld.GetJobCompletedEvent().GetJob()
	if job == nil {
		err = errors.New("can't found jobCompletedEvent.job field")
		return
	}

	// if referenced tables is empty, we don't count it
	stats := job.GetJobStatistics()
	if stats == nil {
		err = errors.Errorf("job statistics is nil")
		return
	}

	if len(stats.ReferencedTables) == 0 {
		err = errors.Errorf("no referenced tables found")
		return
	}

	jobStatus := job.GetJobStatus()
	if jobStatus == nil {
		err = errors.New("can't found jobCompletedEvent.job.jobStatus field")
		return
	}

	jobState := jobStatus.GetState()
	if jobState == "" {
		err = errors.New("jobCompletedEvent.job.jobStatus.state is empty")
		return
	}

	// ignoring the job that has not finished
	if jobState != "DONE" {
		err = errors.New("job status state is not DONE")
		return
	}

	if jobStatus.GetError() != nil {
		if jobErrMsg := jobStatus.GetError().GetMessage(); jobErrMsg != "" {
			err = errors.Errorf("job status has error: %s", jobErrMsg)
			return
		}
	}

	return
}
