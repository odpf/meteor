package util

import (
	"fmt"

	"github.com/odpf/meteor/models"
)

func TableURN(projectID, datasetID, tableID string) string {
	tableFQN := fmt.Sprintf("%s:%s.%s", projectID, datasetID, tableID)

	return models.NewURN("bigquery", projectID, "table", tableFQN)
}
