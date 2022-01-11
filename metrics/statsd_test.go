package metrics_test

import (
	"fmt"
	"testing"

	"github.com/odpf/meteor/agent"
	"github.com/odpf/meteor/metrics"
	"github.com/odpf/meteor/recipe"
	"github.com/stretchr/testify/mock"
)

type mockStatsdClient struct {
	mock.Mock
}

func (c *mockStatsdClient) Timing(name string, val int64) {
	c.Called(name, val)
}

func (c *mockStatsdClient) IncrementByValue(name string, val int) {
	c.Called(name, val)
}

func (c *mockStatsdClient) Increment(name string) {
	c.Called(name)
}

func TestStatsdMonitorRecordRun(t *testing.T) {
	statsdPrefix := "testprefix"

	t.Run("should create metrics with the correct name and value", func(t *testing.T) {
		recipe := recipe.Recipe{
			Name: "test-recipe",
		}
		duration := 100
		recordCount := 2
		timingMetric := fmt.Sprintf(
			"%s.runDuration,name=%s,success=%s,records=%d",
			statsdPrefix,
			recipe.Name,
			"false",
			recordCount,
		)
		incrementMetric := fmt.Sprintf(
			"%s.run,name=%s,success=%s,records=%d",
			statsdPrefix,
			recipe.Name,
			"false",
			recordCount,
		)
		recordIncrementMetric := fmt.Sprintf(
			"%s.runRecordCount,name=%s,success=%s,records=%d",
			statsdPrefix,
			recipe.Name,
			"false",
			recordCount,
		)

		client := new(mockStatsdClient)
		client.On("Timing", timingMetric, int64(duration))
		client.On("Increment", incrementMetric)
		client.On("IncrementByValue", recordIncrementMetric, recordCount)
		defer client.AssertExpectations(t)

		monitor := metrics.NewStatsdMonitor(client, statsdPrefix)
		monitor.RecordRun(agent.Run{Recipe: recipe, DurationInSec: duration, RecordCount: 2, Success: false})
	})

	t.Run("should set success field to true on success", func(t *testing.T) {
		recipe := recipe.Recipe{
			Name: "test-recipe",
		}
		duration := 100
		recordCount := 2
		timingMetric := fmt.Sprintf(
			"%s.runDuration,name=%s,success=%s,records=%d",
			statsdPrefix,
			recipe.Name,
			"true",
			recordCount,
		)
		incrementMetric := fmt.Sprintf(
			"%s.run,name=%s,success=%s,records=%d",
			statsdPrefix,
			recipe.Name,
			"true",
			recordCount,
		)
		recordIncrementMetric := fmt.Sprintf(
			"%s.runRecordCount,name=%s,success=%s,records=%d",
			statsdPrefix,
			recipe.Name,
			"true",
			recordCount,
		)

		client := new(mockStatsdClient)
		client.On("Timing", timingMetric, int64(duration))
		client.On("Increment", incrementMetric)
		client.On("IncrementByValue", recordIncrementMetric, recordCount)
		defer client.AssertExpectations(t)

		monitor := metrics.NewStatsdMonitor(client, statsdPrefix)
		monitor.RecordRun(agent.Run{Recipe: recipe, DurationInSec: duration, RecordCount: 2, Success: true})
	})
}
