package metrics_test

import (
	"fmt"
	"testing"

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
		dataCount := 0
		timingMetric := fmt.Sprintf(
			"%s.runDuration,name=%s,success=%s,dataCount=%d",
			statsdPrefix,
			recipe.Name,
			"false",
			dataCount,
		)
		incrementMetric := fmt.Sprintf(
			"%s.run,name=%s,success=%s,dataCount=%d",
			statsdPrefix,
			recipe.Name,
			"false",
			dataCount,
		)

		client := new(mockStatsdClient)
		client.On("Timing", timingMetric, int64(duration))
		client.On("Increment", incrementMetric)
		defer client.AssertExpectations(t)

		monitor := metrics.NewStatsdMonitor(client, statsdPrefix)
		monitor.RecordRun(recipe, duration, false, 0)
	})

	t.Run("should set success field to true on success", func(t *testing.T) {
		recipe := recipe.Recipe{
			Name: "test-recipe",
		}
		duration := 100
		dataCount := 0
		timingMetric := fmt.Sprintf(
			"%s.runDuration,name=%s,success=%s,dataCount=%d",
			statsdPrefix,
			recipe.Name,
			"true",
			dataCount,
		)
		incrementMetric := fmt.Sprintf(
			"%s.run,name=%s,success=%s,dataCount=%d",
			statsdPrefix,
			recipe.Name,
			"true",
			dataCount,
		)

		client := new(mockStatsdClient)
		client.On("Timing", timingMetric, int64(duration))
		client.On("Increment", incrementMetric)
		defer client.AssertExpectations(t)

		monitor := metrics.NewStatsdMonitor(client, statsdPrefix)
		monitor.RecordRun(recipe, duration, true, dataCount)
	})
}
